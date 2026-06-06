"""
Реализация трёх стратегий параллельных запросов:

1. FIXED — ограничение семафором на max_concurrent
2. TIMEOUT_RACE — запуск всех сразу, отсечка по timeout
3. ADAPTIVE — динамическое изменение семафора на основе истории
"""

import asyncio
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Optional, Dict, Any

import aiohttp

from .stats import collector


@dataclass
class RequestResult:
    """Результат одного HTTP-запроса."""
    url: str
    status: Optional[int] = None
    data: Any = None
    error: Optional[str] = None
    timeout: bool = False
    elapsed_ms: float = 0.0


class BaseStrategy(ABC):
    """Базовый класс стратегии."""
    
    def __init__(self, timeout_sec: float = 5.0, max_concurrent: int = 3):
        self.timeout_sec = timeout_sec
        self.max_concurrent = max_concurrent
    
    @abstractmethod
    async def execute(self, urls: List[str], session: aiohttp.ClientSession) -> List[RequestResult]:
        """Выполнить запросы по URL согласно стратегии."""
        pass
    
    def _create_timeout(self) -> aiohttp.ClientTimeout:
        """Таймаут для aiohttp."""
        return aiohttp.ClientTimeout(total=self.timeout_sec)


class FixedStrategy(BaseStrategy):
    """
    Стратегия "Fixed": фиксированное количество одновременных запросов.
    
    Использует asyncio.Semaphore для ограничения concurrency.
    Все URL обрабатываются параллельно, но не более max_concurrent одновременно.
    """
    
    async def execute(self, urls: List[str], session: aiohttp.ClientSession) -> List[RequestResult]:
        semaphore = asyncio.Semaphore(self.max_concurrent)
        
        async def fetch_one(url: str) -> RequestResult:
            async with semaphore:
                return await self._do_request(url, session)
        
        tasks = [fetch_one(url) for url in urls]
        return await asyncio.gather(*tasks, return_exceptions=True)
    
    async def _do_request(self, url: str, session: aiohttp.ClientSession) -> RequestResult:
        """Выполнить один запрос с измерением времени."""
        start = time.perf_counter()
        try:
            async with session.get(url, timeout=self._create_timeout()) as resp:
                data = await resp.json()
                elapsed = (time.perf_counter() - start) * 1000
                return RequestResult(
                    url=url,
                    status=resp.status,
                    data=data,
                    elapsed_ms=round(elapsed, 2)
                )
        except asyncio.TimeoutError:
            elapsed = (time.perf_counter() - start) * 1000
            return RequestResult(
                url=url,
                timeout=True,
                elapsed_ms=round(elapsed, 2),
                error="Request timeout"
            )
        except Exception as e:
            elapsed = (time.perf_counter() - start) * 1000
            return RequestResult(
                url=url,
                error=str(e),
                elapsed_ms=round(elapsed, 2)
            )


class TimeoutRaceStrategy(BaseStrategy):
    """
    Стратегия "Timeout Race": запускает ВСЕ запросы одновременно,
    но отменяет те, что не уложились в timeout_sec.
    
    Использует asyncio.wait_for для жёсткой отсечки.
    """
    
    async def execute(self, urls: List[str], session: aiohttp.ClientSession) -> List[RequestResult]:
        # Запускаем все задачи одновременно (без семафора!)
        tasks = [
            asyncio.create_task(self._do_request(url, session))
            for url in urls
        ]
        return await asyncio.gather(*tasks, return_exceptions=True)
    
    async def _do_request(self, url: str, session: aiohttp.ClientSession) -> RequestResult:
        """Выполнить запрос с жёстким таймаутом."""
        start = time.perf_counter()
        
        async def _fetch():
            async with session.get(url, timeout=self._create_timeout()) as resp:
                return await resp.json(), resp.status
        
        try:
            # Жёсткий таймаут: если не успел — отмена
            data, status = await asyncio.wait_for(_fetch(), timeout=self.timeout_sec)
            elapsed = (time.perf_counter() - start) * 1000
            return RequestResult(
                url=url,
                status=status,
                data=data,
                elapsed_ms=round(elapsed, 2)
            )
        except asyncio.TimeoutError:
            elapsed = (time.perf_counter() - start) * 1000
            return RequestResult(
                url=url,
                timeout=True,
                elapsed_ms=round(elapsed, 2),
                error=f"Hard timeout after {self.timeout_sec}s"
            )
        except Exception as e:
            elapsed = (time.perf_counter() - start) * 1000
            return RequestResult(
                url=url,
                error=str(e),
                elapsed_ms=round(elapsed, 2)
            )


class AdaptiveStrategy(BaseStrategy):
    """
    Стратегия "Adaptive": динамически меняет concurrency для каждого URL.
    
    Перед каждым запросом проверяет статистику и обновляет семафор.
    После запроса записывает результат в статистику для следующих вызовов.
    
    Ключевая особенность: concurrency персонализирован для каждого хоста,
    а не глобальное. Это позволяет быстрым API работать на полной,
    а медленным — на сниженной скорости.
    """
    
    def __init__(self, timeout_sec: float = 5.0, max_concurrent: int = 3):
        super().__init__(timeout_sec, max_concurrent)
        # Семафоры для каждого хоста (создаются динамически)
        self._semaphores: Dict[str, asyncio.Semaphore] = {}
        self._sem_lock = asyncio.Lock()
    
    async def _get_semaphore(self, url: str, initial: int) -> asyncio.Semaphore:
        """Получить или создать семафор для хоста с актуальным лимитом."""
        from urllib.parse import urlparse
        host = urlparse(url).netloc or url
        
        async with self._sem_lock:
            # Получаем актуальную статистику
            stats = await collector.get_or_create(url, initial)
            limit = stats.current_concurrent
            
            if host not in self._semaphores:
                self._semaphores[host] = asyncio.Semaphore(limit)
            else:
                # Если лимит изменился — пересоздаём семафор
                # (простое решение; в продакшене лучше менять value у семафора)
                old_sem = self._semaphores[host]
                if old_sem._value != limit:
                    self._semaphores[host] = asyncio.Semaphore(limit)
            
            return self._semaphores[host]
    
    async def execute(self, urls: List[str], session: aiohttp.ClientSession) -> List[RequestResult]:
        tasks = [
            asyncio.create_task(self._do_request(url, session))
            for url in urls
        ]
        return await asyncio.gather(*tasks, return_exceptions=True)
    
    async def _do_request(self, url: str, session: aiohttp.ClientSession) -> RequestResult:
        """Выполнить запрос с адаптивным семафором и обновить статистику."""
        # Получаем персональный семафор для этого хоста
        sem = await self._get_semaphore(url, self.max_concurrent)
        
        async with sem:
            start = time.perf_counter()
            success = False
            status_code = None
            
            try:
                async with session.get(url, timeout=self._create_timeout()) as resp:
                    data = await resp.json()
                    elapsed = (time.perf_counter() - start) * 1000
                    status_code = resp.status
                    success = 200 <= resp.status < 300
                    
                    result = RequestResult(
                        url=url,
                        status=resp.status,
                        data=data,
                        elapsed_ms=round(elapsed, 2)
                    )
            except asyncio.TimeoutError:
                elapsed = (time.perf_counter() - start) * 1000
                result = RequestResult(
                    url=url,
                    timeout=True,
                    elapsed_ms=round(elapsed, 2),
                    error="Request timeout"
                )
            except Exception as e:
                elapsed = (time.perf_counter() - start) * 1000
                result = RequestResult(
                    url=url,
                    error=str(e),
                    elapsed_ms=round(elapsed, 2)
                )
            
            # Записываем в статистику и обновляем concurrency
            await collector.record_request(
                url=url,
                success=success,
                elapsed_ms=result.elapsed_ms,
                status_code=status_code,
                initial_concurrent=self.max_concurrent
            )
            # Обновляем concurrency для следующих запросов
            stats = await collector.get_or_create(url, self.max_concurrent)
            await stats.update_concurrency()
            
            return result


# Фабрика стратегий
STRATEGIES = {
    "fixed": FixedStrategy,
    "timeout_race": TimeoutRaceStrategy,
    "adaptive": AdaptiveStrategy,
}


def get_strategy(name: str, timeout_sec: float, max_concurrent: int) -> BaseStrategy:
    """Создать стратегию по имени."""
    if name not in STRATEGIES:
        raise ValueError(f"Unknown strategy: {name}. Available: {list(STRATEGIES.keys())}")
    return STRATEGIES[name](timeout_sec=timeout_sec, max_concurrent=max_concurrent)