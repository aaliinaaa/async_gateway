"""
Реализация трёх стратегий параллельных запросов.
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
    wait_ms: float = 0.0  # Время ожидания в семафоре (или 0 если нет семафора)


class BaseStrategy(ABC):
    def __init__(self, timeout_sec: float = 5.0, max_concurrent: int = 3):
        self.timeout_sec = timeout_sec
        self.max_concurrent = max_concurrent
    
    @abstractmethod
    async def execute(self, urls: List[str], session: aiohttp.ClientSession) -> List[RequestResult]:
        pass
    
    def _create_timeout(self) -> aiohttp.ClientTimeout:
        return aiohttp.ClientTimeout(total=self.timeout_sec)


class FixedStrategy(BaseStrategy):
    async def execute(self, urls: List[str], session: aiohttp.ClientSession) -> List[RequestResult]:
        semaphore = asyncio.Semaphore(self.max_concurrent)
        
        async def fetch_one(url: str) -> RequestResult:
            wait_start = time.perf_counter()
            async with semaphore:
                wait_ms = (time.perf_counter() - wait_start) * 1000
                return await self._do_request(url, session, wait_ms)
        
        tasks = [fetch_one(url) for url in urls]
        return await asyncio.gather(*tasks, return_exceptions=True)
    
    async def _do_request(self, url: str, session: aiohttp.ClientSession, wait_ms: float = 0.0) -> RequestResult:
        start = time.perf_counter()
        try:
            async with session.get(url, timeout=self._create_timeout()) as resp:
                text = await resp.text()
                try:
                    import json
                    data = json.loads(text) if text else {}
                except json.JSONDecodeError:
                    data = {"raw_response": text[:200]}
                
                elapsed = (time.perf_counter() - start) * 1000
                return RequestResult(
                    url=url,
                    status=resp.status,
                    data=data,
                    elapsed_ms=round(elapsed, 2),
                    wait_ms=round(wait_ms, 2)
                )
        except asyncio.TimeoutError:
            elapsed = (time.perf_counter() - start) * 1000
            return RequestResult(
                url=url,
                timeout=True,
                elapsed_ms=round(elapsed, 2),
                wait_ms=round(wait_ms, 2),
                error="Request timeout"
            )
        except Exception as e:
            elapsed = (time.perf_counter() - start) * 1000
            return RequestResult(
                url=url,
                error=str(e),
                elapsed_ms=round(elapsed, 2),
                wait_ms=round(wait_ms, 2)
            )


class TimeoutRaceStrategy(BaseStrategy):
    async def execute(self, urls: List[str], session: aiohttp.ClientSession) -> List[RequestResult]:
        tasks = [
            asyncio.create_task(self._do_request(url, session))
            for url in urls
        ]
        return await asyncio.gather(*tasks, return_exceptions=True)
    
    async def _do_request(self, url: str, session: aiohttp.ClientSession) -> RequestResult:
        start = time.perf_counter()
        
        async def _fetch():
            async with session.get(url, timeout=self._create_timeout()) as resp:
                text = await resp.text()
                try:
                    import json
                    data = json.loads(text) if text else {}
                except json.JSONDecodeError:
                    data = {"raw_response": text[:200]}
                return data, resp.status
        
        try:
            data, status = await asyncio.wait_for(_fetch(), timeout=self.timeout_sec)
            elapsed = (time.perf_counter() - start) * 1000
            return RequestResult(
                url=url,
                status=status,
                data=data,
                elapsed_ms=round(elapsed, 2),
                wait_ms=0.0  # Нет семафора
            )
        except asyncio.TimeoutError:
            elapsed = (time.perf_counter() - start) * 1000
            return RequestResult(
                url=url,
                timeout=True,
                elapsed_ms=round(elapsed, 2),
                wait_ms=0.0,
                error=f"Hard timeout after {self.timeout_sec}s"
            )
        except Exception as e:
            elapsed = (time.perf_counter() - start) * 1000
            return RequestResult(
                url=url,
                error=str(e),
                elapsed_ms=round(elapsed, 2),
                wait_ms=0.0
            )


class AdaptiveStrategy(BaseStrategy):
    def __init__(self, timeout_sec: float = 5.0, max_concurrent: int = 3):
        super().__init__(timeout_sec, max_concurrent)
        self._semaphores: Dict[str, asyncio.Semaphore] = {}
        self._sem_lock = asyncio.Lock()
    
    async def _get_semaphore(self, url: str, initial: int) -> asyncio.Semaphore:
        from urllib.parse import urlparse
        host = urlparse(url).netloc or url
        
        async with self._sem_lock:
            stats = await collector.get_or_create(url, initial)
            limit = stats.current_concurrent
            
            if host not in self._semaphores:
                self._semaphores[host] = asyncio.Semaphore(limit)
            else:
                old_sem = self._semaphores[host]
                if old_sem._value != limit:
                    self._semaphores[host] = asyncio.Semaphore(limit)
            
            return self._semaphores[host]
    
    async def execute(self, urls: List[str], session: aiohttp.ClientSession) -> List[RequestResult]:
        tasks = [
            asyncio.create_task(self._fetch_one(url, session))
            for url in urls
        ]
        return await asyncio.gather(*tasks, return_exceptions=True)
    
    async def _fetch_one(self, url: str, session: aiohttp.ClientSession) -> RequestResult:
        sem = await self._get_semaphore(url, self.max_concurrent)
        
        wait_start = time.perf_counter()
        async with sem:
            wait_ms = (time.perf_counter() - wait_start) * 1000
            
            start = time.perf_counter()
            success = False
            status_code = None
            
            try:
                async with session.get(url, timeout=self._create_timeout()) as resp:
                    text = await resp.text()
                    try:
                        import json
                        data = json.loads(text) if text else {}
                    except json.JSONDecodeError:
                        data = {"raw_response": text[:200]}
                    
                    elapsed = (time.perf_counter() - start) * 1000
                    status_code = resp.status
                    success = 200 <= resp.status < 300
                    
                    result = RequestResult(
                        url=url,
                        status=resp.status,
                        data=data,
                        elapsed_ms=round(elapsed, 2),
                        wait_ms=round(wait_ms, 2)
                    )
            except asyncio.TimeoutError:
                elapsed = (time.perf_counter() - start) * 1000
                result = RequestResult(
                    url=url,
                    timeout=True,
                    elapsed_ms=round(elapsed, 2),
                    wait_ms=round(wait_ms, 2),
                    error="Request timeout"
                )
            except Exception as e:
                elapsed = (time.perf_counter() - start) * 1000
                result = RequestResult(
                    url=url,
                    error=str(e),
                    elapsed_ms=round(elapsed, 2),
                    wait_ms=round(wait_ms, 2)
                )
            
            await collector.record_request(
                url=url,
                success=success,
                elapsed_ms=result.elapsed_ms,
                status_code=status_code,
                wait_ms=result.wait_ms,
                initial_concurrent=self.max_concurrent
            )
            stats = await collector.get_or_create(url, self.max_concurrent)
            await stats.update_concurrency()
            
            return result


STRATEGIES = {
    "fixed": FixedStrategy,
    "timeout_race": TimeoutRaceStrategy,
    "adaptive": AdaptiveStrategy,
}


def get_strategy(name: str, timeout_sec: float, max_concurrent: int) -> BaseStrategy:
    if name not in STRATEGIES:
        raise ValueError(f"Unknown strategy: {name}")
    return STRATEGIES[name](timeout_sec=timeout_sec, max_concurrent=max_concurrent)