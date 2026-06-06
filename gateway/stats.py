"""
Модуль сбора и анализа статистики для адаптивной стратегии.
"""

import asyncio
import time
from collections import deque
from dataclasses import dataclass
from typing import Dict, Optional
from urllib.parse import urlparse


@dataclass
class RequestRecord:
    """Запись об одном запросе к API."""
    timestamp: float
    success: bool
    elapsed_ms: float
    status_code: Optional[int] = None
    wait_ms: float = 0.0


class URLStats:
    WINDOW_SIZE = 10
    
    ERROR_THRESHOLD = 0.20
    SLOW_THRESHOLD_MS = 1500.0
    FAST_THRESHOLD_MS = 300.0
    
    MIN_CONCURRENT = 1
    MAX_CONCURRENT = 5
    
    def __init__(self, initial_concurrent: int = 3):
        self.records: deque[RequestRecord] = deque(maxlen=self.WINDOW_SIZE)
        self.current_concurrent = initial_concurrent
        self.lock = asyncio.Lock()
    
    def add_record(self, success: bool, elapsed_ms: float,
                   status_code: Optional[int] = None,
                   wait_ms: float = 0.0):
        """Добавить запись о завершённом запросе."""
        record = RequestRecord(
            timestamp=time.time(),
            success=success,
            elapsed_ms=elapsed_ms,
            status_code=status_code,
            wait_ms=wait_ms
        )
        self.records.append(record)
    
    @property
    def success_rate(self) -> float:
        if not self.records:
            return 1.0
        successful = sum(1 for r in self.records if r.success)
        return successful / len(self.records)
    
    @property
    def avg_ms(self) -> float:
        if not self.records:
            return 0.0
        return sum(r.elapsed_ms for r in self.records) / len(self.records)
    
    @property
    def avg_wait_ms(self) -> float:
        """Среднее время ожидания в семафоре (мс)."""
        if not self.records:
            return 0.0
        return sum(r.wait_ms for r in self.records) / len(self.records)
    
    @property
    def variance_ms(self) -> float:
        """Дисперсия времени ответа (мс²)."""
        if len(self.records) < 2:
            return 0.0
        avg = self.avg_ms
        return sum((r.elapsed_ms - avg) ** 2 for r in self.records) / len(self.records)
    
    def _calculate_adjusted_concurrency(self) -> int:
        rate = self.success_rate
        avg = self.avg_ms
        
        if rate < (1 - self.ERROR_THRESHOLD) or avg > self.SLOW_THRESHOLD_MS:
            return max(self.MIN_CONCURRENT, self.current_concurrent - 1)
        
        if rate > 0.90 and avg < self.FAST_THRESHOLD_MS:
            return min(self.MAX_CONCURRENT, self.current_concurrent + 1)
        
        return self.current_concurrent
    
    async def update_concurrency(self) -> int:
        async with self.lock:
            new_value = self._calculate_adjusted_concurrency()
            self.current_concurrent = new_value
            return new_value
    
    def get_stats(self) -> dict:
        return {
            "success_rate": round(self.success_rate, 2),
            "avg_ms": round(self.avg_ms, 2),
            "avg_wait_ms": round(self.avg_wait_ms, 2),
            "variance_ms": round(self.variance_ms, 2),
            "adjusted_concurrency": self.current_concurrent,
            "window_size": len(self.records),
        }


class StatsCollector:
    def __init__(self):
        self._stats: Dict[str, URLStats] = {}
        self._lock = asyncio.Lock()
    
    async def get_or_create(self, url: str, initial_concurrent: int = 3) -> URLStats:
        parsed = urlparse(url)
        key = parsed.netloc or url
        
        async with self._lock:
            if key not in self._stats:
                self._stats[key] = URLStats(initial_concurrent)
            return self._stats[key]
    
    async def record_request(self, url: str, success: bool, elapsed_ms: float,
                            status_code: Optional[int] = None,
                            wait_ms: float = 0.0,
                            initial_concurrent: int = 3) -> URLStats:
        """Записать результат запроса и вернуть обновлённую статистику."""
        stats = await self.get_or_create(url, initial_concurrent)
        stats.add_record(success, elapsed_ms, status_code, wait_ms)
        return stats
    
    async def get_all_stats(self) -> Dict[str, dict]:
        async with self._lock:
            return {k: v.get_stats() for k, v in self._stats.items()}
    
    def reset(self):
        self._stats.clear()


collector = StatsCollector()