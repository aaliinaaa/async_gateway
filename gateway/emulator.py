"""
Встроенные эмуляторы API для тестирования.
"""

import asyncio
import random
import time
from typing import Optional, List, Dict, Any

from aiohttp import web


class APIEmulator:
    def __init__(
        self,
        port: int,
        name: str = "api",
        delay_ms: float = 100.0,
        error_rate: float = 0.0,
        error_status: int = 500,
        response_data: Optional[Dict[str, Any]] = None
    ):
        self.port = port
        self.name = name
        self.delay_ms = delay_ms
        self.error_rate = error_rate
        self.error_status = error_status
        self.response_data = response_data or {"source": name, "timestamp": None}
        self.app = web.Application()
        self.app.router.add_get('/data', self.handle)
        self.runner = None
        self.site = None
        self.request_count = 0
    
    def _get_delay(self) -> float:
        if isinstance(self.delay_ms, tuple):
            low, high = self.delay_ms
            return random.uniform(low, high) / 1000.0
        return self.delay_ms / 1000.0
    
    async def handle(self, request: web.Request) -> web.Response:
        self.request_count += 1
        delay = self._get_delay()
        await asyncio.sleep(delay)
        
        if random.random() < self.error_rate:
            return web.json_response(
                {"error": "Internal Server Error", "source": self.name},
                status=self.error_status
            )
        
        data = dict(self.response_data)
        data["timestamp"] = time.time()
        data["request_num"] = self.request_count
        return web.json_response(data)
    
    async def start(self):
        self.runner = web.AppRunner(self.app)
        await self.runner.setup()
        self.site = web.TCPSite(self.runner, '127.0.0.1', self.port)
        await self.site.start()
        await asyncio.sleep(0.3)
        return self
    
    async def stop(self):
        if self.site:
            await self.site.stop()
        if self.runner:
            await self.runner.cleanup()
        await asyncio.sleep(0.2)
    
    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}/data"


class EmulatorSuite:
    @staticmethod
    def create_scenario_a() -> List[APIEmulator]:
        return [
            APIEmulator(port=9001, name="stable_1", delay_ms=50),
            APIEmulator(port=9002, name="stable_2", delay_ms=80),
            APIEmulator(port=9003, name="stable_3", delay_ms=120),
        ]
    
    @staticmethod
    def create_scenario_b() -> List[APIEmulator]:
        return [
            APIEmulator(port=9001, name="fast_1", delay_ms=50),
            APIEmulator(port=9002, name="fast_2", delay_ms=80),
            APIEmulator(port=9003, name="slow_1", delay_ms=(3000, 5000)),
        ]
    
    @staticmethod
    def create_scenario_c() -> List[APIEmulator]:
        return [
            APIEmulator(port=9001, name="reliable", delay_ms=100, error_rate=0.0),
            APIEmulator(port=9002, name="flaky_30", delay_ms=150, error_rate=0.30, error_status=500),
            APIEmulator(port=9003, name="flaky_50", delay_ms=200, error_rate=0.50, error_status=503),
        ]
    
    @staticmethod
    def get_scenario(name: str) -> List[APIEmulator]:
        scenarios = {
            "A": EmulatorSuite.create_scenario_a,
            "B": EmulatorSuite.create_scenario_b,
            "C": EmulatorSuite.create_scenario_c,
        }
        if name not in scenarios:
            raise ValueError(f"Unknown scenario: {name}")
        return scenarios[name]()