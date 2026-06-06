"""
Ядро агрегации: объединяет результаты от всех стратегий в единый ответ.
"""

import uuid
import time
from typing import List, Dict, Any

import aiohttp

from .strategies import get_strategy, RequestResult
from .stats import collector


async def aggregate(
    urls: List[str],
    strategy_name: str = "fixed",
    max_concurrent: int = 3,
    timeout_sec: float = 5.0
) -> Dict[str, Any]:
    request_id = str(uuid.uuid4())
    start_time = time.perf_counter()
    
    timeout = aiohttp.ClientTimeout(total=timeout_sec + 5)
    conn = aiohttp.TCPConnector(limit=100, limit_per_host=30)
    
    session = aiohttp.ClientSession(connector=conn, timeout=timeout)
    
    try:
        strategy = get_strategy(strategy_name, timeout_sec, max_concurrent)
        raw_results = await strategy.execute(urls, session)
        
        if strategy_name == "adaptive":
            for r in raw_results:
                if isinstance(r, RequestResult):
                    success = r.status is not None and 200 <= r.status < 300
                    await collector.record_request(
                        url=r.url,
                        success=success,
                        elapsed_ms=r.elapsed_ms,
                        status_code=r.status,
                        wait_ms=getattr(r, 'wait_ms', 0.0),
                        initial_concurrent=max_concurrent
                    )
                    stats = await collector.get_or_create(r.url, max_concurrent)
                    await stats.update_concurrency()
    
    finally:
        await session.close()
        await conn.close()
    
    total_time_ms = round((time.perf_counter() - start_time) * 1000, 2)
    
    results: List[Dict[str, Any]] = []
    total_wait_ms = 0.0
    max_wait_ms = 0.0
    wait_count = 0
    
    for r in raw_results:
        if isinstance(r, Exception):
            results.append({
                "url": "unknown",
                "error": str(r),
                "elapsed_ms": 0
            })
        else:
            entry = {
                "url": r.url,
                "elapsed_ms": r.elapsed_ms,
                "wait_ms": getattr(r, 'wait_ms', 0.0),
            }
            if r.timeout:
                entry["timeout"] = True
            elif r.error:
                entry["error"] = r.error
                entry["status"] = r.status or 0
            elif r.status and r.status >= 300:
                entry["error"] = f"HTTP {r.status}"
                entry["status"] = r.status
            else:
                entry["status"] = r.status
                entry["data"] = r.data
            results.append(entry)
            
            wait = getattr(r, 'wait_ms', 0.0)
            total_wait_ms += wait
            max_wait_ms = max(max_wait_ms, wait)
            wait_count += 1
    
    avg_wait_ms = round(total_wait_ms / wait_count, 2) if wait_count > 0 else 0.0
    
    successful = sum(1 for r in results if "data" in r and r.get("status", 999) < 300)
    failed = sum(1 for r in results if "error" in r or r.get("timeout") or r.get("status", 999) >= 300)
    
    adaptive_stats = {}
    if strategy_name == "adaptive":
        all_stats = await collector.get_all_stats()
        adaptive_stats = all_stats
    
    response = {
        "request_id": request_id,
        "results": results,
        "summary": {
            "total": len(urls),
            "successful": successful,
            "failed": failed,
            "total_time_ms": total_time_ms,
            "avg_wait_ms": avg_wait_ms,
            "max_wait_ms": round(max_wait_ms, 2),
            "strategy_used": strategy_name,
            "concurrent_used": max_concurrent if strategy_name != "adaptive" else "dynamic",
        },
    }
    
    if adaptive_stats:
        response["adaptive_stats"] = adaptive_stats
    
    return response