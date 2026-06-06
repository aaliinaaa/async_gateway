"""
FastAPI приложение с эндпоинтом /aggregate.
"""

from typing import List, Optional
from pydantic import BaseModel, Field

from fastapi import FastAPI, HTTPException

from .aggregator import aggregate


app = FastAPI(
    title="Async Aggregation Gateway",
    description="Асинхронный шлюз агрегации данных с динамической балансировкой",
    version="1.0.0",
)


class AggregateRequest(BaseModel):
    urls: List[str] = Field(..., description="Список URL для опроса", min_length=1)
    strategy: Optional[str] = Field("fixed", description="Стратегия: fixed, timeout_race, adaptive")
    max_concurrent: Optional[int] = Field(3, ge=1, le=50, description="Макс. одновременных запросов")
    timeout_sec: Optional[float] = Field(5.0, ge=0.1, le=60, description="Таймаут на запрос (сек)")


@app.post("/aggregate")
async def aggregate_endpoint(request: AggregateRequest):
    try:
        result = await aggregate(
            urls=request.urls,
            strategy_name=request.strategy,
            max_concurrent=request.max_concurrent,
            timeout_sec=request.timeout_sec
        )
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal error: {str(e)}")


@app.get("/health")
async def health_check():
    return {"status": "ok", "service": "async-gateway"}


@app.get("/stats")
async def get_stats():
    from .stats import collector
    stats = await collector.get_all_stats()
    return {"stats": stats}