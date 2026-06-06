"""
Модульные тесты логики адаптации и стратегий.
"""

import pytest
import asyncio

from gateway.stats import URLStats, StatsCollector


class TestURLStats:
    """Тесты статистики и алгоритма адаптации."""
    
    def test_empty_stats(self):
        """Пустая статистика: success_rate=1.0, avg=0."""
        stats = URLStats()
        assert stats.success_rate == 1.0
        assert stats.avg_ms == 0.0
        assert stats.current_concurrent == 3  # начальное
    
    def test_success_rate_calculation(self):
        """Проверка расчёта success_rate."""
        stats = URLStats()
        # 15 успешных, 5 неуспешных
        for i in range(15):
            stats.add_record(True, 100.0, 200)
        for i in range(5):
            stats.add_record(False, 100.0, 500)
        
        assert stats.success_rate == 0.75
        assert len(stats.records) == 20
    
    def test_window_overflow(self):
        """Проверка скользящего окна: старые записи удаляются."""
        stats = URLStats()
        for i in range(25):
            stats.add_record(True, 100.0, 200)
        
        # Должно остаться только 20
        assert len(stats.records) == 20
    
    def test_concurrency_decrease_on_errors(self):
        """При >30% ошибок concurrency должно уменьшаться."""
        stats = URLStats(initial_concurrent=5)
        
        # 50% ошибок (> порога 30%)
        for i in range(10):
            stats.add_record(False, 100.0, 500)
        for i in range(10):
            stats.add_record(True, 100.0, 200)
        
        new_conc = stats._calculate_adjusted_concurrency()
        assert new_conc < 5  # Уменьшилось
        assert new_conc >= URLStats.MIN_CONCURRENT
    
    def test_concurrency_decrease_on_slow(self):
        """При медленных ответах (>2с) concurrency уменьшается."""
        stats = URLStats(initial_concurrent=4)
        
        # Все ответы медленные
        for i in range(10):
            stats.add_record(True, 2500.0, 200)  # 2.5 сек
        
        new_conc = stats._calculate_adjusted_concurrency()
        assert new_conc < 4
    
    def test_concurrency_increase_on_fast_stable(self):
        """При быстрых и стабильных ответах concurrency растёт."""
        stats = URLStats(initial_concurrent=2)
        
        # Все ответы быстрые и успешные
        for i in range(10):
            stats.add_record(True, 300.0, 200)  # 300 мс < 500 порога
        
        new_conc = stats._calculate_adjusted_concurrency()
        assert new_conc > 2
        assert new_conc <= URLStats.MAX_CONCURRENT
    
    def test_hysteresis_no_change(self):
        """Гистерезис: если метрики в "нейтральной зоне" — не меняем."""
        stats = URLStats(initial_concurrent=3)
        
        # 15% ошибок (между 10% и 30%), 1000 мс (между 500 и 2000)
        for i in range(17):
            stats.add_record(True, 1000.0, 200)
        for i in range(3):
            stats.add_record(False, 1000.0, 500)
        
        new_conc = stats._calculate_adjusted_concurrency()
        assert new_conc == 3  # Не изменилось
    
    @pytest.mark.asyncio
    async def test_async_update(self):
        """Асинхронное обновление concurrency."""
        stats = URLStats(initial_concurrent=5)
        
        for i in range(20):
            stats.add_record(False, 100.0, 500)
        
        new_val = await stats.update_concurrency()
        assert new_val < 5


class TestStatsCollector:
    """Тесты глобального сборщика статистики."""
    
    @pytest.fixture(autouse=True)
    def reset_collector(self):
        """Сброс коллектора перед каждым тестом."""
        from gateway.stats import collector
        collector.reset()
        yield
        collector.reset()
    
    @pytest.mark.asyncio
    async def test_get_or_create(self):
        """Создание и повторное получение статистики."""
        from gateway.stats import collector
        
        stats1 = await collector.get_or_create("http://api1.com/data", 3)
        stats2 = await collector.get_or_create("http://api1.com/other", 5)
        
        # Должны быть одним объектом (ключ — netloc)
        assert stats1 is stats2
        assert stats1.current_concurrent == 3  # первое создание
    
    @pytest.mark.asyncio
    async def test_record_request(self):
        """Запись запроса и получение обновлённой статистики."""
        from gateway.stats import collector
        
        stats = await collector.record_request(
            "http://api2.com/test",
            success=True,
            elapsed_ms=150.0,
            status_code=200,
            initial_concurrent=3
        )
        
        assert stats.success_rate == 1.0
        assert stats.avg_ms == 150.0


class TestStrategyFactory:
    """Тесты фабрики стратегий."""
    
    def test_valid_strategies(self):
        """Проверка создания всех валидных стратегий."""
        from gateway.strategies import get_strategy, STRATEGIES
        
        for name in STRATEGIES:
            strategy = get_strategy(name, timeout_sec=5.0, max_concurrent=3)
            assert strategy is not None
    
    def test_invalid_strategy(self):
        """Невалидная стратегия должна вызывать ValueError."""
        from gateway.strategies import get_strategy
        
        with pytest.raises(ValueError):
            get_strategy("invalid", 5.0, 3)