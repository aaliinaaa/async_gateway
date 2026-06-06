"""
CLI-модуль для запуска тестовых сценариев.

Использование:
    python main.py test --scenario all --repeats 10 --output report/
"""

import argparse
import asyncio
import json
import csv
import os
import time
from dataclasses import dataclass, asdict
from typing import List, Dict, Any

import matplotlib.pyplot as plt
import numpy as np

from gateway.emulator import EmulatorSuite, APIEmulator
from gateway.aggregator import aggregate
from gateway.stats import collector


@dataclass
class TestRunResult:
    scenario: str
    strategy: str
    repeat: int
    total_time_ms: float
    successful: int
    failed: int
    timeouts: int
    total_requests: int
    avg_wait_ms: float = 0.0
    max_wait_ms: float = 0.0
    variance_ms: float = 0.0
    concurrency_history: List[int] = None
    
    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        if d["concurrency_history"] is None:
            d["concurrency_history"] = []
        return d


class TestRunner:
    STRATEGIES = ["fixed", "timeout_race", "adaptive"]
    
    def __init__(self, repeats: int = 10, output_dir: str = "report"):
        self.repeats = repeats
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        self.results: List[TestRunResult] = []
    
    async def run_scenario(self, scenario_name: str, emulators: List[APIEmulator]):
        for emu in emulators:
            await emu.start()
            print(f"  ✓ {emu.name} on port {emu.port}")

        await asyncio.sleep(0.5)
        
        urls = [emu.url for emu in emulators]
        
        try:
            for strategy in self.STRATEGIES:
                print(f"\n  → Сценарий {scenario_name}, стратегия: {strategy}")
                
                for i in range(self.repeats):
                    if strategy != "adaptive":
                        collector.reset()
                    
                    start = time.perf_counter()
                    response = await aggregate(
                        urls=urls,
                        strategy_name=strategy,
                        max_concurrent=3,
                        timeout_sec=5.0
                    )
                    elapsed = (time.perf_counter() - start) * 1000
                    
                    summary = response["summary"]
                    timeouts = sum(1 for r in response["results"] if r.get("timeout"))
                    
                    avg_wait = 0.0
                    max_wait = 0.0
                    variance = 0.0
                    concurrency_hist = None
                    
                    if strategy == "adaptive" and "adaptive_stats" in response:
                        stats = response["adaptive_stats"]
                        concurrency_hist = [s["adjusted_concurrency"] for s in stats.values()]
                        
                        waits = [s.get("avg_wait_ms", 0) for s in stats.values()]
                        variances = [s.get("variance_ms", 0) for s in stats.values()]
                        if waits:
                            avg_wait = sum(waits) / len(waits)
                            max_wait = max(waits)
                        if variances:
                            variance = sum(variances) / len(variances)
                    
                    result = TestRunResult(
                        scenario=scenario_name,
                        strategy=strategy,
                        repeat=i + 1,
                        total_time_ms=round(elapsed, 2),
                        successful=summary["successful"],
                        failed=summary["failed"],
                        timeouts=timeouts,
                        total_requests=summary["total"],
                        avg_wait_ms=round(avg_wait, 2),
                        max_wait_ms=round(max_wait, 2),
                        variance_ms=round(variance, 2),
                        concurrency_history=concurrency_hist
                    )
                    self.results.append(result)
                    print(f"    Прогон {i+1}/{self.repeats}: {elapsed:.0f}ms "
                          f"(OK:{summary['successful']}, FAIL:{summary['failed']}, TO:{timeouts}, "
                          f"wait:{avg_wait:.1f}ms)")
        finally:
            for emu in emulators:
                await emu.stop()
    
    def save_json(self):
        path = os.path.join(self.output_dir, "results.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump([r.to_dict() for r in self.results], f, indent=2, ensure_ascii=False)
        print(f"\n✓ JSON сохранён: {path}")
    
    def save_csv(self):
        path = os.path.join(self.output_dir, "results.csv")
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=[
                "scenario", "strategy", "repeat", "total_time_ms",
                "successful", "failed", "timeouts", "total_requests",
                "avg_wait_ms", "max_wait_ms", "variance_ms"
            ])
            writer.writeheader()
            for r in self.results:
                d = r.to_dict()
                d.pop("concurrency_history")
                writer.writerow(d)
        print(f"✓ CSV сохранён: {path}")
    
    def build_charts(self):
        self._chart_avg_time_by_strategy()
        self._chart_success_failures()
        self._chart_adaptive_concurrency()
        self._chart_wait_time_comparison()
        self._chart_variance_comparison()
        print(f"✓ Графики сохранены в: {self.output_dir}/")
    
    def _chart_avg_time_by_strategy(self):
        fig, axes = plt.subplots(1, 3, figsize=(15, 5))
        scenarios = ["A", "B", "C"]
        
        for idx, scenario in enumerate(scenarios):
            ax = axes[idx]
            data = {}
            for strategy in self.STRATEGIES:
                times = [
                    r.total_time_ms for r in self.results
                    if r.scenario == scenario and r.strategy == strategy
                ]
                data[strategy] = times
            
            positions = [1, 2, 3]
            bp = ax.boxplot([data[s] for s in self.STRATEGIES], positions=positions, widths=0.6)
            ax.set_xticks(positions)
            ax.set_xticklabels(self.STRATEGIES, rotation=15)
            ax.set_ylabel("Время выполнения (мс)")
            ax.set_title(f"Сценарий {scenario}")
            ax.grid(True, alpha=0.3)
        
        plt.suptitle("Сравнение времени выполнения по стратегиям", fontsize=14)
        plt.tight_layout()
        plt.savefig(os.path.join(self.output_dir, "chart_time_comparison.png"), dpi=150)
        plt.close()
    
    def _chart_success_failures(self):
        fig, ax = plt.subplots(figsize=(10, 6))
        
        scenarios = ["A", "B", "C"]
        x = np.arange(len(scenarios))
        width = 0.25
        
        for i, strategy in enumerate(self.STRATEGIES):
            success_rates = []
            for scenario in scenarios:
                runs = [r for r in self.results if r.scenario == scenario and r.strategy == strategy]
                if runs:
                    total_success = sum(r.successful for r in runs)
                    total = sum(r.total_requests for r in runs)
                    rate = (total_success / total * 100) if total > 0 else 0
                    success_rates.append(rate)
                else:
                    success_rates.append(0)
            
            offset = width * (i - 1)
            bars = ax.bar(x + offset, success_rates, width, label=strategy)
            
            for bar in bars:
                height = bar.get_height()
                ax.annotate(f'{height:.0f}%',
                           xy=(bar.get_x() + bar.get_width() / 2, height),
                           xytext=(0, 3), textcoords="offset points",
                           ha='center', va='bottom', fontsize=8)
        
        ax.set_xlabel("Сценарий")
        ax.set_ylabel("Успешные запросы (%)")
        ax.set_title("Успешность запросов по стратегиям и сценариям")
        ax.set_xticks(x)
        ax.set_xticklabels(scenarios)
        ax.legend()
        ax.set_ylim(0, 105)
        ax.grid(True, alpha=0.3, axis='y')
        
        plt.tight_layout()
        plt.savefig(os.path.join(self.output_dir, "chart_success_rate.png"), dpi=150)
        plt.close()

    def _chart_wait_time_comparison(self):
        fig, ax = plt.subplots(figsize=(10, 6))
        
        scenarios = ["A", "B", "C"]
        x = np.arange(len(scenarios))
        width = 0.25
        
        for i, strategy in enumerate(self.STRATEGIES):
            waits = []
            for scenario in scenarios:
                runs = [r for r in self.results 
                       if r.scenario == scenario and r.strategy == strategy]
                if runs:
                    avg_wait = sum(r.avg_wait_ms for r in runs) / len(runs)
                    waits.append(avg_wait)
                else:
                    waits.append(0)
            
            offset = width * (i - 1)
            ax.bar(x + offset, waits, width, label=strategy)
        
        ax.set_xlabel("Сценарий")
        ax.set_ylabel("Среднее время ожидания (мс)")
        ax.set_title("Сравнение времени ожидания в семафоре")
        ax.set_xticks(x)
        ax.set_xticklabels(scenarios)
        ax.legend()
        ax.grid(True, alpha=0.3, axis='y')
        
        plt.tight_layout()
        plt.savefig(os.path.join(self.output_dir, "chart_wait_time.png"), dpi=150)
        plt.close()

    def _chart_variance_comparison(self):
        fig, ax = plt.subplots(figsize=(10, 6))
        
        scenarios = ["A", "B", "C"]
        x = np.arange(len(scenarios))
        width = 0.25
        
        for i, strategy in enumerate(self.STRATEGIES):
            variances = []
            for scenario in scenarios:
                runs = [r for r in self.results 
                       if r.scenario == scenario and r.strategy == strategy]
                if runs:
                    avg_var = sum(r.variance_ms for r in runs) / len(runs)
                    variances.append(avg_var)
                else:
                    variances.append(0)
            
            offset = width * (i - 1)
            ax.bar(x + offset, variances, width, label=strategy)
        
        ax.set_xlabel("Сценарий")
        ax.set_ylabel("Средняя дисперсия (мс²)")
        ax.set_title("Сравнение стабильности времени ответа")
        ax.set_xticks(x)
        ax.set_xticklabels(scenarios)
        ax.legend()
        ax.grid(True, alpha=0.3, axis='y')
        
        plt.tight_layout()
        plt.savefig(os.path.join(self.output_dir, "chart_variance.png"), dpi=150)
        plt.close()
    
    def _chart_adaptive_concurrency(self):
        fig, axes = plt.subplots(1, 3, figsize=(15, 5))
        scenarios = ["A", "B", "C"]
        
        for idx, scenario in enumerate(scenarios):
            ax = axes[idx]
            adaptive_runs = [
                r for r in self.results 
                if r.scenario == scenario and r.strategy == "adaptive" and r.concurrency_history
            ]
            
            if not adaptive_runs:
                ax.set_title(f"Сценарий {scenario} — нет данных")
                continue
            
            max_hosts = max(len(r.concurrency_history) for r in adaptive_runs)
            avg_concurrency = []
            for h in range(max_hosts):
                values = [r.concurrency_history[h] for r in adaptive_runs 
                         if len(r.concurrency_history) > h]
                avg_concurrency.append(sum(values) / len(values) if values else 0)
            
            ax.plot(range(1, len(avg_concurrency) + 1), avg_concurrency, 
                   marker='o', linewidth=2, markersize=8)
            ax.set_xlabel("Номер хоста")
            ax.set_ylabel("Средний concurrency")
            ax.set_title(f"Сценарий {scenario}")
            ax.grid(True, alpha=0.3)
            ax.set_ylim(0, 6)
        
        plt.suptitle("Adaptive: динамика concurrency по хостам", fontsize=14)
        plt.tight_layout()
        plt.savefig(os.path.join(self.output_dir, "chart_adaptive_concurrency.png"), dpi=150)
        plt.close()
    
    async def run_all(self, scenarios: List[str]):
        if "all" in scenarios:
            scenarios = ["A", "B", "C"]
        
        for sc_name in scenarios:
            print(f"\n{'='*50}")
            print(f"Запуск сценария {sc_name}")
            print(f"{'='*50}")
            emulators = EmulatorSuite.get_scenario(sc_name)
            await self.run_scenario(sc_name, emulators)
        
        print(f"\n{'='*50}")
        print("Сохранение результатов...")
        self.save_json()
        self.save_csv()
        self.build_charts()
        print("Готово!")


def main():
    parser = argparse.ArgumentParser(description="Тестовая система Async Gateway")
    subparsers = parser.add_subparsers(dest="command")
    
    test_parser = subparsers.add_parser("test", help="Запустить тестовые сценарии")
    test_parser.add_argument("--scenario", nargs="+", default=["all"],
                           choices=["A", "B", "C", "all"],
                           help="Сценарии для запуска")
    test_parser.add_argument("--repeats", type=int, default=10,
                           help="Количество повторений")
    test_parser.add_argument("--output", default="report",
                           help="Директория для отчётов")
    
    serve_parser = subparsers.add_parser("serve", help="Запустить API сервер")
    serve_parser.add_argument("--host", default="127.0.0.1")
    serve_parser.add_argument("--port", type=int, default=8000)
    
    args = parser.parse_args()
    
    if args.command == "test":
        runner = TestRunner(repeats=args.repeats, output_dir=args.output)
        asyncio.run(runner.run_all(args.scenario))
    elif args.command == "serve":
        import uvicorn
        from gateway.api import app
        uvicorn.run(app, host=args.host, port=args.port)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()