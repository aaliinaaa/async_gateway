#!/usr/bin/env python3
"""
Async Aggregation Gateway — точка входа.

Использование:
    # Запуск API сервера
    python main.py serve --port 8000
    
    # Запуск тестов
    python main.py test --scenario all --repeats 10 --output report/
"""

from cli import main

if __name__ == "__main__":
    main()