# Инструкция по запуску

## 1. Установка зависимостей

```bash
pip install -r requirements.txt
```

## 2. Запуск API-сервера

```bash
python main.py serve --port 8000
```

## 3. Ручное тестирование (curl)

```bash
curl -X POST http://localhost:8000/aggregate \
  -H "Content-Type: application/json" \
  -d '{"urls": ["http://127.0.0.1:9001/data"], "strategy": "adaptive"}'
```


## 4. Запуск тестовой системы
### Стандартные сценарии (A, B, C — по 3 хоста)
```bash
python main.py test --scenario all --repeats 10 --output report/
```

### Масштабированный сценарий B_large (50 хостов)
```bash
python main.py test --scenario B_large --repeats 5 --output report_large/
```

## 5. Запуск pytest
```bash
pytest tests/ -v
```

