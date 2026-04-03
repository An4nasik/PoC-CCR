# PoC OpenSearch Cross-Cluster Replication

## Конфигурация

Все скрипты читают параметры из `.env`.

Файл уже есть в проекте. При необходимости можешь обновить его по шаблону `.env.example`.

Основные параметры:

- `LEADER_URL` — адрес лидер кластера (по умолчанию `http://localhost:9200`)
- `FOLLOWER_URL` — адрес фолловер кластера (по умолчанию `http://localhost:9201`)
- `CCR_INDEX` — имя индекса для репликации (по умолчанию `rag_data`)
- `CCR_ALIAS` — алиас удаленного кластера в follower (по умолчанию `leader-cluster`)
- `OPENSEARCH_URLS` — список URL через запятую для producer и consumer (по умолчанию оба кластера)
- `PRODUCER_RETRY_DELAY` — пауза при неуспешной записи (по умолчанию `1`)
- `CONSUMER_POLL_INTERVAL` — интервал опроса индекса consumer-ом (по умолчанию `2`)
- `CONSUMER_QUERY_SIZE` — сколько документов за один запрос читает consumer (по умолчанию `5`)

Producer теперь работает в failover-only режиме:
- определяет writable endpoint автоматически;
- пишет только в writable endpoint;
- при падении текущей точки записи переходит на новую writable точку.

Consumer работает в round-robin режиме:
- читает индекс по очереди с URL из `OPENSEARCH_URLS`;
- при недоступности одного endpoint читает с другого.

## Быстрый запуск (Windows)

### 1) Поднять кластеры
```powershell
docker compose up -d
```

### 2) Настроить CCR
```powershell
python setup.py
```

### 3) Запустить продюсер
```powershell
python producer.py
```

### 4) Запустить consumer (опционально)
```powershell
python consumer.py
```

### 5) Проверить статус
```powershell
python check_status.py
```

### 6) Эмулировать падение лидера
```powershell
docker stop os-cluster-1
```

### 7) Выполнить failover
```powershell
python failover.py
```

### 8) Проверить, что запись продолжается
```powershell
python producer.py
```

## Полный сценарий одной командой

```powershell
powershell -ExecutionPolicy Bypass -File .\run_all.ps1
```

## Смена индекса

Меняешь `CCR_INDEX` в `.env` и повторно запускаешь:

```powershell
python setup.py
python check_status.py
```

## Структура файлов
- `docker-compose.yml` — два OpenSearch кластера
- `config.py` — загрузка `.env`
- `setup.py` — создание индекса и настройка CCR
- `producer.py` — запись документов (автоопределение writable endpoint)
- `consumer.py` — чтение документов в round-robin по endpoint-ам
- `failover.py` — перевод follower в writable режим
- `check_status.py` — проверка состояния кластеров и репликации
- `run_all.ps1` — автоматический Stage-1 прогон

## Порты
- `os-cluster-1` (leader): `http://localhost:9200`
- `os-cluster-2` (follower): `http://localhost:9201`
