# PoC OpenSearch Cross-Cluster Replication

## Быстрый старт

### 1. Запуск кластеров
```bash
docker compose up -d
```

### 2. Инициализация CCR
```bash
pip install requests
python setup.py
```

По умолчанию используется индекс `rag_data`.
Если нужен новый индекс, перед запуском задайте:
```bash
set CCR_INDEX=rag_data_2
```

### 3. Запуск producer (пишет в Leader)
```bash
set OPENSEARCH_URL=http://localhost:9200
python producer.py
```

Round robin между кластерами:
```bash
set OPENSEARCH_URLS=http://localhost:9200,http://localhost:9201
set PRODUCER_STRATEGY=round_robin
python producer.py
```

### 4. Проверка репликации (в другом терминале)
```bash
python check_status.py
```

`check_status.py` использует переменную `CCR_INDEX`, поэтому индекс можно менять без правки кода:
```bash
set CCR_INDEX=rag_data_2
python check_status.py
```

### 5. Эмуляция аварии Leader
```bash
docker stop os-cluster-1
# Producer начнёт сыпать ошибками
```

### 6. Failover на Follower
```bash
python failover.py
```

### 7. Перезапуск producer на новый Leader
```bash
set OPENSEARCH_URL=http://localhost:9201
python producer.py
```

## Скрипт "Сделать все" (Windows)

Один скрипт прогоняет весь Stage-1 сценарий: поднимает стек, настраивает CCR, запускает producer, эмулирует падение лидера, делает failover и проверяет итог.

```powershell
powershell -ExecutionPolicy Bypass -File .\run_all.ps1
```

Для запуска с новым индексом:
```powershell
$env:CCR_INDEX="rag_data_2"
powershell -ExecutionPolicy Bypass -File .\run_all.ps1
```

## Структура файлов
- `docker-compose.yml` — два OpenSearch кластера
- `setup.py` — инициализация индекса и CCR
- `producer.py` — запись документов
- `failover.py` — повышение Follower до Leader
- `check_status.py` — проверка статуса
- `run_all.ps1` — полный автоматический Stage-1 прогон для Windows

## Порты
- Leader (cluster-1): `http://localhost:9200`
- Follower (cluster-2): `http://localhost:9201`
