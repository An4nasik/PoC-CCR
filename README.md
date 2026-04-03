# PoC OpenSearch Cross-Cluster Replication

## Конфигурация

Все скрипты читают параметры из `.env`.

Файл уже есть в проекте. При необходимости можешь обновить его по шаблону `.env.example`.

Основные параметры:

- `LEADER_URL` — адрес лидер кластера (по умолчанию `http://localhost:9200`)
- `FOLLOWER_URL` — адрес фолловер кластера (по умолчанию `http://localhost:9201`)
- `CCR_INDEX` — имя индекса для репликации (по умолчанию `rag_data`)
- `CCR_ALIAS` — алиас удаленного кластера в follower (по умолчанию `leader-cluster`)
- `OPENSEARCH_URLS` — список URL через запятую для распределения записей продюсером (по умолчанию оба кластера)
- `PRODUCER_STRATEGY` — стратегия продюсера: `auto` (начинает round-robin, переходит в failover), `round_robin` или `failover`
- `FAILOVER_ERRORS_THRESHOLD` — сколько подряд идущих ошибок до автопереключения в failover режим (по умолчанию 3)

`PRODUCER_STRATEGY=auto` работает так:
- старт в `round_robin`;
- после серии ошибок автоматическое переключение в `failover`;
- в `failover` продюсер закрепляется на writable endpoint.

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

### 4) Проверить статус
```powershell
python check_status.py
```

### 5) Эмулировать падение лидера
```powershell
docker stop os-cluster-1
```

### 6) Выполнить failover
```powershell
python failover.py
```

### 7) Проверить, что запись продолжается
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
- `producer.py` — запись документов
- `failover.py` — перевод follower в writable режим
- `check_status.py` — проверка состояния кластеров и репликации
- `run_all.ps1` — автоматический Stage-1 прогон

## Порты
- `os-cluster-1` (leader): `http://localhost:9200`
- `os-cluster-2` (follower): `http://localhost:9201`
