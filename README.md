# PoC OpenSearch Cross-Cluster Replication

README описывает текущее поведение проекта и сценарии запуска. История заметных изменений вынесена в
[`changes.md`](./changes.md), чтобы рабочая инструкция не зависела от того, кто помнит прошлую версию.

## Конфигурация

Все скрипты читают параметры из `.env`.

Основные параметры:
- `LEADER_URL` — адрес лидер кластера (по умолчанию `http://localhost:9200`)
- `FOLLOWER_URL` — адрес фолловер кластера (по умолчанию `http://localhost:9201`)
- `CCR_INDEX` — имя индекса для репликации (по умолчанию `rag_data`)
- `CCR_ALIAS` — алиас удаленного кластера в follower (по умолчанию `leader-cluster`)
- `OPENSEARCH_URLS` — список URL через запятую для producer и consumer (по умолчанию оба кластера)
- `PRODUCER_RETRY_DELAY` — пауза при неуспешной записи (по умолчанию `1`)
- `CONSUMER_POLL_INTERVAL` — интервал опроса индекса consumer-ом (по умолчанию `2`)
- `CONSUMER_QUERY_SIZE` — сколько документов за один запрос читает consumer (по умолчанию `5`)

Producer работает в failover-only режиме:
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

`run_all.ps1` выполняет полный smoke-сценарий без перезапуска `producer.py` и `consumer.py`:
- старт CCR;
- failover на `cluster-2`;
- failback с возвратом `cluster-1` в follower;
- повторный failover обратно на `cluster-1`.

## Смена индекса

Чтобы использовать другой индекс, обновите `CCR_INDEX` в `.env` и повторно запустите:

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
- `failback.py` — понижение старого leader до follower (обратная репликация)
- `check_status.py` — проверка состояния кластеров и репликации
- `run_all.ps1` — автоматический multi-cycle smoke прогон без рестарта producer/consumer

## Порты
- `os-cluster-1` (leader): `http://localhost:9200`
- `os-cluster-2` (follower): `http://localhost:9201`


## Failback после восстановления `cluster-1`

После восстановления `cluster-1` можно вернуть его в роль follower:

### 1) Поднять старый кластер
```powershell
docker start os-cluster-1
```

### 2) Выполнить failback
```powershell
python failback.py
```

Скрипт автоматически:
- определяет текущий leader по статусу репликации;
- удаляет устаревший индекс на `cluster-1`;
- настраивает обратную репликацию, в которой `cluster-1` становится follower для `cluster-2`.

### 3) Проверить статус
```powershell
python check_status.py
```

После failback запись остается на `cluster-2`, пока он остается writable-кластером.

## Дополнительно
- Во время переключения между leader и follower возможна потеря части запросов на запись.
- В этом PoC failover запускается вручную после проверки недоступности кластера. В production такую
  логику обычно выносят в отдельный механизм оркестрации или health-check.
- Follower может отставать от leader примерно на 0.5 секунды по данным.
