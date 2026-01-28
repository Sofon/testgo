# Data Pipeline Service

Сервис для обработки данных из MQTT-брокера (EMQX) и записи в time-series базу данных (QuestDB).

## Архитектура

```
┌─────────────┐     ┌──────────────────┐     ┌───────────┐
│    EMQX     │────▶│  Data Pipeline   │────▶│  QuestDB  │
│ (Data Bus)  │     │     Service      │     │  (TSDB)   │
└─────────────┘     └──────────────────┘     └───────────┘
                            │
       ┌────────────────────┼────────────────────┐
       │                    │                    │
       ▼                    ▼                    ▼
┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│   MongoDB    │    │    EMQX      │    │    File      │
│ (ServiceCfg) │    │ (Event Bus)  │    │  (Fallback)  │
└──────────────┘    └──────────────┘    └──────────────┘
```

## Возможности

- Подписка на MQTT топики (EMQX) для приёма данных
- Парсинг JSON-сообщений с маппингом полей
- Запись данных в QuestDB через ILP (InfluxDB Line Protocol)
- **Разделённая конфигурация**:
  - `ServiceConfig` (batch_size, flush_interval, stream_mapping) — в MongoDB
  - Настройки подключений (MQTT, QuestDB, EventBus) — в файле
- **Event Bus** — публикация событий при изменении конфига и статуса
- REST API для управления конфигурацией и мониторинга
- Graceful shutdown
- Автоматический reconnect при потере соединения

## Структура конфигурации

### ServiceConfig (хранится в MongoDB)

```json
{
  "batch_size": 1000,
  "flush_interval": 1000,
  "write_timeout": 5000,
  "stream_mapping": {
    "sensors/#": "sensor_data",
    "telemetry/#": "telemetry_data"
  }
}
```

Это единственная часть конфигурации, которая **динамически изменяется** через API и хранится в MongoDB с файловым fallback.

### FileConfig (читается из config.yaml)

Настройки подключений, которые обычно не меняются в runtime:
- MQTT Data Bus — подключение к шине данных
- Event Bus — подключение к шине событий
- QuestDB — подключение к БД
- Pipeline — параметры обработки (workers, buffer_size, field_mappings)

## Структура проекта

```
data-pipeline-service/
├── cmd/
│   └── server/
│       └── main.go              # Точка входа
├── internal/
│   ├── api/
│   │   ├── handlers.go          # HTTP handlers
│   │   └── router.go            # HTTP router и middleware
│   ├── config/
│   │   └── config.go            # Загрузка конфигурации приложения
│   ├── eventbus/
│   │   └── eventbus.go          # Клиент шины событий
│   ├── models/
│   │   ├── config.go            # Модели конфигурации
│   │   ├── events.go            # Модели событий
│   │   ├── message.go           # Модели сообщений
│   │   └── status.go            # Модели статуса
│   ├── mqtt/
│   │   └── client.go            # MQTT клиент
│   ├── questdb/
│   │   └── writer.go            # QuestDB writer
│   ├── service/
│   │   └── service.go           # Основной сервис
│   └── storage/
│       ├── file.go              # Файловое хранилище (fallback)
│       ├── mongo.go             # MongoDB хранилище
│       └── storage.go           # Гибридное хранилище
├── pkg/
│   └── logger/
│       └── logger.go            # Логгер
├── configs/
│   └── config.yaml              # Конфигурация приложения
├── deployments/
│   └── docker-compose.yaml      # Docker Compose
├── scripts/
│   ├── init-config.sh           # Инициализация конфига
│   └── api-examples.sh          # Примеры API запросов
├── Dockerfile
├── Makefile
└── README.md
```

## Быстрый старт

### 1. Запуск через Docker Compose

```bash
# Запустить все сервисы (MongoDB, EMQX, QuestDB, Pipeline)
make docker-up

# Или напрямую
cd deployments && docker-compose up -d
```

### 2. Проверка статуса

```bash
curl http://localhost:8080/api/v1/status | jq .
```

## Конфигурация

### Переменные окружения

| Переменная | Описание | По умолчанию |
|------------|----------|--------------|
| `SERVER_HOST` | Хост HTTP-сервера | `0.0.0.0` |
| `SERVER_PORT` | Порт HTTP-сервера | `8080` |
| `MONGODB_URI` | URI подключения к MongoDB | `mongodb://localhost:27017` |
| `MONGODB_DATABASE` | База данных MongoDB | `data_pipeline` |
| `MQTT_BROKER` | MQTT брокер для данных | `localhost` |
| `MQTT_PORT` | MQTT порт | `1883` |
| `EVENTBUS_ENABLED` | Включить шину событий | `true` |
| `EVENTBUS_BROKER` | MQTT брокер для событий | `localhost` |
| `EVENTBUS_TOPIC_PREFIX` | Префикс топиков событий | `events/data-pipeline` |
| `QUESTDB_HOST` | Хост QuestDB | `localhost` |
| `QUESTDB_ILP_PORT` | ILP порт QuestDB | `9009` |
| `LOG_LEVEL` | Уровень логирования | `info` |

## REST API

### Endpoints

| Метод | Путь | Описание |
|-------|------|----------|
| `GET` | `/health` | Health check |
| `GET` | `/ready` | Readiness probe |
| `GET` | `/live` | Liveness probe |
| `GET` | `/api/v1/config` | Получить полную конфигурацию (ServiceConfig + FileConfig) |
| `GET` | `/api/v1/config/service` | Получить только ServiceConfig из MongoDB |
| `PUT` | `/api/v1/config` | Обновить ServiceConfig |
| `PATCH` | `/api/v1/config` | Частично обновить ServiceConfig |
| `GET` | `/api/v1/status` | Получить статус сервиса |
| `POST` | `/api/v1/reload` | Перезагрузить ServiceConfig из MongoDB |

### Примеры

```bash
# Получить статус
curl http://localhost:8080/api/v1/status | jq .

# Получить полную конфигурацию
curl http://localhost:8080/api/v1/config | jq .

# Получить только ServiceConfig
curl http://localhost:8080/api/v1/config/service | jq .

# Обновить batch_size
curl -X PATCH http://localhost:8080/api/v1/config \
  -H "Content-Type: application/json" \
  -d '{"batch_size": 2000}'

# Обновить stream_mapping
curl -X PATCH http://localhost:8080/api/v1/config \
  -H "Content-Type: application/json" \
  -d '{
    "stream_mapping": {
      "sensors/#": "sensor_data",
      "telemetry/#": "telemetry_data",
      "devices/#": "device_data"
    }
  }'

# Перезагрузить конфиг из MongoDB
curl -X POST http://localhost:8080/api/v1/reload
```

## Event Bus

Сервис публикует события в отдельную шину EMQX:

### Типы событий

| Тип | Топик | Описание |
|-----|-------|----------|
| `config.updated` | `events/data-pipeline/config.updated` | Конфиг успешно обновлён |
| `config.update_failed` | `events/data-pipeline/config.update_failed` | Ошибка обновления конфига |
| `status.changed` | `events/data-pipeline/status.changed` | Статус сервиса изменился |

### Формат события

```json
{
  "type": "config.updated",
  "timestamp": "2024-01-15T10:30:00Z",
  "source": "data-pipeline-service",
  "data": {
    "version": 5,
    "batch_size": 2000,
    "flush_interval": 1000,
    "write_timeout": 5000,
    "stream_mapping": {...},
    "updated_by": "api"
  }
}
```

### Подписка на события

```bash
# Через MQTT CLI
mosquitto_sub -h localhost -t 'events/data-pipeline/#' -v
```

## Маппинг полей

Типы полей для QuestDB:

| Тип | Описание |
|-----|----------|
| `symbol` | Индексируемая строка (для GROUP BY) |
| `string` | Строка |
| `long` | Целое число (int64) |
| `double` | Число с плавающей точкой |
| `boolean` | Булево значение |
| `timestamp` | Временная метка |

Пример маппинга (в config.yaml):

```yaml
pipeline:
  pipeline:
    field_mappings:
      - source: "device_id"
        target: "device_id"
        type: "symbol"
        required: true
      - source: "temperature"
        target: "temp"
        type: "double"
        required: true
      - source: "timestamp"
        target: "ts"
        type: "timestamp"
        required: true
```

## Stream Mapping

ServiceConfig содержит `stream_mapping` — маппинг MQTT топиков на таблицы QuestDB:

```json
{
  "stream_mapping": {
    "sensors/#": "sensor_data",
    "telemetry/#": "telemetry_data"
  }
}
```

Поддерживается wildcard `#` в конце паттерна.

## Мониторинг

### Доступные дашборды

- **EMQX Dashboard**: http://localhost:18083 (admin/public)
- **QuestDB Web Console**: http://localhost:9000
- **Service Status**: http://localhost:8080/api/v1/status

### Метрики статуса

```json
{
  "status": "running",
  "uptime": "1h30m45s",
  "mqtt": {
    "connected": true,
    "messages_received": 150000
  },
  "questdb": {
    "connected": true,
    "rows_written": 149500
  },
  "mongodb": {
    "connected": true
  },
  "event_bus": {
    "enabled": true,
    "connected": true,
    "topic_prefix": "events/data-pipeline"
  },
  "pipeline": {
    "running": true,
    "buffer_usage": 5,
    "processed_messages": 149500,
    "active_workers": 4
  }
}
```

## Разработка

```bash
# Установить инструменты разработки
make tools

# Запустить локально
make run

# Запустить с hot-reload
make dev

# Запустить тесты
make test

# Собрать Docker образ
make docker-build
```

## Хранение конфигурации

### ServiceConfig (MongoDB + File fallback)

ServiceConfig хранится в MongoDB с файловым fallback:

1. При старте сервис подключается к MongoDB
2. Если MongoDB недоступен — используется файловый storage
3. При восстановлении MongoDB — данные синхронизируются
4. Каждое обновление сохраняется в обоих местах

### FileConfig (только файл)

Настройки подключений читаются из `config.yaml` при старте и не меняются в runtime.

## Что менять

### Изменить batch_size, flush_interval или write_timeout
```bash
curl -X PATCH http://localhost:8080/api/v1/config \
  -H "Content-Type: application/json" \
  -d '{"batch_size": 2000, "flush_interval": 500}'
```

### Добавить новый маппинг topic → table
```bash
curl -X PATCH http://localhost:8080/api/v1/config \
  -H "Content-Type: application/json" \
  -d '{
    "stream_mapping": {
      "sensors/#": "sensor_data",
      "devices/#": "device_data"
    }
  }'
```

### Изменить параметры подключения (MQTT, QuestDB)
Отредактировать `configs/config.yaml`, перезапустить сервис.

### Добавить новое поле в маппинг
Отредактировать `configs/config.yaml` раздел `field_mappings`, перезапустить сервис.
