# Data Pipeline Service

Сервис для обработки данных из MQTT-брокера (EMQX) и записи в time-series базу данных (QuestDB).

## Архитектура

```
┌─────────────┐     ┌──────────────────┐     ┌───────────┐
│    EMQX     │────▶│  Data Pipeline   │────▶│  QuestDB  │
│   (MQTT)    │     │     Service      │     │  (TSDB)   │
└─────────────┘     └──────────────────┘     └───────────┘
                            │
                            │ config
                            ▼
                    ┌──────────────────┐
                    │     MongoDB      │
                    │   (+ file        │
                    │    fallback)     │
                    └──────────────────┘
```

## Возможности

- Подписка на MQTT топики (EMQX)
- Парсинг JSON-сообщений с маппингом полей
- Запись данных в QuestDB через ILP (InfluxDB Line Protocol)
- Хранение конфигурации в MongoDB с файловым fallback
- REST API для управления конфигурацией и мониторинга
- Graceful shutdown
- Автоматический reconnect при потере соединения

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
│   ├── models/
│   │   ├── config.go            # Модели конфигурации пайплайна
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

### 2. Инициализация конфигурации

```bash
# Подождите ~10 секунд пока сервисы запустятся
./scripts/init-config.sh
```

### 3. Проверка статуса

```bash
curl http://localhost:8080/api/v1/status | jq .
```

## Конфигурация

### Уровни конфигурации

1. **Конфигурация приложения** (`configs/config.yaml`) — параметры подключения к MongoDB, настройки HTTP-сервера, логирование
2. **Конфигурация пайплайна** (хранится в MongoDB) — MQTT топики, QuestDB таблицы, маппинг полей

### Переменные окружения

| Переменная | Описание | По умолчанию |
|------------|----------|--------------|
| `SERVER_HOST` | Хост HTTP-сервера | `0.0.0.0` |
| `SERVER_PORT` | Порт HTTP-сервера | `8080` |
| `SERVER_MODE` | Режим Gin (debug/release) | `release` |
| `MONGODB_URI` | URI подключения к MongoDB | `mongodb://localhost:27017` |
| `MONGODB_DATABASE` | База данных MongoDB | `data_pipeline` |
| `CONFIG_FILE_PATH` | Путь к файловому fallback | `./config/pipeline-config.json` |
| `LOG_LEVEL` | Уровень логирования | `info` |
| `LOG_FORMAT` | Формат логов (json/console) | `json` |

### Конфигурация пайплайна (MongoDB)

```json
{
  "mqtt": {
    "broker": "emqx",
    "port": 1883,
    "client_id": "data-pipeline-service",
    "topics": ["sensors/#", "devices/#"],
    "qos": 1,
    "clean_start": true,
    "keep_alive": 60,
    "use_tls": false
  },
  "questdb": {
    "host": "questdb",
    "ilp_port": 9009,
    "http_port": 9000,
    "table_name": "sensor_data",
    "flush_interval": 1000,
    "batch_size": 1000
  },
  "pipeline": {
    "buffer_size": 10000,
    "workers": 4,
    "retry_attempts": 3,
    "retry_delay": 1000,
    "message_format": "json",
    "timestamp_field": "timestamp",
    "symbol_field": "device_id",
    "field_mappings": [
      {"source": "device_id", "target": "device_id", "type": "symbol", "required": true},
      {"source": "value", "target": "value", "type": "double", "required": true},
      {"source": "timestamp", "target": "ts", "type": "timestamp", "required": true}
    ]
  }
}
```

## REST API

### Endpoints

| Метод | Путь | Описание |
|-------|------|----------|
| `GET` | `/health` | Health check |
| `GET` | `/ready` | Readiness probe |
| `GET` | `/live` | Liveness probe |
| `GET` | `/api/v1/config` | Получить текущую конфигурацию |
| `PUT` | `/api/v1/config` | Обновить всю конфигурацию |
| `PATCH` | `/api/v1/config` | Частично обновить конфигурацию |
| `GET` | `/api/v1/status` | Получить статус сервиса |
| `POST` | `/api/v1/reload` | Перезагрузить конфигурацию |

### Примеры

```bash
# Получить статус
curl http://localhost:8080/api/v1/status | jq .

# Получить конфигурацию
curl http://localhost:8080/api/v1/config | jq .

# Обновить MQTT топики
curl -X PATCH http://localhost:8080/api/v1/config \
  -H "Content-Type: application/json" \
  -d '{
    "mqtt": {
      "broker": "emqx",
      "port": 1883,
      "topics": ["sensors/#", "devices/#", "telemetry/#"]
    }
  }'

# Перезагрузить сервис
curl -X POST http://localhost:8080/api/v1/reload
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

Пример маппинга:

```json
{
  "field_mappings": [
    {"source": "device_id", "target": "device_id", "type": "symbol", "required": true},
    {"source": "temperature", "target": "temp", "type": "double", "required": true},
    {"source": "humidity", "target": "humidity", "type": "double", "required": false, "default_val": "0"},
    {"source": "ts", "target": "timestamp", "type": "timestamp", "required": true}
  ]
}
```

## Формат входящих сообщений

Ожидаемый JSON-формат:

```json
{
  "device_id": "sensor-001",
  "sensor_type": "temperature",
  "value": 23.5,
  "unit": "celsius",
  "timestamp": 1704067200000
}
```

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
    "messages_received": 150000,
    "errors": 0
  },
  "questdb": {
    "connected": true,
    "rows_written": 149500,
    "write_errors": 500,
    "pending_rows": 0
  },
  "pipeline": {
    "running": true,
    "buffer_usage": 5,
    "processed_messages": 149500,
    "failed_messages": 500,
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

# Запустить линтер
make lint

# Собрать Docker образ
make docker-build
```

## Хранение конфигурации

Конфигурация хранится в **MongoDB** с автоматическим **файловым fallback**:

1. При старте сервис пытается подключиться к MongoDB
2. Если MongoDB недоступен — используется файловый storage
3. При восстановлении MongoDB — данные синхронизируются
4. Каждое обновление сохраняется в обоих местах

Это гарантирует, что конфигурация не потеряется при сбоях MongoDB.

## Что менять

### Добавить новый MQTT топик
1. `PATCH /api/v1/config` с обновленным списком `topics`
2. Или отредактировать в MongoDB напрямую

### Изменить таблицу QuestDB
1. `PATCH /api/v1/config` с новым `table_name`
2. Таблица создастся автоматически при первой записи

### Добавить новое поле
1. Добавить маппинг в `field_mappings` через API
2. Поле появится в QuestDB автоматически

### Изменить параметры подключения
Отредактировать `configs/config.yaml` или переменные окружения, перезапустить сервис.
