#!/bin/bash

# Initialize default pipeline configuration via REST API

BASE_URL="${API_URL:-http://localhost:8080}"

echo "Initializing pipeline configuration..."

# Create default configuration
curl -X PUT "${BASE_URL}/api/v1/config" \
  -H "Content-Type: application/json" \
  -d '{
    "mqtt": {
      "broker": "emqx",
      "port": 1883,
      "client_id": "data-pipeline-service",
      "topics": ["sensors/#", "devices/#"],
      "qos": 1,
      "clean_start": true,
      "keep_alive": 60
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
        {"source": "sensor_type", "target": "sensor_type", "type": "symbol", "required": false},
        {"source": "value", "target": "value", "type": "double", "required": true},
        {"source": "unit", "target": "unit", "type": "string", "required": false},
        {"source": "timestamp", "target": "ts", "type": "timestamp", "required": true}
      ]
    }
  }'

echo ""
echo "Configuration initialized successfully!"
echo ""
echo "Checking service status..."
curl -s "${BASE_URL}/api/v1/status" | jq .
