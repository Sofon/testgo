#!/bin/bash

# API Examples for Data Pipeline Service

BASE_URL="${API_URL:-http://localhost:8080}"

echo "=== Data Pipeline Service API Examples ==="
echo ""

# Health check
echo "1. Health Check (GET /health)"
echo "curl ${BASE_URL}/health"
curl -s "${BASE_URL}/health" | jq .
echo ""

# Readiness check
echo "2. Readiness Check (GET /ready)"
echo "curl ${BASE_URL}/ready"
curl -s "${BASE_URL}/ready" | jq .
echo ""

# Get status
echo "3. Get Service Status (GET /api/v1/status)"
echo "curl ${BASE_URL}/api/v1/status"
curl -s "${BASE_URL}/api/v1/status" | jq .
echo ""

# Get config
echo "4. Get Configuration (GET /api/v1/config)"
echo "curl ${BASE_URL}/api/v1/config"
curl -s "${BASE_URL}/api/v1/config" | jq .
echo ""

# Update config (full)
echo "5. Update Configuration (PUT /api/v1/config)"
echo 'curl -X PUT ${BASE_URL}/api/v1/config -H "Content-Type: application/json" -d "{...}"'
cat << 'EOF'
Example body:
{
  "mqtt": {
    "broker": "emqx",
    "port": 1883,
    "client_id": "data-pipeline-service",
    "topics": ["sensors/#"],
    "qos": 1
  },
  "questdb": {
    "host": "questdb",
    "ilp_port": 9009,
    "table_name": "sensor_data"
  },
  "pipeline": {
    "buffer_size": 10000,
    "workers": 4
  }
}
EOF
echo ""

# Patch config (partial)
echo "6. Patch Configuration (PATCH /api/v1/config)"
echo 'curl -X PATCH ${BASE_URL}/api/v1/config -H "Content-Type: application/json" -d "{...}"'
cat << 'EOF'
Example: Update only MQTT topics
{
  "mqtt": {
    "broker": "emqx",
    "port": 1883,
    "topics": ["sensors/#", "devices/#", "telemetry/#"]
  }
}
EOF
echo ""

# Reload service
echo "7. Reload Service (POST /api/v1/reload)"
echo "curl -X POST ${BASE_URL}/api/v1/reload"
echo ""

echo "=== End of Examples ==="
