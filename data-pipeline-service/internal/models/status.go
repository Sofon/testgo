package models

import "time"

// ServiceStatus — текущий статус сервиса
type ServiceStatus struct {
	Status      string            `json:"status"` // running, stopped, error
	StartedAt   time.Time         `json:"started_at"`
	Uptime      string            `json:"uptime"`
	MQTT        MQTTStatus        `json:"mqtt"`
	QuestDB     QuestDBStatus     `json:"questdb"`
	MongoDB     MongoDBStatus     `json:"mongodb"`
	Pipeline    PipelineStatus    `json:"pipeline"`
	Version     string            `json:"version"`
	BuildTime   string            `json:"build_time,omitempty"`
}

// MQTTStatus — статус подключения к MQTT
type MQTTStatus struct {
	Connected        bool      `json:"connected"`
	Broker           string    `json:"broker"`
	SubscribedTopics []string  `json:"subscribed_topics"`
	LastMessageAt    time.Time `json:"last_message_at,omitempty"`
	MessagesReceived int64     `json:"messages_received"`
	Errors           int64     `json:"errors"`
}

// QuestDBStatus — статус подключения к QuestDB
type QuestDBStatus struct {
	Connected       bool      `json:"connected"`
	Host            string    `json:"host"`
	TableName       string    `json:"table_name"`
	LastWriteAt     time.Time `json:"last_write_at,omitempty"`
	RowsWritten     int64     `json:"rows_written"`
	WriteErrors     int64     `json:"write_errors"`
	PendingRows     int64     `json:"pending_rows"`
}

// MongoDBStatus — статус подключения к MongoDB
type MongoDBStatus struct {
	Connected bool   `json:"connected"`
	Host      string `json:"host"`
	Database  string `json:"database"`
}

// PipelineStatus — статус пайплайна обработки данных
type PipelineStatus struct {
	Running           bool    `json:"running"`
	BufferUsage       int     `json:"buffer_usage"`       // процент заполнения
	BufferCapacity    int     `json:"buffer_capacity"`
	ProcessedMessages int64   `json:"processed_messages"`
	FailedMessages    int64   `json:"failed_messages"`
	AvgLatencyMs      float64 `json:"avg_latency_ms"`
	ActiveWorkers     int     `json:"active_workers"`
}

// HealthCheck — ответ на health-проверку
type HealthCheck struct {
	Status    string    `json:"status"`
	Timestamp time.Time `json:"timestamp"`
}
