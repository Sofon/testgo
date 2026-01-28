package models

import "time"

// ServiceStatus represents the current status of the service
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

// MQTTStatus represents MQTT connection status
type MQTTStatus struct {
	Connected        bool      `json:"connected"`
	Broker           string    `json:"broker"`
	SubscribedTopics []string  `json:"subscribed_topics"`
	LastMessageAt    time.Time `json:"last_message_at,omitempty"`
	MessagesReceived int64     `json:"messages_received"`
	Errors           int64     `json:"errors"`
}

// QuestDBStatus represents QuestDB connection status
type QuestDBStatus struct {
	Connected       bool      `json:"connected"`
	Host            string    `json:"host"`
	TableName       string    `json:"table_name"`
	LastWriteAt     time.Time `json:"last_write_at,omitempty"`
	RowsWritten     int64     `json:"rows_written"`
	WriteErrors     int64     `json:"write_errors"`
	PendingRows     int64     `json:"pending_rows"`
}

// MongoDBStatus represents MongoDB connection status
type MongoDBStatus struct {
	Connected bool   `json:"connected"`
	Host      string `json:"host"`
	Database  string `json:"database"`
}

// PipelineStatus represents data pipeline status
type PipelineStatus struct {
	Running           bool    `json:"running"`
	BufferUsage       int     `json:"buffer_usage"`       // percentage
	BufferCapacity    int     `json:"buffer_capacity"`
	ProcessedMessages int64   `json:"processed_messages"`
	FailedMessages    int64   `json:"failed_messages"`
	AvgLatencyMs      float64 `json:"avg_latency_ms"`
	ActiveWorkers     int     `json:"active_workers"`
}

// HealthCheck represents a simple health check response
type HealthCheck struct {
	Status    string    `json:"status"`
	Timestamp time.Time `json:"timestamp"`
}
