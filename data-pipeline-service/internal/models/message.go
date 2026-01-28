package models

import "time"

// IncomingMessage represents a message received from MQTT
type IncomingMessage struct {
	Topic       string                 `json:"topic"`
	Payload     []byte                 `json:"-"`
	ParsedData  map[string]interface{} `json:"data,omitempty"`
	ReceivedAt  time.Time              `json:"received_at"`
	QoS         byte                   `json:"qos"`
	Retained    bool                   `json:"retained"`
	MessageID   uint16                 `json:"message_id"`
}

// QuestDBRow represents a row to be written to QuestDB
type QuestDBRow struct {
	TableName  string
	Symbol     string
	Columns    map[string]interface{}
	Timestamp  time.Time
}
