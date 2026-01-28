package models

import "time"

// IncomingMessage — входящее сообщение из MQTT
type IncomingMessage struct {
	Topic       string                 `json:"topic"`
	Payload     []byte                 `json:"-"`
	ParsedData  map[string]interface{} `json:"data,omitempty"`
	ReceivedAt  time.Time              `json:"received_at"`
	QoS         byte                   `json:"qos"`
	Retained    bool                   `json:"retained"`
	MessageID   uint16                 `json:"message_id"`
}

// QuestDBRow — строка для записи в QuestDB
type QuestDBRow struct {
	TableName  string
	Symbol     string
	Columns    map[string]interface{}
	Timestamp  time.Time
}
