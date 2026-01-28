package models

import "time"

// EventType — тип события
type EventType string

const (
	// События конфигурации
	EventConfigUpdated      EventType = "config.updated"       // конфиг успешно обновлён
	EventConfigUpdateFailed EventType = "config.update_failed" // ошибка обновления конфига

	// События статуса
	EventStatusChanged EventType = "status.changed" // статус сервиса изменился
)

// Event — базовая структура события
type Event struct {
	Type      EventType   `json:"type"`       // тип события
	Timestamp time.Time   `json:"timestamp"`  // время события
	Source    string      `json:"source"`     // источник события (имя сервиса)
	Data      interface{} `json:"data"`       // данные события
}

// ConfigUpdatedEvent — данные события успешного обновления конфига
type ConfigUpdatedEvent struct {
	Version       int               `json:"version"`        // новая версия конфига
	BatchSize     int               `json:"batch_size"`     // новый batch_size
	FlushInterval int               `json:"flush_interval"` // новый flush_interval
	WriteTimeout  int               `json:"write_timeout"`  // новый write_timeout
	StreamMapping map[string]string `json:"stream_mapping"` // новый маппинг
	UpdatedBy     string            `json:"updated_by"`     // кто обновил (API, reload и т.д.)
}

// ConfigUpdateFailedEvent — данные события неудачного обновления конфига
type ConfigUpdateFailedEvent struct {
	Reason    string `json:"reason"`     // причина ошибки
	ErrorCode string `json:"error_code"` // код ошибки
	UpdatedBy string `json:"updated_by"` // кто пытался обновить
}

// StatusChangedEvent — данные события изменения статуса
type StatusChangedEvent struct {
	OldStatus string `json:"old_status"` // предыдущий статус
	NewStatus string `json:"new_status"` // новый статус
	Reason    string `json:"reason"`     // причина изменения
	Details   string `json:"details"`    // дополнительные детали
}

// NewEvent создаёт новое событие
func NewEvent(eventType EventType, source string, data interface{}) *Event {
	return &Event{
		Type:      eventType,
		Timestamp: time.Now(),
		Source:    source,
		Data:      data,
	}
}

// NewConfigUpdatedEvent создаёт событие успешного обновления конфига
func NewConfigUpdatedEvent(config *ServiceConfig, updatedBy string) *Event {
	return NewEvent(EventConfigUpdated, "data-pipeline-service", &ConfigUpdatedEvent{
		Version:       config.Version,
		BatchSize:     config.BatchSize,
		FlushInterval: config.FlushInterval,
		WriteTimeout:  config.WriteTimeout,
		StreamMapping: config.StreamMapping,
		UpdatedBy:     updatedBy,
	})
}

// NewConfigUpdateFailedEvent создаёт событие неудачного обновления конфига
func NewConfigUpdateFailedEvent(reason, errorCode, updatedBy string) *Event {
	return NewEvent(EventConfigUpdateFailed, "data-pipeline-service", &ConfigUpdateFailedEvent{
		Reason:    reason,
		ErrorCode: errorCode,
		UpdatedBy: updatedBy,
	})
}

// NewStatusChangedEvent создаёт событие изменения статуса
func NewStatusChangedEvent(oldStatus, newStatus, reason, details string) *Event {
	return NewEvent(EventStatusChanged, "data-pipeline-service", &StatusChangedEvent{
		OldStatus: oldStatus,
		NewStatus: newStatus,
		Reason:    reason,
		Details:   details,
	})
}
