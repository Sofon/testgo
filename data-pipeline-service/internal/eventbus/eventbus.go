package eventbus

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/sofon/data-pipeline-service/internal/models"
	"github.com/sofon/data-pipeline-service/pkg/logger"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"go.uber.org/zap"
)

// EventBus — клиент для публикации событий в EMQX (отдельная шина событий)
type EventBus struct {
	config    *models.EventBusConfig
	client    mqtt.Client
	mu        sync.RWMutex
	connected bool
}

// NewEventBus создаёт новый EventBus клиент
func NewEventBus(config *models.EventBusConfig) (*EventBus, error) {
	if config == nil {
		return nil, fmt.Errorf("требуется конфигурация EventBus")
	}

	if !config.Enabled {
		logger.Info("EventBus отключён в конфигурации")
		return &EventBus{config: config}, nil
	}

	return &EventBus{
		config: config,
	}, nil
}

// Connect подключается к MQTT брокеру для событий
func (e *EventBus) Connect(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if !e.config.Enabled {
		return nil
	}

	broker := fmt.Sprintf("tcp://%s:%d", e.config.Broker, e.config.Port)
	if e.config.UseTLS {
		broker = fmt.Sprintf("ssl://%s:%d", e.config.Broker, e.config.Port)
	}

	opts := mqtt.NewClientOptions().
		AddBroker(broker).
		SetClientID(e.config.ClientID).
		SetCleanSession(true).
		SetAutoReconnect(true).
		SetConnectRetry(true).
		SetConnectRetryInterval(5 * time.Second).
		SetConnectionLostHandler(e.onConnectionLost).
		SetOnConnectHandler(e.onConnect)

	if e.config.Username != "" {
		opts.SetUsername(e.config.Username)
	}
	if e.config.Password != "" {
		opts.SetPassword(e.config.Password)
	}

	e.client = mqtt.NewClient(opts)

	// Подключаемся
	token := e.client.Connect()
	if token.WaitTimeout(10 * time.Second) {
		if token.Error() != nil {
			return fmt.Errorf("ошибка подключения к EventBus: %w", token.Error())
		}
	} else {
		return fmt.Errorf("таймаут подключения к EventBus")
	}

	e.connected = true
	logger.Info("подключено к EventBus",
		zap.String("broker", broker),
		zap.String("topic_prefix", e.config.TopicPrefix),
	)

	return nil
}

func (e *EventBus) onConnect(client mqtt.Client) {
	e.mu.Lock()
	e.connected = true
	e.mu.Unlock()
	logger.Info("EventBus переподключён")
}

func (e *EventBus) onConnectionLost(client mqtt.Client, err error) {
	e.mu.Lock()
	e.connected = false
	e.mu.Unlock()
	logger.Warn("EventBus соединение потеряно", zap.Error(err))
}

// Disconnect отключается от брокера
func (e *EventBus) Disconnect() {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.client != nil && e.client.IsConnected() {
		e.client.Disconnect(1000)
		e.connected = false
		logger.Info("отключено от EventBus")
	}
}

// IsConnected возвращает статус подключения
func (e *EventBus) IsConnected() bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.connected && e.config.Enabled
}

// Publish публикует событие в шину событий
func (e *EventBus) Publish(ctx context.Context, event *models.Event) error {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if !e.config.Enabled {
		return nil // EventBus отключён
	}

	if !e.connected || e.client == nil {
		return fmt.Errorf("нет подключения к EventBus")
	}

	// Формируем топик: prefix/event_type
	topic := fmt.Sprintf("%s/%s", e.config.TopicPrefix, event.Type)

	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("ошибка сериализации события: %w", err)
	}

	token := e.client.Publish(topic, byte(e.config.QoS), false, payload)
	if token.WaitTimeout(5 * time.Second) {
		if token.Error() != nil {
			return fmt.Errorf("ошибка публикации события: %w", token.Error())
		}
	} else {
		return fmt.Errorf("таймаут публикации события")
	}

	logger.Debug("событие опубликовано",
		zap.String("topic", topic),
		zap.String("type", string(event.Type)),
	)

	return nil
}

// PublishConfigUpdated публикует событие успешного обновления конфига
func (e *EventBus) PublishConfigUpdated(ctx context.Context, config *models.ServiceConfig, updatedBy string) error {
	event := models.NewConfigUpdatedEvent(config, updatedBy)
	return e.Publish(ctx, event)
}

// PublishConfigUpdateFailed публикует событие неудачного обновления конфига
func (e *EventBus) PublishConfigUpdateFailed(ctx context.Context, reason, errorCode, updatedBy string) error {
	event := models.NewConfigUpdateFailedEvent(reason, errorCode, updatedBy)
	return e.Publish(ctx, event)
}

// PublishStatusChanged публикует событие изменения статуса
func (e *EventBus) PublishStatusChanged(ctx context.Context, oldStatus, newStatus, reason, details string) error {
	event := models.NewStatusChangedEvent(oldStatus, newStatus, reason, details)
	return e.Publish(ctx, event)
}

// GetStatus возвращает статус EventBus
func (e *EventBus) GetStatus() models.EventBusStatus {
	e.mu.RLock()
	defer e.mu.RUnlock()

	status := models.EventBusStatus{
		Enabled:   e.config.Enabled,
		Connected: e.connected,
	}

	if e.config.Enabled {
		status.Broker = fmt.Sprintf("%s:%d", e.config.Broker, e.config.Port)
		status.TopicPrefix = e.config.TopicPrefix
	}

	return status
}
