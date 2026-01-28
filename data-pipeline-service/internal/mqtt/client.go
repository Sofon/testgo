package mqtt

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	pahomqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/sofon/data-pipeline-service/internal/models"
	"github.com/sofon/data-pipeline-service/pkg/logger"
	"go.uber.org/zap"
)

// MessageHandler — функция обработки входящих MQTT сообщений
type MessageHandler func(msg *models.IncomingMessage)

// Client — обёртка над MQTT клиентом с дополнительной функциональностью
type Client struct {
	client           pahomqtt.Client
	config           *models.MQTTConfig
	handler          MessageHandler
	mu               sync.RWMutex
	connected        atomic.Bool
	messagesReceived atomic.Int64
	errors           atomic.Int64
	lastMessageAt    atomic.Value // time.Time
	subscribedTopics []string
}

// NewClient создаёт новый MQTT клиент
func NewClient(config *models.MQTTConfig, handler MessageHandler) (*Client, error) {
	if config == nil {
		return nil, fmt.Errorf("требуется конфигурация MQTT")
	}
	if handler == nil {
		return nil, fmt.Errorf("требуется обработчик сообщений")
	}

	c := &Client{
		config:  config,
		handler: handler,
	}

	opts, err := c.buildClientOptions()
	if err != nil {
		return nil, err
	}

	c.client = pahomqtt.NewClient(opts)

	return c, nil
}

func (c *Client) buildClientOptions() (*pahomqtt.ClientOptions, error) {
	opts := pahomqtt.NewClientOptions()

	// Формируем URL брокера
	scheme := "tcp"
	if c.config.UseTLS {
		scheme = "ssl"
	}
	brokerURL := fmt.Sprintf("%s://%s:%d", scheme, c.config.Broker, c.config.Port)
	opts.AddBroker(brokerURL)

	opts.SetClientID(c.config.ClientID)
	opts.SetCleanSession(c.config.CleanStart)
	opts.SetAutoReconnect(true)
	opts.SetConnectRetry(true)
	opts.SetConnectRetryInterval(5 * time.Second)
	opts.SetMaxReconnectInterval(60 * time.Second)

	if c.config.KeepAlive > 0 {
		opts.SetKeepAlive(time.Duration(c.config.KeepAlive) * time.Second)
	}

	if c.config.Username != "" {
		opts.SetUsername(c.config.Username)
		opts.SetPassword(c.config.Password)
	}

	// Настройка TLS
	if c.config.UseTLS {
		tlsConfig, err := c.buildTLSConfig()
		if err != nil {
			return nil, fmt.Errorf("ошибка создания TLS конфига: %w", err)
		}
		opts.SetTLSConfig(tlsConfig)
	}

	// Колбэки
	opts.SetOnConnectHandler(c.onConnect)
	opts.SetConnectionLostHandler(c.onConnectionLost)
	opts.SetReconnectingHandler(c.onReconnecting)

	// Обработчик сообщений по умолчанию
	opts.SetDefaultPublishHandler(c.onMessage)

	return opts, nil
}

func (c *Client) buildTLSConfig() (*tls.Config, error) {
	tlsConfig := &tls.Config{
		MinVersion: tls.VersionTLS12,
	}

	// Загружаем CA сертификат если указан
	if c.config.TLSCAPath != "" {
		caCert, err := os.ReadFile(c.config.TLSCAPath)
		if err != nil {
			return nil, fmt.Errorf("ошибка чтения CA сертификата: %w", err)
		}
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)
		tlsConfig.RootCAs = caCertPool
	}

	// Загружаем клиентский сертификат если указан
	if c.config.TLSCertPath != "" && c.config.TLSKeyPath != "" {
		cert, err := tls.LoadX509KeyPair(c.config.TLSCertPath, c.config.TLSKeyPath)
		if err != nil {
			return nil, fmt.Errorf("ошибка загрузки клиентского сертификата: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	return tlsConfig, nil
}

func (c *Client) onConnect(client pahomqtt.Client) {
	c.connected.Store(true)
	logger.Info("подключено к MQTT брокеру",
		zap.String("broker", c.config.Broker),
		zap.Int("port", c.config.Port),
	)

	// Подписываемся на топики
	c.subscribeToTopics()
}

func (c *Client) onConnectionLost(client pahomqtt.Client, err error) {
	c.connected.Store(false)
	c.errors.Add(1)
	logger.Error("потеряно соединение с MQTT",
		zap.Error(err),
		zap.String("broker", c.config.Broker),
	)
}

func (c *Client) onReconnecting(client pahomqtt.Client, opts *pahomqtt.ClientOptions) {
	logger.Info("переподключение к MQTT брокеру",
		zap.String("broker", c.config.Broker),
	)
}

func (c *Client) onMessage(client pahomqtt.Client, msg pahomqtt.Message) {
	c.messagesReceived.Add(1)
	c.lastMessageAt.Store(time.Now())

	incoming := &models.IncomingMessage{
		Topic:      msg.Topic(),
		Payload:    msg.Payload(),
		ReceivedAt: time.Now(),
		QoS:        msg.Qos(),
		Retained:   msg.Retained(),
		MessageID:  msg.MessageID(),
	}

	// Пробуем распарсить как JSON
	var data map[string]interface{}
	if err := json.Unmarshal(msg.Payload(), &data); err == nil {
		incoming.ParsedData = data
	}

	// Вызываем обработчик
	c.handler(incoming)
}

func (c *Client) subscribeToTopics() {
	c.mu.Lock()
	defer c.mu.Unlock()

	filters := make(map[string]byte)
	for _, topic := range c.config.Topics {
		filters[topic] = byte(c.config.QoS)
	}

	token := c.client.SubscribeMultiple(filters, nil)
	if token.Wait() && token.Error() != nil {
		logger.Error("ошибка подписки на топики",
			zap.Error(token.Error()),
			zap.Strings("topics", c.config.Topics),
		)
		c.errors.Add(1)
		return
	}

	c.subscribedTopics = c.config.Topics
	logger.Info("подписка на топики выполнена",
		zap.Strings("topics", c.config.Topics),
		zap.Int("qos", c.config.QoS),
	)
}

// Connect устанавливает подключение к MQTT брокеру
func (c *Client) Connect(ctx context.Context) error {
	token := c.client.Connect()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-token.Done():
		if token.Error() != nil {
			return fmt.Errorf("ошибка подключения к MQTT брокеру: %w", token.Error())
		}
	}

	return nil
}

// Disconnect закрывает соединение с MQTT
func (c *Client) Disconnect() {
	if c.client != nil && c.client.IsConnected() {
		c.client.Disconnect(1000) // Ждём 1 секунду для graceful отключения
		c.connected.Store(false)
		logger.Info("отключено от MQTT брокера")
	}
}

// IsConnected возвращает статус подключения
func (c *Client) IsConnected() bool {
	return c.connected.Load()
}

// GetStatus возвращает статус MQTT клиента
func (c *Client) GetStatus() models.MQTTStatus {
	c.mu.RLock()
	defer c.mu.RUnlock()

	status := models.MQTTStatus{
		Connected:        c.connected.Load(),
		Broker:           fmt.Sprintf("%s:%d", c.config.Broker, c.config.Port),
		SubscribedTopics: c.subscribedTopics,
		MessagesReceived: c.messagesReceived.Load(),
		Errors:           c.errors.Load(),
	}

	if lastMsg := c.lastMessageAt.Load(); lastMsg != nil {
		status.LastMessageAt = lastMsg.(time.Time)
	}

	return status
}

// UpdateConfig обновляет конфигурацию MQTT.
// Отключается и переподключается с новыми настройками.
func (c *Client) UpdateConfig(ctx context.Context, config *models.MQTTConfig) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Отключаем текущее соединение
	if c.client != nil && c.client.IsConnected() {
		c.client.Disconnect(1000)
	}

	c.config = config

	opts, err := c.buildClientOptions()
	if err != nil {
		return err
	}

	c.client = pahomqtt.NewClient(opts)

	// Переподключаемся
	token := c.client.Connect()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-token.Done():
		if token.Error() != nil {
			return fmt.Errorf("ошибка переподключения: %w", token.Error())
		}
	}

	return nil
}

// Unsubscribe отписывается от топиков
func (c *Client) Unsubscribe(topics []string) error {
	token := c.client.Unsubscribe(topics...)
	if token.Wait() && token.Error() != nil {
		return token.Error()
	}
	return nil
}

// ResetStats сбрасывает счётчики сообщений
func (c *Client) ResetStats() {
	c.messagesReceived.Store(0)
	c.errors.Store(0)
}
