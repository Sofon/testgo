package service

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sofon/data-pipeline-service/internal/models"
	"github.com/sofon/data-pipeline-service/internal/mqtt"
	"github.com/sofon/data-pipeline-service/internal/questdb"
	"github.com/sofon/data-pipeline-service/internal/storage"
	"github.com/sofon/data-pipeline-service/pkg/logger"
	"go.uber.org/zap"
)

// Информация о версии (устанавливается при сборке)
var (
	Version   = "dev"
	BuildTime = "unknown"
)

// Service — основной сервис обработки данных
type Service struct {
	storage       *storage.HybridStorage
	mqttClient    *mqtt.Client
	questdbWriter *questdb.Writer
	config        *models.Config

	mu              sync.RWMutex
	running         atomic.Bool
	startedAt       time.Time
	processedMsgs   atomic.Int64
	failedMsgs      atomic.Int64
	bufferChan      chan *models.IncomingMessage
	stopChan        chan struct{}
	workers         int
	activeWorkers   atomic.Int32
}

// NewService создаёт новый сервис обработки данных
func NewService(store *storage.HybridStorage) *Service {
	return &Service{
		storage:  store,
		stopChan: make(chan struct{}),
	}
}

// Start инициализирует и запускает сервис
func (s *Service) Start(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Загружаем конфигурацию
	config, err := s.storage.GetConfig(ctx)
	if err != nil {
		if err == storage.ErrConfigNotFound {
			// Создаём конфигурацию по умолчанию
			config = models.DefaultConfig()
			if err := s.storage.SaveConfig(ctx, config); err != nil {
				return fmt.Errorf("ошибка сохранения конфига по умолчанию: %w", err)
			}
			logger.Info("создана конфигурация по умолчанию")
		} else {
			return fmt.Errorf("ошибка загрузки конфига: %w", err)
		}
	}
	s.config = config

	// Инициализируем буфер
	bufferSize := config.Pipeline.BufferSize
	if bufferSize <= 0 {
		bufferSize = 10000
	}
	s.bufferChan = make(chan *models.IncomingMessage, bufferSize)

	// Инициализируем воркеры
	s.workers = config.Pipeline.Workers
	if s.workers <= 0 {
		s.workers = 4
	}

	// Инициализируем QuestDB writer
	s.questdbWriter, err = questdb.NewWriter(&config.QuestDB)
	if err != nil {
		return fmt.Errorf("ошибка создания QuestDB writer: %w", err)
	}

	if err := s.questdbWriter.Connect(ctx); err != nil {
		return fmt.Errorf("ошибка подключения к QuestDB: %w", err)
	}

	// Инициализируем MQTT клиент
	s.mqttClient, err = mqtt.NewClient(&config.MQTT, s.handleMessage)
	if err != nil {
		return fmt.Errorf("ошибка создания MQTT клиента: %w", err)
	}

	if err := s.mqttClient.Connect(ctx); err != nil {
		return fmt.Errorf("ошибка подключения к MQTT: %w", err)
	}

	// Запускаем горутины воркеров
	s.startWorkers()

	s.running.Store(true)
	s.startedAt = time.Now()

	logger.Info("сервис запущен",
		zap.Int("workers", s.workers),
		zap.Int("buffer_size", bufferSize),
	)

	return nil
}

func (s *Service) startWorkers() {
	for i := 0; i < s.workers; i++ {
		go s.worker(i)
	}
}

func (s *Service) worker(id int) {
	s.activeWorkers.Add(1)
	defer s.activeWorkers.Add(-1)

	logger.Debug("воркер запущен", zap.Int("worker_id", id))

	for {
		select {
		case msg := <-s.bufferChan:
			s.processMessage(msg)
		case <-s.stopChan:
			logger.Debug("воркер остановлен", zap.Int("worker_id", id))
			return
		}
	}
}

func (s *Service) handleMessage(msg *models.IncomingMessage) {
	select {
	case s.bufferChan <- msg:
		// Сообщение успешно добавлено в очередь
	default:
		// Буфер полон, сбрасываем сообщение
		s.failedMsgs.Add(1)
		logger.Warn("буфер сообщений полон, сообщение сброшено",
			zap.String("topic", msg.Topic),
		)
	}
}

func (s *Service) processMessage(msg *models.IncomingMessage) {
	s.mu.RLock()
	config := s.config
	s.mu.RUnlock()

	// Парсим сообщение если ещё не распарсено
	if msg.ParsedData == nil {
		var data map[string]interface{}
		if err := json.Unmarshal(msg.Payload, &data); err != nil {
			s.failedMsgs.Add(1)
			logger.Warn("ошибка парсинга сообщения",
				zap.String("topic", msg.Topic),
				zap.Error(err),
			)
			return
		}
		msg.ParsedData = data
	}

	// Записываем в QuestDB
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := s.questdbWriter.WriteRow(
		ctx,
		msg.ParsedData,
		config.Pipeline.FieldMappings,
		config.QuestDB.TableName,
		config.Pipeline.TimestampField,
		config.Pipeline.SymbolField,
	)

	if err != nil {
		s.failedMsgs.Add(1)
		logger.Warn("ошибка записи в QuestDB",
			zap.String("topic", msg.Topic),
			zap.Error(err),
		)

		// Логика повторных попыток
		for i := 0; i < config.Pipeline.RetryAttempts; i++ {
			time.Sleep(time.Duration(config.Pipeline.RetryDelay) * time.Millisecond)

			err = s.questdbWriter.WriteRow(
				ctx,
				msg.ParsedData,
				config.Pipeline.FieldMappings,
				config.QuestDB.TableName,
				config.Pipeline.TimestampField,
				config.Pipeline.SymbolField,
			)
			if err == nil {
				break
			}
		}

		if err != nil {
			return
		}
	}

	s.processedMsgs.Add(1)
}

// Stop выполняет graceful остановку сервиса
func (s *Service) Stop(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.running.Load() {
		return nil
	}

	logger.Info("остановка сервиса...")

	// Сигнализируем воркерам об остановке
	close(s.stopChan)

	// Отключаем MQTT
	if s.mqttClient != nil {
		s.mqttClient.Disconnect()
	}

	// Закрываем соединение с QuestDB
	if s.questdbWriter != nil {
		if err := s.questdbWriter.Close(ctx); err != nil {
			logger.Warn("ошибка закрытия соединения с QuestDB", zap.Error(err))
		}
	}

	s.running.Store(false)
	logger.Info("сервис остановлен")

	return nil
}

// GetConfig возвращает текущую конфигурацию
func (s *Service) GetConfig() (*models.Config, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.config != nil {
		return s.config, nil
	}

	return s.storage.GetConfig(context.Background())
}

// UpdateConfig обновляет конфигурацию сервиса
func (s *Service) UpdateConfig(update *models.ConfigUpdate) (*models.Config, error) {
	ctx := context.Background()

	config, err := s.storage.UpdateConfig(ctx, update)
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	s.config = config
	s.mu.Unlock()

	logger.Info("конфиг обновлён", zap.Int("version", config.Version))

	return config, nil
}

// GetStatus возвращает текущий статус сервиса
func (s *Service) GetStatus() *models.ServiceStatus {
	s.mu.RLock()
	config := s.config
	s.mu.RUnlock()

	status := &models.ServiceStatus{
		Status:    "stopped",
		StartedAt: s.startedAt,
		Version:   Version,
		BuildTime: BuildTime,
	}

	if s.running.Load() {
		status.Status = "running"
		status.Uptime = time.Since(s.startedAt).String()
	}

	// Статус MQTT
	if s.mqttClient != nil {
		status.MQTT = s.mqttClient.GetStatus()
	}

	// Статус QuestDB
	if s.questdbWriter != nil {
		status.QuestDB = s.questdbWriter.GetStatus()
	}

	// Статус MongoDB
	status.MongoDB = models.MongoDBStatus{
		Connected: s.storage.IsMongoConnected(),
	}
	host, db := s.storage.GetMongoInfo()
	status.MongoDB.Host = host
	status.MongoDB.Database = db

	// Статус пайплайна
	bufferCap := 0
	if config != nil {
		bufferCap = config.Pipeline.BufferSize
	}
	bufferLen := len(s.bufferChan)
	bufferUsage := 0
	if bufferCap > 0 {
		bufferUsage = (bufferLen * 100) / bufferCap
	}

	status.Pipeline = models.PipelineStatus{
		Running:           s.running.Load(),
		BufferUsage:       bufferUsage,
		BufferCapacity:    bufferCap,
		ProcessedMessages: s.processedMsgs.Load(),
		FailedMessages:    s.failedMsgs.Load(),
		ActiveWorkers:     int(s.activeWorkers.Load()),
	}

	return status
}

// IsHealthy возвращает здоровье сервиса
func (s *Service) IsHealthy() bool {
	if !s.running.Load() {
		return false
	}

	// Проверяем подключение к MQTT
	if s.mqttClient != nil && !s.mqttClient.IsConnected() {
		return false
	}

	// Проверяем подключение к QuestDB
	if s.questdbWriter != nil && !s.questdbWriter.IsConnected() {
		return false
	}

	return true
}

// Reload перезагружает конфигурацию сервиса и переподключается
func (s *Service) Reload() error {
	ctx := context.Background()

	// Загружаем свежий конфиг
	config, err := s.storage.GetConfig(ctx)
	if err != nil {
		return fmt.Errorf("ошибка загрузки конфига: %w", err)
	}

	s.mu.Lock()
	s.config = config
	s.mu.Unlock()

	// Обновляем MQTT клиент
	if s.mqttClient != nil {
		if err := s.mqttClient.UpdateConfig(ctx, &config.MQTT); err != nil {
			logger.Warn("ошибка перезагрузки конфига MQTT", zap.Error(err))
		}
	}

	logger.Info("сервис перезагружен", zap.Int("config_version", config.Version))

	return nil
}
