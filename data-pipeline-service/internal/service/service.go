package service

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sofon/data-pipeline-service/internal/eventbus"
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
	eventBus      *eventbus.EventBus    // шина событий
	fileConfig    *models.FileConfig    // конфиг из файла (подключения)
	serviceConfig *models.ServiceConfig // конфиг из MongoDB (параметры обработки)

	mu              sync.RWMutex
	running         atomic.Bool
	lastStatus      string // для отслеживания изменения статуса
	startedAt       time.Time
	processedMsgs   atomic.Int64
	failedMsgs      atomic.Int64
	bufferChan      chan *models.IncomingMessage
	stopChan        chan struct{}
	workers         int
	activeWorkers   atomic.Int32
}

// NewService создаёт новый сервис обработки данных
func NewService(store *storage.HybridStorage, fileConfig *models.FileConfig) *Service {
	return &Service{
		storage:    store,
		fileConfig: fileConfig,
		stopChan:   make(chan struct{}),
		lastStatus: "stopped",
	}
}

// Start инициализирует и запускает сервис
func (s *Service) Start(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	oldStatus := s.lastStatus

	// Загружаем ServiceConfig из MongoDB
	serviceConfig, err := s.storage.GetServiceConfig(ctx)
	if err != nil {
		if err == storage.ErrConfigNotFound {
			// Создаём конфигурацию по умолчанию
			serviceConfig = models.DefaultServiceConfig()
			if err := s.storage.SaveServiceConfig(ctx, serviceConfig); err != nil {
				return fmt.Errorf("ошибка сохранения конфига по умолчанию: %w", err)
			}
			logger.Info("создана ServiceConfig по умолчанию")
		} else {
			return fmt.Errorf("ошибка загрузки ServiceConfig: %w", err)
		}
	}
	s.serviceConfig = serviceConfig

	// Инициализируем буфер
	bufferSize := s.fileConfig.Pipeline.BufferSize
	if bufferSize <= 0 {
		bufferSize = 10000
	}
	s.bufferChan = make(chan *models.IncomingMessage, bufferSize)

	// Инициализируем воркеры
	s.workers = s.fileConfig.Pipeline.Workers
	if s.workers <= 0 {
		s.workers = 4
	}

	// Инициализируем QuestDB writer с параметрами из ServiceConfig
	writerConfig := &questdb.WriterConfig{
		Connection:    &s.fileConfig.QuestDB,
		BatchSize:     serviceConfig.BatchSize,
		FlushInterval: serviceConfig.FlushInterval,
		WriteTimeout:  serviceConfig.WriteTimeout,
	}
	s.questdbWriter, err = questdb.NewWriter(writerConfig)
	if err != nil {
		return fmt.Errorf("ошибка создания QuestDB writer: %w", err)
	}

	if err := s.questdbWriter.Connect(ctx); err != nil {
		return fmt.Errorf("ошибка подключения к QuestDB: %w", err)
	}

	// Инициализируем MQTT клиент для данных
	s.mqttClient, err = mqtt.NewClient(&s.fileConfig.MQTT, s.handleMessage)
	if err != nil {
		return fmt.Errorf("ошибка создания MQTT клиента: %w", err)
	}

	if err := s.mqttClient.Connect(ctx); err != nil {
		return fmt.Errorf("ошибка подключения к MQTT: %w", err)
	}

	// Инициализируем EventBus (шина событий)
	s.eventBus, err = eventbus.NewEventBus(&s.fileConfig.EventBus)
	if err != nil {
		logger.Warn("не удалось создать EventBus", zap.Error(err))
	} else {
		if err := s.eventBus.Connect(ctx); err != nil {
			logger.Warn("не удалось подключиться к EventBus", zap.Error(err))
		}
	}

	// Запускаем горутины воркеров
	s.startWorkers()

	s.running.Store(true)
	s.startedAt = time.Now()
	s.lastStatus = "running"

	logger.Info("сервис запущен",
		zap.Int("workers", s.workers),
		zap.Int("buffer_size", bufferSize),
	)

	// Публикуем событие изменения статуса
	s.publishStatusChange(ctx, oldStatus, "running", "service_started", "сервис успешно запущен")

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
	serviceConfig := s.serviceConfig
	fileConfig := s.fileConfig
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

	// Определяем таблицу по маппингу topic -> table
	tableName := s.getTableForTopic(msg.Topic, serviceConfig.StreamMapping)
	if tableName == "" {
		s.failedMsgs.Add(1)
		logger.Warn("не найден маппинг для топика",
			zap.String("topic", msg.Topic),
		)
		return
	}

	// Записываем в QuestDB
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(serviceConfig.WriteTimeout)*time.Millisecond)
	defer cancel()

	err := s.questdbWriter.WriteRow(
		ctx,
		msg.ParsedData,
		fileConfig.Pipeline.FieldMappings,
		tableName,
		fileConfig.Pipeline.TimestampField,
		fileConfig.Pipeline.SymbolField,
	)

	if err != nil {
		s.failedMsgs.Add(1)
		logger.Warn("ошибка записи в QuestDB",
			zap.String("topic", msg.Topic),
			zap.Error(err),
		)

		// Логика повторных попыток
		for i := 0; i < fileConfig.Pipeline.RetryAttempts; i++ {
			time.Sleep(time.Duration(fileConfig.Pipeline.RetryDelay) * time.Millisecond)

			err = s.questdbWriter.WriteRow(
				ctx,
				msg.ParsedData,
				fileConfig.Pipeline.FieldMappings,
				tableName,
				fileConfig.Pipeline.TimestampField,
				fileConfig.Pipeline.SymbolField,
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

// getTableForTopic определяет таблицу QuestDB по топику используя маппинг
func (s *Service) getTableForTopic(topic string, mapping map[string]string) string {
	// Сначала точное совпадение
	if table, ok := mapping[topic]; ok {
		return table
	}

	// Проверяем wildcard совпадения (простой вариант с # в конце)
	for pattern, table := range mapping {
		if matchTopicPattern(pattern, topic) {
			return table
		}
	}

	return ""
}

// matchTopicPattern проверяет соответствие топика паттерну MQTT
func matchTopicPattern(pattern, topic string) bool {
	// Простая реализация: поддержка # в конце паттерна
	if len(pattern) > 0 && pattern[len(pattern)-1] == '#' {
		prefix := pattern[:len(pattern)-1]
		return len(topic) >= len(prefix) && topic[:len(prefix)] == prefix
	}
	return pattern == topic
}

// Stop выполняет graceful остановку сервиса
func (s *Service) Stop(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.running.Load() {
		return nil
	}

	oldStatus := s.lastStatus
	logger.Info("остановка сервиса...")

	// Сигнализируем воркерам об остановке
	close(s.stopChan)

	// Отключаем MQTT
	if s.mqttClient != nil {
		s.mqttClient.Disconnect()
	}

	// Отключаем EventBus
	if s.eventBus != nil {
		s.eventBus.Disconnect()
	}

	// Закрываем соединение с QuestDB
	if s.questdbWriter != nil {
		if err := s.questdbWriter.Close(ctx); err != nil {
			logger.Warn("ошибка закрытия соединения с QuestDB", zap.Error(err))
		}
	}

	s.running.Store(false)
	s.lastStatus = "stopped"
	logger.Info("сервис остановлен")

	// Публикуем событие изменения статуса (EventBus уже отключён, но попробуем)
	s.publishStatusChange(ctx, oldStatus, "stopped", "service_stopped", "сервис остановлен")

	return nil
}

// GetServiceConfig возвращает текущую ServiceConfig
func (s *Service) GetServiceConfig() (*models.ServiceConfig, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.serviceConfig != nil {
		return s.serviceConfig, nil
	}

	return s.storage.GetServiceConfig(context.Background())
}

// GetFileConfig возвращает текущую конфигурацию из файла
func (s *Service) GetFileConfig() *models.FileConfig {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.fileConfig
}

// UpdateServiceConfig обновляет ServiceConfig сервиса
func (s *Service) UpdateServiceConfig(update *models.ServiceConfigUpdate) (*models.ServiceConfig, error) {
	ctx := context.Background()

	config, err := s.storage.UpdateServiceConfig(ctx, update)
	if err != nil {
		// Публикуем событие о неудачном обновлении
		if s.eventBus != nil {
			s.eventBus.PublishConfigUpdateFailed(ctx, err.Error(), "UPDATE_FAILED", "api")
		}
		return nil, err
	}

	s.mu.Lock()
	s.serviceConfig = config
	s.mu.Unlock()

	// Обновляем параметры QuestDB writer
	if s.questdbWriter != nil {
		s.questdbWriter.UpdateConfig(config.BatchSize, config.FlushInterval, config.WriteTimeout)
	}

	logger.Info("ServiceConfig обновлён", zap.Int("version", config.Version))

	// Публикуем событие об успешном обновлении
	if s.eventBus != nil {
		s.eventBus.PublishConfigUpdated(ctx, config, "api")
	}

	return config, nil
}

// GetStatus возвращает текущий статус сервиса
func (s *Service) GetStatus() *models.ServiceStatus {
	s.mu.RLock()
	fileConfig := s.fileConfig
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

	// Статус EventBus
	if s.eventBus != nil {
		status.EventBus = s.eventBus.GetStatus()
	}

	// Статус пайплайна
	bufferCap := 0
	if fileConfig != nil {
		bufferCap = fileConfig.Pipeline.BufferSize
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

// Reload перезагружает ServiceConfig из MongoDB
func (s *Service) Reload() error {
	ctx := context.Background()

	// Загружаем свежий ServiceConfig
	config, err := s.storage.GetServiceConfig(ctx)
	if err != nil {
		if s.eventBus != nil {
			s.eventBus.PublishConfigUpdateFailed(ctx, err.Error(), "RELOAD_FAILED", "reload")
		}
		return fmt.Errorf("ошибка загрузки ServiceConfig: %w", err)
	}

	s.mu.Lock()
	oldConfig := s.serviceConfig
	s.serviceConfig = config
	s.mu.Unlock()

	// Обновляем параметры QuestDB writer
	if s.questdbWriter != nil {
		s.questdbWriter.UpdateConfig(config.BatchSize, config.FlushInterval, config.WriteTimeout)
	}

	logger.Info("ServiceConfig перезагружен", zap.Int("config_version", config.Version))

	// Публикуем событие об обновлении если версия изменилась
	if s.eventBus != nil && (oldConfig == nil || oldConfig.Version != config.Version) {
		s.eventBus.PublishConfigUpdated(ctx, config, "reload")
	}

	return nil
}

// publishStatusChange публикует событие изменения статуса
func (s *Service) publishStatusChange(ctx context.Context, oldStatus, newStatus, reason, details string) {
	if s.eventBus != nil && oldStatus != newStatus {
		if err := s.eventBus.PublishStatusChanged(ctx, oldStatus, newStatus, reason, details); err != nil {
			logger.Warn("не удалось опубликовать событие статуса", zap.Error(err))
		}
	}
}
