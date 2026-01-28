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

// Version information (set at build time)
var (
	Version   = "dev"
	BuildTime = "unknown"
)

// Service is the main data pipeline service
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

// NewService creates a new data pipeline service
func NewService(store *storage.HybridStorage) *Service {
	return &Service{
		storage:  store,
		stopChan: make(chan struct{}),
	}
}

// Start initializes and starts the service
func (s *Service) Start(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Load configuration
	config, err := s.storage.GetConfig(ctx)
	if err != nil {
		if err == storage.ErrConfigNotFound {
			// Create default config
			config = models.DefaultConfig()
			if err := s.storage.SaveConfig(ctx, config); err != nil {
				return fmt.Errorf("failed to save default config: %w", err)
			}
			logger.Info("created default configuration")
		} else {
			return fmt.Errorf("failed to load config: %w", err)
		}
	}
	s.config = config

	// Initialize buffer
	bufferSize := config.Pipeline.BufferSize
	if bufferSize <= 0 {
		bufferSize = 10000
	}
	s.bufferChan = make(chan *models.IncomingMessage, bufferSize)

	// Initialize workers
	s.workers = config.Pipeline.Workers
	if s.workers <= 0 {
		s.workers = 4
	}

	// Initialize QuestDB writer
	s.questdbWriter, err = questdb.NewWriter(&config.QuestDB)
	if err != nil {
		return fmt.Errorf("failed to create QuestDB writer: %w", err)
	}

	if err := s.questdbWriter.Connect(ctx); err != nil {
		return fmt.Errorf("failed to connect to QuestDB: %w", err)
	}

	// Initialize MQTT client
	s.mqttClient, err = mqtt.NewClient(&config.MQTT, s.handleMessage)
	if err != nil {
		return fmt.Errorf("failed to create MQTT client: %w", err)
	}

	if err := s.mqttClient.Connect(ctx); err != nil {
		return fmt.Errorf("failed to connect to MQTT: %w", err)
	}

	// Start worker goroutines
	s.startWorkers()

	s.running.Store(true)
	s.startedAt = time.Now()

	logger.Info("service started",
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

	logger.Debug("worker started", zap.Int("worker_id", id))

	for {
		select {
		case msg := <-s.bufferChan:
			s.processMessage(msg)
		case <-s.stopChan:
			logger.Debug("worker stopped", zap.Int("worker_id", id))
			return
		}
	}
}

func (s *Service) handleMessage(msg *models.IncomingMessage) {
	select {
	case s.bufferChan <- msg:
		// Message queued successfully
	default:
		// Buffer full, drop message
		s.failedMsgs.Add(1)
		logger.Warn("message buffer full, dropping message",
			zap.String("topic", msg.Topic),
		)
	}
}

func (s *Service) processMessage(msg *models.IncomingMessage) {
	s.mu.RLock()
	config := s.config
	s.mu.RUnlock()

	// Parse message if not already parsed
	if msg.ParsedData == nil {
		var data map[string]interface{}
		if err := json.Unmarshal(msg.Payload, &data); err != nil {
			s.failedMsgs.Add(1)
			logger.Warn("failed to parse message",
				zap.String("topic", msg.Topic),
				zap.Error(err),
			)
			return
		}
		msg.ParsedData = data
	}

	// Write to QuestDB
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
		logger.Warn("failed to write to QuestDB",
			zap.String("topic", msg.Topic),
			zap.Error(err),
		)

		// Retry logic
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

// Stop gracefully stops the service
func (s *Service) Stop(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.running.Load() {
		return nil
	}

	logger.Info("stopping service...")

	// Signal workers to stop
	close(s.stopChan)

	// Disconnect MQTT
	if s.mqttClient != nil {
		s.mqttClient.Disconnect()
	}

	// Close QuestDB connection
	if s.questdbWriter != nil {
		if err := s.questdbWriter.Close(ctx); err != nil {
			logger.Warn("error closing QuestDB connection", zap.Error(err))
		}
	}

	s.running.Store(false)
	logger.Info("service stopped")

	return nil
}

// GetConfig returns the current configuration
func (s *Service) GetConfig() (*models.Config, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.config != nil {
		return s.config, nil
	}

	return s.storage.GetConfig(context.Background())
}

// UpdateConfig updates the service configuration
func (s *Service) UpdateConfig(update *models.ConfigUpdate) (*models.Config, error) {
	ctx := context.Background()

	config, err := s.storage.UpdateConfig(ctx, update)
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	s.config = config
	s.mu.Unlock()

	logger.Info("config updated", zap.Int("version", config.Version))

	return config, nil
}

// GetStatus returns the current service status
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

	// MQTT status
	if s.mqttClient != nil {
		status.MQTT = s.mqttClient.GetStatus()
	}

	// QuestDB status
	if s.questdbWriter != nil {
		status.QuestDB = s.questdbWriter.GetStatus()
	}

	// MongoDB status
	status.MongoDB = models.MongoDBStatus{
		Connected: s.storage.IsMongoConnected(),
	}
	host, db := s.storage.GetMongoInfo()
	status.MongoDB.Host = host
	status.MongoDB.Database = db

	// Pipeline status
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

// IsHealthy returns whether the service is healthy
func (s *Service) IsHealthy() bool {
	if !s.running.Load() {
		return false
	}

	// Check MQTT connection
	if s.mqttClient != nil && !s.mqttClient.IsConnected() {
		return false
	}

	// Check QuestDB connection
	if s.questdbWriter != nil && !s.questdbWriter.IsConnected() {
		return false
	}

	return true
}

// Reload reloads the service configuration and reconnects
func (s *Service) Reload() error {
	ctx := context.Background()

	// Load fresh config
	config, err := s.storage.GetConfig(ctx)
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	s.mu.Lock()
	s.config = config
	s.mu.Unlock()

	// Update MQTT client
	if s.mqttClient != nil {
		if err := s.mqttClient.UpdateConfig(ctx, &config.MQTT); err != nil {
			logger.Warn("failed to reload MQTT config", zap.Error(err))
		}
	}

	logger.Info("service reloaded", zap.Int("config_version", config.Version))

	return nil
}
