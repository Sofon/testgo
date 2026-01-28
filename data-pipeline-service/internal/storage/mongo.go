package storage

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/sofon/data-pipeline-service/internal/models"
	"github.com/sofon/data-pipeline-service/pkg/logger"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
)

var (
	ErrConfigNotFound = errors.New("конфигурация не найдена")
	ErrNoConnection   = errors.New("нет подключения к базе данных")
)

// MongoStorage — хранилище конфигурации в MongoDB
type MongoStorage struct {
	client     *mongo.Client
	database   *mongo.Database
	collection *mongo.Collection
	config     *MongoConfig
	mu         sync.RWMutex
	connected  bool
}

// MongoConfig — настройки подключения к MongoDB
type MongoConfig struct {
	URI            string `json:"uri" yaml:"uri"`
	Database       string `json:"database" yaml:"database"`
	Collection     string `json:"collection" yaml:"collection"`
	ConnectTimeout int    `json:"connect_timeout" yaml:"connect_timeout"` // секунды
	MaxPoolSize    uint64 `json:"max_pool_size" yaml:"max_pool_size"`
}

// NewMongoStorage создаёт новый экземпляр MongoDB хранилища
func NewMongoStorage(cfg *MongoConfig) (*MongoStorage, error) {
	if cfg == nil {
		return nil, errors.New("требуется конфигурация MongoDB")
	}

	if cfg.URI == "" {
		cfg.URI = "mongodb://localhost:27017"
	}
	if cfg.Database == "" {
		cfg.Database = "data_pipeline"
	}
	if cfg.Collection == "" {
		cfg.Collection = "config"
	}
	if cfg.ConnectTimeout == 0 {
		cfg.ConnectTimeout = 10
	}
	if cfg.MaxPoolSize == 0 {
		cfg.MaxPoolSize = 10
	}

	return &MongoStorage{
		config: cfg,
	}, nil
}

// Connect устанавливает подключение к MongoDB
func (s *MongoStorage) Connect(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	timeout := time.Duration(s.config.ConnectTimeout) * time.Second
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	clientOpts := options.Client().
		ApplyURI(s.config.URI).
		SetMaxPoolSize(s.config.MaxPoolSize).
		SetServerSelectionTimeout(timeout)

	client, err := mongo.Connect(ctx, clientOpts)
	if err != nil {
		return fmt.Errorf("ошибка подключения к MongoDB: %w", err)
	}

	// Проверяем подключение
	if err := client.Ping(ctx, nil); err != nil {
		return fmt.Errorf("ошибка ping MongoDB: %w", err)
	}

	s.client = client
	s.database = client.Database(s.config.Database)
	s.collection = s.database.Collection(s.config.Collection)
	s.connected = true

	logger.Info("подключено к MongoDB",
		zap.String("uri", s.config.URI),
		zap.String("database", s.config.Database),
	)

	// Создаём индексы
	if err := s.ensureIndexes(ctx); err != nil {
		logger.Warn("не удалось создать индексы", zap.Error(err))
	}

	return nil
}

func (s *MongoStorage) ensureIndexes(ctx context.Context) error {
	indexes := []mongo.IndexModel{
		{
			Keys:    bson.D{{Key: "version", Value: -1}},
			Options: options.Index().SetUnique(false),
		},
		{
			Keys:    bson.D{{Key: "updated_at", Value: -1}},
			Options: options.Index().SetUnique(false),
		},
	}

	_, err := s.collection.Indexes().CreateMany(ctx, indexes)
	return err
}

// Disconnect закрывает подключение к MongoDB
func (s *MongoStorage) Disconnect(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.client != nil {
		if err := s.client.Disconnect(ctx); err != nil {
			return fmt.Errorf("ошибка отключения от MongoDB: %w", err)
		}
		s.connected = false
		logger.Info("отключено от MongoDB")
	}
	return nil
}

// IsConnected возвращает статус подключения
func (s *MongoStorage) IsConnected() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.connected
}

// GetConfig получает последнюю версию конфигурации
func (s *MongoStorage) GetConfig(ctx context.Context) (*models.Config, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if !s.connected {
		return nil, ErrNoConnection
	}

	opts := options.FindOne().SetSort(bson.D{{Key: "version", Value: -1}})

	var config models.Config
	err := s.collection.FindOne(ctx, bson.M{}, opts).Decode(&config)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, ErrConfigNotFound
		}
		return nil, fmt.Errorf("ошибка получения конфига: %w", err)
	}

	return &config, nil
}

// SaveConfig сохраняет новую версию конфигурации
func (s *MongoStorage) SaveConfig(ctx context.Context, config *models.Config) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.connected {
		return ErrNoConnection
	}

	config.UpdatedAt = time.Now()
	if config.CreatedAt.IsZero() {
		config.CreatedAt = config.UpdatedAt
	}

	// Получаем текущую версию и инкрементируем
	currentConfig, err := s.getLatestConfigUnsafe(ctx)
	if err != nil && !errors.Is(err, ErrConfigNotFound) {
		return fmt.Errorf("ошибка получения текущей версии конфига: %w", err)
	}

	if currentConfig != nil {
		config.Version = currentConfig.Version + 1
	} else {
		config.Version = 1
	}

	_, err = s.collection.InsertOne(ctx, config)
	if err != nil {
		return fmt.Errorf("ошибка сохранения конфига: %w", err)
	}

	logger.Info("конфиг сохранён", zap.Int("version", config.Version))
	return nil
}

// UpdateConfig обновляет отдельные поля конфигурации
func (s *MongoStorage) UpdateConfig(ctx context.Context, update *models.ConfigUpdate) (*models.Config, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.connected {
		return nil, ErrNoConnection
	}

	// Получаем текущий конфиг
	currentConfig, err := s.getLatestConfigUnsafe(ctx)
	if err != nil {
		if errors.Is(err, ErrConfigNotFound) {
			// Создаём конфиг по умолчанию если его нет
			currentConfig = models.DefaultConfig()
		} else {
			return nil, fmt.Errorf("ошибка получения текущего конфига: %w", err)
		}
	}

	// Применяем обновления
	if update.MQTT != nil {
		currentConfig.MQTT = *update.MQTT
	}
	if update.QuestDB != nil {
		currentConfig.QuestDB = *update.QuestDB
	}
	if update.Pipeline != nil {
		currentConfig.Pipeline = *update.Pipeline
	}

	currentConfig.Version++
	currentConfig.UpdatedAt = time.Now()
	currentConfig.ID = [12]byte{} // Сбрасываем ID для нового документа

	// Вставляем новую версию
	_, err = s.collection.InsertOne(ctx, currentConfig)
	if err != nil {
		return nil, fmt.Errorf("ошибка сохранения обновлённого конфига: %w", err)
	}

	logger.Info("конфиг обновлён", zap.Int("version", currentConfig.Version))
	return currentConfig, nil
}

func (s *MongoStorage) getLatestConfigUnsafe(ctx context.Context) (*models.Config, error) {
	opts := options.FindOne().SetSort(bson.D{{Key: "version", Value: -1}})

	var config models.Config
	err := s.collection.FindOne(ctx, bson.M{}, opts).Decode(&config)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, ErrConfigNotFound
		}
		return nil, err
	}

	return &config, nil
}

// GetConfigHistory получает историю изменений конфигурации
func (s *MongoStorage) GetConfigHistory(ctx context.Context, limit int) ([]*models.Config, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if !s.connected {
		return nil, ErrNoConnection
	}

	if limit <= 0 {
		limit = 10
	}

	opts := options.Find().
		SetSort(bson.D{{Key: "version", Value: -1}}).
		SetLimit(int64(limit))

	cursor, err := s.collection.Find(ctx, bson.M{}, opts)
	if err != nil {
		return nil, fmt.Errorf("ошибка получения истории конфига: %w", err)
	}
	defer cursor.Close(ctx)

	var configs []*models.Config
	if err := cursor.All(ctx, &configs); err != nil {
		return nil, fmt.Errorf("ошибка декодирования истории конфига: %w", err)
	}

	return configs, nil
}

// Ping проверяет активность подключения
func (s *MongoStorage) Ping(ctx context.Context) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if !s.connected || s.client == nil {
		return ErrNoConnection
	}

	return s.client.Ping(ctx, nil)
}

// GetConnectionInfo возвращает информацию о подключении
func (s *MongoStorage) GetConnectionInfo() (host, database string) {
	if s.config != nil {
		return s.config.URI, s.config.Database
	}
	return "", ""
}
