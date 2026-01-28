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
	ErrConfigNotFound = errors.New("config not found")
	ErrNoConnection   = errors.New("no database connection")
)

// MongoStorage handles configuration persistence in MongoDB
type MongoStorage struct {
	client     *mongo.Client
	database   *mongo.Database
	collection *mongo.Collection
	config     *MongoConfig
	mu         sync.RWMutex
	connected  bool
}

// MongoConfig holds MongoDB connection settings
type MongoConfig struct {
	URI            string `json:"uri" yaml:"uri"`
	Database       string `json:"database" yaml:"database"`
	Collection     string `json:"collection" yaml:"collection"`
	ConnectTimeout int    `json:"connect_timeout" yaml:"connect_timeout"` // seconds
	MaxPoolSize    uint64 `json:"max_pool_size" yaml:"max_pool_size"`
}

// NewMongoStorage creates a new MongoDB storage instance
func NewMongoStorage(cfg *MongoConfig) (*MongoStorage, error) {
	if cfg == nil {
		return nil, errors.New("mongo config is required")
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

// Connect establishes connection to MongoDB
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
		return fmt.Errorf("failed to connect to MongoDB: %w", err)
	}

	// Verify connection
	if err := client.Ping(ctx, nil); err != nil {
		return fmt.Errorf("failed to ping MongoDB: %w", err)
	}

	s.client = client
	s.database = client.Database(s.config.Database)
	s.collection = s.database.Collection(s.config.Collection)
	s.connected = true

	logger.Info("connected to MongoDB",
		zap.String("uri", s.config.URI),
		zap.String("database", s.config.Database),
	)

	// Ensure indexes
	if err := s.ensureIndexes(ctx); err != nil {
		logger.Warn("failed to create indexes", zap.Error(err))
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

// Disconnect closes the MongoDB connection
func (s *MongoStorage) Disconnect(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.client != nil {
		if err := s.client.Disconnect(ctx); err != nil {
			return fmt.Errorf("failed to disconnect from MongoDB: %w", err)
		}
		s.connected = false
		logger.Info("disconnected from MongoDB")
	}
	return nil
}

// IsConnected returns the connection status
func (s *MongoStorage) IsConnected() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.connected
}

// GetConfig retrieves the latest configuration
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
		return nil, fmt.Errorf("failed to get config: %w", err)
	}

	return &config, nil
}

// SaveConfig saves a new configuration version
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

	// Get current version and increment
	currentConfig, err := s.getLatestConfigUnsafe(ctx)
	if err != nil && !errors.Is(err, ErrConfigNotFound) {
		return fmt.Errorf("failed to get current config version: %w", err)
	}

	if currentConfig != nil {
		config.Version = currentConfig.Version + 1
	} else {
		config.Version = 1
	}

	_, err = s.collection.InsertOne(ctx, config)
	if err != nil {
		return fmt.Errorf("failed to save config: %w", err)
	}

	logger.Info("config saved", zap.Int("version", config.Version))
	return nil
}

// UpdateConfig updates specific fields of the configuration
func (s *MongoStorage) UpdateConfig(ctx context.Context, update *models.ConfigUpdate) (*models.Config, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.connected {
		return nil, ErrNoConnection
	}

	// Get current config
	currentConfig, err := s.getLatestConfigUnsafe(ctx)
	if err != nil {
		if errors.Is(err, ErrConfigNotFound) {
			// Create default config if none exists
			currentConfig = models.DefaultConfig()
		} else {
			return nil, fmt.Errorf("failed to get current config: %w", err)
		}
	}

	// Apply updates
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
	currentConfig.ID = [12]byte{} // Reset ID for new document

	// Insert new version
	_, err = s.collection.InsertOne(ctx, currentConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to save updated config: %w", err)
	}

	logger.Info("config updated", zap.Int("version", currentConfig.Version))
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

// GetConfigHistory retrieves configuration history
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
		return nil, fmt.Errorf("failed to get config history: %w", err)
	}
	defer cursor.Close(ctx)

	var configs []*models.Config
	if err := cursor.All(ctx, &configs); err != nil {
		return nil, fmt.Errorf("failed to decode config history: %w", err)
	}

	return configs, nil
}

// Ping checks if the connection is alive
func (s *MongoStorage) Ping(ctx context.Context) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if !s.connected || s.client == nil {
		return ErrNoConnection
	}

	return s.client.Ping(ctx, nil)
}

// GetConnectionInfo returns connection information
func (s *MongoStorage) GetConnectionInfo() (host, database string) {
	if s.config != nil {
		return s.config.URI, s.config.Database
	}
	return "", ""
}
