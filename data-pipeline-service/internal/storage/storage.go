package storage

import (
	"context"
	"errors"

	"github.com/sofon/data-pipeline-service/internal/models"
	"github.com/sofon/data-pipeline-service/pkg/logger"
	"go.uber.org/zap"
)

// ConfigStorage defines the interface for configuration storage
type ConfigStorage interface {
	GetConfig(ctx context.Context) (*models.Config, error)
	SaveConfig(ctx context.Context, config *models.Config) error
	UpdateConfig(ctx context.Context, update *models.ConfigUpdate) (*models.Config, error)
}

// HybridStorage provides MongoDB storage with file fallback
// It uses MongoDB as primary storage and falls back to file storage
// if MongoDB is unavailable. This ensures config is never lost.
type HybridStorage struct {
	mongo *MongoStorage
	file  *FileStorage
}

// NewHybridStorage creates a hybrid storage with MongoDB primary and file fallback
func NewHybridStorage(mongoCfg *MongoConfig, fileCfg *FileConfig) (*HybridStorage, error) {
	mongo, err := NewMongoStorage(mongoCfg)
	if err != nil {
		return nil, err
	}

	file, err := NewFileStorage(fileCfg)
	if err != nil {
		return nil, err
	}

	return &HybridStorage{
		mongo: mongo,
		file:  file,
	}, nil
}

// Connect connects to MongoDB
func (s *HybridStorage) Connect(ctx context.Context) error {
	err := s.mongo.Connect(ctx)
	if err != nil {
		logger.Warn("MongoDB connection failed, using file storage as fallback",
			zap.Error(err),
		)
		return nil // Don't fail, we have file fallback
	}

	// Sync file config to MongoDB if MongoDB was down
	if err := s.syncFileToMongo(ctx); err != nil {
		logger.Warn("failed to sync file config to MongoDB", zap.Error(err))
	}

	return nil
}

// syncFileToMongo syncs any changes made to file storage while MongoDB was down
func (s *HybridStorage) syncFileToMongo(ctx context.Context) error {
	if !s.mongo.IsConnected() {
		return nil
	}

	fileConfig, err := s.file.GetConfig(ctx)
	if err != nil {
		if errors.Is(err, ErrConfigNotFound) {
			return nil // No file config to sync
		}
		return err
	}

	mongoConfig, err := s.mongo.GetConfig(ctx)
	if err != nil && !errors.Is(err, ErrConfigNotFound) {
		return err
	}

	// If file config is newer, sync to MongoDB
	if mongoConfig == nil || fileConfig.UpdatedAt.After(mongoConfig.UpdatedAt) {
		logger.Info("syncing file config to MongoDB",
			zap.Int("file_version", fileConfig.Version),
		)
		return s.mongo.SaveConfig(ctx, fileConfig)
	}

	return nil
}

// Disconnect disconnects from MongoDB
func (s *HybridStorage) Disconnect(ctx context.Context) error {
	return s.mongo.Disconnect(ctx)
}

// GetConfig retrieves configuration, trying MongoDB first, then file
func (s *HybridStorage) GetConfig(ctx context.Context) (*models.Config, error) {
	if s.mongo.IsConnected() {
		config, err := s.mongo.GetConfig(ctx)
		if err == nil {
			return config, nil
		}
		if !errors.Is(err, ErrConfigNotFound) {
			logger.Warn("MongoDB get config failed, trying file", zap.Error(err))
		}
	}

	return s.file.GetConfig(ctx)
}

// SaveConfig saves configuration to both MongoDB and file
func (s *HybridStorage) SaveConfig(ctx context.Context, config *models.Config) error {
	// Always save to file as backup
	if err := s.file.SaveConfig(ctx, config); err != nil {
		logger.Warn("failed to save config to file", zap.Error(err))
	}

	if s.mongo.IsConnected() {
		if err := s.mongo.SaveConfig(ctx, config); err != nil {
			logger.Warn("failed to save config to MongoDB", zap.Error(err))
			return nil // File save succeeded, don't fail
		}
	}

	return nil
}

// UpdateConfig updates configuration in both storages
func (s *HybridStorage) UpdateConfig(ctx context.Context, update *models.ConfigUpdate) (*models.Config, error) {
	var config *models.Config
	var err error

	if s.mongo.IsConnected() {
		config, err = s.mongo.UpdateConfig(ctx, update)
		if err != nil {
			logger.Warn("MongoDB update failed, falling back to file", zap.Error(err))
		} else {
			// Sync to file
			if saveErr := s.file.SaveConfig(ctx, config); saveErr != nil {
				logger.Warn("failed to sync config to file", zap.Error(saveErr))
			}
			return config, nil
		}
	}

	// Fallback to file
	config, err = s.file.UpdateConfig(ctx, update)
	if err != nil {
		return nil, err
	}

	return config, nil
}

// IsMongoConnected returns MongoDB connection status
func (s *HybridStorage) IsMongoConnected() bool {
	return s.mongo.IsConnected()
}

// GetMongoInfo returns MongoDB connection info
func (s *HybridStorage) GetMongoInfo() (host, database string) {
	return s.mongo.GetConnectionInfo()
}

// Ping checks MongoDB connection
func (s *HybridStorage) Ping(ctx context.Context) error {
	return s.mongo.Ping(ctx)
}
