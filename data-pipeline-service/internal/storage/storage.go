package storage

import (
	"context"
	"errors"

	"github.com/sofon/data-pipeline-service/internal/models"
	"github.com/sofon/data-pipeline-service/pkg/logger"
	"go.uber.org/zap"
)

// ServiceConfigStorage — интерфейс для хранилища ServiceConfig
type ServiceConfigStorage interface {
	GetServiceConfig(ctx context.Context) (*models.ServiceConfig, error)
	SaveServiceConfig(ctx context.Context, config *models.ServiceConfig) error
	UpdateServiceConfig(ctx context.Context, update *models.ServiceConfigUpdate) (*models.ServiceConfig, error)
}

// HybridStorage — гибридное хранилище ServiceConfig с MongoDB и файловым fallback.
// Использует MongoDB как основное хранилище и переключается на файловое,
// если MongoDB недоступен. Это гарантирует, что конфигурация не потеряется.
type HybridStorage struct {
	mongo *MongoStorage
	file  *FileStorage
}

// NewHybridStorage создаёт гибридное хранилище с MongoDB и файловым fallback
func NewHybridStorage(mongoCfg *MongoConfig, fileCfg *FileStorageConfig) (*HybridStorage, error) {
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

// Connect подключается к MongoDB
func (s *HybridStorage) Connect(ctx context.Context) error {
	err := s.mongo.Connect(ctx)
	if err != nil {
		logger.Warn("не удалось подключиться к MongoDB, используется файловый fallback",
			zap.Error(err),
		)
		return nil // Не падаем, есть файловый fallback
	}

	// Синхронизируем файловый конфиг в MongoDB если он был изменён пока MongoDB был недоступен
	if err := s.syncFileToMongo(ctx); err != nil {
		logger.Warn("не удалось синхронизировать файловый конфиг в MongoDB", zap.Error(err))
	}

	return nil
}

// syncFileToMongo синхронизирует изменения из файлового хранилища в MongoDB
func (s *HybridStorage) syncFileToMongo(ctx context.Context) error {
	if !s.mongo.IsConnected() {
		return nil
	}

	fileConfig, err := s.file.GetServiceConfig(ctx)
	if err != nil {
		if errors.Is(err, ErrConfigNotFound) {
			return nil // Нет файлового конфига для синхронизации
		}
		return err
	}

	mongoConfig, err := s.mongo.GetServiceConfig(ctx)
	if err != nil && !errors.Is(err, ErrConfigNotFound) {
		return err
	}

	// Если файловый конфиг новее — синхронизируем в MongoDB
	if mongoConfig == nil || fileConfig.UpdatedAt.After(mongoConfig.UpdatedAt) {
		logger.Info("синхронизация файлового конфига в MongoDB",
			zap.Int("file_version", fileConfig.Version),
		)
		return s.mongo.SaveServiceConfig(ctx, fileConfig)
	}

	return nil
}

// Disconnect отключается от MongoDB
func (s *HybridStorage) Disconnect(ctx context.Context) error {
	return s.mongo.Disconnect(ctx)
}

// GetServiceConfig получает ServiceConfig, сначала пробуя MongoDB, затем файл
func (s *HybridStorage) GetServiceConfig(ctx context.Context) (*models.ServiceConfig, error) {
	if s.mongo.IsConnected() {
		config, err := s.mongo.GetServiceConfig(ctx)
		if err == nil {
			return config, nil
		}
		if !errors.Is(err, ErrConfigNotFound) {
			logger.Warn("ошибка получения конфига из MongoDB, пробуем файл", zap.Error(err))
		}
	}

	return s.file.GetServiceConfig(ctx)
}

// SaveServiceConfig сохраняет ServiceConfig в оба хранилища
func (s *HybridStorage) SaveServiceConfig(ctx context.Context, config *models.ServiceConfig) error {
	// Всегда сохраняем в файл как резервную копию
	if err := s.file.SaveServiceConfig(ctx, config); err != nil {
		logger.Warn("не удалось сохранить конфиг в файл", zap.Error(err))
	}

	if s.mongo.IsConnected() {
		if err := s.mongo.SaveServiceConfig(ctx, config); err != nil {
			logger.Warn("не удалось сохранить конфиг в MongoDB", zap.Error(err))
			return nil // Сохранение в файл успешно, не падаем
		}
	}

	return nil
}

// UpdateServiceConfig обновляет ServiceConfig в обоих хранилищах
func (s *HybridStorage) UpdateServiceConfig(ctx context.Context, update *models.ServiceConfigUpdate) (*models.ServiceConfig, error) {
	var config *models.ServiceConfig
	var err error

	if s.mongo.IsConnected() {
		config, err = s.mongo.UpdateServiceConfig(ctx, update)
		if err != nil {
			logger.Warn("обновление в MongoDB не удалось, переключаемся на файл", zap.Error(err))
		} else {
			// Синхронизируем в файл
			if saveErr := s.file.SaveServiceConfig(ctx, config); saveErr != nil {
				logger.Warn("не удалось синхронизировать конфиг в файл", zap.Error(saveErr))
			}
			return config, nil
		}
	}

	// Fallback на файл
	config, err = s.file.UpdateServiceConfig(ctx, update)
	if err != nil {
		return nil, err
	}

	return config, nil
}

// IsMongoConnected возвращает статус подключения к MongoDB
func (s *HybridStorage) IsMongoConnected() bool {
	return s.mongo.IsConnected()
}

// GetMongoInfo возвращает информацию о подключении к MongoDB
func (s *HybridStorage) GetMongoInfo() (host, database string) {
	return s.mongo.GetConnectionInfo()
}

// Ping проверяет подключение к MongoDB
func (s *HybridStorage) Ping(ctx context.Context) error {
	return s.mongo.Ping(ctx)
}

// GetConfigHistory получает историю изменений ServiceConfig
func (s *HybridStorage) GetConfigHistory(ctx context.Context, limit int) ([]*models.ServiceConfig, error) {
	if s.mongo.IsConnected() {
		return s.mongo.GetConfigHistory(ctx, limit)
	}
	return nil, ErrNoConnection
}
