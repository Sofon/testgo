package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/sofon/data-pipeline-service/internal/models"
	"github.com/sofon/data-pipeline-service/pkg/logger"
	"go.uber.org/zap"
)

// FileStorage — файловое хранилище ServiceConfig (fallback для MongoDB)
type FileStorage struct {
	filePath string
	mu       sync.RWMutex
}

// FileStorageConfig — настройки файлового хранилища ServiceConfig
type FileStorageConfig struct {
	Path string `json:"path" yaml:"path"`
}

// NewFileStorage создаёт новое файловое хранилище для ServiceConfig
func NewFileStorage(cfg *FileStorageConfig) (*FileStorage, error) {
	if cfg == nil || cfg.Path == "" {
		cfg = &FileStorageConfig{Path: "./config/service-config.json"}
	}

	// Создаём директорию если не существует
	dir := filepath.Dir(cfg.Path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("ошибка создания директории конфига: %w", err)
	}

	return &FileStorage{
		filePath: cfg.Path,
	}, nil
}

// GetServiceConfig читает ServiceConfig из файла
func (s *FileStorage) GetServiceConfig(ctx context.Context) (*models.ServiceConfig, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	data, err := os.ReadFile(s.filePath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, ErrConfigNotFound
		}
		return nil, fmt.Errorf("ошибка чтения файла конфига: %w", err)
	}

	var config models.ServiceConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("ошибка парсинга файла конфига: %w", err)
	}

	return &config, nil
}

// SaveServiceConfig записывает ServiceConfig в файл
func (s *FileStorage) SaveServiceConfig(ctx context.Context, config *models.ServiceConfig) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	config.UpdatedAt = time.Now()
	if config.CreatedAt.IsZero() {
		config.CreatedAt = config.UpdatedAt
	}

	// Читаем текущую версию если файл существует
	if data, err := os.ReadFile(s.filePath); err == nil {
		var current models.ServiceConfig
		if json.Unmarshal(data, &current) == nil {
			config.Version = current.Version + 1
		}
	} else {
		config.Version = 1
	}

	data, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return fmt.Errorf("ошибка сериализации конфига: %w", err)
	}

	// Сначала пишем во временный файл
	tmpPath := s.filePath + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return fmt.Errorf("ошибка записи временного файла конфига: %w", err)
	}

	// Атомарное переименование
	if err := os.Rename(tmpPath, s.filePath); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("ошибка переименования файла конфига: %w", err)
	}

	logger.Info("ServiceConfig сохранён в файл",
		zap.String("path", s.filePath),
		zap.Int("version", config.Version),
	)

	return nil
}

// UpdateServiceConfig обновляет отдельные поля ServiceConfig в файле
func (s *FileStorage) UpdateServiceConfig(ctx context.Context, update *models.ServiceConfigUpdate) (*models.ServiceConfig, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Читаем текущий конфиг
	var config *models.ServiceConfig

	data, err := os.ReadFile(s.filePath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			config = models.DefaultServiceConfig()
		} else {
			return nil, fmt.Errorf("ошибка чтения файла конфига: %w", err)
		}
	} else {
		config = &models.ServiceConfig{}
		if err := json.Unmarshal(data, config); err != nil {
			return nil, fmt.Errorf("ошибка парсинга файла конфига: %w", err)
		}
	}

	// Применяем обновления
	if update.BatchSize != nil {
		config.BatchSize = *update.BatchSize
	}
	if update.FlushInterval != nil {
		config.FlushInterval = *update.FlushInterval
	}
	if update.WriteTimeout != nil {
		config.WriteTimeout = *update.WriteTimeout
	}
	if update.StreamMapping != nil {
		config.StreamMapping = *update.StreamMapping
	}

	config.Version++
	config.UpdatedAt = time.Now()

	// Сохраняем обновлённый конфиг
	newData, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("ошибка сериализации конфига: %w", err)
	}

	tmpPath := s.filePath + ".tmp"
	if err := os.WriteFile(tmpPath, newData, 0644); err != nil {
		return nil, fmt.Errorf("ошибка записи временного файла конфига: %w", err)
	}

	if err := os.Rename(tmpPath, s.filePath); err != nil {
		os.Remove(tmpPath)
		return nil, fmt.Errorf("ошибка переименования файла конфига: %w", err)
	}

	logger.Info("ServiceConfig обновлён в файле",
		zap.String("path", s.filePath),
		zap.Int("version", config.Version),
	)

	return config, nil
}

// Exists проверяет существование файла конфига
func (s *FileStorage) Exists() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	_, err := os.Stat(s.filePath)
	return err == nil
}

// GetPath возвращает путь к файлу конфига
func (s *FileStorage) GetPath() string {
	return s.filePath
}
