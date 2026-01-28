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

// FileStorage — файловое хранилище конфигурации (fallback)
type FileStorage struct {
	filePath string
	mu       sync.RWMutex
}

// FileConfig — настройки файлового хранилища
type FileConfig struct {
	Path string `json:"path" yaml:"path"`
}

// NewFileStorage создаёт новое файловое хранилище
func NewFileStorage(cfg *FileConfig) (*FileStorage, error) {
	if cfg == nil || cfg.Path == "" {
		cfg = &FileConfig{Path: "./config/pipeline-config.json"}
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

// GetConfig читает конфигурацию из файла
func (s *FileStorage) GetConfig(ctx context.Context) (*models.Config, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	data, err := os.ReadFile(s.filePath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, ErrConfigNotFound
		}
		return nil, fmt.Errorf("ошибка чтения файла конфига: %w", err)
	}

	var config models.Config
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("ошибка парсинга файла конфига: %w", err)
	}

	return &config, nil
}

// SaveConfig записывает конфигурацию в файл
func (s *FileStorage) SaveConfig(ctx context.Context, config *models.Config) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	config.UpdatedAt = time.Now()
	if config.CreatedAt.IsZero() {
		config.CreatedAt = config.UpdatedAt
	}

	// Читаем текущую версию если файл существует
	if data, err := os.ReadFile(s.filePath); err == nil {
		var current models.Config
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

	logger.Info("конфиг сохранён в файл",
		zap.String("path", s.filePath),
		zap.Int("version", config.Version),
	)

	return nil
}

// UpdateConfig обновляет отдельные поля конфигурации
func (s *FileStorage) UpdateConfig(ctx context.Context, update *models.ConfigUpdate) (*models.Config, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Читаем текущий конфиг
	var config *models.Config

	data, err := os.ReadFile(s.filePath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			config = models.DefaultConfig()
		} else {
			return nil, fmt.Errorf("ошибка чтения файла конфига: %w", err)
		}
	} else {
		config = &models.Config{}
		if err := json.Unmarshal(data, config); err != nil {
			return nil, fmt.Errorf("ошибка парсинга файла конфига: %w", err)
		}
	}

	// Применяем обновления
	if update.MQTT != nil {
		config.MQTT = *update.MQTT
	}
	if update.QuestDB != nil {
		config.QuestDB = *update.QuestDB
	}
	if update.Pipeline != nil {
		config.Pipeline = *update.Pipeline
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

	logger.Info("конфиг обновлён в файле",
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
