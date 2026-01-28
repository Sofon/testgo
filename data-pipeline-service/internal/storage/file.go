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

// FileStorage provides file-based configuration storage as fallback
type FileStorage struct {
	filePath string
	mu       sync.RWMutex
}

// FileConfig holds file storage settings
type FileConfig struct {
	Path string `json:"path" yaml:"path"`
}

// NewFileStorage creates a new file-based storage
func NewFileStorage(cfg *FileConfig) (*FileStorage, error) {
	if cfg == nil || cfg.Path == "" {
		cfg = &FileConfig{Path: "./config/pipeline-config.json"}
	}

	// Ensure directory exists
	dir := filepath.Dir(cfg.Path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create config directory: %w", err)
	}

	return &FileStorage{
		filePath: cfg.Path,
	}, nil
}

// GetConfig reads configuration from file
func (s *FileStorage) GetConfig(ctx context.Context) (*models.Config, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	data, err := os.ReadFile(s.filePath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, ErrConfigNotFound
		}
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var config models.Config
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	return &config, nil
}

// SaveConfig writes configuration to file
func (s *FileStorage) SaveConfig(ctx context.Context, config *models.Config) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	config.UpdatedAt = time.Now()
	if config.CreatedAt.IsZero() {
		config.CreatedAt = config.UpdatedAt
	}

	// Read current version if exists
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
		return fmt.Errorf("failed to marshal config: %w", err)
	}

	// Write to temp file first
	tmpPath := s.filePath + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write temp config file: %w", err)
	}

	// Atomic rename
	if err := os.Rename(tmpPath, s.filePath); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("failed to rename config file: %w", err)
	}

	logger.Info("config saved to file",
		zap.String("path", s.filePath),
		zap.Int("version", config.Version),
	)

	return nil
}

// UpdateConfig updates specific fields of the configuration
func (s *FileStorage) UpdateConfig(ctx context.Context, update *models.ConfigUpdate) (*models.Config, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Read current config
	var config *models.Config

	data, err := os.ReadFile(s.filePath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			config = models.DefaultConfig()
		} else {
			return nil, fmt.Errorf("failed to read config file: %w", err)
		}
	} else {
		config = &models.Config{}
		if err := json.Unmarshal(data, config); err != nil {
			return nil, fmt.Errorf("failed to parse config file: %w", err)
		}
	}

	// Apply updates
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

	// Save updated config
	newData, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("failed to marshal config: %w", err)
	}

	tmpPath := s.filePath + ".tmp"
	if err := os.WriteFile(tmpPath, newData, 0644); err != nil {
		return nil, fmt.Errorf("failed to write temp config file: %w", err)
	}

	if err := os.Rename(tmpPath, s.filePath); err != nil {
		os.Remove(tmpPath)
		return nil, fmt.Errorf("failed to rename config file: %w", err)
	}

	logger.Info("config updated in file",
		zap.String("path", s.filePath),
		zap.Int("version", config.Version),
	)

	return config, nil
}

// Exists checks if config file exists
func (s *FileStorage) Exists() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	_, err := os.Stat(s.filePath)
	return err == nil
}

// GetPath returns the config file path
func (s *FileStorage) GetPath() string {
	return s.filePath
}
