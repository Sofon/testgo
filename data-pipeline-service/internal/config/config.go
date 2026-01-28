package config

import (
	"fmt"
	"os"
	"strconv"

	"github.com/sofon/data-pipeline-service/internal/api"
	"github.com/sofon/data-pipeline-service/internal/storage"
	"github.com/sofon/data-pipeline-service/pkg/logger"
	"gopkg.in/yaml.v3"
)

// AppConfig — конфигурация приложения
type AppConfig struct {
	Server  api.ServerConfig     `json:"server" yaml:"server"`
	MongoDB storage.MongoConfig  `json:"mongodb" yaml:"mongodb"`
	File    storage.FileConfig   `json:"file" yaml:"file"`
	Logger  logger.Config        `json:"logger" yaml:"logger"`
}

// Load загружает конфигурацию из файла и переменных окружения
func Load(configPath string) (*AppConfig, error) {
	cfg := DefaultConfig()

	// Загружаем из файла если существует
	if configPath != "" {
		if err := cfg.loadFromFile(configPath); err != nil {
			return nil, err
		}
	}

	// Переопределяем переменными окружения
	cfg.loadFromEnv()

	return cfg, nil
}

// DefaultConfig возвращает конфигурацию по умолчанию
func DefaultConfig() *AppConfig {
	return &AppConfig{
		Server: api.ServerConfig{
			Host:         "0.0.0.0",
			Port:         8080,
			ReadTimeout:  30,
			WriteTimeout: 30,
			Mode:         "release",
		},
		MongoDB: storage.MongoConfig{
			URI:            "mongodb://localhost:27017",
			Database:       "data_pipeline",
			Collection:     "config",
			ConnectTimeout: 10,
			MaxPoolSize:    10,
		},
		File: storage.FileConfig{
			Path: "./config/pipeline-config.json",
		},
		Logger: logger.Config{
			Level:  "info",
			Format: "json",
		},
	}
}

func (c *AppConfig) loadFromFile(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // Файл не существует, используем значения по умолчанию
		}
		return fmt.Errorf("ошибка чтения файла конфига: %w", err)
	}

	if err := yaml.Unmarshal(data, c); err != nil {
		return fmt.Errorf("ошибка парсинга файла конфига: %w", err)
	}

	return nil
}

func (c *AppConfig) loadFromEnv() {
	// Сервер
	if v := os.Getenv("SERVER_HOST"); v != "" {
		c.Server.Host = v
	}
	if v := os.Getenv("SERVER_PORT"); v != "" {
		if port, err := strconv.Atoi(v); err == nil {
			c.Server.Port = port
		}
	}
	if v := os.Getenv("SERVER_MODE"); v != "" {
		c.Server.Mode = v
	}

	// MongoDB
	if v := os.Getenv("MONGODB_URI"); v != "" {
		c.MongoDB.URI = v
	}
	if v := os.Getenv("MONGODB_DATABASE"); v != "" {
		c.MongoDB.Database = v
	}
	if v := os.Getenv("MONGODB_COLLECTION"); v != "" {
		c.MongoDB.Collection = v
	}

	// Файловое хранилище
	if v := os.Getenv("CONFIG_FILE_PATH"); v != "" {
		c.File.Path = v
	}

	// Логгер
	if v := os.Getenv("LOG_LEVEL"); v != "" {
		c.Logger.Level = v
	}
	if v := os.Getenv("LOG_FORMAT"); v != "" {
		c.Logger.Format = v
	}
}

// Validate проверяет валидность конфигурации
func (c *AppConfig) Validate() error {
	if c.Server.Port <= 0 || c.Server.Port > 65535 {
		return fmt.Errorf("неверный порт сервера: %d", c.Server.Port)
	}

	return nil
}
