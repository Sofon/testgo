package config

import (
	"fmt"
	"os"
	"strconv"

	"github.com/sofon/data-pipeline-service/internal/api"
	"github.com/sofon/data-pipeline-service/internal/models"
	"github.com/sofon/data-pipeline-service/internal/storage"
	"github.com/sofon/data-pipeline-service/pkg/logger"
	"gopkg.in/yaml.v3"
)

// AppConfig — конфигурация приложения (загружается из файла и переменных окружения)
type AppConfig struct {
	Server   api.ServerConfig          `json:"server" yaml:"server"`
	MongoDB  storage.MongoConfig       `json:"mongodb" yaml:"mongodb"`
	File     storage.FileStorageConfig `json:"file" yaml:"file"`
	Logger   logger.Config             `json:"logger" yaml:"logger"`
	Pipeline models.FileConfig         `json:"pipeline" yaml:"pipeline"` // конфиг пайплайна из файла
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
			Collection:     "service_config",
			ConnectTimeout: 10,
			MaxPoolSize:    10,
		},
		File: storage.FileStorageConfig{
			Path: "./config/service-config.json",
		},
		Logger: logger.Config{
			Level:  "info",
			Format: "json",
		},
		Pipeline: *models.DefaultFileConfig(),
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

	// Файловое хранилище ServiceConfig
	if v := os.Getenv("SERVICE_CONFIG_FILE_PATH"); v != "" {
		c.File.Path = v
	}

	// Логгер
	if v := os.Getenv("LOG_LEVEL"); v != "" {
		c.Logger.Level = v
	}
	if v := os.Getenv("LOG_FORMAT"); v != "" {
		c.Logger.Format = v
	}

	// MQTT Data Bus
	if v := os.Getenv("MQTT_BROKER"); v != "" {
		c.Pipeline.MQTT.Broker = v
	}
	if v := os.Getenv("MQTT_PORT"); v != "" {
		if port, err := strconv.Atoi(v); err == nil {
			c.Pipeline.MQTT.Port = port
		}
	}
	if v := os.Getenv("MQTT_CLIENT_ID"); v != "" {
		c.Pipeline.MQTT.ClientID = v
	}
	if v := os.Getenv("MQTT_USERNAME"); v != "" {
		c.Pipeline.MQTT.Username = v
	}
	if v := os.Getenv("MQTT_PASSWORD"); v != "" {
		c.Pipeline.MQTT.Password = v
	}

	// Event Bus
	if v := os.Getenv("EVENTBUS_ENABLED"); v != "" {
		c.Pipeline.EventBus.Enabled = v == "true" || v == "1"
	}
	if v := os.Getenv("EVENTBUS_BROKER"); v != "" {
		c.Pipeline.EventBus.Broker = v
	}
	if v := os.Getenv("EVENTBUS_PORT"); v != "" {
		if port, err := strconv.Atoi(v); err == nil {
			c.Pipeline.EventBus.Port = port
		}
	}
	if v := os.Getenv("EVENTBUS_CLIENT_ID"); v != "" {
		c.Pipeline.EventBus.ClientID = v
	}
	if v := os.Getenv("EVENTBUS_TOPIC_PREFIX"); v != "" {
		c.Pipeline.EventBus.TopicPrefix = v
	}

	// QuestDB
	if v := os.Getenv("QUESTDB_HOST"); v != "" {
		c.Pipeline.QuestDB.Host = v
	}
	if v := os.Getenv("QUESTDB_ILP_PORT"); v != "" {
		if port, err := strconv.Atoi(v); err == nil {
			c.Pipeline.QuestDB.ILPPort = port
		}
	}
	if v := os.Getenv("QUESTDB_HTTP_PORT"); v != "" {
		if port, err := strconv.Atoi(v); err == nil {
			c.Pipeline.QuestDB.HTTPPort = port
		}
	}
}

// Validate проверяет валидность конфигурации
func (c *AppConfig) Validate() error {
	if c.Server.Port <= 0 || c.Server.Port > 65535 {
		return fmt.Errorf("неверный порт сервера: %d", c.Server.Port)
	}

	if c.Pipeline.MQTT.Broker == "" {
		return fmt.Errorf("не указан MQTT брокер")
	}

	if c.Pipeline.QuestDB.Host == "" {
		return fmt.Errorf("не указан хост QuestDB")
	}

	return nil
}
