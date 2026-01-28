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

// AppConfig holds the application configuration
type AppConfig struct {
	Server  api.ServerConfig     `json:"server" yaml:"server"`
	MongoDB storage.MongoConfig  `json:"mongodb" yaml:"mongodb"`
	File    storage.FileConfig   `json:"file" yaml:"file"`
	Logger  logger.Config        `json:"logger" yaml:"logger"`
}

// Load loads configuration from file and environment
func Load(configPath string) (*AppConfig, error) {
	cfg := DefaultConfig()

	// Load from file if exists
	if configPath != "" {
		if err := cfg.loadFromFile(configPath); err != nil {
			return nil, err
		}
	}

	// Override with environment variables
	cfg.loadFromEnv()

	return cfg, nil
}

// DefaultConfig returns default configuration
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
			return nil // File doesn't exist, use defaults
		}
		return fmt.Errorf("failed to read config file: %w", err)
	}

	if err := yaml.Unmarshal(data, c); err != nil {
		return fmt.Errorf("failed to parse config file: %w", err)
	}

	return nil
}

func (c *AppConfig) loadFromEnv() {
	// Server
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

	// File storage
	if v := os.Getenv("CONFIG_FILE_PATH"); v != "" {
		c.File.Path = v
	}

	// Logger
	if v := os.Getenv("LOG_LEVEL"); v != "" {
		c.Logger.Level = v
	}
	if v := os.Getenv("LOG_FORMAT"); v != "" {
		c.Logger.Format = v
	}
}

// Validate validates the configuration
func (c *AppConfig) Validate() error {
	if c.Server.Port <= 0 || c.Server.Port > 65535 {
		return fmt.Errorf("invalid server port: %d", c.Server.Port)
	}

	return nil
}
