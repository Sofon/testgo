package models

import (
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

// ServiceConfig — конфигурация сервиса, хранящаяся в MongoDB
// Это единственная часть конфигурации, которая хранится в базе данных
type ServiceConfig struct {
	ID            primitive.ObjectID `bson:"_id,omitempty" json:"id,omitempty"`
	Version       int                `bson:"version" json:"version"`
	BatchSize     int                `bson:"batch_size" json:"batch_size"`         // размер батча для записи
	FlushInterval int                `bson:"flush_interval" json:"flush_interval"` // интервал сброса в мс
	WriteTimeout  int                `bson:"write_timeout" json:"write_timeout"`   // таймаут записи в мс
	StreamMapping map[string]string  `bson:"stream_mapping" json:"stream_mapping"` // маппинг потоков topic -> table
	CreatedAt     time.Time          `bson:"created_at" json:"created_at"`
	UpdatedAt     time.Time          `bson:"updated_at" json:"updated_at"`
}

// ServiceConfigUpdate — запрос на обновление ServiceConfig
type ServiceConfigUpdate struct {
	BatchSize     *int               `json:"batch_size,omitempty"`
	FlushInterval *int               `json:"flush_interval,omitempty"`
	WriteTimeout  *int               `json:"write_timeout,omitempty"`
	StreamMapping *map[string]string `json:"stream_mapping,omitempty"`
}

// DefaultServiceConfig возвращает ServiceConfig по умолчанию
func DefaultServiceConfig() *ServiceConfig {
	return &ServiceConfig{
		Version:       1,
		BatchSize:     1000,
		FlushInterval: 1000,
		WriteTimeout:  5000,
		StreamMapping: map[string]string{
			"sensors/#":   "sensor_data",
			"telemetry/#": "telemetry_data",
		},
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
}

// MQTTConfig — настройки подключения к EMQX/MQTT брокеру (читается из файла)
type MQTTConfig struct {
	Broker      string   `json:"broker" yaml:"broker" validate:"required"`
	Port        int      `json:"port" yaml:"port" validate:"required,min=1,max=65535"`
	ClientID    string   `json:"client_id" yaml:"client_id" validate:"required"`
	Username    string   `json:"username" yaml:"username"`
	Password    string   `json:"password" yaml:"password,omitempty"`
	Topics      []string `json:"topics" yaml:"topics" validate:"required,min=1"`
	QoS         int      `json:"qos" yaml:"qos" validate:"min=0,max=2"`
	CleanStart  bool     `json:"clean_start" yaml:"clean_start"`
	KeepAlive   int      `json:"keep_alive" yaml:"keep_alive"` // секунды
	UseTLS      bool     `json:"use_tls" yaml:"use_tls"`
	TLSCertPath string   `json:"tls_cert_path,omitempty" yaml:"tls_cert_path,omitempty"`
	TLSKeyPath  string   `json:"tls_key_path,omitempty" yaml:"tls_key_path,omitempty"`
	TLSCAPath   string   `json:"tls_ca_path,omitempty" yaml:"tls_ca_path,omitempty"`
}

// EventBusConfig — настройки подключения к шине событий (отдельный EMQX)
type EventBusConfig struct {
	Enabled     bool   `json:"enabled" yaml:"enabled"`
	Broker      string `json:"broker" yaml:"broker"`
	Port        int    `json:"port" yaml:"port"`
	ClientID    string `json:"client_id" yaml:"client_id"`
	Username    string `json:"username" yaml:"username"`
	Password    string `json:"password" yaml:"password,omitempty"`
	TopicPrefix string `json:"topic_prefix" yaml:"topic_prefix"` // префикс для топиков событий
	QoS         int    `json:"qos" yaml:"qos"`
	UseTLS      bool   `json:"use_tls" yaml:"use_tls"`
}

// QuestDBConfig — настройки подключения к QuestDB (читается из файла)
type QuestDBConfig struct {
	Host      string `json:"host" yaml:"host" validate:"required"`
	ILPPort   int    `json:"ilp_port" yaml:"ilp_port" validate:"required"` // порт InfluxDB Line Protocol (9009)
	HTTPPort  int    `json:"http_port" yaml:"http_port"`                   // HTTP порт для health-проверок (9000)
	UseTLS    bool   `json:"use_tls" yaml:"use_tls"`
	AuthToken string `json:"auth_token,omitempty" yaml:"auth_token,omitempty"`
}

// PipelineConfig — настройки обработки данных (читается из файла)
type PipelineConfig struct {
	BufferSize     int        `json:"buffer_size" yaml:"buffer_size"`
	Workers        int        `json:"workers" yaml:"workers"`
	RetryAttempts  int        `json:"retry_attempts" yaml:"retry_attempts"`
	RetryDelay     int        `json:"retry_delay" yaml:"retry_delay"`           // миллисекунды
	MessageFormat  string     `json:"message_format" yaml:"message_format"`     // json, msgpack, protobuf
	TimestampField string     `json:"timestamp_field" yaml:"timestamp_field"`
	SymbolField    string     `json:"symbol_field" yaml:"symbol_field"`
	FieldMappings  []FieldMap `json:"field_mappings" yaml:"field_mappings"`
}

// FieldMap — определяет маппинг полей входящего сообщения на колонки QuestDB
type FieldMap struct {
	Source     string `json:"source" yaml:"source" validate:"required"`
	Target     string `json:"target" yaml:"target" validate:"required"`
	Type       string `json:"type" yaml:"type" validate:"required,oneof=string long double boolean timestamp symbol"`
	Required   bool   `json:"required" yaml:"required"`
	DefaultVal string `json:"default_val,omitempty" yaml:"default_val,omitempty"`
}

// FileConfig — полная конфигурация из файла (всё кроме ServiceConfig)
type FileConfig struct {
	MQTT     MQTTConfig     `json:"mqtt" yaml:"mqtt"`
	EventBus EventBusConfig `json:"event_bus" yaml:"event_bus"`
	QuestDB  QuestDBConfig  `json:"questdb" yaml:"questdb"`
	Pipeline PipelineConfig `json:"pipeline" yaml:"pipeline"`
}

// DefaultFileConfig возвращает конфигурацию файла по умолчанию
func DefaultFileConfig() *FileConfig {
	return &FileConfig{
		MQTT: MQTTConfig{
			Broker:     "localhost",
			Port:       1883,
			ClientID:   "data-pipeline-service",
			Topics:     []string{"sensors/#"},
			QoS:        1,
			CleanStart: true,
			KeepAlive:  60,
		},
		EventBus: EventBusConfig{
			Enabled:     true,
			Broker:      "localhost",
			Port:        1883,
			ClientID:    "data-pipeline-events",
			TopicPrefix: "events/data-pipeline",
			QoS:         1,
		},
		QuestDB: QuestDBConfig{
			Host:     "localhost",
			ILPPort:  9009,
			HTTPPort: 9000,
		},
		Pipeline: PipelineConfig{
			BufferSize:     10000,
			Workers:        4,
			RetryAttempts:  3,
			RetryDelay:     1000,
			MessageFormat:  "json",
			TimestampField: "timestamp",
			SymbolField:    "device_id",
			FieldMappings: []FieldMap{
				{Source: "device_id", Target: "device_id", Type: "symbol", Required: true},
				{Source: "value", Target: "value", Type: "double", Required: true},
				{Source: "timestamp", Target: "ts", Type: "timestamp", Required: true},
			},
		},
	}
}
