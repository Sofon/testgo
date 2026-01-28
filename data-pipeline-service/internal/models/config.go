package models

import (
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

// Config — конфигурация сервиса, хранящаяся в MongoDB
type Config struct {
	ID        primitive.ObjectID `bson:"_id,omitempty" json:"id,omitempty"`
	Version   int                `bson:"version" json:"version"`
	MQTT      MQTTConfig         `bson:"mqtt" json:"mqtt"`
	QuestDB   QuestDBConfig      `bson:"questdb" json:"questdb"`
	Pipeline  PipelineConfig     `bson:"pipeline" json:"pipeline"`
	CreatedAt time.Time          `bson:"created_at" json:"created_at"`
	UpdatedAt time.Time          `bson:"updated_at" json:"updated_at"`
}

// MQTTConfig — настройки подключения к EMQX/MQTT брокеру
type MQTTConfig struct {
	Broker      string   `bson:"broker" json:"broker" validate:"required"`
	Port        int      `bson:"port" json:"port" validate:"required,min=1,max=65535"`
	ClientID    string   `bson:"client_id" json:"client_id" validate:"required"`
	Username    string   `bson:"username" json:"username"`
	Password    string   `bson:"password" json:"password,omitempty"`
	Topics      []string `bson:"topics" json:"topics" validate:"required,min=1"`
	QoS         int      `bson:"qos" json:"qos" validate:"min=0,max=2"`
	CleanStart  bool     `bson:"clean_start" json:"clean_start"`
	KeepAlive   int      `bson:"keep_alive" json:"keep_alive"` // секунды
	UseTLS      bool     `bson:"use_tls" json:"use_tls"`
	TLSCertPath string   `bson:"tls_cert_path,omitempty" json:"tls_cert_path,omitempty"`
	TLSKeyPath  string   `bson:"tls_key_path,omitempty" json:"tls_key_path,omitempty"`
	TLSCAPath   string   `bson:"tls_ca_path,omitempty" json:"tls_ca_path,omitempty"`
}

// QuestDBConfig — настройки подключения к QuestDB
type QuestDBConfig struct {
	Host          string `bson:"host" json:"host" validate:"required"`
	ILPPort       int    `bson:"ilp_port" json:"ilp_port" validate:"required"` // порт InfluxDB Line Protocol (9009)
	HTTPPort      int    `bson:"http_port" json:"http_port"`                   // HTTP порт для health-проверок (9000)
	TableName     string `bson:"table_name" json:"table_name" validate:"required"`
	FlushInterval int    `bson:"flush_interval" json:"flush_interval"` // миллисекунды
	BatchSize     int    `bson:"batch_size" json:"batch_size"`
	UseTLS        bool   `bson:"use_tls" json:"use_tls"`
	AuthToken     string `bson:"auth_token,omitempty" json:"auth_token,omitempty"`
}

// PipelineConfig — настройки обработки данных
type PipelineConfig struct {
	BufferSize      int           `bson:"buffer_size" json:"buffer_size"`
	Workers         int           `bson:"workers" json:"workers"`
	RetryAttempts   int           `bson:"retry_attempts" json:"retry_attempts"`
	RetryDelay      int           `bson:"retry_delay" json:"retry_delay"` // миллисекунды
	MessageFormat   string        `bson:"message_format" json:"message_format"` // json, msgpack, protobuf
	TimestampField  string        `bson:"timestamp_field" json:"timestamp_field"`
	SymbolField     string        `bson:"symbol_field" json:"symbol_field"`
	FieldMappings   []FieldMap    `bson:"field_mappings" json:"field_mappings"`
}

// FieldMap — определяет маппинг полей входящего сообщения на колонки QuestDB
type FieldMap struct {
	Source      string `bson:"source" json:"source" validate:"required"`
	Target      string `bson:"target" json:"target" validate:"required"`
	Type        string `bson:"type" json:"type" validate:"required,oneof=string long double boolean timestamp symbol"`
	Required    bool   `bson:"required" json:"required"`
	DefaultVal  string `bson:"default_val,omitempty" json:"default_val,omitempty"`
}

// ConfigUpdate — запрос на частичное обновление конфигурации
type ConfigUpdate struct {
	MQTT     *MQTTConfig     `json:"mqtt,omitempty"`
	QuestDB  *QuestDBConfig  `json:"questdb,omitempty"`
	Pipeline *PipelineConfig `json:"pipeline,omitempty"`
}

// DefaultConfig возвращает конфигурацию по умолчанию
func DefaultConfig() *Config {
	return &Config{
		Version: 1,
		MQTT: MQTTConfig{
			Broker:     "localhost",
			Port:       1883,
			ClientID:   "data-pipeline-service",
			Topics:     []string{"sensors/#"},
			QoS:        1,
			CleanStart: true,
			KeepAlive:  60,
		},
		QuestDB: QuestDBConfig{
			Host:          "localhost",
			ILPPort:       9009,
			HTTPPort:      9000,
			TableName:     "sensor_data",
			FlushInterval: 1000,
			BatchSize:     1000,
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
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
}
