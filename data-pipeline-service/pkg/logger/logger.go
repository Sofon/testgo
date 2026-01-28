package logger

import (
	"os"
	"sync"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	log  *zap.Logger
	once sync.Once
)

// Config — конфигурация логгера
type Config struct {
	Level      string `json:"level" yaml:"level"`
	Format     string `json:"format" yaml:"format"` // json или console
	OutputPath string `json:"output_path" yaml:"output_path"`
}

// Init инициализирует глобальный логгер
func Init(cfg *Config) error {
	var err error
	once.Do(func() {
		err = initLogger(cfg)
	})
	return err
}

func initLogger(cfg *Config) error {
	level := zapcore.InfoLevel
	if cfg != nil && cfg.Level != "" {
		if err := level.UnmarshalText([]byte(cfg.Level)); err != nil {
			return err
		}
	}

	encoderConfig := zapcore.EncoderConfig{
		TimeKey:        "timestamp",
		LevelKey:       "level",
		NameKey:        "logger",
		CallerKey:      "caller",
		FunctionKey:    zapcore.OmitKey,
		MessageKey:     "message",
		StacktraceKey:  "stacktrace",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.LowercaseLevelEncoder,
		EncodeTime:     zapcore.ISO8601TimeEncoder,
		EncodeDuration: zapcore.SecondsDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	}

	var encoder zapcore.Encoder
	if cfg != nil && cfg.Format == "console" {
		encoder = zapcore.NewConsoleEncoder(encoderConfig)
	} else {
		encoder = zapcore.NewJSONEncoder(encoderConfig)
	}

	var output zapcore.WriteSyncer
	if cfg != nil && cfg.OutputPath != "" && cfg.OutputPath != "stdout" {
		file, err := os.OpenFile(cfg.OutputPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return err
		}
		output = zapcore.AddSync(file)
	} else {
		output = zapcore.AddSync(os.Stdout)
	}

	core := zapcore.NewCore(encoder, output, level)
	log = zap.New(core, zap.AddCaller(), zap.AddStacktrace(zapcore.ErrorLevel))

	return nil
}

// Get возвращает глобальный экземпляр логгера
func Get() *zap.Logger {
	if log == nil {
		// Инициализируем с конфигом по умолчанию если не инициализирован
		_ = Init(&Config{Level: "info", Format: "json"})
	}
	return log
}

// With создаёт дочерний логгер с дополнительными полями
func With(fields ...zap.Field) *zap.Logger {
	return Get().With(fields...)
}

// Info логирует сообщение уровня info
func Info(msg string, fields ...zap.Field) {
	Get().Info(msg, fields...)
}

// Debug логирует сообщение уровня debug
func Debug(msg string, fields ...zap.Field) {
	Get().Debug(msg, fields...)
}

// Warn логирует сообщение уровня warn
func Warn(msg string, fields ...zap.Field) {
	Get().Warn(msg, fields...)
}

// Error логирует сообщение уровня error
func Error(msg string, fields ...zap.Field) {
	Get().Error(msg, fields...)
}

// Fatal логирует сообщение и завершает программу
func Fatal(msg string, fields ...zap.Field) {
	Get().Fatal(msg, fields...)
}

// Sync сбрасывает буферизованные записи лога
func Sync() error {
	if log != nil {
		return log.Sync()
	}
	return nil
}
