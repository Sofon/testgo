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

// Config holds logger configuration
type Config struct {
	Level      string `json:"level" yaml:"level"`
	Format     string `json:"format" yaml:"format"` // json or console
	OutputPath string `json:"output_path" yaml:"output_path"`
}

// Init initializes the global logger
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

// Get returns the global logger instance
func Get() *zap.Logger {
	if log == nil {
		// Initialize with default config if not initialized
		_ = Init(&Config{Level: "info", Format: "json"})
	}
	return log
}

// With creates a child logger with additional fields
func With(fields ...zap.Field) *zap.Logger {
	return Get().With(fields...)
}

// Info logs an info message
func Info(msg string, fields ...zap.Field) {
	Get().Info(msg, fields...)
}

// Debug logs a debug message
func Debug(msg string, fields ...zap.Field) {
	Get().Debug(msg, fields...)
}

// Warn logs a warning message
func Warn(msg string, fields ...zap.Field) {
	Get().Warn(msg, fields...)
}

// Error logs an error message
func Error(msg string, fields ...zap.Field) {
	Get().Error(msg, fields...)
}

// Fatal logs a fatal message and exits
func Fatal(msg string, fields ...zap.Field) {
	Get().Fatal(msg, fields...)
}

// Sync flushes any buffered log entries
func Sync() error {
	if log != nil {
		return log.Sync()
	}
	return nil
}
