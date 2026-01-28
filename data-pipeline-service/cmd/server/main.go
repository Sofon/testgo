package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/sofon/data-pipeline-service/internal/api"
	"github.com/sofon/data-pipeline-service/internal/config"
	"github.com/sofon/data-pipeline-service/internal/service"
	"github.com/sofon/data-pipeline-service/internal/storage"
	"github.com/sofon/data-pipeline-service/pkg/logger"
	"go.uber.org/zap"
)

func main() {
	// Парсим флаги
	configPath := flag.String("config", "", "Путь к файлу конфигурации")
	flag.Parse()

	// Загружаем конфигурацию приложения из файла
	cfg, err := config.Load(*configPath)
	if err != nil {
		panic("ошибка загрузки конфига: " + err.Error())
	}

	// Валидируем конфигурацию
	if err := cfg.Validate(); err != nil {
		panic("неверная конфигурация: " + err.Error())
	}

	// Инициализируем логгер
	if err := logger.Init(&cfg.Logger); err != nil {
		panic("ошибка инициализации логгера: " + err.Error())
	}
	defer logger.Sync()

	logger.Info("запуск сервиса обработки данных",
		zap.String("version", service.Version),
	)

	// Создаём контекст с отменой
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Инициализируем хранилище для ServiceConfig (MongoDB + файловый fallback)
	store, err := storage.NewHybridStorage(&cfg.MongoDB, &cfg.File)
	if err != nil {
		logger.Fatal("ошибка создания хранилища", zap.Error(err))
	}

	// Подключаемся к MongoDB
	if err := store.Connect(ctx); err != nil {
		logger.Warn("подключение к MongoDB не удалось, используется файловое хранилище", zap.Error(err))
	}
	defer store.Disconnect(context.Background())

	// Создаём сервис с конфигурацией из файла (MQTT, QuestDB, EventBus, Pipeline)
	// ServiceConfig (batch_size, flush_interval, write_timeout, stream_mapping) загружается из MongoDB
	svc := service.NewService(store, &cfg.Pipeline)

	// Запускаем сервис
	if err := svc.Start(ctx); err != nil {
		logger.Fatal("ошибка запуска сервиса", zap.Error(err))
	}

	// Создаём и запускаем HTTP сервер
	server := api.NewServer(&cfg.Server, svc)

	// Запускаем сервер в горутине
	go func() {
		if err := server.Start(); err != nil {
			logger.Error("ошибка HTTP сервера", zap.Error(err))
			cancel()
		}
	}()

	logger.Info("сервис успешно запущен",
		zap.String("http_address", cfg.Server.Host),
		zap.Int("http_port", cfg.Server.Port),
	)

	// Ожидаем сигнал завершения
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	<-sigChan
	logger.Info("получен сигнал завершения")

	// Graceful shutdown
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	// Останавливаем HTTP сервер
	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("ошибка остановки HTTP сервера", zap.Error(err))
	}

	// Останавливаем сервис
	if err := svc.Stop(shutdownCtx); err != nil {
		logger.Error("ошибка остановки сервиса", zap.Error(err))
	}

	logger.Info("завершение работы")
}
