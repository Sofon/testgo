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
	// Parse flags
	configPath := flag.String("config", "", "Path to config file")
	flag.Parse()

	// Load application config
	cfg, err := config.Load(*configPath)
	if err != nil {
		panic("failed to load config: " + err.Error())
	}

	// Validate config
	if err := cfg.Validate(); err != nil {
		panic("invalid config: " + err.Error())
	}

	// Initialize logger
	if err := logger.Init(&cfg.Logger); err != nil {
		panic("failed to initialize logger: " + err.Error())
	}
	defer logger.Sync()

	logger.Info("starting data pipeline service",
		zap.String("version", service.Version),
	)

	// Create context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize storage
	store, err := storage.NewHybridStorage(&cfg.MongoDB, &cfg.File)
	if err != nil {
		logger.Fatal("failed to create storage", zap.Error(err))
	}

	// Connect to MongoDB
	if err := store.Connect(ctx); err != nil {
		logger.Warn("MongoDB connection failed, using file storage", zap.Error(err))
	}
	defer store.Disconnect(context.Background())

	// Create service
	svc := service.NewService(store)

	// Start service
	if err := svc.Start(ctx); err != nil {
		logger.Fatal("failed to start service", zap.Error(err))
	}

	// Create and start HTTP server
	server := api.NewServer(&cfg.Server, svc)

	// Start server in goroutine
	go func() {
		if err := server.Start(); err != nil {
			logger.Error("HTTP server error", zap.Error(err))
			cancel()
		}
	}()

	logger.Info("service started successfully",
		zap.String("http_address", cfg.Server.Host),
		zap.Int("http_port", cfg.Server.Port),
	)

	// Wait for shutdown signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	<-sigChan
	logger.Info("received shutdown signal")

	// Graceful shutdown
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	// Stop HTTP server
	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("HTTP server shutdown error", zap.Error(err))
	}

	// Stop service
	if err := svc.Stop(shutdownCtx); err != nil {
		logger.Error("service shutdown error", zap.Error(err))
	}

	logger.Info("shutdown complete")
}
