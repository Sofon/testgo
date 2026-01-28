package api

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sofon/data-pipeline-service/pkg/logger"
	"go.uber.org/zap"
)

// ServerConfig — настройки HTTP сервера
type ServerConfig struct {
	Host         string `json:"host" yaml:"host"`
	Port         int    `json:"port" yaml:"port"`
	ReadTimeout  int    `json:"read_timeout" yaml:"read_timeout"`   // секунды
	WriteTimeout int    `json:"write_timeout" yaml:"write_timeout"` // секунды
	Mode         string `json:"mode" yaml:"mode"`                   // debug, release, test
}

// Server — HTTP сервер
type Server struct {
	config  *ServerConfig
	router  *gin.Engine
	server  *http.Server
	handler *Handler
}

// NewServer создаёт новый HTTP сервер
func NewServer(cfg *ServerConfig, service ServiceInterface) *Server {
	if cfg == nil {
		cfg = &ServerConfig{
			Host:         "0.0.0.0",
			Port:         8080,
			ReadTimeout:  30,
			WriteTimeout: 30,
			Mode:         "release",
		}
	}

	gin.SetMode(cfg.Mode)

	router := gin.New()
	router.Use(gin.Recovery())
	router.Use(RequestLogger())
	router.Use(CORS())

	handler := NewHandler(service)

	server := &Server{
		config:  cfg,
		router:  router,
		handler: handler,
	}

	server.setupRoutes()

	return server
}

func (s *Server) setupRoutes() {
	// Health/readiness проверки
	s.router.GET("/health", s.handler.HealthCheck)
	s.router.GET("/ready", s.handler.ReadinessCheck)
	s.router.GET("/live", s.handler.LivenessCheck)

	// API v1
	v1 := s.router.Group("/api/v1")
	{
		// Эндпоинты конфигурации
		v1.GET("/config", s.handler.GetConfig)                 // полный конфиг (ServiceConfig + FileConfig)
		v1.GET("/config/service", s.handler.GetServiceConfig)  // только ServiceConfig из MongoDB
		v1.PUT("/config", s.handler.UpdateConfig)              // обновить ServiceConfig
		v1.PATCH("/config", s.handler.PatchConfig)             // частично обновить ServiceConfig

		// Эндпоинт статуса
		v1.GET("/status", s.handler.GetStatus)

		// Эндпоинты управления
		v1.POST("/reload", s.handler.Reload) // перезагрузить ServiceConfig из MongoDB
	}
}

// Start запускает HTTP сервер
func (s *Server) Start() error {
	addr := fmt.Sprintf("%s:%d", s.config.Host, s.config.Port)

	s.server = &http.Server{
		Addr:         addr,
		Handler:      s.router,
		ReadTimeout:  time.Duration(s.config.ReadTimeout) * time.Second,
		WriteTimeout: time.Duration(s.config.WriteTimeout) * time.Second,
	}

	logger.Info("запуск HTTP сервера", zap.String("address", addr))

	if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("ошибка запуска HTTP сервера: %w", err)
	}

	return nil
}

// Shutdown выполняет graceful остановку сервера
func (s *Server) Shutdown(ctx context.Context) error {
	if s.server != nil {
		logger.Info("остановка HTTP сервера")
		return s.server.Shutdown(ctx)
	}
	return nil
}

// RequestLogger — middleware для логирования запросов
func RequestLogger() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		path := c.Request.URL.Path

		c.Next()

		latency := time.Since(start)
		status := c.Writer.Status()

		logger.Debug("http запрос",
			zap.String("method", c.Request.Method),
			zap.String("path", path),
			zap.Int("status", status),
			zap.Duration("latency", latency),
			zap.String("client_ip", c.ClientIP()),
		)
	}
}

// CORS — middleware для обработки CORS
func CORS() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Writer.Header().Set("Access-Control-Allow-Origin", "*")
		c.Writer.Header().Set("Access-Control-Allow-Credentials", "true")
		c.Writer.Header().Set("Access-Control-Allow-Headers", "Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization, accept, origin, Cache-Control, X-Requested-With")
		c.Writer.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS, GET, PUT, PATCH, DELETE")

		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(http.StatusNoContent)
			return
		}

		c.Next()
	}
}
