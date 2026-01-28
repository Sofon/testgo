package api

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sofon/data-pipeline-service/internal/models"
)

// Handler — содержит зависимости для HTTP обработчиков
type Handler struct {
	service ServiceInterface
}

// ServiceInterface — определяет методы, которые должен реализовать сервис для API
type ServiceInterface interface {
	GetServiceConfig() (*models.ServiceConfig, error)
	GetFileConfig() *models.FileConfig
	UpdateServiceConfig(update *models.ServiceConfigUpdate) (*models.ServiceConfig, error)
	GetStatus() *models.ServiceStatus
	IsHealthy() bool
	Reload() error
}

// NewHandler создаёт новый экземпляр Handler
func NewHandler(service ServiceInterface) *Handler {
	return &Handler{
		service: service,
	}
}

// ConfigResponse — ответ с полной конфигурацией
type ConfigResponse struct {
	ServiceConfig *models.ServiceConfig `json:"service_config"` // из MongoDB
	FileConfig    *models.FileConfig    `json:"file_config"`    // из файла
}

// GetConfig обрабатывает GET /api/v1/config
func (h *Handler) GetConfig(c *gin.Context) {
	serviceConfig, err := h.service.GetServiceConfig()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error":   "ошибка получения ServiceConfig",
			"details": err.Error(),
		})
		return
	}

	fileConfig := h.service.GetFileConfig()

	// Маскируем чувствительные поля
	maskedFileConfig := *fileConfig
	if maskedFileConfig.MQTT.Password != "" {
		maskedFileConfig.MQTT.Password = "********"
	}
	if maskedFileConfig.EventBus.Password != "" {
		maskedFileConfig.EventBus.Password = "********"
	}
	if maskedFileConfig.QuestDB.AuthToken != "" {
		maskedFileConfig.QuestDB.AuthToken = "********"
	}

	c.JSON(http.StatusOK, ConfigResponse{
		ServiceConfig: serviceConfig,
		FileConfig:    &maskedFileConfig,
	})
}

// GetServiceConfig обрабатывает GET /api/v1/config/service — только ServiceConfig из MongoDB
func (h *Handler) GetServiceConfig(c *gin.Context) {
	config, err := h.service.GetServiceConfig()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error":   "ошибка получения ServiceConfig",
			"details": err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, config)
}

// UpdateConfig обрабатывает PUT /api/v1/config — обновление ServiceConfig
func (h *Handler) UpdateConfig(c *gin.Context) {
	var update models.ServiceConfigUpdate
	if err := c.ShouldBindJSON(&update); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error":   "неверное тело запроса",
			"details": err.Error(),
		})
		return
	}

	config, err := h.service.UpdateServiceConfig(&update)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error":   "ошибка обновления конфига",
			"details": err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "ServiceConfig успешно обновлён",
		"version": config.Version,
		"config":  config,
	})
}

// PatchConfig обрабатывает PATCH /api/v1/config — частичное обновление ServiceConfig
func (h *Handler) PatchConfig(c *gin.Context) {
	var update models.ServiceConfigUpdate
	if err := c.ShouldBindJSON(&update); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error":   "неверное тело запроса",
			"details": err.Error(),
		})
		return
	}

	config, err := h.service.UpdateServiceConfig(&update)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error":   "ошибка обновления конфига",
			"details": err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "ServiceConfig успешно обновлён",
		"version": config.Version,
		"config":  config,
	})
}

// GetStatus обрабатывает GET /api/v1/status
func (h *Handler) GetStatus(c *gin.Context) {
	status := h.service.GetStatus()
	c.JSON(http.StatusOK, status)
}

// HealthCheck обрабатывает GET /health
func (h *Handler) HealthCheck(c *gin.Context) {
	healthy := h.service.IsHealthy()

	status := "healthy"
	httpStatus := http.StatusOK
	if !healthy {
		status = "unhealthy"
		httpStatus = http.StatusServiceUnavailable
	}

	c.JSON(httpStatus, models.HealthCheck{
		Status:    status,
		Timestamp: time.Now(),
	})
}

// ReadinessCheck обрабатывает GET /ready
func (h *Handler) ReadinessCheck(c *gin.Context) {
	healthy := h.service.IsHealthy()

	if !healthy {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"status": "not ready",
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status": "ready",
	})
}

// LivenessCheck обрабатывает GET /live
func (h *Handler) LivenessCheck(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"status": "alive",
	})
}

// Reload обрабатывает POST /api/v1/reload — перезагрузка ServiceConfig из MongoDB
func (h *Handler) Reload(c *gin.Context) {
	if err := h.service.Reload(); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error":   "ошибка перезагрузки",
			"details": err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "ServiceConfig успешно перезагружен",
	})
}
