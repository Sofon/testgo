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
	GetConfig() (*models.Config, error)
	UpdateConfig(update *models.ConfigUpdate) (*models.Config, error)
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

// GetConfig обрабатывает GET /api/v1/config
func (h *Handler) GetConfig(c *gin.Context) {
	config, err := h.service.GetConfig()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error":   "ошибка получения конфига",
			"details": err.Error(),
		})
		return
	}

	// Маскируем чувствительные поля
	configResponse := *config
	if configResponse.MQTT.Password != "" {
		configResponse.MQTT.Password = "********"
	}
	if configResponse.QuestDB.AuthToken != "" {
		configResponse.QuestDB.AuthToken = "********"
	}

	c.JSON(http.StatusOK, configResponse)
}

// UpdateConfig обрабатывает PUT /api/v1/config
func (h *Handler) UpdateConfig(c *gin.Context) {
	var update models.ConfigUpdate
	if err := c.ShouldBindJSON(&update); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error":   "неверное тело запроса",
			"details": err.Error(),
		})
		return
	}

	config, err := h.service.UpdateConfig(&update)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error":   "ошибка обновления конфига",
			"details": err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "конфиг успешно обновлён",
		"version": config.Version,
	})
}

// PatchConfig обрабатывает PATCH /api/v1/config — частичное обновление
func (h *Handler) PatchConfig(c *gin.Context) {
	var update models.ConfigUpdate
	if err := c.ShouldBindJSON(&update); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error":   "неверное тело запроса",
			"details": err.Error(),
		})
		return
	}

	config, err := h.service.UpdateConfig(&update)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error":   "ошибка обновления конфига",
			"details": err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "конфиг успешно обновлён",
		"version": config.Version,
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

// Reload обрабатывает POST /api/v1/reload — перезагрузка конфигурации
func (h *Handler) Reload(c *gin.Context) {
	if err := h.service.Reload(); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error":   "ошибка перезагрузки",
			"details": err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "сервис успешно перезагружен",
	})
}
