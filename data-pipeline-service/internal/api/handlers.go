package api

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sofon/data-pipeline-service/internal/models"
)

// Handler holds dependencies for HTTP handlers
type Handler struct {
	service ServiceInterface
}

// ServiceInterface defines methods that the service must implement for the API
type ServiceInterface interface {
	GetConfig() (*models.Config, error)
	UpdateConfig(update *models.ConfigUpdate) (*models.Config, error)
	GetStatus() *models.ServiceStatus
	IsHealthy() bool
	Reload() error
}

// NewHandler creates a new Handler instance
func NewHandler(service ServiceInterface) *Handler {
	return &Handler{
		service: service,
	}
}

// GetConfig handles GET /api/v1/config
func (h *Handler) GetConfig(c *gin.Context) {
	config, err := h.service.GetConfig()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error":   "failed to get config",
			"details": err.Error(),
		})
		return
	}

	// Mask sensitive fields
	configResponse := *config
	if configResponse.MQTT.Password != "" {
		configResponse.MQTT.Password = "********"
	}
	if configResponse.QuestDB.AuthToken != "" {
		configResponse.QuestDB.AuthToken = "********"
	}

	c.JSON(http.StatusOK, configResponse)
}

// UpdateConfig handles PUT /api/v1/config
func (h *Handler) UpdateConfig(c *gin.Context) {
	var update models.ConfigUpdate
	if err := c.ShouldBindJSON(&update); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error":   "invalid request body",
			"details": err.Error(),
		})
		return
	}

	config, err := h.service.UpdateConfig(&update)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error":   "failed to update config",
			"details": err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "config updated successfully",
		"version": config.Version,
	})
}

// PatchConfig handles PATCH /api/v1/config - partial update
func (h *Handler) PatchConfig(c *gin.Context) {
	var update models.ConfigUpdate
	if err := c.ShouldBindJSON(&update); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error":   "invalid request body",
			"details": err.Error(),
		})
		return
	}

	config, err := h.service.UpdateConfig(&update)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error":   "failed to update config",
			"details": err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "config patched successfully",
		"version": config.Version,
	})
}

// GetStatus handles GET /api/v1/status
func (h *Handler) GetStatus(c *gin.Context) {
	status := h.service.GetStatus()
	c.JSON(http.StatusOK, status)
}

// HealthCheck handles GET /health
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

// ReadinessCheck handles GET /ready
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

// LivenessCheck handles GET /live
func (h *Handler) LivenessCheck(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"status": "alive",
	})
}

// Reload handles POST /api/v1/reload - triggers config reload
func (h *Handler) Reload(c *gin.Context) {
	if err := h.service.Reload(); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error":   "failed to reload",
			"details": err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "service reloaded successfully",
	})
}
