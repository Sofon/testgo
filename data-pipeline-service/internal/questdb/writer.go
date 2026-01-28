package questdb

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	qdb "github.com/questdb/go-questdb-client/v3"
	"github.com/sofon/data-pipeline-service/internal/models"
	"github.com/sofon/data-pipeline-service/pkg/logger"
	"go.uber.org/zap"
)

// Writer handles writing data to QuestDB
type Writer struct {
	sender        *qdb.LineSender
	config        *models.QuestDBConfig
	mu            sync.RWMutex
	connected     atomic.Bool
	rowsWritten   atomic.Int64
	writeErrors   atomic.Int64
	pendingRows   atomic.Int64
	lastWriteAt   atomic.Value // time.Time
	flushTicker   *time.Ticker
	stopChan      chan struct{}
}

// NewWriter creates a new QuestDB writer
func NewWriter(config *models.QuestDBConfig) (*Writer, error) {
	if config == nil {
		return nil, fmt.Errorf("QuestDB config is required")
	}

	return &Writer{
		config:   config,
		stopChan: make(chan struct{}),
	}, nil
}

// Connect establishes connection to QuestDB
func (w *Writer) Connect(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	address := fmt.Sprintf("%s:%d", w.config.Host, w.config.ILPPort)

	opts := []qdb.LineSenderOption{
		qdb.WithAddress(address),
	}

	if w.config.UseTLS {
		opts = append(opts, qdb.WithTls())
	}

	if w.config.AuthToken != "" {
		opts = append(opts, qdb.WithAuth(w.config.AuthToken, w.config.AuthToken))
	}

	sender, err := qdb.NewLineSender(ctx, opts...)
	if err != nil {
		return fmt.Errorf("failed to create QuestDB sender: %w", err)
	}

	w.sender = sender
	w.connected.Store(true)

	logger.Info("connected to QuestDB",
		zap.String("host", w.config.Host),
		zap.Int("port", w.config.ILPPort),
	)

	// Start auto-flush goroutine
	w.startAutoFlush()

	return nil
}

func (w *Writer) startAutoFlush() {
	if w.config.FlushInterval <= 0 {
		return
	}

	w.flushTicker = time.NewTicker(time.Duration(w.config.FlushInterval) * time.Millisecond)

	go func() {
		for {
			select {
			case <-w.flushTicker.C:
				if err := w.Flush(context.Background()); err != nil {
					logger.Warn("auto-flush failed", zap.Error(err))
				}
			case <-w.stopChan:
				return
			}
		}
	}()
}

// Write writes a row to QuestDB
func (w *Writer) Write(ctx context.Context, row *models.QuestDBRow) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if !w.connected.Load() || w.sender == nil {
		w.writeErrors.Add(1)
		return fmt.Errorf("not connected to QuestDB")
	}

	tableName := row.TableName
	if tableName == "" {
		tableName = w.config.TableName
	}

	// Start building the row
	w.sender.Table(tableName)

	// Add symbol (partition key)
	if row.Symbol != "" {
		w.sender.Symbol("symbol", row.Symbol)
	}

	// Add columns
	for key, value := range row.Columns {
		switch v := value.(type) {
		case string:
			w.sender.StringColumn(key, v)
		case int:
			w.sender.Int64Column(key, int64(v))
		case int64:
			w.sender.Int64Column(key, v)
		case float64:
			w.sender.Float64Column(key, v)
		case float32:
			w.sender.Float64Column(key, float64(v))
		case bool:
			w.sender.BoolColumn(key, v)
		case time.Time:
			w.sender.TimestampColumn(key, v)
		default:
			w.sender.StringColumn(key, fmt.Sprintf("%v", v))
		}
	}

	// Set timestamp
	ts := row.Timestamp
	if ts.IsZero() {
		ts = time.Now()
	}

	if err := w.sender.At(ctx, ts); err != nil {
		w.writeErrors.Add(1)
		return fmt.Errorf("failed to write row: %w", err)
	}

	w.pendingRows.Add(1)

	// Auto-flush if batch size reached
	if w.config.BatchSize > 0 && w.pendingRows.Load() >= int64(w.config.BatchSize) {
		return w.flushUnsafe(ctx)
	}

	return nil
}

// WriteRow writes a row using field mappings from config
func (w *Writer) WriteRow(ctx context.Context, data map[string]interface{}, mappings []models.FieldMap, tableName, timestampField, symbolField string) error {
	row := &models.QuestDBRow{
		TableName: tableName,
		Columns:   make(map[string]interface{}),
		Timestamp: time.Now(),
	}

	// Extract symbol
	if symbolField != "" {
		if sym, ok := data[symbolField]; ok {
			row.Symbol = fmt.Sprintf("%v", sym)
		}
	}

	// Extract timestamp
	if timestampField != "" {
		if ts, ok := data[timestampField]; ok {
			row.Timestamp = parseTimestamp(ts)
		}
	}

	// Map fields
	for _, mapping := range mappings {
		val, exists := data[mapping.Source]
		if !exists {
			if mapping.Required {
				return fmt.Errorf("required field %s not found", mapping.Source)
			}
			if mapping.DefaultVal != "" {
				val = mapping.DefaultVal
			} else {
				continue
			}
		}

		// Skip symbol and timestamp as they're handled separately
		if mapping.Source == symbolField || mapping.Source == timestampField {
			continue
		}

		// Convert to target type
		converted, err := convertValue(val, mapping.Type)
		if err != nil {
			if mapping.Required {
				return fmt.Errorf("failed to convert field %s: %w", mapping.Source, err)
			}
			continue
		}

		row.Columns[mapping.Target] = converted
	}

	return w.Write(ctx, row)
}

func parseTimestamp(val interface{}) time.Time {
	switch v := val.(type) {
	case time.Time:
		return v
	case int64:
		// Assume milliseconds
		return time.UnixMilli(v)
	case float64:
		return time.UnixMilli(int64(v))
	case string:
		// Try various formats
		formats := []string{
			time.RFC3339,
			time.RFC3339Nano,
			"2006-01-02T15:04:05Z",
			"2006-01-02 15:04:05",
		}
		for _, format := range formats {
			if t, err := time.Parse(format, v); err == nil {
				return t
			}
		}
	}
	return time.Now()
}

func convertValue(val interface{}, targetType string) (interface{}, error) {
	switch targetType {
	case "string":
		return fmt.Sprintf("%v", val), nil
	case "long":
		switch v := val.(type) {
		case int:
			return int64(v), nil
		case int64:
			return v, nil
		case float64:
			return int64(v), nil
		case string:
			var i int64
			_, err := fmt.Sscanf(v, "%d", &i)
			return i, err
		}
	case "double":
		switch v := val.(type) {
		case float64:
			return v, nil
		case float32:
			return float64(v), nil
		case int:
			return float64(v), nil
		case int64:
			return float64(v), nil
		case string:
			var f float64
			_, err := fmt.Sscanf(v, "%f", &f)
			return f, err
		}
	case "boolean":
		switch v := val.(type) {
		case bool:
			return v, nil
		case string:
			return v == "true" || v == "1", nil
		case int:
			return v != 0, nil
		}
	case "timestamp":
		return parseTimestamp(val), nil
	case "symbol":
		return fmt.Sprintf("%v", val), nil
	}
	return val, nil
}

// Flush sends all pending rows to QuestDB
func (w *Writer) Flush(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.flushUnsafe(ctx)
}

func (w *Writer) flushUnsafe(ctx context.Context) error {
	if w.sender == nil {
		return nil
	}

	pending := w.pendingRows.Load()
	if pending == 0 {
		return nil
	}

	if err := w.sender.Flush(ctx); err != nil {
		w.writeErrors.Add(1)
		return fmt.Errorf("failed to flush: %w", err)
	}

	w.rowsWritten.Add(pending)
	w.pendingRows.Store(0)
	w.lastWriteAt.Store(time.Now())

	logger.Debug("flushed rows to QuestDB", zap.Int64("rows", pending))

	return nil
}

// Close closes the connection to QuestDB
func (w *Writer) Close(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.flushTicker != nil {
		w.flushTicker.Stop()
	}
	close(w.stopChan)

	if w.sender != nil {
		// Flush remaining data
		if err := w.sender.Flush(ctx); err != nil {
			logger.Warn("failed to flush on close", zap.Error(err))
		}
		if err := w.sender.Close(ctx); err != nil {
			return fmt.Errorf("failed to close QuestDB connection: %w", err)
		}
		w.connected.Store(false)
		logger.Info("closed QuestDB connection")
	}

	return nil
}

// IsConnected returns the connection status
func (w *Writer) IsConnected() bool {
	return w.connected.Load()
}

// GetStatus returns the QuestDB writer status
func (w *Writer) GetStatus() models.QuestDBStatus {
	status := models.QuestDBStatus{
		Connected:   w.connected.Load(),
		Host:        fmt.Sprintf("%s:%d", w.config.Host, w.config.ILPPort),
		TableName:   w.config.TableName,
		RowsWritten: w.rowsWritten.Load(),
		WriteErrors: w.writeErrors.Load(),
		PendingRows: w.pendingRows.Load(),
	}

	if lastWrite := w.lastWriteAt.Load(); lastWrite != nil {
		status.LastWriteAt = lastWrite.(time.Time)
	}

	return status
}

// HealthCheck performs a health check on QuestDB
func (w *Writer) HealthCheck(ctx context.Context) error {
	if w.config.HTTPPort == 0 {
		return nil
	}

	url := fmt.Sprintf("http://%s:%d/exec?query=select+1", w.config.Host, w.config.HTTPPort)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("health check failed with status: %d", resp.StatusCode)
	}

	return nil
}

// ResetStats resets write counters
func (w *Writer) ResetStats() {
	w.rowsWritten.Store(0)
	w.writeErrors.Store(0)
}
