// Package api serves the HelixObs HTTP query API on a dedicated port.
// It is intentionally separate from the Prometheus metrics server so the
// two can be configured with different access controls.
package api

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/HelixObs/herald/internal/db"
	"github.com/HelixObs/herald/internal/monitor"
)

// Querier is the database interface the Handler depends on.
type Querier interface {
	QueryEntityGraph(ctx context.Context, entityID string, maxDepth int) (*db.EntityGraph, error)
	QueryEntityOperations(ctx context.Context, entityID string) ([]db.EntityOperationRow, error)
}

// MonitorStore is the database interface for the monitor binning API.
type MonitorStore interface {
	QueryEntitiesRaw(ctx context.Context, instrument string, fromNs, toNs int64, metaFilter string) ([]db.RawEntityRow, error)
	QueryMetadataKeys(ctx context.Context, instrument string) ([]string, error)
}

// SilenceStore is the database interface for silence CRUD.
type SilenceStore interface {
	CreateSilence(ctx context.Context, s db.Silence) (int, error)
	DeleteSilence(ctx context.Context, id int) error
	ListSilences(ctx context.Context, instrumentID string) ([]db.Silence, error)
}

// AlertStore is the database interface for querying active alerts.
type AlertStore interface {
	QueryAlerts(ctx context.Context, instrumentID string) ([]db.AlertRow, error)
	QueryInstruments(ctx context.Context) ([]string, error)
}

// apiMetrics is the subset of herald metrics used by the API handler.
type apiMetrics interface {
	APIRequestRecord(handler, status string, dur time.Duration)
}

// Handler serves the HelixObs query API.
type Handler struct {
	db      Querier
	monitor MonitorStore
	silence SilenceStore
	alerts  AlertStore
	m       apiMetrics
	mux     *http.ServeMux
}

// New registers all API routes and returns a ready Handler.
func New(d Querier, ms MonitorStore, m apiMetrics) *Handler {
	ss, _ := d.(SilenceStore) // db.Store implements SilenceStore; cast is safe
	as, _ := d.(AlertStore)   // db.Store implements AlertStore; cast is safe
	h := &Handler{db: d, monitor: ms, silence: ss, alerts: as, m: m, mux: http.NewServeMux()}
	h.mux.HandleFunc("GET /api/v1/entity/{entity_id}/graph", h.entityGraph)
	h.mux.HandleFunc("GET /api/v1/entity/{entity_id}/operations", h.entityOperations)
	h.mux.HandleFunc("GET /api/v1/monitor/bins", h.monitorBins)
	h.mux.HandleFunc("GET /api/v1/monitor/fields", h.monitorFields)
	h.mux.HandleFunc("POST /api/v1/notifications/silence", h.createSilence)
	h.mux.HandleFunc("DELETE /api/v1/notifications/silence/{id}", h.deleteSilence)
	h.mux.HandleFunc("GET /api/v1/notifications/silences", h.listSilences)
	h.mux.HandleFunc("GET /api/v1/notifications/alerts", h.listAlerts)
	h.mux.HandleFunc("GET /api/v1/instruments", h.listInstruments)
	return h
}

// createSilence creates a new silence rule.
//
// POST /api/v1/notifications/silence
// Body: { instrument_id, event_type?, fingerprint?, duration_minutes, silenced_by, reason? }
func (h *Handler) createSilence(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	var req struct {
		InstrumentID    string `json:"instrument_id"`
		EventType       string `json:"event_type"`
		Fingerprint     string `json:"fingerprint"`
		DurationMinutes int    `json:"duration_minutes"`
		SilencedBy      string `json:"silenced_by"`
		Reason          string `json:"reason"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		h.m.APIRequestRecord("create_silence", "400", time.Since(start))
		return
	}
	if req.InstrumentID == "" || req.DurationMinutes <= 0 || req.SilencedBy == "" {
		http.Error(w, "instrument_id, duration_minutes, and silenced_by are required", http.StatusBadRequest)
		h.m.APIRequestRecord("create_silence", "400", time.Since(start))
		return
	}

	silence := db.Silence{
		InstrumentID: req.InstrumentID,
		EventType:    req.EventType,
		Fingerprint:  req.Fingerprint,
		SilencedBy:   req.SilencedBy,
		ExpiresAt:    time.Now().Add(time.Duration(req.DurationMinutes) * time.Minute),
		Reason:       req.Reason,
	}
	id, err := h.silence.CreateSilence(r.Context(), silence)
	if err != nil {
		slog.Error("create silence failed", "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		h.m.APIRequestRecord("create_silence", "500", time.Since(start))
		return
	}
	silence.ID = id
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(silence) //nolint:errcheck
	h.m.APIRequestRecord("create_silence", "201", time.Since(start))
}

// deleteSilence removes a silence rule by ID.
//
// DELETE /api/v1/notifications/silence/{id}
func (h *Handler) deleteSilence(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	id, err := strconv.Atoi(r.PathValue("id"))
	if err != nil {
		http.Error(w, "invalid id", http.StatusBadRequest)
		h.m.APIRequestRecord("delete_silence", "400", time.Since(start))
		return
	}
	if err := h.silence.DeleteSilence(r.Context(), id); err != nil {
		slog.Error("delete silence failed", "id", id, "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		h.m.APIRequestRecord("delete_silence", "500", time.Since(start))
		return
	}
	w.WriteHeader(http.StatusNoContent)
	h.m.APIRequestRecord("delete_silence", "204", time.Since(start))
}

// listSilences returns all silences for an instrument.
//
// GET /api/v1/notifications/silences?instrument_id=CHIMEFRB
func (h *Handler) listSilences(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	instrumentID := r.URL.Query().Get("instrument_id")
	if instrumentID == "" {
		http.Error(w, "instrument_id query param required", http.StatusBadRequest)
		h.m.APIRequestRecord("list_silences", "400", time.Since(start))
		return
	}
	silences, err := h.silence.ListSilences(r.Context(), instrumentID)
	if err != nil {
		slog.Error("list silences failed", "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		h.m.APIRequestRecord("list_silences", "500", time.Since(start))
		return
	}
	if silences == nil {
		silences = []db.Silence{}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(silences) //nolint:errcheck
	h.m.APIRequestRecord("list_silences", "200", time.Since(start))
}

// listInstruments returns all distinct instrument IDs that have sent data.
//
// GET /api/v1/instruments
func (h *Handler) listInstruments(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	if h.alerts == nil {
		http.Error(w, "store not configured", http.StatusInternalServerError)
		h.m.APIRequestRecord("list_instruments", "500", time.Since(start))
		return
	}
	instruments, err := h.alerts.QueryInstruments(r.Context())
	if err != nil {
		slog.Error("list instruments failed", "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		h.m.APIRequestRecord("list_instruments", "500", time.Since(start))
		return
	}
	if instruments == nil {
		instruments = []string{}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(instruments) //nolint:errcheck
	h.m.APIRequestRecord("list_instruments", "200", time.Since(start))
}

// listAlerts returns grouped helix.error events from the last 7 days.
//
// GET /api/v1/notifications/alerts?instrument_id=CHIMEFRB
func (h *Handler) listAlerts(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	instrumentID := r.URL.Query().Get("instrument_id")
	if instrumentID == "" {
		http.Error(w, "instrument_id query param required", http.StatusBadRequest)
		h.m.APIRequestRecord("list_alerts", "400", time.Since(start))
		return
	}
	if h.alerts == nil {
		http.Error(w, "alerts store not configured", http.StatusInternalServerError)
		h.m.APIRequestRecord("list_alerts", "500", time.Since(start))
		return
	}
	alerts, err := h.alerts.QueryAlerts(r.Context(), instrumentID)
	if err != nil {
		slog.Error("list alerts failed", "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		h.m.APIRequestRecord("list_alerts", "500", time.Since(start))
		return
	}
	if alerts == nil {
		alerts = []db.AlertRow{}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(alerts) //nolint:errcheck
	h.m.APIRequestRecord("list_alerts", "200", time.Since(start))
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mux.ServeHTTP(w, r)
}

// entityGraph returns the provenance DAG for a single entity as JSON.
//
// GET /api/v1/entity/{entity_id}/graph
func (h *Handler) entityGraph(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	status := "success"
	defer func() {
		if h.m != nil {
			h.m.APIRequestRecord("entity_graph", status, time.Since(start))
		}
	}()

	entityID := r.PathValue("entity_id")
	if entityID == "" {
		status = "bad_request"
		http.Error(w, "missing entity_id", http.StatusBadRequest)
		return
	}

	graph, err := h.db.QueryEntityGraph(r.Context(), entityID, 10)
	if err != nil {
		status = "error"
		slog.Error("entity graph query failed", "entity_id", entityID, "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	if graph == nil {
		status = "not_found"
		http.Error(w, "entity not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	if err := json.NewEncoder(w).Encode(graph); err != nil {
		slog.Error("entity graph encode failed", "entity_id", entityID, "error", err)
	}
}

// entityOperations returns all operations for a single entity as a JSON array.
//
// GET /api/v1/entity/{entity_id}/operations
func (h *Handler) entityOperations(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	status := "success"
	defer func() {
		if h.m != nil {
			h.m.APIRequestRecord("entity_operations", status, time.Since(start))
		}
	}()

	entityID := r.PathValue("entity_id")
	if entityID == "" {
		status = "bad_request"
		http.Error(w, "missing entity_id", http.StatusBadRequest)
		return
	}

	if h.db == nil {
		status = "error"
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	ops, err := h.db.QueryEntityOperations(r.Context(), entityID)
	if err != nil {
		status = "error"
		slog.Error("entity operations query failed", "entity_id", entityID, "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	if ops == nil {
		ops = []db.EntityOperationRow{}
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	if err := json.NewEncoder(w).Encode(ops); err != nil {
		slog.Error("entity operations encode failed", "entity_id", entityID, "error", err)
	}
}

// monitorFields returns distinct metadata keys seen in the last 24 h for an instrument.
//
// GET /api/v1/monitor/fields?instrument=CHIMEFRB
func (h *Handler) monitorFields(w http.ResponseWriter, r *http.Request) {
	instrument := r.URL.Query().Get("instrument")
	if instrument == "" {
		http.Error(w, "instrument is required", http.StatusBadRequest)
		return
	}
	if h.monitor == nil {
		http.Error(w, "monitor store not configured", http.StatusInternalServerError)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	keys, err := h.monitor.QueryMetadataKeys(ctx, instrument)
	if err != nil {
		slog.Error("monitor fields query failed", "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	if keys == nil {
		keys = []string{}
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	if err := json.NewEncoder(w).Encode(keys); err != nil {
		slog.Error("monitor fields encode failed", "error", err)
	}
}

// monitorBins bins entities into a 2D grid and returns populated cells.
//
// GET /api/v1/monitor/bins?y_field=dm&weight_field=snr&instrument=CHIMEFRB&from_ms=...&to_ms=...
// &t_bins=1200&y_bins=300&y_min=0&y_max=3000
func (h *Handler) monitorBins(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	status := "success"
	defer func() {
		if h.m != nil {
			h.m.APIRequestRecord("monitor_bins", status, time.Since(start))
		}
	}()

	q := r.URL.Query()
	yField := q.Get("y_field")
	weightField := q.Get("weight_field")
	instrument := q.Get("instrument")
	fromMs := parseInt(q.Get("from_ms"), 0)
	toMs := parseInt(q.Get("to_ms"), 0)
	tBins := clamp(parseInt(q.Get("t_bins"), 1200), 1, 4096)
	yBins := clamp(parseInt(q.Get("y_bins"), 300), 1, 4096)
	yMin := parseFloat(q.Get("y_min"), -1)
	yMax := parseFloat(q.Get("y_max"), 0)
	yMinDefault := parseFloat(q.Get("y_min_default"), 0)
	yMaxDefault := parseFloat(q.Get("y_max_default"), 1000)

	if yField == "" || instrument == "" || fromMs == 0 || toMs == 0 {
		status = "bad_request"
		http.Error(w, "y_field, instrument, from_ms, to_ms are required", http.StatusBadRequest)
		return
	}
	if !monitor.ValidateFieldName(yField) {
		status = "bad_request"
		http.Error(w, "invalid y_field", http.StatusBadRequest)
		return
	}
	if weightField != "" && !monitor.ValidateFieldName(weightField) {
		status = "bad_request"
		http.Error(w, "invalid weight_field", http.StatusBadRequest)
		return
	}

	fromNs := fromMs * 1_000_000
	toNs := toMs * 1_000_000
	windowNs := toNs - fromNs
	if windowNs < monitor.MinWindowNs {
		status = "bad_request"
		http.Error(w, "time window too small (min 5 minutes)", http.StatusBadRequest)
		return
	}
	if windowNs > monitor.MaxWindowNs {
		status = "bad_request"
		http.Error(w, "time window too large (max 24 hours)", http.StatusBadRequest)
		return
	}

	if h.monitor == nil {
		status = "error"
		http.Error(w, "monitor store not configured", http.StatusInternalServerError)
		return
	}

	// Require the y_field key to exist in metadata — allows TimescaleDB chunk pruning.
	metaFilter := "metadata ? '" + yField + "'"

	ctx, cancel := context.WithTimeout(r.Context(), 8*time.Second)
	defer cancel()
	rows, err := h.monitor.QueryEntitiesRaw(ctx, instrument, fromNs, toNs, metaFilter)
	if err != nil {
		status = "error"
		if ctx.Err() != nil {
			slog.Warn("monitor bins timed out", "y_field", yField)
			http.Error(w, "service temporarily unavailable", http.StatusServiceUnavailable)
		} else {
			slog.Error("monitor bins query failed", "y_field", yField, "error", err)
			http.Error(w, "internal error", http.StatusInternalServerError)
		}
		return
	}

	bins, weightMax := monitor.Bin(rows, yField, weightField, yMinDefault, yMaxDefault, fromNs, toNs, tBins, yBins, yMin, yMax)

	yMinActual := yMinDefault
	if yMin >= 0 && yMin < yMaxDefault {
		yMinActual = yMin
	}
	yMaxActual := yMaxDefault
	if yMax > yMinActual {
		yMaxActual = yMax
	}

	resp := monitor.BinsResponse{
		Bins:      bins,
		TBins:     tBins,
		YBins:     yBins,
		FromNs:    fromNs,
		ToNs:      toNs,
		YMin:      yMinActual,
		YMax:      yMaxActual,
		WeightMax: weightMax,
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		slog.Error("monitor bins encode failed", "error", err)
	}
}

func parseInt(s string, def int64) int64 {
	if s == "" {
		return def
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return def
	}
	return v
}

func parseFloat(s string, def float64) float64 {
	if s == "" {
		return def
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return def
	}
	return v
}

func clamp(v, min, max int64) int {
	if v < min {
		return int(min)
	}
	if v > max {
		return int(max)
	}
	return int(v)
}
