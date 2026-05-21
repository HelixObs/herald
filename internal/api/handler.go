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
}

// SilenceStore is the database interface for silence CRUD.
type SilenceStore interface {
	CreateSilence(ctx context.Context, s db.Silence) (int, error)
	DeleteSilence(ctx context.Context, id int) error
	ListSilences(ctx context.Context, instrumentID string) ([]db.Silence, error)
}

// apiMetrics is the subset of gateway metrics used by the API handler.
type apiMetrics interface {
	APIRequestRecord(handler, status string, dur time.Duration)
}

// Handler serves the HelixObs query API.
type Handler struct {
	db      Querier
	monitor MonitorStore
	silence SilenceStore
	m       apiMetrics
	mux     *http.ServeMux
}

// New registers all API routes and returns a ready Handler.
func New(d Querier, ms MonitorStore, m apiMetrics) *Handler {
	ss, _ := d.(SilenceStore) // db.Store implements SilenceStore; cast is safe
	h := &Handler{db: d, monitor: ms, silence: ss, m: m, mux: http.NewServeMux()}
	h.mux.HandleFunc("GET /api/v1/entity/{entity_id}/graph", h.entityGraph)
	h.mux.HandleFunc("GET /api/v1/entity/{entity_id}/operations", h.entityOperations)
	h.mux.HandleFunc("GET /api/v1/monitor/bins", h.monitorBins)
	h.mux.HandleFunc("GET /api/v1/monitor/plots", h.monitorPlots)
	h.mux.HandleFunc("POST /api/v1/notifications/silence", h.createSilence)
	h.mux.HandleFunc("DELETE /api/v1/notifications/silence/{id}", h.deleteSilence)
	h.mux.HandleFunc("GET /api/v1/notifications/silences", h.listSilences)
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

// monitorPlots returns all registered plot configs.
//
// GET /api/v1/monitor/plots
func (h *Handler) monitorPlots(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	if err := json.NewEncoder(w).Encode(monitor.AllConfigs()); err != nil {
		slog.Error("monitor plots encode failed", "error", err)
	}
}

// monitorBins bins entities into a 2D grid and returns populated cells.
//
// GET /api/v1/monitor/bins?plot=chime_dm_time&instrument=CHIME&from_ms=...&to_ms=...&t_bins=1200&y_bins=300&y_max=3000
func (h *Handler) monitorBins(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	status := "success"
	defer func() {
		if h.m != nil {
			h.m.APIRequestRecord("monitor_bins", status, time.Since(start))
		}
	}()

	q := r.URL.Query()
	plotName := q.Get("plot")
	instrument := q.Get("instrument")
	fromMs := parseInt(q.Get("from_ms"), 0)
	toMs := parseInt(q.Get("to_ms"), 0)
	tBins := clamp(parseInt(q.Get("t_bins"), 1200), 1, 4096)
	yBins := clamp(parseInt(q.Get("y_bins"), 300), 1, 4096)
	yMin := parseFloat(q.Get("y_min"), -1)
	yMax := parseFloat(q.Get("y_max"), 0)

	if plotName == "" || instrument == "" || fromMs == 0 || toMs == 0 {
		status = "bad_request"
		http.Error(w, "plot, instrument, from_ms, to_ms are required", http.StatusBadRequest)
		return
	}

	p := monitor.Get(plotName)
	if p == nil {
		status = "not_found"
		http.Error(w, "unknown plot", http.StatusNotFound)
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

	metaFilter, err := monitor.MetadataFilter(p)
	if err != nil {
		status = "error"
		slog.Error("monitor metadata filter error", "plot", plotName, "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	if h.monitor == nil {
		status = "error"
		http.Error(w, "monitor store not configured", http.StatusInternalServerError)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 8*time.Second)
	defer cancel()
	rows, err := h.monitor.QueryEntitiesRaw(ctx, instrument, fromNs, toNs, metaFilter)
	if err != nil {
		status = "error"
		if ctx.Err() != nil {
			slog.Warn("monitor bins timed out (pool likely saturated)", "plot", plotName)
			http.Error(w, "service temporarily unavailable", http.StatusServiceUnavailable)
		} else {
			slog.Error("monitor bins query failed", "plot", plotName, "error", err)
			http.Error(w, "internal error", http.StatusInternalServerError)
		}
		return
	}

	cfg := p.Config()
	bins, snrMax := monitor.Bin(p, cfg, rows, fromNs, toNs, tBins, yBins, yMin, yMax)

	yMinActual := cfg.YMin
	if yMin >= 0 && yMin < cfg.YMax {
		yMinActual = yMin
	}
	yMaxActual := cfg.YMax
	if yMax > yMinActual {
		yMaxActual = yMax
	}

	resp := monitor.BinsResponse{
		Bins:   bins,
		TBins:  tBins,
		YBins:  yBins,
		FromNs: fromNs,
		ToNs:   toNs,
		YMin:   yMinActual,
		YMax:   yMaxActual,
		SNRMax: snrMax,
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
