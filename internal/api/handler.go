// Package api serves the HelixObs HTTP query API on a dedicated port.
// It is intentionally separate from the Prometheus metrics server so the
// two can be configured with different access controls.
package api

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/HelixObs/gateway/internal/db"
)

// Querier is the database interface the Handler depends on.
type Querier interface {
	QueryEntityGraph(ctx context.Context, entityID string, maxDepth int) (*db.EntityGraph, error)
}

// apiMetrics is the subset of gateway metrics used by the API handler.
type apiMetrics interface {
	APIRequestRecord(handler, status string, dur time.Duration)
}

// Handler serves the HelixObs query API.
type Handler struct {
	db  Querier
	m   apiMetrics
	mux *http.ServeMux
}

// New registers all API routes and returns a ready Handler.
func New(d Querier, m apiMetrics) *Handler {
	h := &Handler{db: d, m: m, mux: http.NewServeMux()}
	h.mux.HandleFunc("GET /api/v1/entity/{entity_id}/graph", h.entityGraph)
	return h
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mux.ServeHTTP(w, r)
}

// entityGraph returns the provenance DAG for a single entity as JSON.
// The response is a node-link structure ready for Cytoscape.js.
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
