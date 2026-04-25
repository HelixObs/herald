package api_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/HelixObs/gateway/internal/api"
	"github.com/HelixObs/gateway/internal/db"
)

// mockQuerier satisfies api.Querier without a real DB.
type mockQuerier struct {
	graph *db.EntityGraph
	err   error
}

func (m *mockQuerier) QueryEntityGraph(_ context.Context, _ string, _ int) (*db.EntityGraph, error) {
	return m.graph, m.err
}

func TestEntityGraphRouting(t *testing.T) {
	// Unregistered routes return 404 from the mux — no DB needed.
	h := api.New(nil, nil, nil)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/entity/", nil)
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d", rec.Code)
	}
}

func TestEntityGraphNotFound(t *testing.T) {
	h := api.New(&mockQuerier{graph: nil, err: nil}, nil, nil)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/entity/unknown/graph", nil)
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d", rec.Code)
	}
}

func TestEntityGraphOK(t *testing.T) {
	g := &db.EntityGraph{
		Nodes: []db.GraphNode{{
			ID:           "frb-1",
			InstrumentID: "CHIME",
			ParentIDs:    []string{"cand-1"},
			Metadata:     map[string]string{"snr": "18.3"},
		}},
		Edges: []db.GraphEdge{{Source: "cand-1", Target: "frb-1"}},
	}
	h := api.New(&mockQuerier{graph: g}, nil, nil)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/entity/frb-1/graph", nil)
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "application/json") {
		t.Errorf("expected application/json Content-Type, got %q", ct)
	}

	var out db.EntityGraph
	if err := json.NewDecoder(rec.Body).Decode(&out); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(out.Nodes) != 1 || out.Nodes[0].ID != "frb-1" {
		t.Errorf("unexpected nodes: %+v", out.Nodes)
	}
	if len(out.Edges) != 1 || out.Edges[0].Source != "cand-1" {
		t.Errorf("unexpected edges: %+v", out.Edges)
	}
}

func TestEntityGraphDBError(t *testing.T) {
	h := api.New(&mockQuerier{err: errors.New("db failure")}, nil, nil)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/entity/any/graph", nil)
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", rec.Code)
	}
}

func TestEntityGraphCORSHeader(t *testing.T) {
	g := &db.EntityGraph{Nodes: []db.GraphNode{}, Edges: []db.GraphEdge{}}
	h := api.New(&mockQuerier{graph: g}, nil, nil)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/entity/x/graph", nil)
	h.ServeHTTP(rec, req)
	if rec.Header().Get("Access-Control-Allow-Origin") != "*" {
		t.Error("expected Access-Control-Allow-Origin: *")
	}
}

func TestGraphNodeJSONShape(t *testing.T) {
	// Verify the JSON shape the UI expects.
	g := db.EntityGraph{
		Nodes: []db.GraphNode{{
			ID:           "frb-1",
			InstrumentID: "CHIME",
			ParentIDs:    []string{"cand-1"},
			Metadata:     map[string]string{"snr": "18.3"},
			HasError:     false,
		}},
		Edges: []db.GraphEdge{{Source: "cand-1", Target: "frb-1"}},
	}
	b, err := json.Marshal(g)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out map[string]any
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if _, ok := out["nodes"]; !ok {
		t.Error("missing nodes key")
	}
	if _, ok := out["edges"]; !ok {
		t.Error("missing edges key")
	}
}
