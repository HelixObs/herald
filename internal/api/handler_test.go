package api_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/HelixObs/gateway/internal/api"
	"github.com/HelixObs/gateway/internal/db"
)

func TestEntityGraphRouting(t *testing.T) {
	// Unregistered routes return 404 from the mux — no DB needed.
	h := api.New(nil)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/entity/", nil)
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d", rec.Code)
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
