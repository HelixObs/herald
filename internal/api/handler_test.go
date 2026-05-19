package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

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

func (m *mockQuerier) QueryEntityOperations(_ context.Context, _ string) ([]db.EntityOperationRow, error) {
	return nil, nil
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

// ── Helpers for new tests ─────────────────────────────────────────────────────

// noopMetrics satisfies apiMetrics without recording anything.
type noopMetrics struct{}

func (n *noopMetrics) APIRequestRecord(_, _ string, _ time.Duration) {}

// silenceQuerier wraps mockQuerier and also satisfies api.SilenceStore.
type silenceQuerier struct {
	mockQuerier
	silences []db.Silence
	err      error
	nextID   int
}

func (s *silenceQuerier) CreateSilence(_ context.Context, si db.Silence) (int, error) {
	if s.err != nil {
		return 0, s.err
	}
	s.nextID++
	si.ID = s.nextID
	s.silences = append(s.silences, si)
	return si.ID, nil
}

func (s *silenceQuerier) DeleteSilence(_ context.Context, _ int) error {
	return s.err
}

func (s *silenceQuerier) ListSilences(_ context.Context, _ string) ([]db.Silence, error) {
	return s.silences, s.err
}

// errQuerier is a Querier that returns an error for QueryEntityOperations.
type errQuerier struct {
	mockQuerier
	opsErr error
}

func (e *errQuerier) QueryEntityOperations(_ context.Context, _ string) ([]db.EntityOperationRow, error) {
	return nil, e.opsErr
}

func silenceBody(instrumentID string, durationMinutes int, silencedBy string) *bytes.Buffer {
	body, _ := json.Marshal(map[string]any{
		"instrument_id":    instrumentID,
		"duration_minutes": durationMinutes,
		"silenced_by":      silencedBy,
	})
	return bytes.NewBuffer(body)
}

// ── Silence: create ───────────────────────────────────────────────────────────

func TestCreateSilenceOK(t *testing.T) {
	sq := &silenceQuerier{}
	h := api.New(sq, nil, &noopMetrics{})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/notifications/silence",
		silenceBody("CHIME", 60, "operator"))
	req.Header.Set("Content-Type", "application/json")
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Errorf("expected 201, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestCreateSilenceMissingFields(t *testing.T) {
	sq := &silenceQuerier{}
	h := api.New(sq, nil, &noopMetrics{})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/notifications/silence",
		bytes.NewBufferString(`{}`))
	req.Header.Set("Content-Type", "application/json")
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", rec.Code)
	}
}

func TestCreateSilenceInvalidJSON(t *testing.T) {
	sq := &silenceQuerier{}
	h := api.New(sq, nil, &noopMetrics{})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/notifications/silence",
		bytes.NewBufferString(`not-json`))
	req.Header.Set("Content-Type", "application/json")
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", rec.Code)
	}
}

func TestCreateSilenceDBError(t *testing.T) {
	sq := &silenceQuerier{err: errors.New("db error")}
	h := api.New(sq, nil, &noopMetrics{})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/notifications/silence",
		silenceBody("CHIME", 60, "operator"))
	req.Header.Set("Content-Type", "application/json")
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", rec.Code)
	}
}

// ── Silence: delete ───────────────────────────────────────────────────────────

func TestDeleteSilenceOK(t *testing.T) {
	sq := &silenceQuerier{}
	h := api.New(sq, nil, &noopMetrics{})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodDelete, "/api/v1/notifications/silence/1", nil)
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Errorf("expected 204, got %d", rec.Code)
	}
}

func TestDeleteSilenceInvalidID(t *testing.T) {
	sq := &silenceQuerier{}
	h := api.New(sq, nil, &noopMetrics{})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodDelete, "/api/v1/notifications/silence/notanint", nil)
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", rec.Code)
	}
}

func TestDeleteSilenceDBError(t *testing.T) {
	sq := &silenceQuerier{err: errors.New("delete failed")}
	h := api.New(sq, nil, &noopMetrics{})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodDelete, "/api/v1/notifications/silence/1", nil)
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", rec.Code)
	}
}

// ── Silence: list ─────────────────────────────────────────────────────────────

func TestListSilencesOK(t *testing.T) {
	sq := &silenceQuerier{
		silences: []db.Silence{
			{ID: 1, InstrumentID: "CHIME", SilencedBy: "ops", ExpiresAt: time.Now().Add(time.Hour)},
		},
	}
	h := api.New(sq, nil, &noopMetrics{})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/notifications/silences?instrument_id=CHIME", nil)
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	var out []db.Silence
	if err := json.NewDecoder(rec.Body).Decode(&out); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(out) == 0 {
		t.Error("expected at least one silence in response")
	}
}

func TestListSilencesMissingParam(t *testing.T) {
	sq := &silenceQuerier{}
	h := api.New(sq, nil, &noopMetrics{})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/notifications/silences", nil)
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", rec.Code)
	}
}

func TestListSilencesDBError(t *testing.T) {
	sq := &silenceQuerier{err: errors.New("list failed")}
	h := api.New(sq, nil, &noopMetrics{})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/notifications/silences?instrument_id=CHIME", nil)
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", rec.Code)
	}
}

// ── entityOperations ──────────────────────────────────────────────────────────

func TestEntityOperationsOK(t *testing.T) {
	h := api.New(&mockQuerier{}, nil, nil)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/entity/frb-1/operations", nil)
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
}

func TestEntityOperationsNilDB(t *testing.T) {
	h := api.New(nil, nil, nil)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/entity/frb-1/operations", nil)
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", rec.Code)
	}
}

func TestEntityOperationsDBError(t *testing.T) {
	eq := &errQuerier{opsErr: errors.New("ops db error")}
	h := api.New(eq, nil, nil)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/entity/frb-1/operations", nil)
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", rec.Code)
	}
}

// ── monitorPlots ──────────────────────────────────────────────────────────────

func TestMonitorPlots(t *testing.T) {
	h := api.New(&mockQuerier{}, nil, nil)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/monitor/plots", nil)
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	var out []map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&out); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(out) == 0 {
		t.Error("expected at least one plot config in response")
	}
}

// ── monitorBins parameter validation ─────────────────────────────────────────

func TestMonitorBinsMissingParams(t *testing.T) {
	h := api.New(&mockQuerier{}, nil, nil)
	rec := httptest.NewRecorder()
	// Missing all required params.
	req := httptest.NewRequest(http.MethodGet, "/api/v1/monitor/bins", nil)
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for missing params, got %d", rec.Code)
	}
}

func TestMonitorBinsUnknownPlot(t *testing.T) {
	h := api.New(&mockQuerier{}, nil, nil)
	rec := httptest.NewRecorder()
	// 10-minute window: non-zero from_ms so required-params check passes.
	req := httptest.NewRequest(http.MethodGet,
		"/api/v1/monitor/bins?plot=nonexistent&instrument=CHIME&from_ms=1000000&to_ms=1600000", nil)
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Errorf("expected 404 for unknown plot, got %d", rec.Code)
	}
}

func TestMonitorBinsWindowTooSmall(t *testing.T) {
	h := api.New(&mockQuerier{}, nil, nil)
	rec := httptest.NewRecorder()
	// from_ms=1000000, to_ms=1001000 → 1-second window, below the 5-minute minimum.
	req := httptest.NewRequest(http.MethodGet,
		"/api/v1/monitor/bins?plot=chime_dm_time&instrument=CHIME&from_ms=1000000&to_ms=1001000", nil)
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for window too small, got %d", rec.Code)
	}
}

func TestMonitorBinsWindowTooLarge(t *testing.T) {
	h := api.New(&mockQuerier{}, nil, nil)
	rec := httptest.NewRecorder()
	// from_ms=1000000, to_ms=1000000+90_000_000 → 25-hour window, above the 24-hour max.
	req := httptest.NewRequest(http.MethodGet,
		"/api/v1/monitor/bins?plot=chime_dm_time&instrument=CHIME&from_ms=1000000&to_ms=91000000", nil)
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for window too large, got %d", rec.Code)
	}
}

// ── mockMonitorStore ──────────────────────────────────────────────────────────

type mockMonitorStore struct {
	rows []db.RawEntityRow
	err  error
}

func (m *mockMonitorStore) QueryEntitiesRaw(_ context.Context, _ string, _, _ int64, _ string) ([]db.RawEntityRow, error) {
	return m.rows, m.err
}

// ── monitorBins success + error paths ─────────────────────────────────────────

func TestMonitorBinsNilStore(t *testing.T) {
	// Monitor store is nil — handler returns 500 after passing all validation.
	h := api.New(&mockQuerier{}, nil, &noopMetrics{})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet,
		"/api/v1/monitor/bins?plot=chime_dm_time&instrument=CHIME&from_ms=1000000&to_ms=1600000", nil)
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected 500 for nil monitor store, got %d", rec.Code)
	}
}

func TestMonitorBinsStoreError(t *testing.T) {
	ms := &mockMonitorStore{err: errors.New("db error")}
	h := api.New(&mockQuerier{}, ms, &noopMetrics{})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet,
		"/api/v1/monitor/bins?plot=chime_dm_time&instrument=CHIME&from_ms=1000000&to_ms=1600000", nil)
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected 500 for store error, got %d", rec.Code)
	}
}

func TestMonitorBinsSuccess(t *testing.T) {
	ms := &mockMonitorStore{rows: []db.RawEntityRow{}}
	h := api.New(&mockQuerier{}, ms, &noopMetrics{})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet,
		"/api/v1/monitor/bins?plot=chime_dm_time&instrument=CHIME&from_ms=1000000&to_ms=1600000", nil)
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "application/json") {
		t.Errorf("expected JSON content-type, got %q", ct)
	}
}

func TestMonitorBinsSuccessWithYOverrides(t *testing.T) {
	// y_min=100 → yMinActual override; y_max=5000 → yMaxActual override.
	ms := &mockMonitorStore{rows: []db.RawEntityRow{}}
	h := api.New(&mockQuerier{}, ms, nil)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet,
		"/api/v1/monitor/bins?plot=chime_dm_time&instrument=CHIME&from_ms=1000000&to_ms=1600000&y_min=100&y_max=5000", nil)
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestMonitorBinsClampExtremes(t *testing.T) {
	// t_bins=0 → clamp to min (1); y_bins=9999 → clamp to max (4096).
	// Request still returns 500 (nil monitor) but clamp branches are hit.
	h := api.New(&mockQuerier{}, nil, nil)
	for _, params := range []string{
		"t_bins=0&y_bins=100",    // clamp min branch for t_bins
		"t_bins=9999&y_bins=100", // clamp max branch for t_bins
	} {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet,
			"/api/v1/monitor/bins?plot=chime_dm_time&instrument=CHIME&from_ms=1000000&to_ms=1600000&"+params, nil)
		h.ServeHTTP(rec, req)
		// nil monitor → 500; the clamp calls happen before this check
		if rec.Code != http.StatusInternalServerError {
			t.Errorf("params=%s: expected 500 (nil monitor), got %d", params, rec.Code)
		}
	}
}

func TestMonitorBinsParseHelpers(t *testing.T) {
	h := api.New(&mockQuerier{}, nil, nil)

	// parseInt error: from_ms=bad → defaults to 0 → 400 missing params.
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet,
		"/api/v1/monitor/bins?plot=chime_dm_time&instrument=CHIME&from_ms=bad&to_ms=1600000", nil)
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for bad from_ms, got %d", rec.Code)
	}

	// parseFloat error: y_max=notafloat → defaults to 0 (request reaches nil monitor → 500).
	ms := &mockMonitorStore{rows: []db.RawEntityRow{}}
	h2 := api.New(&mockQuerier{}, ms, nil)
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet,
		"/api/v1/monitor/bins?plot=chime_dm_time&instrument=CHIME&from_ms=1000000&to_ms=1600000&y_max=notafloat", nil)
	h2.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("expected 200 even with unparseable y_max, got %d", rec.Code)
	}
}

// ── entityGraph / entityOperations with metrics ───────────────────────────────

func TestEntityGraphWithMetrics(t *testing.T) {
	g := &db.EntityGraph{Nodes: []db.GraphNode{}, Edges: []db.GraphEdge{}}
	h := api.New(&mockQuerier{graph: g}, nil, &noopMetrics{})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/entity/x/graph", nil)
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
}

func TestEntityGraphErrorWithMetrics(t *testing.T) {
	h := api.New(&mockQuerier{err: errors.New("db down")}, nil, &noopMetrics{})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/entity/x/graph", nil)
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", rec.Code)
	}
}

func TestEntityGraphNotFoundWithMetrics(t *testing.T) {
	h := api.New(&mockQuerier{graph: nil, err: nil}, nil, &noopMetrics{})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/entity/x/graph", nil)
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d", rec.Code)
	}
}

func TestEntityOperationsWithMetrics(t *testing.T) {
	h := api.New(&mockQuerier{}, nil, &noopMetrics{})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/entity/frb-1/operations", nil)
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
}

func TestEntityOperationsErrorWithMetrics(t *testing.T) {
	eq := &errQuerier{opsErr: errors.New("ops down")}
	h := api.New(eq, nil, &noopMetrics{})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/entity/frb-1/operations", nil)
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", rec.Code)
	}
}

func TestListSilencesWithMetrics(t *testing.T) {
	sq := &silenceQuerier{silences: []db.Silence{}}
	h := api.New(sq, nil, &noopMetrics{})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/notifications/silences?instrument_id=CHIME", nil)
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
}
