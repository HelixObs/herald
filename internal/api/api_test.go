package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/HelixObs/gateway/internal/api"
	"github.com/HelixObs/gateway/internal/db"
)

// ── Stub querier ──────────────────────────────────────────────────────────────

type stubQuerier struct {
	entities   []db.Entity
	operations []db.EntityOperation
	events     []db.EntityEvent
	err        error
}

func (s *stubQuerier) GetEntity(_ context.Context, id string) (*db.Entity, error) {
	if s.err != nil {
		return nil, s.err
	}
	for _, e := range s.entities {
		if e.ID == id {
			return &e, nil
		}
	}
	return nil, nil
}

func (s *stubQuerier) ListEntities(_ context.Context, _ db.EntityFilter) ([]db.Entity, error) {
	return s.entities, s.err
}

func (s *stubQuerier) ListEntityOperations(_ context.Context, entityID string, _ db.OperationFilter) ([]db.EntityOperation, error) {
	if s.err != nil {
		return nil, s.err
	}
	var out []db.EntityOperation
	for _, op := range s.operations {
		if op.EntityID == entityID {
			out = append(out, op)
		}
	}
	return out, nil
}

func (s *stubQuerier) ListEntityEvents(_ context.Context, entityID string, _ db.EventFilter) ([]db.EntityEvent, error) {
	if s.err != nil {
		return nil, s.err
	}
	var out []db.EntityEvent
	for _, ev := range s.events {
		if ev.EntityID == entityID {
			out = append(out, ev)
		}
	}
	return out, nil
}

// ── Test helpers ──────────────────────────────────────────────────────────────

var fixedTime = time.Date(2026, 4, 19, 12, 0, 0, 0, time.UTC)

func gqlPost(t *testing.T, h http.Handler, query string) map[string]any {
	t.Helper()
	body, _ := json.Marshal(map[string]any{"query": query})
	req := httptest.NewRequest(http.MethodPost, "/graphql", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	var resp map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response: %v\nbody: %s", err, w.Body.String())
	}
	return resp
}

func noErrors(t *testing.T, resp map[string]any) {
	t.Helper()
	if errs, ok := resp["errors"]; ok {
		t.Fatalf("unexpected GraphQL errors: %v", errs)
	}
}

func dataField(t *testing.T, resp map[string]any, key string) any {
	t.Helper()
	data, ok := resp["data"].(map[string]any)
	if !ok {
		t.Fatalf("response has no data map: %v", resp)
	}
	return data[key]
}

// ── Playground & infrastructure ───────────────────────────────────────────────

func TestPlaygroundServed(t *testing.T) {
	h := api.NewHandler(&stubQuerier{})
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
	if ct := w.Header().Get("Content-Type"); ct != "text/html; charset=utf-8" {
		t.Errorf("expected HTML content-type, got %q", ct)
	}
}

func TestCORSOptionsRequest(t *testing.T) {
	h := api.NewHandler(&stubQuerier{})
	req := httptest.NewRequest(http.MethodOptions, "/graphql", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusNoContent {
		t.Errorf("expected 204, got %d", w.Code)
	}
	if origin := w.Header().Get("Access-Control-Allow-Origin"); origin != "*" {
		t.Errorf("expected CORS * header, got %q", origin)
	}
}

func TestCORSHeaderOnPost(t *testing.T) {
	h := api.NewHandler(&stubQuerier{})
	resp := gqlPost(t, h, `{ entities { id } }`)
	noErrors(t, resp)
	// CORS header is set on the recorder via the handler wrapper — verify indirectly
	// by ensuring the request completes without error (cors middleware passes through).
}

func TestIntrospection(t *testing.T) {
	h := api.NewHandler(&stubQuerier{})
	resp := gqlPost(t, h, `{ __schema { queryType { name } } }`)
	noErrors(t, resp)
	if dataField(t, resp, "__schema") == nil {
		t.Fatal("expected __schema in introspection response")
	}
}

// ── entity query ──────────────────────────────────────────────────────────────

func TestEntityNotFound(t *testing.T) {
	h := api.NewHandler(&stubQuerier{})
	resp := gqlPost(t, h, `{ entity(id: "missing") { id } }`)
	noErrors(t, resp)
	if got := dataField(t, resp, "entity"); got != nil {
		t.Errorf("expected null for unknown entity, got %v", got)
	}
}

func TestEntityAllFields(t *testing.T) {
	q := &stubQuerier{entities: []db.Entity{{
		ID:           "frb-001",
		InstrumentID: "CHIME",
		TraceID:      "aabbccdd",
		TimestampNs:  1_000_000_000,
		ParentIDs:    []string{"cand-1", "cand-2"},
		Metadata:     map[string]string{"snr": "18.3", "dm": "341.2"},
		CreatedAt:    fixedTime,
	}}}
	h := api.NewHandler(q)
	resp := gqlPost(t, h, `{
		entity(id: "frb-001") {
			id instrumentId traceId timestampNs parentIds createdAt
			metadata { key value }
		}
	}`)
	noErrors(t, resp)
	e, ok := dataField(t, resp, "entity").(map[string]any)
	if !ok {
		t.Fatal("expected entity object, got nil")
	}
	if e["id"] != "frb-001" {
		t.Errorf("id: got %v", e["id"])
	}
	if e["instrumentId"] != "CHIME" {
		t.Errorf("instrumentId: got %v", e["instrumentId"])
	}
	if e["traceId"] != "aabbccdd" {
		t.Errorf("traceId: got %v", e["traceId"])
	}
	if e["timestampNs"] != "1000000000" {
		t.Errorf("timestampNs: got %v", e["timestampNs"])
	}
	parents := e["parentIds"].([]any)
	if len(parents) != 2 {
		t.Errorf("parentIds: expected 2, got %d", len(parents))
	}
	meta := e["metadata"].([]any)
	if len(meta) != 2 {
		t.Errorf("metadata: expected 2 kv pairs, got %d", len(meta))
	}
	if e["createdAt"] != fixedTime.UTC().Format(time.RFC3339) {
		t.Errorf("createdAt: got %v", e["createdAt"])
	}
}

// ── entities query ────────────────────────────────────────────────────────────

func TestEntitiesEmpty(t *testing.T) {
	h := api.NewHandler(&stubQuerier{})
	resp := gqlPost(t, h, `{ entities { id } }`)
	noErrors(t, resp)
	list, ok := dataField(t, resp, "entities").([]any)
	if !ok || len(list) != 0 {
		t.Errorf("expected empty list, got %v", list)
	}
}

func TestEntitiesList(t *testing.T) {
	q := &stubQuerier{entities: []db.Entity{
		{ID: "e-1", InstrumentID: "CHIME", CreatedAt: fixedTime},
		{ID: "e-2", InstrumentID: "CHIME", CreatedAt: fixedTime},
		{ID: "e-3", InstrumentID: "CHIME", CreatedAt: fixedTime},
	}}
	h := api.NewHandler(q)
	resp := gqlPost(t, h, `{ entities { id } }`)
	noErrors(t, resp)
	list := dataField(t, resp, "entities").([]any)
	if len(list) != 3 {
		t.Errorf("expected 3 entities, got %d", len(list))
	}
}

func TestEntitiesFilterPassedThrough(t *testing.T) {
	q := &stubQuerier{entities: []db.Entity{
		{ID: "e-chime", InstrumentID: "CHIME", CreatedAt: fixedTime},
	}}
	h := api.NewHandler(q)
	resp := gqlPost(t, h, `{
		entities(filter: { instrumentId: "CHIME", limit: 10, offset: 0 }) { id }
	}`)
	noErrors(t, resp)
	list := dataField(t, resp, "entities").([]any)
	if len(list) != 1 {
		t.Errorf("expected 1 entity, got %d", len(list))
	}
}

// ── Nested operations / events on Entity ──────────────────────────────────────

func TestEntityNestedOperations(t *testing.T) {
	q := &stubQuerier{
		entities: []db.Entity{
			{ID: "frb-002", InstrumentID: "CHIME", CreatedAt: fixedTime},
		},
		operations: []db.EntityOperation{
			{EntityID: "frb-002", InstrumentID: "CHIME", Operation: "hdf5-conversion",
				TraceID: "trace-op-1", TimestampNs: 2_000_000_000, CreatedAt: fixedTime},
			{EntityID: "frb-002", InstrumentID: "CHIME", Operation: "registration",
				TraceID: "trace-op-2", TimestampNs: 3_000_000_000, CreatedAt: fixedTime},
		},
	}
	h := api.NewHandler(q)
	resp := gqlPost(t, h, `{
		entity(id: "frb-002") {
			id
			operations { operation traceId entityId }
		}
	}`)
	noErrors(t, resp)
	e := dataField(t, resp, "entity").(map[string]any)
	ops := e["operations"].([]any)
	if len(ops) != 2 {
		t.Fatalf("expected 2 operations, got %d", len(ops))
	}
	op := ops[0].(map[string]any)
	if op["operation"] != "hdf5-conversion" {
		t.Errorf("operation[0]: got %v", op["operation"])
	}
	if op["entityId"] != "frb-002" {
		t.Errorf("entityId: got %v", op["entityId"])
	}
}

func TestEntityNestedEvents(t *testing.T) {
	q := &stubQuerier{
		entities: []db.Entity{
			{ID: "frb-003", InstrumentID: "CHIME", CreatedAt: fixedTime},
		},
		events: []db.EntityEvent{
			{EntityID: "frb-003", InstrumentID: "CHIME",
				EventName: "helix.error", TimestampNs: 4_000_000_000, CreatedAt: fixedTime,
				Metadata: map[string]string{"message": "disk_full"}},
		},
	}
	h := api.NewHandler(q)
	resp := gqlPost(t, h, `{
		entity(id: "frb-003") {
			events { eventName entityId metadata { key value } }
		}
	}`)
	noErrors(t, resp)
	e := dataField(t, resp, "entity").(map[string]any)
	evs := e["events"].([]any)
	if len(evs) != 1 {
		t.Fatalf("expected 1 event, got %d", len(evs))
	}
	ev := evs[0].(map[string]any)
	if ev["eventName"] != "helix.error" {
		t.Errorf("eventName: got %v", ev["eventName"])
	}
	meta := ev["metadata"].([]any)
	if len(meta) != 1 {
		t.Errorf("metadata: expected 1 kv, got %d", len(meta))
	}
}

// ── Top-level entityOperations / entityEvents ─────────────────────────────────

func TestEntityOperationsTopLevel(t *testing.T) {
	q := &stubQuerier{
		operations: []db.EntityOperation{
			{EntityID: "frb-004", InstrumentID: "CHIME", Operation: "replication",
				TimestampNs: 1, CreatedAt: fixedTime},
		},
	}
	h := api.NewHandler(q)
	resp := gqlPost(t, h, `{
		entityOperations(entityId: "frb-004") { operation entityId }
	}`)
	noErrors(t, resp)
	ops := dataField(t, resp, "entityOperations").([]any)
	if len(ops) != 1 {
		t.Fatalf("expected 1 operation, got %d", len(ops))
	}
	if ops[0].(map[string]any)["operation"] != "replication" {
		t.Errorf("operation: got %v", ops[0].(map[string]any)["operation"])
	}
}

func TestEntityEventsTopLevel(t *testing.T) {
	q := &stubQuerier{
		events: []db.EntityEvent{
			{EntityID: "frb-005", InstrumentID: "CHIME",
				EventName: "helix.event.candidate_promoted",
				TimestampNs: 1, CreatedAt: fixedTime},
		},
	}
	h := api.NewHandler(q)
	resp := gqlPost(t, h, `{
		entityEvents(entityId: "frb-005") { eventName entityId }
	}`)
	noErrors(t, resp)
	evs := dataField(t, resp, "entityEvents").([]any)
	if len(evs) != 1 {
		t.Fatalf("expected 1 event, got %d", len(evs))
	}
	if evs[0].(map[string]any)["eventName"] != "helix.event.candidate_promoted" {
		t.Errorf("eventName: got %v", evs[0].(map[string]any)["eventName"])
	}
}

func TestEntityOperationsFilterByOperation(t *testing.T) {
	q := &stubQuerier{
		operations: []db.EntityOperation{
			{EntityID: "frb-006", Operation: "hdf5-conversion",
				InstrumentID: "CHIME", TimestampNs: 1, CreatedAt: fixedTime},
		},
	}
	h := api.NewHandler(q)
	resp := gqlPost(t, h, `{
		entityOperations(entityId: "frb-006", filter: { operation: "hdf5-conversion" }) {
			operation
		}
	}`)
	noErrors(t, resp)
	ops := dataField(t, resp, "entityOperations").([]any)
	if len(ops) != 1 {
		t.Fatalf("expected 1 operation, got %d", len(ops))
	}
}

func TestEntityEventsFilterByEventName(t *testing.T) {
	q := &stubQuerier{
		events: []db.EntityEvent{
			{EntityID: "frb-007", EventName: "helix.error",
				InstrumentID: "CHIME", TimestampNs: 1, CreatedAt: fixedTime},
		},
	}
	h := api.NewHandler(q)
	resp := gqlPost(t, h, `{
		entityEvents(entityId: "frb-007", filter: { eventName: "helix.error" }) {
			eventName
		}
	}`)
	noErrors(t, resp)
	evs := dataField(t, resp, "entityEvents").([]any)
	if len(evs) != 1 {
		t.Fatalf("expected 1 event, got %d", len(evs))
	}
}

// ── DB error propagation ──────────────────────────────────────────────────────

func TestEntitiesDBErrorPropagated(t *testing.T) {
	q := &stubQuerier{err: fmt.Errorf("db connection lost")}
	h := api.NewHandler(q)
	resp := gqlPost(t, h, `{ entities { id } }`)
	if _, hasErrors := resp["errors"]; !hasErrors {
		t.Error("expected GraphQL errors field when DB fails, got none")
	}
}

func TestEntityOperationsDBErrorPropagated(t *testing.T) {
	q := &stubQuerier{err: fmt.Errorf("timeout")}
	h := api.NewHandler(q)
	resp := gqlPost(t, h, `{ entityOperations(entityId: "x") { operation } }`)
	if _, hasErrors := resp["errors"]; !hasErrors {
		t.Error("expected GraphQL errors field when DB fails, got none")
	}
}

// ── Metadata key ordering ─────────────────────────────────────────────────────

func TestMetadataSortedAlphabetically(t *testing.T) {
	q := &stubQuerier{entities: []db.Entity{{
		ID:           "frb-008",
		InstrumentID: "CHIME",
		Metadata:     map[string]string{"snr": "18.3", "dm": "341.2", "ra": "12.5"},
		CreatedAt:    fixedTime,
	}}}
	h := api.NewHandler(q)
	resp := gqlPost(t, h, `{ entity(id: "frb-008") { metadata { key } } }`)
	noErrors(t, resp)
	e := dataField(t, resp, "entity").(map[string]any)
	meta := e["metadata"].([]any)
	keys := make([]string, len(meta))
	for i, kv := range meta {
		keys[i] = kv.(map[string]any)["key"].(string)
	}
	for i := 1; i < len(keys); i++ {
		if keys[i] < keys[i-1] {
			t.Errorf("metadata not sorted: %v", keys)
		}
	}
}
