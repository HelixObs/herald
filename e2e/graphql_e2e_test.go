//go:build e2e

package e2e_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	collectortracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

// ── GraphQL helpers ───────────────────────────────────────────────────────────

func gqlQuery(t *testing.T, query string) map[string]any {
	t.Helper()
	body, _ := json.Marshal(map[string]any{"query": query})
	resp, err := http.Post(apiURL()+"/graphql", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("graphql POST: %v", err)
	}
	defer resp.Body.Close()
	var result map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode graphql response: %v", err)
	}
	return result
}

func gqlData(t *testing.T, resp map[string]any, field string) any {
	t.Helper()
	if errs, ok := resp["errors"]; ok {
		t.Fatalf("GraphQL errors: %v", errs)
	}
	data, ok := resp["data"].(map[string]any)
	if !ok {
		t.Fatalf("no data in response: %v", resp)
	}
	return data[field]
}

// ── OTLP helpers for operations and events ────────────────────────────────────

func sendOperationSpan(t *testing.T, entityID, instrumentID, operation string) {
	t.Helper()
	sendRawSpanFull(t, map[string]string{
		"helix.entity.id":           entityID,
		"helix.instrument.id":       instrumentID,
		"helix.entity.is_operation": "true",
	}, operation, nil)
}

func sendHelixSpanWithEvent(t *testing.T, entityID, instrumentID, stage, eventName string, eventMeta map[string]string) {
	t.Helper()
	attrs := map[string]string{
		"helix.entity.id":     entityID,
		"helix.instrument.id": instrumentID,
	}
	pbAttrs := make([]*commonpb.KeyValue, 0, len(attrs))
	for k, v := range attrs {
		pbAttrs = append(pbAttrs, strKV(k, v))
	}

	evAttrs := make([]*commonpb.KeyValue, 0, len(eventMeta))
	for k, v := range eventMeta {
		evAttrs = append(evAttrs, strKV(k, v))
	}

	var traceID [16]byte
	var spanID [8]byte
	rand.Read(traceID[:])
	rand.Read(spanID[:])

	exportSpan(t, &tracepb.Span{
		TraceId:           traceID[:],
		SpanId:            spanID[:],
		Name:              stage,
		StartTimeUnixNano: uint64(time.Now().UnixNano()),
		EndTimeUnixNano:   uint64(time.Now().UnixNano() + 1_000_000),
		Attributes:        pbAttrs,
		Events: []*tracepb.Span_Event{{
			Name:         eventName,
			TimeUnixNano: uint64(time.Now().UnixNano()),
			Attributes:   evAttrs,
		}},
	})
}

func sendRawSpanFull(t *testing.T, attrs map[string]string, name string, events []*tracepb.Span_Event) {
	t.Helper()
	pbAttrs := make([]*commonpb.KeyValue, 0, len(attrs))
	for k, v := range attrs {
		pbAttrs = append(pbAttrs, strKV(k, v))
	}
	var traceID [16]byte
	var spanID [8]byte
	rand.Read(traceID[:])
	rand.Read(spanID[:])
	exportSpan(t, &tracepb.Span{
		TraceId:           traceID[:],
		SpanId:            spanID[:],
		Name:              name,
		StartTimeUnixNano: uint64(time.Now().UnixNano()),
		EndTimeUnixNano:   uint64(time.Now().UnixNano() + 1_000_000),
		Attributes:        pbAttrs,
		Events:            events,
	})
}

func exportSpan(t *testing.T, span *tracepb.Span) {
	t.Helper()
	conn, err := grpc.NewClient(gatewayAddr(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc dial: %v", err)
	}
	defer conn.Close()

	client := collectortracepb.NewTraceServiceClient(conn)
	req := &collectortracepb.ExportTraceServiceRequest{
		ResourceSpans: []*tracepb.ResourceSpans{{
			Resource:   &resourcepb.Resource{},
			ScopeSpans: []*tracepb.ScopeSpans{{Spans: []*tracepb.Span{span}}},
		}},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := client.Export(ctx, req); err != nil {
		t.Fatalf("Export: %v", err)
	}
}

func strKV(k, v string) *commonpb.KeyValue {
	return &commonpb.KeyValue{
		Key:   k,
		Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: v}},
	}
}

// ── GraphQL e2e tests ─────────────────────────────────────────────────────────

func TestGraphQLEntityQuery(t *testing.T) {
	entityID := fmt.Sprintf("gql-entity-%d", rand.Int63())
	sendHelixSpan(t, entityID, "CHIME", "correlator", nil)

	// Wait for the entity to reach the DB before querying GraphQL.
	conn := mustConnectDB(t)
	defer conn.Close(context.Background())
	pollUntil(t, 10*time.Second, func() bool {
		var id string
		return conn.QueryRow(context.Background(),
			"SELECT id FROM entities WHERE id = $1", entityID).Scan(&id) == nil
	}, "entity %q never appeared in DB", entityID)

	resp := gqlQuery(t, fmt.Sprintf(`{
		entity(id: %q) { id instrumentId traceId parentIds createdAt }
	}`, entityID))

	e, ok := gqlData(t, resp, "entity").(map[string]any)
	if !ok {
		t.Fatal("expected entity object, got nil")
	}
	if e["id"] != entityID {
		t.Errorf("id: got %v, want %q", e["id"], entityID)
	}
	if e["instrumentId"] != "CHIME" {
		t.Errorf("instrumentId: got %v", e["instrumentId"])
	}
	if e["traceId"] == "" {
		t.Error("expected non-empty traceId")
	}
	if e["createdAt"] == "" {
		t.Error("expected non-empty createdAt")
	}
}

func TestGraphQLEntityNotFound(t *testing.T) {
	resp := gqlQuery(t, `{ entity(id: "does-not-exist-gql-xyz") { id } }`)
	got := gqlData(t, resp, "entity")
	if got != nil {
		t.Errorf("expected null for unknown entity, got %v", got)
	}
}

func TestGraphQLEntitiesFilterByInstrument(t *testing.T) {
	instrument := fmt.Sprintf("GQL-TEST-INST-%d", rand.Int63())
	id1 := fmt.Sprintf("gql-list-e1-%d", rand.Int63())
	id2 := fmt.Sprintf("gql-list-e2-%d", rand.Int63())

	sendHelixSpan(t, id1, instrument, "correlator", nil)
	sendHelixSpan(t, id2, instrument, "correlator", nil)

	conn := mustConnectDB(t)
	defer conn.Close(context.Background())
	pollUntil(t, 10*time.Second, func() bool {
		var count int
		conn.QueryRow(context.Background(),
			"SELECT COUNT(*) FROM entities WHERE instrument_id = $1", instrument,
		).Scan(&count)
		return count >= 2
	}, "entities with instrument %q never appeared", instrument)

	resp := gqlQuery(t, fmt.Sprintf(`{
		entities(filter: { instrumentId: %q, limit: 10 }) { id instrumentId }
	}`, instrument))

	list, ok := gqlData(t, resp, "entities").([]any)
	if !ok {
		t.Fatal("expected entities array")
	}
	if len(list) < 2 {
		t.Errorf("expected at least 2 entities, got %d", len(list))
	}
	for _, item := range list {
		e := item.(map[string]any)
		if e["instrumentId"] != instrument {
			t.Errorf("unexpected instrumentId: %v", e["instrumentId"])
		}
	}
}

func TestGraphQLEntityWithParents(t *testing.T) {
	parentID := fmt.Sprintf("gql-parent-%d", rand.Int63())
	childID := fmt.Sprintf("gql-child-%d", rand.Int63())

	sendHelixSpan(t, parentID, "CHIME", "x-engine", nil)
	sendHelixSpan(t, childID, "CHIME", "classifier", []string{parentID})

	conn := mustConnectDB(t)
	defer conn.Close(context.Background())
	pollUntil(t, 10*time.Second, func() bool {
		var parentIDs []string
		err := conn.QueryRow(context.Background(),
			"SELECT parent_ids FROM entities WHERE id = $1", childID).Scan(&parentIDs)
		if err != nil {
			return false
		}
		for _, pid := range parentIDs {
			if pid == parentID {
				return true
			}
		}
		return false
	}, "child entity %q never got parent %q", childID, parentID)

	resp := gqlQuery(t, fmt.Sprintf(`{
		entity(id: %q) { id parentIds }
	}`, childID))

	e := gqlData(t, resp, "entity").(map[string]any)
	parents := e["parentIds"].([]any)
	found := false
	for _, p := range parents {
		if p.(string) == parentID {
			found = true
		}
	}
	if !found {
		t.Errorf("parentIds %v does not contain %q", parents, parentID)
	}
}

func TestGraphQLEntityOperationsQuery(t *testing.T) {
	entityID := fmt.Sprintf("gql-ops-entity-%d", rand.Int63())
	sendHelixSpan(t, entityID, "CHIME", "clustering", nil)
	sendOperationSpan(t, entityID, "CHIME", "hdf5-conversion")
	sendOperationSpan(t, entityID, "CHIME", "registration")

	conn := mustConnectDB(t)
	defer conn.Close(context.Background())
	pollUntil(t, 10*time.Second, func() bool {
		var count int
		conn.QueryRow(context.Background(),
			"SELECT COUNT(*) FROM entity_operations WHERE entity_id = $1", entityID).Scan(&count)
		return count >= 2
	}, "operations for entity %q never appeared", entityID)

	resp := gqlQuery(t, fmt.Sprintf(`{
		entityOperations(entityId: %q) { operation entityId traceId createdAt }
	}`, entityID))

	ops, ok := gqlData(t, resp, "entityOperations").([]any)
	if !ok {
		t.Fatal("expected entityOperations array")
	}
	if len(ops) < 2 {
		t.Fatalf("expected at least 2 operations, got %d", len(ops))
	}
	names := make([]string, len(ops))
	for i, op := range ops {
		o := op.(map[string]any)
		names[i] = o["operation"].(string)
		if o["entityId"] != entityID {
			t.Errorf("operation entityId: got %v, want %q", o["entityId"], entityID)
		}
		if o["traceId"] == "" {
			t.Error("expected non-empty traceId on operation")
		}
	}
	if !strings.Contains(strings.Join(names, ","), "hdf5-conversion") {
		t.Errorf("hdf5-conversion not found in operations: %v", names)
	}
}

func TestGraphQLEntityEventsQuery(t *testing.T) {
	entityID := fmt.Sprintf("gql-events-entity-%d", rand.Int63())
	sendHelixSpanWithEvent(t, entityID, "CHIME", "classifier", "helix.error",
		map[string]string{"message": "timeout"})

	conn := mustConnectDB(t)
	defer conn.Close(context.Background())
	pollUntil(t, 10*time.Second, func() bool {
		var count int
		conn.QueryRow(context.Background(),
			"SELECT COUNT(*) FROM entity_events WHERE entity_id = $1 AND event_name = 'helix.error'",
			entityID).Scan(&count)
		return count >= 1
	}, "helix.error event for %q never appeared", entityID)

	resp := gqlQuery(t, fmt.Sprintf(`{
		entityEvents(entityId: %q) { eventName entityId metadata { key value } createdAt }
	}`, entityID))

	evs, ok := gqlData(t, resp, "entityEvents").([]any)
	if !ok {
		t.Fatal("expected entityEvents array")
	}
	if len(evs) == 0 {
		t.Fatal("expected at least 1 event, got 0")
	}
	ev := evs[0].(map[string]any)
	if ev["eventName"] != "helix.error" {
		t.Errorf("eventName: got %v", ev["eventName"])
	}
	if ev["entityId"] != entityID {
		t.Errorf("entityId: got %v", ev["entityId"])
	}
	meta := ev["metadata"].([]any)
	found := false
	for _, kv := range meta {
		m := kv.(map[string]any)
		if m["key"] == "message" && m["value"] == "timeout" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected metadata key=message value=timeout, got %v", meta)
	}
}

func TestGraphQLNestedOperationsAndEvents(t *testing.T) {
	entityID := fmt.Sprintf("gql-nested-%d", rand.Int63())
	sendHelixSpan(t, entityID, "CHIME", "clustering", nil)
	sendOperationSpan(t, entityID, "CHIME", "replication")
	sendHelixSpanWithEvent(t, entityID, "CHIME", "clustering", "helix.event.candidate_promoted",
		map[string]string{"classification": "FRB"})

	conn := mustConnectDB(t)
	defer conn.Close(context.Background())
	pollUntil(t, 10*time.Second, func() bool {
		var opCount, evCount int
		conn.QueryRow(context.Background(),
			"SELECT COUNT(*) FROM entity_operations WHERE entity_id = $1", entityID).Scan(&opCount)
		conn.QueryRow(context.Background(),
			"SELECT COUNT(*) FROM entity_events WHERE entity_id = $1", entityID).Scan(&evCount)
		return opCount >= 1 && evCount >= 1
	}, "nested data for %q never fully appeared", entityID)

	// Single round-trip fetches entity + its operations + its events.
	resp := gqlQuery(t, fmt.Sprintf(`{
		entity(id: %q) {
			id
			operations { operation }
			events { eventName }
		}
	}`, entityID))

	e := gqlData(t, resp, "entity").(map[string]any)
	if e["id"] != entityID {
		t.Errorf("id: got %v", e["id"])
	}
	ops := e["operations"].([]any)
	if len(ops) == 0 {
		t.Error("expected at least 1 nested operation")
	}
	evs := e["events"].([]any)
	if len(evs) == 0 {
		t.Error("expected at least 1 nested event")
	}
}
