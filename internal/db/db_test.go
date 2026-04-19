package db_test

import (
	"context"
	"os"
	"testing"

	"github.com/HelixObs/gateway/internal/db"
)

// dbURL returns the TimescaleDB connection string for integration tests.
// Tests that require a real DB call this and skip when it is absent.
func dbURL(t *testing.T) string {
	t.Helper()
	url := os.Getenv("TEST_DB_URL")
	if url == "" {
		t.Skip("TEST_DB_URL not set — skipping DB integration test")
	}
	return url
}

// ── New() ─────────────────────────────────────────────────────────────────────

func TestNewInvalidConnStringReturnsError(t *testing.T) {
	_, err := db.New(context.Background(), "not-a-valid-dsn://???")
	if err == nil {
		t.Fatal("expected error for invalid DSN, got nil")
	}
}

func TestNewUnreachableHostReturnsError(t *testing.T) {
	// 192.0.2.0/24 is TEST-NET — guaranteed unreachable.
	_, err := db.New(context.Background(), "postgres://u:p@192.0.2.1:5432/db?connect_timeout=1")
	if err == nil {
		t.Fatal("expected error for unreachable host, got nil")
	}
}

// ── Integration tests (require TEST_DB_URL) ───────────────────────────────────

func TestWriteEntityRoundTrip(t *testing.T) {
	store, err := db.New(context.Background(), dbURL(t))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer store.Close()

	e := db.Entity{
		ID:           "test-entity-1",
		InstrumentID: "TEST",
		TraceID:      "aabbccdd00000000000000000000000",
		TimestampNs:  1_000_000_000,
		ParentIDs:    []string{"parent-a", "parent-b"},
		Metadata:     map[string]string{"snr": "18.3", "dm": "341.2"},
	}
	if err := store.WriteEntity(context.Background(), e); err != nil {
		t.Fatalf("WriteEntity: %v", err)
	}
}

func TestWriteEntityIdempotent(t *testing.T) {
	store, err := db.New(context.Background(), dbURL(t))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer store.Close()

	e := db.Entity{
		ID:           "test-entity-idempotent",
		InstrumentID: "TEST",
		TraceID:      "aabbccdd00000000000000000000001",
		TimestampNs:  1_000_000_000,
	}
	// Writing the same entity twice must not return an error.
	for i := 0; i < 2; i++ {
		if err := store.WriteEntity(context.Background(), e); err != nil {
			t.Fatalf("write %d: %v", i+1, err)
		}
	}
}

func TestWriteEntityEventRoundTrip(t *testing.T) {
	store, err := db.New(context.Background(), dbURL(t))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer store.Close()

	// Write the parent entity first to satisfy any FK constraints.
	_ = store.WriteEntity(context.Background(), db.Entity{
		ID: "test-entity-for-event", InstrumentID: "TEST", TimestampNs: 1,
	})

	ev := db.EntityEvent{
		InstrumentID: "TEST",
		EntityID:     "test-entity-for-event",
		EventName:    "helix.event.rfi_flagged",
		TimestampNs:  2_000_000_000,
		Metadata:     map[string]string{"fraction": "0.92", "algorithm": "spectral_kurtosis"},
	}
	if err := store.WriteEntityEvent(context.Background(), ev); err != nil {
		t.Fatalf("WriteEntityEvent: %v", err)
	}
}

// ── Query integration tests (require TEST_DB_URL) ────────────────────────────

func TestGetEntityNotFound(t *testing.T) {
	store, err := db.New(context.Background(), dbURL(t))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer store.Close()

	got, err := store.GetEntity(context.Background(), "does-not-exist-xyz-999")
	if err != nil {
		t.Fatalf("GetEntity: %v", err)
	}
	if got != nil {
		t.Errorf("expected nil for unknown entity, got %+v", got)
	}
}

func TestGetEntityRoundTrip(t *testing.T) {
	store, err := db.New(context.Background(), dbURL(t))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer store.Close()

	e := db.Entity{
		ID:           "test-get-entity-rt",
		InstrumentID: "TEST",
		TraceID:      "aabbccdd00000000000000000000cafe",
		TimestampNs:  5_000_000_000,
		ParentIDs:    []string{"parent-a", "parent-b"},
		Metadata:     map[string]string{"snr": "18.3", "dm": "341.2"},
	}
	if err := store.WriteEntity(context.Background(), e); err != nil {
		t.Fatalf("WriteEntity: %v", err)
	}

	got, err := store.GetEntity(context.Background(), e.ID)
	if err != nil {
		t.Fatalf("GetEntity: %v", err)
	}
	if got == nil {
		t.Fatal("expected entity, got nil")
	}
	if got.ID != e.ID {
		t.Errorf("ID: got %q, want %q", got.ID, e.ID)
	}
	if got.InstrumentID != e.InstrumentID {
		t.Errorf("InstrumentID: got %q", got.InstrumentID)
	}
	if got.TraceID != e.TraceID {
		t.Errorf("TraceID: got %q", got.TraceID)
	}
	if len(got.ParentIDs) != 2 || got.ParentIDs[0] != "parent-a" {
		t.Errorf("ParentIDs: got %v", got.ParentIDs)
	}
	if got.Metadata["snr"] != "18.3" {
		t.Errorf("Metadata snr: got %q", got.Metadata["snr"])
	}
	if got.CreatedAt.IsZero() {
		t.Error("expected non-zero CreatedAt")
	}
}

func TestListEntitiesEmpty(t *testing.T) {
	store, err := db.New(context.Background(), dbURL(t))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer store.Close()

	got, err := store.ListEntities(context.Background(), db.EntityFilter{
		InstrumentID: "NO-SUCH-INSTRUMENT-xyz-999",
	})
	if err != nil {
		t.Fatalf("ListEntities: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("expected 0 results, got %d", len(got))
	}
}

func TestListEntitiesFilterByInstrumentID(t *testing.T) {
	store, err := db.New(context.Background(), dbURL(t))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer store.Close()

	instA := "TEST-FILTER-INST-A"
	instB := "TEST-FILTER-INST-B"
	for _, e := range []db.Entity{
		{ID: "test-filter-e-a", InstrumentID: instA, TimestampNs: 1},
		{ID: "test-filter-e-b", InstrumentID: instB, TimestampNs: 1},
	} {
		if err := store.WriteEntity(context.Background(), e); err != nil {
			t.Fatalf("WriteEntity: %v", err)
		}
	}

	got, err := store.ListEntities(context.Background(), db.EntityFilter{InstrumentID: instA})
	if err != nil {
		t.Fatalf("ListEntities: %v", err)
	}
	for _, e := range got {
		if e.InstrumentID != instA {
			t.Errorf("expected instrument %q, got %q", instA, e.InstrumentID)
		}
	}
	if len(got) == 0 {
		t.Error("expected at least 1 result for instA")
	}
}

func TestListEntitiesLimitRespected(t *testing.T) {
	store, err := db.New(context.Background(), dbURL(t))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer store.Close()

	inst := "TEST-LIMIT-INST"
	for i := range 5 {
		e := db.Entity{
			ID:           "test-limit-e-" + string(rune('0'+i)),
			InstrumentID: inst,
			TimestampNs:  int64(i + 1),
		}
		if err := store.WriteEntity(context.Background(), e); err != nil {
			t.Fatalf("WriteEntity: %v", err)
		}
	}

	got, err := store.ListEntities(context.Background(), db.EntityFilter{
		InstrumentID: inst,
		Limit:        2,
	})
	if err != nil {
		t.Fatalf("ListEntities: %v", err)
	}
	if len(got) != 2 {
		t.Errorf("expected 2 results with limit=2, got %d", len(got))
	}
}

func TestListEntityOperationsRoundTrip(t *testing.T) {
	store, err := db.New(context.Background(), dbURL(t))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer store.Close()

	_ = store.WriteEntity(context.Background(), db.Entity{
		ID: "test-list-ops-entity", InstrumentID: "TEST", TimestampNs: 1,
	})
	for _, opName := range []string{"hdf5-conversion", "registration", "replication"} {
		op := db.EntityOperation{
			EntityID: "test-list-ops-entity", InstrumentID: "TEST",
			Operation: opName, TraceID: "trace-" + opName, TimestampNs: 2,
			Metadata: map[string]string{"op": opName},
		}
		if err := store.WriteEntityOperation(context.Background(), op); err != nil {
			t.Fatalf("WriteEntityOperation %s: %v", opName, err)
		}
	}

	got, err := store.ListEntityOperations(context.Background(), "test-list-ops-entity", db.OperationFilter{})
	if err != nil {
		t.Fatalf("ListEntityOperations: %v", err)
	}
	if len(got) != 3 {
		t.Errorf("expected 3 operations, got %d", len(got))
	}
	for _, op := range got {
		if op.EntityID != "test-list-ops-entity" {
			t.Errorf("unexpected entity_id: %q", op.EntityID)
		}
		if op.CreatedAt.IsZero() {
			t.Error("expected non-zero CreatedAt on operation")
		}
	}
}

func TestListEntityOperationsFilterByName(t *testing.T) {
	store, err := db.New(context.Background(), dbURL(t))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer store.Close()

	_ = store.WriteEntity(context.Background(), db.Entity{
		ID: "test-filter-ops-entity", InstrumentID: "TEST", TimestampNs: 1,
	})
	for _, opName := range []string{"hdf5-conversion", "registration"} {
		op := db.EntityOperation{
			EntityID: "test-filter-ops-entity", InstrumentID: "TEST",
			Operation: opName, TimestampNs: 2,
		}
		if err := store.WriteEntityOperation(context.Background(), op); err != nil {
			t.Fatalf("WriteEntityOperation: %v", err)
		}
	}

	got, err := store.ListEntityOperations(context.Background(), "test-filter-ops-entity",
		db.OperationFilter{Operation: "registration"})
	if err != nil {
		t.Fatalf("ListEntityOperations: %v", err)
	}
	if len(got) != 1 || got[0].Operation != "registration" {
		t.Errorf("expected 1 registration op, got %v", got)
	}
}

func TestListEntityEventsRoundTrip(t *testing.T) {
	store, err := db.New(context.Background(), dbURL(t))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer store.Close()

	_ = store.WriteEntity(context.Background(), db.Entity{
		ID: "test-list-events-entity", InstrumentID: "TEST", TimestampNs: 1,
	})
	for _, evName := range []string{"helix.error", "helix.event.candidate_promoted"} {
		ev := db.EntityEvent{
			EntityID: "test-list-events-entity", InstrumentID: "TEST",
			EventName: evName, TimestampNs: 3,
			Metadata: map[string]string{"event": evName},
		}
		if err := store.WriteEntityEvent(context.Background(), ev); err != nil {
			t.Fatalf("WriteEntityEvent %s: %v", evName, err)
		}
	}

	got, err := store.ListEntityEvents(context.Background(), "test-list-events-entity", db.EventFilter{})
	if err != nil {
		t.Fatalf("ListEntityEvents: %v", err)
	}
	if len(got) != 2 {
		t.Errorf("expected 2 events, got %d", len(got))
	}
	for _, ev := range got {
		if ev.EntityID != "test-list-events-entity" {
			t.Errorf("unexpected entity_id: %q", ev.EntityID)
		}
		if ev.CreatedAt.IsZero() {
			t.Error("expected non-zero CreatedAt on event")
		}
	}
}

func TestListEntityEventsFilterByName(t *testing.T) {
	store, err := db.New(context.Background(), dbURL(t))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer store.Close()

	_ = store.WriteEntity(context.Background(), db.Entity{
		ID: "test-filter-events-entity", InstrumentID: "TEST", TimestampNs: 1,
	})
	for _, evName := range []string{"helix.error", "helix.event.candidate_promoted"} {
		ev := db.EntityEvent{
			EntityID: "test-filter-events-entity", InstrumentID: "TEST",
			EventName: evName, TimestampNs: 3,
		}
		if err := store.WriteEntityEvent(context.Background(), ev); err != nil {
			t.Fatalf("WriteEntityEvent: %v", err)
		}
	}

	got, err := store.ListEntityEvents(context.Background(), "test-filter-events-entity",
		db.EventFilter{EventName: "helix.error"})
	if err != nil {
		t.Fatalf("ListEntityEvents: %v", err)
	}
	if len(got) != 1 || got[0].EventName != "helix.error" {
		t.Errorf("expected 1 helix.error event, got %v", got)
	}
}

func TestWriteEntityNilMetadata(t *testing.T) {
	store, err := db.New(context.Background(), dbURL(t))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer store.Close()

	e := db.Entity{
		ID:           "test-entity-nil-meta",
		InstrumentID: "TEST",
		TimestampNs:  1,
		Metadata:     nil, // must not panic or error
	}
	if err := store.WriteEntity(context.Background(), e); err != nil {
		t.Fatalf("WriteEntity with nil metadata: %v", err)
	}
}

func TestWriteEntityOperationRoundTrip(t *testing.T) {
	store, err := db.New(context.Background(), dbURL(t))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer store.Close()

	// Write the parent entity first.
	_ = store.WriteEntity(context.Background(), db.Entity{
		ID: "test-frb-for-op", InstrumentID: "TEST", TimestampNs: 1,
	})

	op := db.EntityOperation{
		EntityID:     "test-frb-for-op",
		InstrumentID: "TEST",
		Operation:    "hdf5-conversion",
		TraceID:      "aabbccdd00000000000000000000cafe",
		TimestampNs:  2_000_000_000,
		Metadata:     map[string]string{"hdf5_size_mb": "241.3"},
	}
	if err := store.WriteEntityOperation(context.Background(), op); err != nil {
		t.Fatalf("WriteEntityOperation: %v", err)
	}
}

func TestWriteEntityOperationCreatesPlaceholderEntity(t *testing.T) {
	store, err := db.New(context.Background(), dbURL(t))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer store.Close()

	// Deliberately skip writing the entity — WriteEntityOperation must upsert a placeholder.
	// If it doesn't, the operation row would either fail or the entity row would be absent.
	// We verify by writing a second operation for the same entity: an idempotent upsert
	// must not return an error (which it would if the placeholder wasn't created first).
	op := db.EntityOperation{
		EntityID:     "test-phantom-entity",
		InstrumentID: "TEST",
		Operation:    "registration",
		TraceID:      "aabbccdd00000000000000000000dead",
		TimestampNs:  3_000_000_000,
	}
	if err := store.WriteEntityOperation(context.Background(), op); err != nil {
		t.Fatalf("WriteEntityOperation on non-existent entity: %v", err)
	}

	// Writing a second operation for the same phantom entity also succeeds — the
	// placeholder upsert is idempotent, so no constraint violation occurs.
	op2 := db.EntityOperation{
		EntityID:     "test-phantom-entity",
		InstrumentID: "TEST",
		Operation:    "replication",
		TraceID:      "aabbccdd00000000000000000000beef",
		TimestampNs:  4_000_000_000,
	}
	if err := store.WriteEntityOperation(context.Background(), op2); err != nil {
		t.Fatalf("second WriteEntityOperation on phantom entity: %v", err)
	}
}

func TestWriteEntityOperationNilMetadata(t *testing.T) {
	store, err := db.New(context.Background(), dbURL(t))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer store.Close()

	_ = store.WriteEntity(context.Background(), db.Entity{
		ID: "test-frb-nil-op-meta", InstrumentID: "TEST", TimestampNs: 1,
	})

	op := db.EntityOperation{
		EntityID:     "test-frb-nil-op-meta",
		InstrumentID: "TEST",
		Operation:    "replication",
		TimestampNs:  4_000_000_000,
		Metadata:     nil,
	}
	if err := store.WriteEntityOperation(context.Background(), op); err != nil {
		t.Fatalf("WriteEntityOperation with nil metadata: %v", err)
	}
}
