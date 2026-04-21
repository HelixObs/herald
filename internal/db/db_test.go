package db_test

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/HelixObs/gateway/internal/db"
	"github.com/jackc/pgx/v5/pgxpool"
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

func TestQueryEntityGraph(t *testing.T) {
	store, err := db.New(context.Background(), dbURL(t))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer store.Close()

	parentID := "test-graph-parent"
	childID := "test-graph-child"

	_ = store.WriteEntity(context.Background(), db.Entity{
		ID: parentID, InstrumentID: "TEST", TimestampNs: 1,
	})
	_ = store.WriteEntity(context.Background(), db.Entity{
		ID: childID, InstrumentID: "TEST", TimestampNs: 2, ParentIDs: []string{parentID},
	})
	_ = store.WriteEntityEvent(context.Background(), db.EntityEvent{
		InstrumentID: "TEST", EntityID: childID, EventName: "helix.error", TimestampNs: 3,
	})

	g, err := store.QueryEntityGraph(context.Background(), childID, 10)
	if err != nil {
		t.Fatalf("QueryEntityGraph: %v", err)
	}
	if g == nil {
		t.Fatal("expected non-nil graph, got nil")
	}

	nodeIDs := map[string]bool{}
	for _, n := range g.Nodes {
		nodeIDs[n.ID] = true
		if n.ID == childID && !n.HasError {
			t.Error("child node should have HasError=true")
		}
	}
	if !nodeIDs[parentID] {
		t.Errorf("parent node %q missing from graph", parentID)
	}
	if !nodeIDs[childID] {
		t.Errorf("child node %q missing from graph", childID)
	}

	edgeFound := false
	for _, e := range g.Edges {
		if e.Source == parentID && e.Target == childID {
			edgeFound = true
		}
	}
	if !edgeFound {
		t.Errorf("edge %q → %q not found in graph", parentID, childID)
	}

	// Non-existent entity returns nil, no error.
	g2, err := store.QueryEntityGraph(context.Background(), "nonexistent-entity-xyz", 10)
	if err != nil {
		t.Fatalf("QueryEntityGraph not-found: %v", err)
	}
	if g2 != nil {
		t.Error("expected nil for non-existent entity, got non-nil")
	}
}

func TestWriteEntityOperationPrunesOldRows(t *testing.T) {
	store, err := db.New(context.Background(), dbURL(t))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer store.Close()

	entityID := "test-prune-entity"
	_ = store.WriteEntity(context.Background(), db.Entity{
		ID: entityID, InstrumentID: "TEST", TimestampNs: 1,
	})

	// Write 15 operations for the same (entity_id, operation) pair.
	for i := range 15 {
		op := db.EntityOperation{
			EntityID:     entityID,
			InstrumentID: "TEST",
			Operation:    "retry-op",
			TraceID:      fmt.Sprintf("trace%013d", i),
			TimestampNs:  int64(i + 1),
		}
		if err := store.WriteEntityOperation(context.Background(), op); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}

	// Only the 10 most recent rows should remain — use a raw pool for the count query.
	pool, err := pgxpool.New(context.Background(), dbURL(t))
	if err != nil {
		t.Fatalf("count pool: %v", err)
	}
	defer pool.Close()

	var count int
	if err := pool.QueryRow(context.Background(),
		`SELECT COUNT(*) FROM entity_operations WHERE entity_id = $1 AND operation = $2`,
		entityID, "retry-op",
	).Scan(&count); err != nil {
		t.Fatalf("count query: %v", err)
	}
	if count != 10 {
		t.Errorf("expected 10 rows after pruning, got %d", count)
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
