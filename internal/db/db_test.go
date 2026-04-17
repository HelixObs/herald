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
