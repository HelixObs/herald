// White-box tests for the db package — access unexported types/methods.
package db

import (
	"context"
	"os"
	"testing"
	"time"
)

// mockDBMetrics satisfies the dbMetrics interface without recording to Prometheus.
type mockDBMetrics struct {
	writes    []string
	queries   []string
	poolInUse int
	poolTotal int
}

func (m *mockDBMetrics) DBWriteRecord(table, status string, _ time.Duration) {
	m.writes = append(m.writes, table+":"+status)
}
func (m *mockDBMetrics) DBQueryRecord(query, status string, _ time.Duration) {
	m.queries = append(m.queries, query+":"+status)
}
func (m *mockDBMetrics) DBPoolStats(inUse, total int) {
	m.poolInUse = inUse
	m.poolTotal = total
}

// ── Integration: recordWrite via WriteEntity ──────────────────────────────────

func TestRecordWriteWithMetrics(t *testing.T) {
	url := os.Getenv("TEST_DB_URL")
	if url == "" {
		t.Skip("TEST_DB_URL not set")
	}
	m := &mockDBMetrics{}
	s, err := New(context.Background(), url, m)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer s.Close()

	// WriteEntity calls recordWrite internally.
	_ = s.WriteEntity(context.Background(), Entity{
		ID:           "test-metrics-wb-entity",
		InstrumentID: "TEST",
		TimestampNs:  1,
	})

	if len(m.writes) == 0 {
		t.Error("expected at least one write recorded via recordWrite")
	}
	if m.poolTotal == 0 {
		t.Error("expected pool total > 0 from DBPoolStats")
	}
}

func TestRecordWriteSuccessStatus(t *testing.T) {
	url := os.Getenv("TEST_DB_URL")
	if url == "" {
		t.Skip("TEST_DB_URL not set")
	}
	m := &mockDBMetrics{}
	s, err := New(context.Background(), url, m)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer s.Close()

	_ = s.WriteEntity(context.Background(), Entity{
		ID:           "test-metrics-wb-success",
		InstrumentID: "TEST",
		TimestampNs:  1,
	})

	// At least one write should have status "success".
	foundSuccess := false
	for _, w := range m.writes {
		if w == "entities:success" {
			foundSuccess = true
			break
		}
	}
	if !foundSuccess {
		t.Errorf("expected entities:success in writes, got %v", m.writes)
	}
}

// ── Integration: recordQuery via QueryEntityGraph ─────────────────────────────

func TestRecordQueryWithMetrics(t *testing.T) {
	url := os.Getenv("TEST_DB_URL")
	if url == "" {
		t.Skip("TEST_DB_URL not set")
	}
	m := &mockDBMetrics{}
	s, err := New(context.Background(), url, m)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer s.Close()

	// QueryEntityGraph calls recordQuery internally.
	_, _ = s.QueryEntityGraph(context.Background(), "nonexistent-wb-entity", 10)

	if len(m.queries) == 0 {
		t.Error("expected at least one query recorded via recordQuery")
	}
}

func TestRecordQuerySuccessStatus(t *testing.T) {
	url := os.Getenv("TEST_DB_URL")
	if url == "" {
		t.Skip("TEST_DB_URL not set")
	}
	m := &mockDBMetrics{}
	s, err := New(context.Background(), url, m)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer s.Close()

	_, _ = s.QueryEntityGraph(context.Background(), "nonexistent-wb-query-status", 10)

	foundSuccess := false
	for _, q := range m.queries {
		if q == "entity_graph:success" {
			foundSuccess = true
			break
		}
	}
	if !foundSuccess {
		t.Errorf("expected entity_graph:success in queries, got %v", m.queries)
	}
}

// ── recordWrite nil metrics guard ─────────────────────────────────────────────

func TestRecordWriteNilMetricsNoOp(t *testing.T) {
	// When m is nil, recordWrite must be a no-op (no panic).
	// We can't call recordWrite directly without a live pool, but we verify
	// the nil guard via the exported New path with nil metrics.
	url := os.Getenv("TEST_DB_URL")
	if url == "" {
		t.Skip("TEST_DB_URL not set")
	}
	s, err := New(context.Background(), url, nil) // nil metrics
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer s.Close()

	// recordWrite is called inside WriteEntity — must not panic with nil metrics.
	if err := s.WriteEntity(context.Background(), Entity{
		ID:           "test-nil-metrics-guard",
		InstrumentID: "TEST",
		TimestampNs:  1,
	}); err != nil {
		t.Fatalf("WriteEntity with nil metrics: %v", err)
	}
}

// ── recordQuery nil metrics guard ─────────────────────────────────────────────

func TestRecordQueryNilMetricsNoOp(t *testing.T) {
	url := os.Getenv("TEST_DB_URL")
	if url == "" {
		t.Skip("TEST_DB_URL not set")
	}
	s, err := New(context.Background(), url, nil)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer s.Close()

	// recordQuery is called inside QueryEntityGraph — must not panic with nil metrics.
	_, _ = s.QueryEntityGraph(context.Background(), "nonexistent-nil-metrics", 10)
}
