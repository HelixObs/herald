package receiver_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	collectortracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/grpc"

	"github.com/HelixObs/herald/internal/interceptor"
	"github.com/HelixObs/herald/internal/metrics"
	"github.com/HelixObs/herald/internal/receiver"
	"github.com/HelixObs/herald/internal/store"
)

// ── Mock downstream collector ─────────────────────────────────────────────────

type mockCollector struct {
	mu       sync.Mutex
	received []*collectortracepb.ExportTraceServiceRequest
	err      error
}

func (m *mockCollector) Export(
	_ context.Context,
	req *collectortracepb.ExportTraceServiceRequest,
	_ ...grpc.CallOption,
) (*collectortracepb.ExportTraceServiceResponse, error) {
	if m.err != nil {
		return nil, m.err
	}
	m.mu.Lock()
	m.received = append(m.received, req)
	m.mu.Unlock()
	return &collectortracepb.ExportTraceServiceResponse{}, nil
}

func (m *mockCollector) calls() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.received)
}

func (m *mockCollector) last() *collectortracepb.ExportTraceServiceRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.received) == 0 {
		return nil
	}
	return m.received[len(m.received)-1]
}

// ── Helpers ───────────────────────────────────────────────────────────────────

func newReceiver(col *mockCollector) *receiver.Receiver {
	s := store.New(1_000, nil)
	m := metrics.New(prometheus.NewRegistry())
	icp := interceptor.New(s, nil, m)
	return receiver.New(icp, col)
}

func helixSpan(entityID string) *tracepb.Span {
	return &tracepb.Span{
		Name:    "correlator",
		TraceId: []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		SpanId:  []byte{1, 2, 3, 4, 5, 6, 7, 8},
		Attributes: []*commonpb.KeyValue{
			{Key: "helix.entity.id", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: entityID}}},
			{Key: "helix.instrument.id", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "CHIME"}}},
		},
	}
}

func makeReq(spans ...*tracepb.Span) *collectortracepb.ExportTraceServiceRequest {
	return &collectortracepb.ExportTraceServiceRequest{
		ResourceSpans: []*tracepb.ResourceSpans{{
			ScopeSpans: []*tracepb.ScopeSpans{{Spans: spans}},
		}},
	}
}

// ── Tests ─────────────────────────────────────────────────────────────────────

func TestExportForwardsToCollector(t *testing.T) {
	col := &mockCollector{}
	recv := newReceiver(col)

	_, err := recv.Export(context.Background(), makeReq(helixSpan("block-1")))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if col.calls() != 1 {
		t.Errorf("expected 1 forwarded batch, got %d", col.calls())
	}
}

func TestExportForwardsNonHelixSpansUnchanged(t *testing.T) {
	col := &mockCollector{}
	recv := newReceiver(col)

	plain := &tracepb.Span{Name: "http.request", TraceId: []byte{1}, SpanId: []byte{2}}
	_, err := recv.Export(context.Background(), makeReq(plain))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	req := col.last()
	if req == nil {
		t.Fatal("no request forwarded")
	}
	span := req.ResourceSpans[0].ScopeSpans[0].Spans[0]
	if len(span.Links) != 0 {
		t.Errorf("non-helix span should have no links added, got %d", len(span.Links))
	}
}

func TestExportAddsParentLinksBeforeForwarding(t *testing.T) {
	col := &mockCollector{}
	recv := newReceiver(col)

	// Register the parent first.
	_, _ = recv.Export(context.Background(), makeReq(helixSpan("block-1")))

	// Child with parent ID — should have link injected.
	child := helixSpan("cand-1")
	child.Attributes = append(child.Attributes, &commonpb.KeyValue{
		Key:   "helix.parent.ids",
		Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "block-1"}},
	})
	_, err := recv.Export(context.Background(), makeReq(child))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	forwarded := col.last().ResourceSpans[0].ScopeSpans[0].Spans[0]
	if len(forwarded.Links) != 1 {
		t.Errorf("expected 1 span link injected by interceptor, got %d", len(forwarded.Links))
	}
}

func TestExportPropagatesCollectorError(t *testing.T) {
	sentinel := errors.New("collector unavailable")
	col := &mockCollector{err: sentinel}
	recv := newReceiver(col)

	_, err := recv.Export(context.Background(), makeReq(helixSpan("block-1")))
	if !errors.Is(err, sentinel) {
		t.Errorf("expected collector error to be propagated, got: %v", err)
	}
}

func TestExportEmptyRequestForwarded(t *testing.T) {
	col := &mockCollector{}
	recv := newReceiver(col)

	_, err := recv.Export(context.Background(), &collectortracepb.ExportTraceServiceRequest{})
	if err != nil {
		t.Fatalf("unexpected error on empty request: %v", err)
	}
	if col.calls() != 1 {
		t.Errorf("empty request should still be forwarded, got %d calls", col.calls())
	}
}

func TestExportMultipleBatches(t *testing.T) {
	col := &mockCollector{}
	recv := newReceiver(col)

	for i := 0; i < 5; i++ {
		_, err := recv.Export(context.Background(), makeReq(helixSpan("entity")))
		if err != nil {
			t.Fatalf("batch %d: unexpected error: %v", i, err)
		}
	}
	if col.calls() != 5 {
		t.Errorf("expected 5 forwarded batches, got %d", col.calls())
	}
}

func TestExportMixedHelixAndPlainSpans(t *testing.T) {
	col := &mockCollector{}
	recv := newReceiver(col)

	plain := &tracepb.Span{Name: "http.request", TraceId: []byte{9}, SpanId: []byte{8}}
	req := makeReq(helixSpan("block-1"), plain)

	_, err := recv.Export(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Both spans should be forwarded in the same batch.
	spans := col.last().ResourceSpans[0].ScopeSpans[0].Spans
	if len(spans) != 2 {
		t.Errorf("expected 2 spans forwarded, got %d", len(spans))
	}
}

func TestDial(t *testing.T) {
	s := store.New(1_000, nil)
	m := metrics.New(prometheus.NewRegistry())
	icp := interceptor.New(s, nil, m)
	r, err := receiver.Dial(icp, "localhost:1")
	if err != nil {
		t.Fatalf("Dial returned unexpected error: %v", err)
	}
	if r == nil {
		t.Fatal("Dial returned nil Receiver")
	}
}
