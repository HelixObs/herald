package notifier_test

import (
	"context"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/HelixObs/gateway/internal/db"
	"github.com/HelixObs/gateway/internal/metrics"
	"github.com/HelixObs/gateway/internal/notifier"
	"github.com/HelixObs/gateway/internal/notifier/config"
	"github.com/HelixObs/gateway/internal/notifier/silence"
)

// ── Helpers ───────────────────────────────────────────────────────────────────

func newTestMetrics() *metrics.Metrics {
	return metrics.New(prometheus.NewRegistry())
}

type noopSilenceDB struct{}

func (noopSilenceDB) ActiveSilences(_ context.Context, _ string) ([]db.Silence, error) {
	return nil, nil
}

type mockSilenceDB struct {
	silences []db.Silence
}

func (m *mockSilenceDB) ActiveSilences(_ context.Context, _ string) ([]db.Silence, error) {
	return m.silences, nil
}

// stubMessaging records calls and optionally blocks until released.
type stubMessaging struct {
	mu      sync.Mutex
	sends   []string
	blocked chan struct{} // if non-nil, Send blocks until closed
}

func (s *stubMessaging) Send(_ context.Context, _, _, msg string, _, _ int) (bool, error) {
	if s.blocked != nil {
		<-s.blocked
	}
	s.mu.Lock()
	s.sends = append(s.sends, msg)
	s.mu.Unlock()
	return true, nil
}

func (s *stubMessaging) FlushDigests(_ context.Context, _ string, _ int) {}

func (s *stubMessaging) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.sends)
}

func (s *stubMessaging) last() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.sends) == 0 {
		return ""
	}
	return s.sends[len(s.sends)-1]
}

// stubSCM records dispatch calls.
type stubSCM struct {
	mu         sync.Mutex
	dispatches int
}

func (s *stubSCM) Dispatch(_ context.Context, _ notifier.SCMParams) error {
	s.mu.Lock()
	s.dispatches++
	s.mu.Unlock()
	return nil
}

// yamlLoader creates a config.Loader backed by a temp dir with the given YAML.
func yamlLoader(t *testing.T, yaml string) *config.Loader {
	t.Helper()
	dir := t.TempDir()
	p := dir + "/inst.yml"
	if err := os.WriteFile(p, []byte(yaml), 0o600); err != nil {
		t.Fatalf("write yaml: %v", err)
	}
	l := config.New(dir, 0, 0)
	l.Start(t.Context(), time.Hour)
	return l
}

// waitFor polls fn until it returns true or the deadline passes.
func waitFor(t *testing.T, timeout time.Duration, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition not met within timeout")
}

// ── Channel and drop tests ────────────────────────────────────────────────────

func TestSend_QueuesEvent(t *testing.T) {
	sl := silence.New(noopSilenceDB{}, time.Minute)
	n := notifier.New(nil, sl, newTestMetrics(), 10, "http://ui", "http://grafana")
	// Should not panic with no backends registered.
	n.Send(notifier.Event{InstrumentID: "X", EventName: "helix.error"})
}

func TestSend_DropWhenFull(t *testing.T) {
	sl := silence.New(noopSilenceDB{}, time.Minute)
	n := notifier.New(nil, sl, newTestMetrics(), 1, "http://ui", "http://grafana")
	n.Send(notifier.Event{InstrumentID: "X", EventName: "helix.error"})
	// Channel full — should drop without blocking or panicking.
	n.Send(notifier.Event{InstrumentID: "X", EventName: "helix.error"})
}

// ── Dispatch routing tests ────────────────────────────────────────────────────

func TestDispatch_SkipsUnknownInstrument(t *testing.T) {
	cfg := yamlLoader(t, `instrument_id: KNOWN`)
	sl := silence.New(noopSilenceDB{}, time.Minute)
	stub := &stubMessaging{}
	n := notifier.New(cfg, sl, newTestMetrics(), 10, "http://ui", "http://grafana")
	n.RegisterMessaging("slack", stub)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go n.Start(ctx)

	n.Send(notifier.Event{InstrumentID: "UNKNOWN", EventName: "helix.error", Message: "x"})
	time.Sleep(100 * time.Millisecond)

	if stub.count() != 0 {
		t.Errorf("expected 0 sends for unknown instrument, got %d", stub.count())
	}
}

func TestDispatch_SkipsUnknownEvent(t *testing.T) {
	t.Setenv("SKIP_UNK_WEBHOOK", "https://hooks.slack.com/test")
	cfg := yamlLoader(t, `
instrument_id: INST
notifications:
  slack_webhook_env: SKIP_UNK_WEBHOOK
  events:
    helix.error:
      slack:
        channel: "#alerts"
        sample_window_seconds: 60
        max_per_window: 5
`)
	sl := silence.New(noopSilenceDB{}, time.Minute)
	stub := &stubMessaging{}
	n := notifier.New(cfg, sl, newTestMetrics(), 10, "http://ui", "http://grafana")
	n.RegisterMessaging("slack", stub)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go n.Start(ctx)

	n.Send(notifier.Event{InstrumentID: "INST", EventName: "helix.unknown"})
	time.Sleep(100 * time.Millisecond)

	if stub.count() != 0 {
		t.Errorf("expected 0 sends for unknown event, got %d", stub.count())
	}
}

func TestDispatch_CallsMessagingBackend(t *testing.T) {
	t.Setenv("DISPATCH_SLACK_WEBHOOK", "https://hooks.slack.com/test")
	cfg := yamlLoader(t, `
instrument_id: INST2
notifications:
  slack_webhook_env: DISPATCH_SLACK_WEBHOOK
  events:
    helix.error:
      slack:
        channel: "#alerts"
        sample_window_seconds: 60
        max_per_window: 5
`)
	sl := silence.New(noopSilenceDB{}, time.Minute)
	stub := &stubMessaging{}
	n := notifier.New(cfg, sl, newTestMetrics(), 10, "http://ui", "http://grafana")
	n.RegisterMessaging("slack", stub)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go n.Start(ctx)

	n.Send(notifier.Event{InstrumentID: "INST2", EventName: "helix.error", EntityID: "e1", Message: "disk full"})

	waitFor(t, 500*time.Millisecond, func() bool { return stub.count() == 1 })
}

func TestDispatch_SilencedEventNotSent(t *testing.T) {
	t.Setenv("SIL_SLACK_WEBHOOK", "https://hooks.slack.com/test")
	cfg := yamlLoader(t, `
instrument_id: SILENCED
notifications:
  slack_webhook_env: SIL_SLACK_WEBHOOK
  events:
    helix.error:
      slack:
        channel: "#alerts"
        sample_window_seconds: 60
        max_per_window: 5
`)
	silDB := &mockSilenceDB{silences: []db.Silence{{InstrumentID: "SILENCED", EventType: ""}}}
	sl := silence.New(silDB, time.Minute)
	stub := &stubMessaging{}
	n := notifier.New(cfg, sl, newTestMetrics(), 10, "http://ui", "http://grafana")
	n.RegisterMessaging("slack", stub)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go n.Start(ctx)

	n.Send(notifier.Event{InstrumentID: "SILENCED", EventName: "helix.error", EntityID: "e1"})
	time.Sleep(100 * time.Millisecond)

	if stub.count() != 0 {
		t.Errorf("expected 0 sends for silenced event, got %d", stub.count())
	}
}

// ── buildMessage tests ────────────────────────────────────────────────────────

func TestBuildMessage_ContainsExpectedFields(t *testing.T) {
	t.Setenv("BLDMSG_SLACK_WEBHOOK", "https://hooks.slack.com/test")
	cfg := yamlLoader(t, `
instrument_id: BLD
notifications:
  slack_webhook_env: BLDMSG_SLACK_WEBHOOK
  events:
    helix.error:
      slack:
        channel: "#alerts"
        sample_window_seconds: 60
        max_per_window: 5
`)
	sl := silence.New(noopSilenceDB{}, time.Minute)
	stub := &stubMessaging{}
	n := notifier.New(cfg, sl, newTestMetrics(), 10, "http://ui", "http://grafana")
	n.RegisterMessaging("slack", stub)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go n.Start(ctx)

	n.Send(notifier.Event{
		InstrumentID: "BLD",
		EventName:    "helix.error",
		EntityID:     "ent-1",
		Message:      "disk full",
		Stage:        "hdf5_archiver",
	})

	waitFor(t, 500*time.Millisecond, func() bool { return stub.count() == 1 })
	msg := stub.last()

	for _, want := range []string{"[BLD]", "helix.error", "disk full", "ent-1", "Inspect", "hdf5_archiver"} {
		if !strings.Contains(msg, want) {
			t.Errorf("expected message to contain %q, got:\n%s", want, msg)
		}
	}
}

func TestBuildMessage_MessageTemplate(t *testing.T) {
	t.Setenv("TMPL_SLACK_WEBHOOK", "https://hooks.slack.com/test")
	cfg := yamlLoader(t, `
instrument_id: TMPL
notifications:
  slack_webhook_env: TMPL_SLACK_WEBHOOK
  events:
    helix.error:
      message_template: "Error: {{.reason}}"
      slack:
        channel: "#alerts"
        sample_window_seconds: 60
        max_per_window: 5
`)
	sl := silence.New(noopSilenceDB{}, time.Minute)
	stub := &stubMessaging{}
	n := notifier.New(cfg, sl, newTestMetrics(), 10, "http://ui", "http://grafana")
	n.RegisterMessaging("slack", stub)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go n.Start(ctx)

	n.Send(notifier.Event{
		InstrumentID: "TMPL",
		EventName:    "helix.error",
		EntityID:     "ent-3",
		Metadata:     map[string]string{"reason": "disk full"},
	})

	waitFor(t, 500*time.Millisecond, func() bool { return stub.count() == 1 })
	msg := stub.last()

	if !strings.Contains(msg, "Error: disk full") {
		t.Errorf("expected template rendered in message, got:\n%s", msg)
	}
}

// ── RegisterMessaging / RegisterSCM ──────────────────────────────────────────

func TestRegisterMessaging_NoPanic(t *testing.T) {
	sl := silence.New(noopSilenceDB{}, time.Minute)
	n := notifier.New(nil, sl, newTestMetrics(), 10, "http://ui", "http://grafana")
	n.RegisterMessaging("slack", &stubMessaging{})
	n.RegisterMessaging("discord", &stubMessaging{})
	n.RegisterSCM("github", &stubSCM{})
	n.RegisterSCM("gitlab", &stubSCM{})
}
