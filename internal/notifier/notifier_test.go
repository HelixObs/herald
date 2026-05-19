package notifier_test

import (
	"context"
	"errors"
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
	sends   []notifier.Message
	blocked chan struct{} // if non-nil, Send blocks until closed
}

func (s *stubMessaging) Send(_ context.Context, _, _ string, msg notifier.Message, _, _ int) (bool, error) {
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

func (s *stubMessaging) last() notifier.Message {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.sends) == 0 {
		return notifier.Message{}
	}
	return s.sends[len(s.sends)-1]
}

// stubSCM records dispatch calls and returns a fake issue URL.
type stubSCM struct {
	mu         sync.Mutex
	dispatches int
	returnURL  string // URL to return from Dispatch (empty string → no URL)
}

func (s *stubSCM) Dispatch(_ context.Context, _ notifier.SCMParams) (string, error) {
	s.mu.Lock()
	s.dispatches++
	u := s.returnURL
	s.mu.Unlock()
	return u, nil
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

// ── SCM → messaging ordering test ─────────────────────────────────────────────

func TestDispatch_IssueURLPassedToMessaging(t *testing.T) {
	t.Setenv("SCM_SLACK_WEBHOOK", "https://hooks.slack.com/test")
	t.Setenv("SCM_GITHUB_TOKEN", "ghp_test")
	cfg := yamlLoader(t, `
instrument_id: SCM_INST
notifications:
  slack_webhook_env: SCM_SLACK_WEBHOOK
  github_token_env: SCM_GITHUB_TOKEN
  events:
    helix.error:
      slack:
        channel: "#alerts"
        sample_window_seconds: 60
        max_per_window: 5
      github:
        repo: owner/repo
        labels: [bug]
        on_recurrence_after_close: reopen
`)
	sl := silence.New(noopSilenceDB{}, time.Minute)
	msgStub := &stubMessaging{}
	scmStub := &stubSCM{returnURL: "https://github.com/owner/repo/issues/99"}
	n := notifier.New(cfg, sl, newTestMetrics(), 10, "http://ui", "http://grafana")
	n.RegisterMessaging("slack", msgStub)
	n.RegisterSCM("github", scmStub)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go n.Start(ctx)

	n.Send(notifier.Event{InstrumentID: "SCM_INST", EventName: "helix.error", EntityID: "e1", Message: "disk full"})

	waitFor(t, time.Second, func() bool { return msgStub.count() == 1 })

	msg := msgStub.last()
	if len(msg.IssueURLs) != 1 || msg.IssueURLs[0] != "https://github.com/owner/repo/issues/99" {
		t.Errorf("expected issue URL in message, got %v", msg.IssueURLs)
	}
}

// ── buildMsg tests ────────────────────────────────────────────────────────────

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

	if msg.InstrumentID != "BLD" {
		t.Errorf("expected InstrumentID=BLD, got %q", msg.InstrumentID)
	}
	if msg.EventName != "helix.error" {
		t.Errorf("expected EventName=helix.error, got %q", msg.EventName)
	}
	if msg.Body != "disk full" {
		t.Errorf("expected Body=%q, got %q", "disk full", msg.Body)
	}
	if msg.EntityID != "ent-1" {
		t.Errorf("expected EntityID=ent-1, got %q", msg.EntityID)
	}
	if msg.Stage != "hdf5_archiver" {
		t.Errorf("expected Stage=hdf5_archiver, got %q", msg.Stage)
	}
	if !strings.Contains(msg.InspectorURL, "ent-1") {
		t.Errorf("expected InspectorURL to contain entity ID, got %q", msg.InspectorURL)
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

	if !strings.Contains(msg.Text, "Error: disk full") {
		t.Errorf("expected template rendered in Text, got:\n%s", msg.Text)
	}
}

func TestBuildMessage_MetadataInMessage(t *testing.T) {
	t.Setenv("META_SLACK_WEBHOOK", "https://hooks.slack.com/test")
	cfg := yamlLoader(t, `
instrument_id: META
notifications:
  slack_webhook_env: META_SLACK_WEBHOOK
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
		InstrumentID: "META",
		EventName:    "helix.error",
		EntityID:     "ent-5",
		Message:      "NFS mount failed",
		Metadata:     map[string]string{"process": "hdf5_archiver_v2", "message": "NFS mount failed"},
	})

	waitFor(t, 500*time.Millisecond, func() bool { return stub.count() == 1 })
	msg := stub.last()

	if msg.Metadata["process"] != "hdf5_archiver_v2" {
		t.Errorf("expected Metadata[process]=hdf5_archiver_v2, got %v", msg.Metadata)
	}
	// "message" key should not appear in Text (it duplicates Body).
	if strings.Contains(msg.Text, "message: NFS") {
		t.Errorf("Text should not duplicate the 'message' metadata key, got:\n%s", msg.Text)
	}
	// "process" key should appear in Text.
	if !strings.Contains(msg.Text, "process") {
		t.Errorf("expected Text to contain process metadata, got:\n%s", msg.Text)
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

// ── doMessaging error / suppressed paths ─────────────────────────────────────

// errMessaging always returns an error from Send.
type errMessaging struct{}

func (e *errMessaging) Send(_ context.Context, _, _ string, _ notifier.Message, _, _ int) (bool, error) {
	return false, errors.New("webhook unreachable")
}
func (e *errMessaging) FlushDigests(_ context.Context, _ string, _ int) {}

// suppressedMessaging returns (false, nil) — rate-limit suppression.
type suppressedMessaging struct{}

func (s *suppressedMessaging) Send(_ context.Context, _, _ string, _ notifier.Message, _, _ int) (bool, error) {
	return false, nil
}
func (s *suppressedMessaging) FlushDigests(_ context.Context, _ string, _ int) {}

// errSCM always returns an error from Dispatch.
type errSCM struct{}

func (e *errSCM) Dispatch(_ context.Context, _ notifier.SCMParams) (string, error) {
	return "", errors.New("github api error")
}

func TestDoMessaging_SendError(t *testing.T) {
	t.Setenv("ERR_MSG_WEBHOOK", "https://hooks.slack.com/test")
	cfg := yamlLoader(t, `
instrument_id: ERRMSG
notifications:
  slack_webhook_env: ERR_MSG_WEBHOOK
  events:
    helix.error:
      slack:
        channel: "#alerts"
        sample_window_seconds: 60
        max_per_window: 5
`)
	sl := silence.New(noopSilenceDB{}, time.Minute)
	n := notifier.New(cfg, sl, newTestMetrics(), 10, "http://ui", "http://grafana")
	n.RegisterMessaging("slack", &errMessaging{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go n.Start(ctx)

	n.Send(notifier.Event{InstrumentID: "ERRMSG", EventName: "helix.error", EntityID: "e1", Message: "test"})
	time.Sleep(200 * time.Millisecond)
}

func TestDoMessaging_Suppressed(t *testing.T) {
	t.Setenv("SUPP_MSG_WEBHOOK", "https://hooks.slack.com/test")
	cfg := yamlLoader(t, `
instrument_id: SUPPMSG
notifications:
  slack_webhook_env: SUPP_MSG_WEBHOOK
  events:
    helix.error:
      slack:
        channel: "#alerts"
        sample_window_seconds: 60
        max_per_window: 5
`)
	sl := silence.New(noopSilenceDB{}, time.Minute)
	n := notifier.New(cfg, sl, newTestMetrics(), 10, "http://ui", "http://grafana")
	n.RegisterMessaging("slack", &suppressedMessaging{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go n.Start(ctx)

	n.Send(notifier.Event{InstrumentID: "SUPPMSG", EventName: "helix.error", EntityID: "e1", Message: "test"})
	time.Sleep(200 * time.Millisecond)
}

// ── dispatch "no backend registered" paths ────────────────────────────────────

func TestDispatch_NoMessagingBackendRegistered(t *testing.T) {
	t.Setenv("NOBE_WEBHOOK", "https://hooks.slack.com/test")
	cfg := yamlLoader(t, `
instrument_id: NOBE
notifications:
  slack_webhook_env: NOBE_WEBHOOK
  events:
    helix.error:
      slack:
        channel: "#alerts"
        sample_window_seconds: 60
        max_per_window: 5
`)
	sl := silence.New(noopSilenceDB{}, time.Minute)
	n := notifier.New(cfg, sl, newTestMetrics(), 10, "http://ui", "http://grafana")
	// Register under a different name so "slack" is not found.
	n.RegisterMessaging("discord", &stubMessaging{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go n.Start(ctx)

	n.Send(notifier.Event{InstrumentID: "NOBE", EventName: "helix.error", EntityID: "e1", Message: "test"})
	time.Sleep(100 * time.Millisecond) // dispatch fires the "no backend" warning and returns
}

func TestDispatch_NoSCMBackendRegistered(t *testing.T) {
	t.Setenv("NOSCM_WEBHOOK", "https://hooks.slack.com/test")
	t.Setenv("NOSCM_TOKEN", "ghp_test")
	cfg := yamlLoader(t, `
instrument_id: NOSCM
notifications:
  slack_webhook_env: NOSCM_WEBHOOK
  github_token_env: NOSCM_TOKEN
  events:
    helix.error:
      slack:
        channel: "#alerts"
        sample_window_seconds: 60
        max_per_window: 5
      github:
        repo: owner/repo
        labels: [bug]
`)
	sl := silence.New(noopSilenceDB{}, time.Minute)
	stub := &stubMessaging{}
	n := notifier.New(cfg, sl, newTestMetrics(), 10, "http://ui", "http://grafana")
	n.RegisterMessaging("slack", stub)
	// Intentionally do NOT register a "github" SCM backend.

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go n.Start(ctx)

	n.Send(notifier.Event{InstrumentID: "NOSCM", EventName: "helix.error", EntityID: "e1", Message: "test"})
	waitFor(t, 500*time.Millisecond, func() bool { return stub.count() >= 1 })
}

// ── doSCM error path ─────────────────────────────────────────────────────────

func TestDoSCM_DispatchError(t *testing.T) {
	t.Setenv("SCMERR_WEBHOOK", "https://hooks.slack.com/test")
	t.Setenv("SCMERR_TOKEN", "ghp_test")
	cfg := yamlLoader(t, `
instrument_id: SCMERR
notifications:
  slack_webhook_env: SCMERR_WEBHOOK
  github_token_env: SCMERR_TOKEN
  events:
    helix.error:
      slack:
        channel: "#alerts"
        sample_window_seconds: 60
        max_per_window: 5
      github:
        repo: owner/repo
        labels: [bug]
`)
	sl := silence.New(noopSilenceDB{}, time.Minute)
	msgStub := &stubMessaging{}
	n := notifier.New(cfg, sl, newTestMetrics(), 10, "http://ui", "http://grafana")
	n.RegisterMessaging("slack", msgStub)
	n.RegisterSCM("github", &errSCM{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go n.Start(ctx)

	n.Send(notifier.Event{InstrumentID: "SCMERR", EventName: "helix.error", EntityID: "e1", Message: "dispatch fails"})
	// Messaging fires after SCM goroutines complete (even on error), so wait for it.
	waitFor(t, 500*time.Millisecond, func() bool { return msgStub.count() >= 1 })
}
