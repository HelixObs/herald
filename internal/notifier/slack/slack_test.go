package slack_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/HelixObs/gateway/internal/notifier"
	"github.com/HelixObs/gateway/internal/notifier/slack"
)

func webhookServer(t *testing.T, status int) (url string, requestCount *atomic.Int32) {
	t.Helper()
	var n atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n.Add(1)
		w.WriteHeader(status)
	}))
	t.Cleanup(srv.Close)
	return srv.URL, &n
}

func msg(text string) notifier.Message {
	return notifier.Message{
		Text:         text,
		Body:         text,
		InstrumentID: "TEST",
		EventName:    "helix.error",
		EntityID:     "ent-1",
		InspectorURL: "http://ui/entity/ent-1",
		ErrDashURL:   "http://grafana/d/helix-error-entities",
	}
}

func TestSend_FirstCallSends(t *testing.T) {
	url, count := webhookServer(t, http.StatusOK)
	c := slack.New()
	sent, err := c.Send(context.Background(), url, "fp1", msg("hello"), 60, 3)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !sent {
		t.Error("expected sent=true for first call")
	}
	if count.Load() != 1 {
		t.Errorf("expected 1 HTTP request, got %d", count.Load())
	}
}

func TestSend_WithinWindowUnderLimit(t *testing.T) {
	url, count := webhookServer(t, http.StatusOK)
	c := slack.New()
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		sent, err := c.Send(ctx, url, "fp1", msg("hello"), 60, 3)
		if err != nil {
			t.Fatalf("call %d: unexpected error: %v", i, err)
		}
		if !sent {
			t.Errorf("call %d: expected sent=true", i)
		}
	}
	if count.Load() != 3 {
		t.Errorf("expected 3 HTTP requests, got %d", count.Load())
	}
}

func TestSend_WithinWindowOverLimit(t *testing.T) {
	url, count := webhookServer(t, http.StatusOK)
	c := slack.New()
	ctx := context.Background()

	// First 3 sends within limit.
	for i := 0; i < 3; i++ {
		c.Send(ctx, url, "fp1", msg("hello"), 60, 3) //nolint:errcheck
	}
	prevCount := count.Load()

	// 4th should be suppressed.
	sent, err := c.Send(ctx, url, "fp1", msg("suppressed"), 60, 3)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sent {
		t.Error("expected sent=false when over limit")
	}
	if count.Load() != prevCount {
		t.Error("expected no HTTP request for suppressed message")
	}
}

func TestSend_NewWindowResetsLimit(t *testing.T) {
	url, count := webhookServer(t, http.StatusOK)
	c := slack.New()
	ctx := context.Background()

	// Fill a short window (0s TTL).
	for i := 0; i < 2; i++ {
		c.Send(ctx, url, "fp1", msg("hello"), 0, 2) //nolint:errcheck
	}
	prevCount := count.Load()

	// Wait for the window to expire, then verify a new send opens a fresh window.
	time.Sleep(5 * time.Millisecond)
	sent, err := c.Send(ctx, url, "fp1", msg("new window"), 0, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !sent {
		t.Error("expected sent=true in new window")
	}
	if count.Load() <= prevCount {
		t.Error("expected additional HTTP request for new window send")
	}
}

func TestFlushDigests_SendsPendingDigest(t *testing.T) {
	url, count := webhookServer(t, http.StatusOK)
	c := slack.New()
	ctx := context.Background()

	// Send within a long window so the second message gets suppressed.
	c.Send(ctx, url, "fp1", msg("first"), 3600, 1)     //nolint:errcheck
	c.Send(ctx, url, "fp1", msg("suppressed"), 3600, 1) //nolint:errcheck

	prevCount := count.Load()
	// FlushDigests with windowSecs=0 forces all windows to flush.
	c.FlushDigests(ctx, url, 0)

	if count.Load() <= prevCount {
		t.Error("expected FlushDigests to send a digest message")
	}
}

func TestSend_ServerError_Retries(t *testing.T) {
	attempts := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		if attempts < 3 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := slack.New()
	sent, err := c.Send(context.Background(), srv.URL, "fp1", msg("hello"), 60, 5)
	if err != nil {
		t.Fatalf("unexpected error after retry: %v", err)
	}
	if !sent {
		t.Error("expected sent=true after successful retry")
	}
	if attempts != 3 {
		t.Errorf("expected 3 attempts, got %d", attempts)
	}
}

func TestSend_ClientError_NoRetry(t *testing.T) {
	attempts := 0
	countSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer countSrv.Close()

	c := slack.New()
	_, err := c.Send(context.Background(), countSrv.URL, "fp1", msg("hello"), 60, 5)
	if err == nil {
		t.Error("expected error for 400 response")
	}
	if attempts > 1 {
		t.Errorf("expected no retry for 4xx, got %d attempts", attempts)
	}
}

func TestSend_ContextCancelled(t *testing.T) {
	// Server that always returns 500 to trigger retry loop.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(50 * time.Millisecond)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	c := slack.New()
	_, err := c.Send(ctx, srv.URL, "fp1", msg("hello"), 60, 5)
	if err == nil {
		t.Error("expected error when context cancelled")
	}
}

func TestSend_MetadataInBlockKit(t *testing.T) {
	var body []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		buf := make([]byte, r.ContentLength)
		r.Body.Read(buf) //nolint:errcheck
		body = buf
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := slack.New()
	m := notifier.Message{
		Text:         "disk full",
		Body:         "disk full",
		InstrumentID: "CHIMEFRB",
		EventName:    "helix.error",
		EntityID:     "frb-2025-001",
		Stage:        "hdf5_archiver",
		InspectorURL: "http://ui/entity/frb-2025-001",
		ErrDashURL:   "http://grafana/d/helix-error-entities",
		Metadata:     map[string]string{"process": "hdf5_archiver_v2"},
	}
	c.Send(context.Background(), srv.URL, "deadbeef", m, 60, 5) //nolint:errcheck

	bodyStr := string(body)
	for _, want := range []string{"hdf5_archiver", "hdf5_archiver_v2", "frb-2025-001", "CHIMEFRB", "helix.error"} {
		if !strings.Contains(bodyStr, want) {
			t.Errorf("expected Block Kit JSON to contain %q, got:\n%s", want, bodyStr)
		}
	}
}

func TestSend_IssueURLInContext(t *testing.T) {
	var body []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		buf := make([]byte, r.ContentLength)
		r.Body.Read(buf) //nolint:errcheck
		body = buf
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := slack.New()
	m := notifier.Message{
		Text:         "disk full",
		Body:         "disk full",
		InstrumentID: "CHIMEFRB",
		EventName:    "helix.error",
		EntityID:     "frb-001",
		InspectorURL: "http://ui/entity/frb-001",
		ErrDashURL:   "http://grafana/d/helix-error-entities",
		IssueURLs:    []string{"https://github.com/owner/repo/issues/42"},
	}
	c.Send(context.Background(), srv.URL, "deadbeef", m, 60, 5) //nolint:errcheck

	if !strings.Contains(string(body), "github.com/owner/repo/issues/42") {
		t.Errorf("expected Block Kit JSON to contain GitHub issue URL, got:\n%s", string(body))
	}
}
