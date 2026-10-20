package notifier

import (
	"bytes"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/HelixObs/herald/internal/notifier/config"
)

func TestWarnStaleOverrides(t *testing.T) {
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })

	t.Setenv("STALE_WEBHOOK", "https://hooks.slack.com/stale")
	dir := t.TempDir()
	yml := `
instrument_id: STALE
notifications:
  slack_webhook_env: STALE_WEBHOOK
  events:
    helix.error:
      slack:
        channel: "#alerts"
        sample_window_seconds: 60
        max_per_window: 1
      overrides:
        - match: { operation: renamed-op }
          playbook: https://wiki.example/renamed
        - match: { operation: busy-op }
          playbook: https://wiki.example/busy
`
	if err := os.WriteFile(filepath.Join(dir, "stale.yml"), []byte(yml), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := config.New(dir, 5*time.Second, 60*time.Second)
	cfg.Start(t.Context(), time.Hour)

	n := New(cfg, nil, nil, 1, "http://ui", "http://grafana")
	warnings := func(op string) int { return strings.Count(buf.String(), "operation="+op) }

	// Within 7 days of start: nothing is stale yet.
	n.warnStaleOverrides(n.startedAt.Add(6 * 24 * time.Hour))
	if warnings("renamed-op") != 0 {
		t.Fatalf("warned before 7 days:\n%s", buf.String())
	}

	// busy-op keeps seeing events; renamed-op never does.
	n.seenOperation("STALE", "busy-op")
	later := time.Now().Add(8 * 24 * time.Hour)
	n.opSeen.Store("STALE|busy-op", later.Add(-time.Hour))
	n.warnStaleOverrides(later)
	n.warnStaleOverrides(later.Add(time.Hour))
	if got := warnings("renamed-op"); got != 1 {
		t.Errorf("renamed-op: got %d warnings, want exactly 1", got)
	}
	if got := warnings("busy-op"); got != 0 {
		t.Errorf("busy-op: got %d warnings, want 0", got)
	}

	// renamed-op recovers, then goes quiet again: warned a second time.
	n.opSeen.Store("STALE|renamed-op", later)
	n.warnStaleOverrides(later.Add(2 * time.Hour))
	n.warnStaleOverrides(later.Add(8 * 24 * time.Hour))
	if got := warnings("renamed-op"); got != 2 {
		t.Errorf("renamed-op after recovering and going quiet: got %d warnings, want 2", got)
	}
}
