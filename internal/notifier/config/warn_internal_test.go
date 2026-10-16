package config

import (
	"bytes"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const deprecatedAutoCloseYAML = `
instrument_id: TEST
notifications:
  github_token_env: TEST_GH_TOKEN
  events:
    helix.error:
      github:
        repo: org/repo
        auto_close_after_days: 7
`

// Reloading an unchanged config must not repeat its warnings every interval;
// a warning that clears and later comes back is logged again.
func TestReload_ConfigWarningsLoggedOnce(t *testing.T) {
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })

	t.Setenv("TEST_GH_TOKEN", "ghp_test")
	dir := t.TempDir()
	path := filepath.Join(dir, "test.yml")
	write := func(content string) {
		t.Helper()
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatalf("write temp yaml: %v", err)
		}
	}
	count := func() int { return strings.Count(buf.String(), "auto_close_after_days is deprecated") }

	l := New(dir, 5*time.Second, 60*time.Second)

	write(deprecatedAutoCloseYAML)
	l.reload()
	l.reload()
	l.reload()
	if got := count(); got != 1 {
		t.Fatalf("after 3 reloads of an unchanged config: got %d warnings, want 1", got)
	}

	write(strings.Replace(deprecatedAutoCloseYAML, "        auto_close_after_days: 7\n", "", 1))
	l.reload()
	if got := count(); got != 1 {
		t.Fatalf("after removing the field: got %d warnings, want still 1", got)
	}

	write(deprecatedAutoCloseYAML)
	l.reload()
	if got := count(); got != 2 {
		t.Fatalf("after re-adding the field: got %d warnings, want 2", got)
	}
}
