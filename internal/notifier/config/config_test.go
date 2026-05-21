package config_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/HelixObs/herald/internal/notifier/config"
)

func writeYAML(t *testing.T, dir, name, content string) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatalf("write temp yaml: %v", err)
	}
	return p
}

func TestLoadFile_Valid(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("TEST_SLACK_WEBHOOK", "https://hooks.slack.com/test")
	t.Setenv("TEST_GH_TOKEN", "ghp_test")

	writeYAML(t, dir, "test.yml", `
instrument_id: TEST
notifications:
  slack_webhook_env: TEST_SLACK_WEBHOOK
  github_token_env:  TEST_GH_TOKEN
  events:
    helix.error:
      slack:
        channel: "#alerts"
        sample_window_seconds: 300
        max_per_window: 5
      github:
        repo: org/repo
        labels: [bug]
        auto_close_after_days: 7
        on_recurrence_after_close: reopen
`)

	l := config.New(dir, 5*time.Second, 60*time.Second)
	l.Start(t.Context(), time.Hour)
	reg := l.Get()

	cfg, ok := reg["TEST"]
	if !ok {
		t.Fatal("expected TEST instrument in registry")
	}

	evCfg, ok := cfg.Events["helix.error"]
	if !ok {
		t.Fatal("expected helix.error event config")
	}
	if len(evCfg.Messaging) != 1 {
		t.Fatalf("expected 1 messaging call, got %d", len(evCfg.Messaging))
	}
	mc := evCfg.Messaging[0]
	if mc.Type != "slack" {
		t.Errorf("expected type slack, got %q", mc.Type)
	}
	if mc.Destination != "https://hooks.slack.com/test" {
		t.Errorf("unexpected destination %q", mc.Destination)
	}
	if mc.Channel != "#alerts" {
		t.Errorf("unexpected channel %q", mc.Channel)
	}
	if mc.SampleWindowSeconds != 300 {
		t.Errorf("unexpected window %d", mc.SampleWindowSeconds)
	}
	if mc.MaxPerWindow != 5 {
		t.Errorf("unexpected max_per_window %d", mc.MaxPerWindow)
	}

	if len(evCfg.SCM) != 1 {
		t.Fatalf("expected 1 SCM call, got %d", len(evCfg.SCM))
	}
	sc := evCfg.SCM[0]
	if sc.Type != "github" {
		t.Errorf("expected type github, got %q", sc.Type)
	}
	if sc.Repo != "org/repo" {
		t.Errorf("unexpected repo %q", sc.Repo)
	}
	if sc.Token != "ghp_test" {
		t.Errorf("unexpected token")
	}
	if sc.OnRecurrenceAfterClose != "reopen" {
		t.Errorf("unexpected on_recurrence %q", sc.OnRecurrenceAfterClose)
	}
}

func TestLoadFile_MissingInstrumentID(t *testing.T) {
	dir := t.TempDir()
	writeYAML(t, dir, "test.yml", `
notifications:
  events:
    helix.error: {}
`)
	l := config.New(dir, 5*time.Second, 60*time.Second)
	l.Start(t.Context(), time.Hour)
	reg := l.Get()
	if len(reg) != 0 {
		t.Errorf("expected empty registry for missing instrument_id, got %d entries", len(reg))
	}
}

func TestLoadFile_NoNotifications(t *testing.T) {
	dir := t.TempDir()
	writeYAML(t, dir, "test.yml", `
instrument_id: SILENT
`)
	l := config.New(dir, 5*time.Second, 60*time.Second)
	l.Start(t.Context(), time.Hour)
	reg := l.Get()
	// No events block — not an error, just not added to registry.
	if _, ok := reg["SILENT"]; ok {
		t.Error("expected SILENT absent from registry (no notifications block)")
	}
}

func TestLoadFile_NonHelixEventName(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("TEST_SLACK_WEBHOOK2", "https://hooks.slack.com/test")
	writeYAML(t, dir, "test.yml", `
instrument_id: BAD
notifications:
  slack_webhook_env: TEST_SLACK_WEBHOOK2
  events:
    not_helix.error:
      slack:
        channel: "#alerts"
        sample_window_seconds: 60
        max_per_window: 1
`)
	l := config.New(dir, 5*time.Second, 60*time.Second)
	l.Start(t.Context(), time.Hour)
	reg := l.Get()
	if _, ok := reg["BAD"]; ok {
		t.Error("expected BAD absent from registry due to invalid event name")
	}
}

func TestLoadFile_SlackWindowClamped(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("CLAMP_SLACK_WEBHOOK", "https://hooks.slack.com/test")
	writeYAML(t, dir, "test.yml", `
instrument_id: CLAMP
notifications:
  slack_webhook_env: CLAMP_SLACK_WEBHOOK
  events:
    helix.error:
      slack:
        channel: "#alerts"
        sample_window_seconds: 1
        max_per_window: 1
`)
	minWindow := 30 * time.Second
	l := config.New(dir, minWindow, 60*time.Second)
	l.Start(t.Context(), time.Hour)
	reg := l.Get()

	cfg, ok := reg["CLAMP"]
	if !ok {
		t.Fatal("expected CLAMP in registry")
	}
	mc := cfg.Events["helix.error"].Messaging[0]
	if mc.SampleWindowSeconds < int(minWindow.Seconds()) {
		t.Errorf("window not clamped: got %d, want >= %d", mc.SampleWindowSeconds, int(minWindow.Seconds()))
	}
}

func TestLoadFile_PerEventWebhookOverride(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("INST_WEBHOOK", "https://hooks.slack.com/default")
	t.Setenv("EVENT_WEBHOOK", "https://hooks.slack.com/override")
	writeYAML(t, dir, "test.yml", `
instrument_id: OVERRIDE
notifications:
  slack_webhook_env: INST_WEBHOOK
  events:
    helix.error:
      slack:
        channel: "#alerts"
        webhook_env: EVENT_WEBHOOK
        sample_window_seconds: 60
        max_per_window: 1
`)
	l := config.New(dir, 5*time.Second, 60*time.Second)
	l.Start(t.Context(), time.Hour)
	reg := l.Get()

	cfg, ok := reg["OVERRIDE"]
	if !ok {
		t.Fatal("expected OVERRIDE in registry")
	}
	mc := cfg.Events["helix.error"].Messaging[0]
	if mc.Destination != "https://hooks.slack.com/override" {
		t.Errorf("expected per-event webhook override, got %q", mc.Destination)
	}
}

func TestLoadFile_SlackMissingChannel(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("ERR_SLACK_WEBHOOK", "https://hooks.slack.com/test")
	writeYAML(t, dir, "test.yml", `
instrument_id: ERR
notifications:
  slack_webhook_env: ERR_SLACK_WEBHOOK
  events:
    helix.error:
      slack:
        sample_window_seconds: 60
        max_per_window: 1
`)
	l := config.New(dir, 5*time.Second, 60*time.Second)
	l.Start(t.Context(), time.Hour)
	reg := l.Get()
	if _, ok := reg["ERR"]; ok {
		t.Error("expected ERR absent from registry due to missing channel")
	}
}

func TestLoadFile_SlackChannelNeedsHash(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("NOHASH_SLACK_WEBHOOK", "https://hooks.slack.com/test")
	writeYAML(t, dir, "test.yml", `
instrument_id: NOHASH
notifications:
  slack_webhook_env: NOHASH_SLACK_WEBHOOK
  events:
    helix.error:
      slack:
        channel: alerts
        sample_window_seconds: 60
        max_per_window: 1
`)
	l := config.New(dir, 5*time.Second, 60*time.Second)
	l.Start(t.Context(), time.Hour)
	reg := l.Get()
	if _, ok := reg["NOHASH"]; ok {
		t.Error("expected NOHASH absent from registry due to channel missing # prefix")
	}
}

func TestLoadFile_GithubInvalidRepo(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("GH_TOKEN_BADREPO", "ghp_test")
	writeYAML(t, dir, "test.yml", `
instrument_id: BADREPO
notifications:
  github_token_env: GH_TOKEN_BADREPO
  events:
    helix.error:
      github:
        repo: not-owner-slash-repo
`)
	l := config.New(dir, 5*time.Second, 60*time.Second)
	l.Start(t.Context(), time.Hour)
	reg := l.Get()
	if _, ok := reg["BADREPO"]; ok {
		t.Error("expected BADREPO absent from registry due to invalid repo format")
	}
}

func TestLoadFile_GithubDefaultRecurrence(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("GH_TOKEN_DFLT", "ghp_test")
	writeYAML(t, dir, "test.yml", `
instrument_id: DFLT
notifications:
  github_token_env: GH_TOKEN_DFLT
  events:
    helix.error:
      github:
        repo: org/repo
`)
	l := config.New(dir, 5*time.Second, 60*time.Second)
	l.Start(t.Context(), time.Hour)
	reg := l.Get()

	cfg, ok := reg["DFLT"]
	if !ok {
		t.Fatal("expected DFLT in registry")
	}
	sc := cfg.Events["helix.error"].SCM[0]
	if sc.OnRecurrenceAfterClose != "reopen" {
		t.Errorf("expected default recurrence=reopen, got %q", sc.OnRecurrenceAfterClose)
	}
}

func TestGetEmptyBeforeLoad(t *testing.T) {
	l := config.New(t.TempDir(), 5*time.Second, 60*time.Second)
	reg := l.Get()
	if len(reg) != 0 {
		t.Errorf("expected empty registry before load, got %d entries", len(reg))
	}
}
