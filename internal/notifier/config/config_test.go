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

// ── Overrides and playbooks ───────────────────────────────────────────────────

// load writes content as the only instrument config and returns the registry.
func load(t *testing.T, content string) config.Registry {
	t.Helper()
	dir := t.TempDir()
	writeYAML(t, dir, "test.yml", content)
	l := config.New(dir, 5*time.Second, 60*time.Second)
	l.Start(t.Context(), time.Hour)
	return l.Get()
}

func overrideTestEnv(t *testing.T) {
	t.Helper()
	t.Setenv("OV_DEFAULT_WEBHOOK", "https://hooks.slack.com/default")
	t.Setenv("OV_BASEBAND_WEBHOOK", "https://hooks.slack.com/baseband")
	t.Setenv("OV_GH_TOKEN", "ghp_test")
}

// eventBlock is a helix.error event with slack, github and a playbook, to
// which tests append an overrides list.
const eventBlock = `
instrument_id: OV
notifications:
  slack_webhook_env: OV_DEFAULT_WEBHOOK
  github_token_env: OV_GH_TOKEN
  playbook: https://wiki.example/Playbook
  events:
    helix.error:
      playbook: https://wiki.example/Errors
      slack:
        channel: "#general-alerts"
        sample_window_seconds: 60
        max_per_window: 1
      github:
        repo: org/troubleshooting
`

func TestLoadFile_OverrideReplacesSetBlocksAndInheritsTheRest(t *testing.T) {
	overrideTestEnv(t)
	reg := load(t, eventBlock+`
      overrides:
        - match: { operation: convert-data-to-hdf5 }
          slack:
            channel: "#baseband-alerts"
            webhook_env: OV_BASEBAND_WEBHOOK
            sample_window_seconds: 60
            max_per_window: 1
`)
	evCfg := reg["OV"].Events["helix.error"]
	if len(evCfg.Overrides) != 1 {
		t.Fatalf("expected 1 override, got %d", len(evCfg.Overrides))
	}
	ov := evCfg.Overrides[0]
	if ov.Operation != "convert-data-to-hdf5" || ov.Stage != "" {
		t.Errorf("unexpected match: operation=%q stage=%q", ov.Operation, ov.Stage)
	}
	if len(ov.Messaging) != 1 || ov.Messaging[0].Destination != "https://hooks.slack.com/baseband" {
		t.Errorf("expected the override's own slack block, got %+v", ov.Messaging)
	}
	if len(ov.SCM) != 1 || ov.SCM[0].Repo != "org/troubleshooting" {
		t.Errorf("expected github inherited from the event, got %+v", ov.SCM)
	}
	if _, _, pb := reg["OV"].Events["helix.error"].Resolve("convert-data-to-hdf5", ""); pb.URL != "https://wiki.example/Errors" {
		t.Errorf("expected the event's playbook inherited, got %+v", pb)
	}
}

func TestLoadFile_OverrideOffTurnsDestinationOff(t *testing.T) {
	overrideTestEnv(t)
	reg := load(t, eventBlock+`
      overrides:
        - match: { operation: ml-plotter }
          slack: off
          github: off
          playbook: https://github.com/org/runbooks/blob/main/ml-plotter.md
`)
	ov := reg["OV"].Events["helix.error"].Overrides[0]
	if len(ov.Messaging) != 0 || len(ov.SCM) != 0 {
		t.Errorf("expected slack and github off, got messaging=%+v scm=%+v", ov.Messaging, ov.SCM)
	}
	if ov.Playbook != "https://github.com/org/runbooks/blob/main/ml-plotter.md" {
		t.Errorf("expected the override's playbook, got %q", ov.Playbook)
	}
}

func TestLoadFile_OverrideKeepsStageFilter(t *testing.T) {
	overrideTestEnv(t)
	reg := load(t, eventBlock+`
      overrides:
        - match: { operation: baseband-callback, stage: request-to-kko-coco }
          playbook: https://wiki.example/kko
`)
	ov := reg["OV"].Events["helix.error"].Overrides[0]
	if ov.Operation != "baseband-callback" || ov.Stage != "request-to-kko-coco" {
		t.Errorf("unexpected match: operation=%q stage=%q", ov.Operation, ov.Stage)
	}
}

func TestLoadFile_OverrideRejected(t *testing.T) {
	cases := map[string]string{
		"stage without operation": `
        - match: { stage: request-to-kko-coco }
          playbook: https://wiki.example/kko
`,
		"no match": `
        - playbook: https://wiki.example/kko
`,
		"sets nothing": `
        - match: { operation: ml-plotter }
`,
		"invalid slack block": `
        - match: { operation: ml-plotter }
          slack:
            channel: missing-hash-prefix
            sample_window_seconds: 60
            max_per_window: 1
`,
		"scalar other than off": `
        - match: { operation: ml-plotter }
          github: on
`,
	}
	for name, overrides := range cases {
		t.Run(name, func(t *testing.T) {
			overrideTestEnv(t)
			reg := load(t, eventBlock+"      overrides:\n"+overrides)
			if _, ok := reg["OV"]; ok {
				t.Error("expected the instrument config to be rejected")
			}
		})
	}
}

func TestLoadFile_PlaybookFallsBackToNotificationsLevel(t *testing.T) {
	overrideTestEnv(t)
	reg := load(t, `
instrument_id: OV
notifications:
  slack_webhook_env: OV_DEFAULT_WEBHOOK
  playbook: https://wiki.example/Playbook
  events:
    helix.error:
      slack:
        channel: "#alerts"
        sample_window_seconds: 60
        max_per_window: 1
`)
	if got := reg["OV"].Events["helix.error"].Playbook; got != "https://wiki.example/Playbook" {
		t.Errorf("expected notifications.playbook, got %q", got)
	}
}

func TestLoadFile_InvalidPlaybookDroppedConfigStillLoads(t *testing.T) {
	overrideTestEnv(t)
	reg := load(t, `
instrument_id: OV
notifications:
  slack_webhook_env: OV_DEFAULT_WEBHOOK
  playbook: https://wiki.example/Playbook
  events:
    helix.error:
      playbook: wiki.example/no-scheme
      slack:
        channel: "#alerts"
        sample_window_seconds: 60
        max_per_window: 1
`)
	cfg, ok := reg["OV"]
	if !ok {
		t.Fatal("an invalid playbook must not reject the config")
	}
	if got := cfg.Events["helix.error"].Playbook; got != "https://wiki.example/Playbook" {
		t.Errorf("expected the invalid playbook dropped in favour of the default, got %q", got)
	}
}

func TestLoadFile_RoutesIgnored(t *testing.T) {
	overrideTestEnv(t)
	reg := load(t, eventBlock+`
      routes:
        - match: { stage: hdf5-conversion }
          slack: { channel: "#baseband-alerts", sample_window_seconds: 60, max_per_window: 1 }
`)
	evCfg, ok := reg["OV"].Events["helix.error"]
	if !ok {
		t.Fatal("a leftover routes key must not reject the config")
	}
	if len(evCfg.Overrides) != 0 {
		t.Errorf("expected routes ignored, got %d overrides", len(evCfg.Overrides))
	}
}

func TestResolve_FirstMatchingOverrideWins(t *testing.T) {
	ev := config.EventConfig{
		Messaging: []config.MessagingCall{{Channel: "#default"}},
		Playbook:  "https://wiki.example/default",
		Overrides: []config.Override{
			{Operation: "baseband-callback", Stage: "request-to-kko-coco", Playbook: "https://wiki.example/kko"},
			{Operation: "baseband-callback", Playbook: "https://wiki.example/callback"},
		},
	}
	cases := []struct{ operation, stage, want string }{
		{"baseband-callback", "request-to-kko-coco", "https://wiki.example/kko"},
		{"baseband-callback", "request-to-gbo-coco", "https://wiki.example/callback"},
		{"baseband-callback", "", "https://wiki.example/callback"},
		{"replicate", "request-to-kko-coco", "https://wiki.example/default"},
		{"Baseband-Callback", "", "https://wiki.example/default"},
	}
	for _, c := range cases {
		messaging, _, playbook := ev.Resolve(c.operation, c.stage)
		if playbook.URL != c.want || playbook.Generated {
			t.Errorf("Resolve(%q, %q) playbook = %+v, want %q", c.operation, c.stage, playbook, c.want)
		}
		if c.want == "https://wiki.example/default" && (len(messaging) != 1 || messaging[0].Channel != "#default") {
			t.Errorf("Resolve(%q, %q): expected the event's own messaging", c.operation, c.stage)
		}
	}
}

func TestResolve_PlaybookBase(t *testing.T) {
	overrideTestEnv(t)
	reg := load(t, `
instrument_id: OV
notifications:
  slack_webhook_env: OV_DEFAULT_WEBHOOK
  playbook: https://wiki.example/Playbook
  playbook_base: https://github.com/org/troubleshooting/tree/main/runbooks/
  events:
    helix.error:
      playbook: https://wiki.example/Errors
      slack:
        channel: "#alerts"
        sample_window_seconds: 60
        max_per_window: 1
      overrides:
        - match: { operation: alert-outriggers }
          playbook: https://wiki.example/Outriggers
        - match: { operation: ml-plotter }
          github: off
`)
	ev := reg["OV"].Events["helix.error"]
	cases := []struct {
		operation string
		want      config.Playbook
	}{
		// trailing slash on the base is trimmed
		{"replicate", config.Playbook{URL: "https://github.com/org/troubleshooting/tree/main/runbooks/replicate", Generated: true}},
		// an override's own playbook wins over the base
		{"alert-outriggers", config.Playbook{URL: "https://wiki.example/Outriggers"}},
		// an override without a playbook still gets the base link
		{"ml-plotter", config.Playbook{URL: "https://github.com/org/troubleshooting/tree/main/runbooks/ml-plotter", Generated: true}},
		// unusual characters are path-escaped rather than creating new path segments
		{"a/b c", config.Playbook{URL: "https://github.com/org/troubleshooting/tree/main/runbooks/a%2Fb%20c", Generated: true}},
		// no operation: nothing to build a link from
		{"", config.Playbook{URL: "https://wiki.example/Errors"}},
	}
	for _, c := range cases {
		if _, _, got := ev.Resolve(c.operation, ""); got != c.want {
			t.Errorf("Resolve(%q) = %+v, want %+v", c.operation, got, c.want)
		}
	}
}
