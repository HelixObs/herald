// Package config loads and hot-reloads per-instrument notification config from YAML files.
package config

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"gopkg.in/yaml.v3"
)

// Registry maps instrument_id → resolved InstrumentConfig.
type Registry map[string]*InstrumentConfig

// InstrumentConfig is the resolved notifications config for one instrument.
type InstrumentConfig struct {
	InstrumentID string
	Events       map[string]EventConfig
}

// EventConfig describes how one event type should be notified.
type EventConfig struct {
	MessageTemplate string
	Messaging       []MessagingCall
	SCM             []SCMCall
}

// MessagingCall holds resolved config for one messaging backend invocation
// (e.g. a specific Slack webhook + channel, a Discord webhook, etc.).
type MessagingCall struct {
	Type                string // backend type: "slack", "discord", …
	Destination         string // resolved webhook URL / endpoint
	Channel             string // display name shown in messages
	MessageTemplate     string // per-call override; falls back to EventConfig.MessageTemplate
	SampleWindowSeconds int
	MaxPerWindow        int
}

// SCMCall holds resolved config for one SCM backend invocation
// (e.g. a GitHub repo, a GitLab project, etc.).
type SCMCall struct {
	Type                   string   // backend type: "github", "gitlab", …
	Token                  string   // resolved credential
	Repo                   string   // "owner/repo" or equivalent
	Labels                 []string
	AutoCloseAfterDays     int
	OnRecurrenceAfterClose string // "reopen" (default) or "new_issue"
}

// ── Raw YAML structs ──────────────────────────────────────────────────────────

type instrumentYAML struct {
	InstrumentID  string            `yaml:"instrument_id"`
	Notifications notificationsYAML `yaml:"notifications"`
}

type notificationsYAML struct {
	SlackWebhookEnv string               `yaml:"slack_webhook_env"`
	GithubTokenEnv  string               `yaml:"github_token_env"`
	Events          map[string]eventYAML `yaml:"events"`
}

type eventYAML struct {
	MessageTemplate string      `yaml:"message_template"`
	Slack           *slackYAML  `yaml:"slack"`
	Github          *githubYAML `yaml:"github"`
}

type slackYAML struct {
	Channel             string `yaml:"channel"`
	WebhookEnv          string `yaml:"webhook_env"`
	SampleWindowSeconds int    `yaml:"sample_window_seconds"`
	MaxPerWindow        int    `yaml:"max_per_window"`
}

type githubYAML struct {
	Repo                   string   `yaml:"repo"`
	Labels                 []string `yaml:"labels"`
	AutoCloseAfterDays     int      `yaml:"auto_close_after_days"`
	OnRecurrenceAfterClose string   `yaml:"on_recurrence_after_close"`
}

// ── Loader ────────────────────────────────────────────────────────────────────

// Loader reads instrument YAMLs from a directory and exposes a hot-reloaded Registry.
type Loader struct {
	dir             string
	current         atomic.Value // holds Registry
	minSlackWindow  time.Duration
	minGithubWindow time.Duration
}

// New creates a Loader. minSlackWindow and minGithubWindow are floors enforced
// on sample_window_seconds regardless of what the YAML specifies.
func New(dir string, minSlackWindow, minGithubWindow time.Duration) *Loader {
	return &Loader{
		dir:             dir,
		minSlackWindow:  minSlackWindow,
		minGithubWindow: minGithubWindow,
	}
}

// Start loads config immediately then reloads on interval until ctx is cancelled.
func (l *Loader) Start(ctx context.Context, interval time.Duration) {
	l.reload()
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				l.reload()
			case <-ctx.Done():
				return
			}
		}
	}()
}

// Get returns the current Registry. Safe for concurrent use.
func (l *Loader) Get() Registry {
	v := l.current.Load()
	if v == nil {
		return Registry{}
	}
	return v.(Registry)
}

func (l *Loader) reload() {
	patterns := []string{
		filepath.Join(l.dir, "*-context.yml"),
		filepath.Join(l.dir, "*-context.yaml"),
		filepath.Join(l.dir, "*.yml"),
		filepath.Join(l.dir, "*.yaml"),
	}

	seen := map[string]bool{}
	var files []string
	for _, pat := range patterns {
		matches, _ := filepath.Glob(pat)
		for _, f := range matches {
			if !seen[f] {
				seen[f] = true
				files = append(files, f)
			}
		}
	}

	reg := make(Registry, len(files))
	for _, f := range files {
		cfg, errs := l.loadFile(f)
		if len(errs) > 0 {
			msgs := make([]string, len(errs))
			for i, e := range errs {
				msgs[i] = e.Error()
			}
			slog.Warn("notifier: skipping instrument config",
				"file", f,
				"errors", strings.Join(msgs, "; "))
			continue
		}
		if cfg == nil {
			continue
		}
		reg[cfg.InstrumentID] = cfg
	}
	l.current.Store(reg)
}

func (l *Loader) loadFile(path string) (*InstrumentConfig, []error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, []error{fmt.Errorf("read file: %w", err)}
	}

	var raw instrumentYAML
	if err := yaml.Unmarshal(data, &raw); err != nil {
		return nil, []error{fmt.Errorf("parse yaml: %w", err)}
	}

	if raw.InstrumentID == "" {
		return nil, []error{fmt.Errorf("instrument_id is required")}
	}
	if len(raw.Notifications.Events) == 0 {
		return nil, nil // no notifications block — not an error
	}

	// Resolve instrument-level credentials.
	defaultSlackURL := ""
	if raw.Notifications.SlackWebhookEnv != "" {
		defaultSlackURL = os.Getenv(raw.Notifications.SlackWebhookEnv)
	}
	defaultGithubToken := ""
	if raw.Notifications.GithubTokenEnv != "" {
		defaultGithubToken = os.Getenv(raw.Notifications.GithubTokenEnv)
	}

	cfg := &InstrumentConfig{
		InstrumentID: raw.InstrumentID,
		Events:       make(map[string]EventConfig, len(raw.Notifications.Events)),
	}

	var errs []error

	for eventType, ev := range raw.Notifications.Events {
		if !strings.HasPrefix(eventType, "helix.") {
			errs = append(errs, fmt.Errorf("notifications.events[%q]: event type must start with helix.", eventType))
			continue
		}

		ecfg := EventConfig{MessageTemplate: ev.MessageTemplate}

		if ev.Slack != nil {
			mc, evErrs := l.resolveSlack(eventType, ev, raw.InstrumentID, defaultSlackURL)
			errs = append(errs, evErrs...)
			if mc != nil {
				mc.MessageTemplate = ev.MessageTemplate
				ecfg.Messaging = append(ecfg.Messaging, *mc)
			}
		}

		if ev.Github != nil {
			sc, evErrs := l.resolveGithub(eventType, ev, raw.InstrumentID, defaultGithubToken)
			errs = append(errs, evErrs...)
			if sc != nil {
				ecfg.SCM = append(ecfg.SCM, *sc)
			}
		}

		if len(errs) == 0 {
			cfg.Events[eventType] = ecfg
		}
	}

	if len(errs) > 0 {
		return nil, errs
	}
	return cfg, nil
}

func (l *Loader) resolveSlack(eventType string, ev eventYAML, instrumentID, defaultURL string) (*MessagingCall, []error) {
	var errs []error
	s := ev.Slack

	if s.Channel == "" {
		errs = append(errs, fmt.Errorf("notifications.events[%q].slack.channel: required", eventType))
	} else if !strings.HasPrefix(s.Channel, "#") {
		errs = append(errs, fmt.Errorf("notifications.events[%q].slack.channel: must start with #", eventType))
	}
	if s.MaxPerWindow < 1 {
		errs = append(errs, fmt.Errorf("notifications.events[%q].slack.max_per_window: must be >= 1", eventType))
	}

	minW := int(l.minSlackWindow.Seconds())
	window := s.SampleWindowSeconds
	if window < minW {
		slog.Warn("notifier: clamping sample_window_seconds to minimum",
			"instrument", instrumentID, "event", eventType,
			"configured", window, "minimum", minW)
		window = minW
	}

	// Resolve webhook URL: per-event override wins over instrument default.
	webhookURL := defaultURL
	if s.WebhookEnv != "" {
		webhookURL = os.Getenv(s.WebhookEnv)
		if webhookURL == "" {
			errs = append(errs, fmt.Errorf("notifications.events[%q].slack.webhook_env=%q: env var not set or empty",
				eventType, s.WebhookEnv))
		}
	} else if webhookURL == "" {
		errs = append(errs, fmt.Errorf("notifications.slack_webhook_env: not set or empty (required for slack events)"))
	}

	if len(errs) > 0 {
		return nil, errs
	}
	return &MessagingCall{
		Type:                "slack",
		Destination:         webhookURL,
		Channel:             s.Channel,
		SampleWindowSeconds: window,
		MaxPerWindow:        s.MaxPerWindow,
	}, nil
}

func (l *Loader) resolveGithub(eventType string, ev eventYAML, instrumentID, defaultToken string) (*SCMCall, []error) {
	var errs []error
	g := ev.Github

	if g.Repo == "" || !strings.Contains(g.Repo, "/") {
		errs = append(errs, fmt.Errorf("notifications.events[%q].github.repo: must be in owner/repo format", eventType))
	}
	if g.AutoCloseAfterDays < 0 {
		errs = append(errs, fmt.Errorf("notifications.events[%q].github.auto_close_after_days: must be >= 0", eventType))
	}
	recurrence := g.OnRecurrenceAfterClose
	if recurrence == "" {
		recurrence = "reopen"
	}
	if recurrence != "reopen" && recurrence != "new_issue" {
		errs = append(errs, fmt.Errorf("notifications.events[%q].github.on_recurrence_after_close: must be reopen or new_issue", eventType))
	}
	if defaultToken == "" {
		errs = append(errs, fmt.Errorf("notifications.github_token_env: not set or empty (required for github events)"))
	}

	if len(errs) > 0 {
		return nil, errs
	}
	return &SCMCall{
		Type:                   "github",
		Token:                  defaultToken,
		Repo:                   g.Repo,
		Labels:                 g.Labels,
		AutoCloseAfterDays:     g.AutoCloseAfterDays,
		OnRecurrenceAfterClose: recurrence,
	}, nil
}
