// Package config loads and hot-reloads per-instrument notification config from YAML files.
package config

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"gopkg.in/yaml.v3"
)

// Off is the YAML value that turns a destination off in an override
// (`slack: off`, `github: off`).
const Off = "off"

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
	// Playbook is the event's playbook URL, already falling back to the
	// instrument-level notifications.playbook. Empty means none configured.
	// Used only when PlaybookBase is empty.
	Playbook string
	// PlaybookBase is notifications.playbook_base without a trailing slash.
	// When set, an event's playbook is PlaybookBase/<operation>, unless the
	// matching override sets its own.
	PlaybookBase string
	// Overrides are checked in order; the first whose Match fits the event
	// applies. Each is fully resolved at load time: anything the override
	// left unset was inherited from this EventConfig.
	Overrides []Override
}

// Override replaces Messaging/SCM/Playbook for events matching Operation (and
// Stage, if set). Operation is compared against the name of the span that
// emitted the event; Stage against the event's "stage" metadata. Both exact.
type Override struct {
	Operation string
	Stage     string // optional; empty matches any stage, including none
	Messaging []MessagingCall
	SCM       []SCMCall
	// Playbook is set only when the override itself sets one; otherwise the
	// event's playbook resolution applies (see EventConfig.Resolve).
	Playbook string
}

// Playbook is a resolved playbook link.
type Playbook struct {
	URL string
	// Generated is true when URL was built from playbook_base and the
	// operation name, so the page may not exist yet.
	Generated bool
}

// Matches reports whether the override applies to an event from operation
// with the given stage metadata.
func (o Override) Matches(operation, stage string) bool {
	return o.Operation == operation && (o.Stage == "" || o.Stage == stage)
}

// Resolve returns the Messaging, SCM and playbook for an event from operation
// with the given stage metadata. Messaging and SCM come from the first
// matching override, or the event if none matches. The playbook is, most
// specific first: the matching override's own, PlaybookBase/<operation>, the
// event's, then notifications.playbook (the last two already folded into
// e.Playbook).
func (e EventConfig) Resolve(operation, stage string) ([]MessagingCall, []SCMCall, Playbook) {
	messaging, scm := e.Messaging, e.SCM
	for _, o := range e.Overrides {
		if o.Matches(operation, stage) {
			if o.Playbook != "" {
				return o.Messaging, o.SCM, Playbook{URL: o.Playbook}
			}
			messaging, scm = o.Messaging, o.SCM
			break
		}
	}
	if e.PlaybookBase != "" && operation != "" {
		return messaging, scm, Playbook{URL: e.PlaybookBase + "/" + url.PathEscape(operation), Generated: true}
	}
	return messaging, scm, Playbook{URL: e.Playbook}
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
	Type                   string // backend type: "github", "gitlab", …
	Token                  string // resolved credential
	Repo                   string // "owner/repo" or equivalent
	Labels                 []string
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
	Playbook        string               `yaml:"playbook"`
	PlaybookBase    string               `yaml:"playbook_base"`
	Events          map[string]eventYAML `yaml:"events"`
}

type eventYAML struct {
	MessageTemplate string         `yaml:"message_template"`
	Slack           *slackYAML     `yaml:"slack"`
	Github          *githubYAML    `yaml:"github"`
	Playbook        string         `yaml:"playbook"`
	Overrides       []overrideYAML `yaml:"overrides"`

	// Deprecated: replaced by overrides. Parsed only to warn that it's ignored.
	Routes yaml.Node `yaml:"routes"`
}

// overrideYAML replaces slack/github/playbook for events matching Match.
// A block left out is inherited from the event; `off` turns it off.
type overrideYAML struct {
	Match    matchYAML              `yaml:"match"`
	Slack    switchable[slackYAML]  `yaml:"slack"`
	Github   switchable[githubYAML] `yaml:"github"`
	Playbook string                 `yaml:"playbook"`
}

type matchYAML struct {
	Operation string `yaml:"operation"`
	Stage     string `yaml:"stage"`
}

// switchable is an override's slack or github value: left out (inherit),
// the scalar `off`, or a block that replaces the event's.
type switchable[T any] struct {
	Set   bool // key present in the YAML
	Off   bool
	Block *T
}

func (s *switchable[T]) UnmarshalYAML(n *yaml.Node) error {
	s.Set = true
	if n.Kind == yaml.ScalarNode {
		if n.Value != Off {
			return fmt.Errorf("line %d: must be a block or %q, got %q", n.Line, Off, n.Value)
		}
		s.Off = true
		return nil
	}
	s.Block = new(T)
	return n.Decode(s.Block)
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
	OnRecurrenceAfterClose string   `yaml:"on_recurrence_after_close"`

	// Deprecated: HelixObs never closes issues -- closing is left to the
	// people triaging them. Still parsed so existing configs keep loading;
	// a non-zero value only logs a warning.
	AutoCloseAfterDays int `yaml:"auto_close_after_days"`
}

// ── Loader ────────────────────────────────────────────────────────────────────

// Loader reads instrument YAMLs from a directory and exposes a hot-reloaded Registry.
type Loader struct {
	dir             string
	current         atomic.Value // holds Registry
	minSlackWindow  time.Duration
	minGithubWindow time.Duration

	// Config warnings are logged once, not on every reload: warned holds the
	// warnings emitted by the previous reload, pending those seen by the one
	// in progress. Only touched from reload(), which never runs concurrently.
	warned  map[string]bool
	pending map[string]bool
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

// warn logs a config warning unless the previous reload already logged the
// identical one, so an unchanged config doesn't repeat it every interval.
// A warning that clears and later reappears is logged again.
func (l *Loader) warn(msg string, args ...any) {
	key := fmt.Sprint(append([]any{msg}, args...)...)
	if l.pending != nil {
		l.pending[key] = true
	}
	if l.warned[key] {
		return
	}
	slog.Warn(msg, args...)
}

func (l *Loader) reload() {
	l.pending = map[string]bool{}
	defer func() { l.warned, l.pending = l.pending, nil }()

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
			l.warn("notifier: skipping instrument config",
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

	defaultPlaybook := l.resolvePlaybook(raw.InstrumentID, "notifications.playbook", raw.Notifications.Playbook)
	playbookBase := strings.TrimRight(
		l.resolvePlaybook(raw.InstrumentID, "notifications.playbook_base", raw.Notifications.PlaybookBase), "/")

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

		ecfg := EventConfig{
			MessageTemplate: ev.MessageTemplate,
			Playbook:        defaultPlaybook,
			PlaybookBase:    playbookBase,
		}
		if p := l.resolvePlaybook(raw.InstrumentID, fmt.Sprintf("events[%q].playbook", eventType), ev.Playbook); p != "" {
			ecfg.Playbook = p
		}

		if ev.Slack != nil {
			mc, evErrs := l.resolveSlack(eventType, ev.Slack, raw.InstrumentID, defaultSlackURL)
			errs = append(errs, evErrs...)
			if mc != nil {
				mc.MessageTemplate = ev.MessageTemplate
				ecfg.Messaging = append(ecfg.Messaging, *mc)
			}
		}

		if ev.Github != nil {
			sc, evErrs := l.resolveGithub(eventType, ev.Github, defaultGithubToken)
			errs = append(errs, evErrs...)
			if sc != nil {
				ecfg.SCM = append(ecfg.SCM, *sc)
			}
		}

		if !ev.Routes.IsZero() {
			l.warn("notifier: routes is no longer supported and is ignored; use overrides (match on operation)",
				"instrument", raw.InstrumentID, "event", eventType)
		}

		for i, o := range ev.Overrides {
			label := fmt.Sprintf("%s.overrides[%d]", eventType, i)
			if o.Match.Operation == "" {
				errs = append(errs, fmt.Errorf("notifications.events[%q].match.operation: required", label))
				continue
			}
			if !o.Slack.Set && !o.Github.Set && o.Playbook == "" {
				errs = append(errs, fmt.Errorf("notifications.events[%q]: sets none of slack, github or playbook", label))
				continue
			}
			ov := Override{
				Operation: o.Match.Operation,
				Stage:     o.Match.Stage,
				Messaging: ecfg.Messaging,
				SCM:       ecfg.SCM,
			}
			switch {
			case o.Slack.Off:
				ov.Messaging = nil
			case o.Slack.Block != nil:
				ov.Messaging = nil
				mc, evErrs := l.resolveSlack(label, o.Slack.Block, raw.InstrumentID, defaultSlackURL)
				errs = append(errs, evErrs...)
				if mc != nil {
					mc.MessageTemplate = ev.MessageTemplate
					ov.Messaging = []MessagingCall{*mc}
				}
			}
			switch {
			case o.Github.Off:
				ov.SCM = nil
			case o.Github.Block != nil:
				ov.SCM = nil
				sc, evErrs := l.resolveGithub(label, o.Github.Block, defaultGithubToken)
				errs = append(errs, evErrs...)
				if sc != nil {
					ov.SCM = []SCMCall{*sc}
				}
			}
			ov.Playbook = l.resolvePlaybook(raw.InstrumentID, label+".playbook", o.Playbook)
			ecfg.Overrides = append(ecfg.Overrides, ov)
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

// resolvePlaybook returns raw if it is an absolute http(s) URL. Anything else
// is dropped with a warning rather than rejecting the config: a typo in a
// documentation link must never stop alerts.
func (l *Loader) resolvePlaybook(instrumentID, field, raw string) string {
	if raw == "" {
		return ""
	}
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		l.warn("notifier: ignoring playbook, not an absolute http(s) URL",
			"instrument", instrumentID, "field", field, "value", raw)
		return ""
	}
	return raw
}

func (l *Loader) resolveSlack(eventType string, s *slackYAML, instrumentID, defaultURL string) (*MessagingCall, []error) {
	var errs []error

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
		l.warn("notifier: clamping sample_window_seconds to minimum",
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

func (l *Loader) resolveGithub(eventType string, g *githubYAML, defaultToken string) (*SCMCall, []error) {
	var errs []error

	if g.Repo == "" || !strings.Contains(g.Repo, "/") {
		errs = append(errs, fmt.Errorf("notifications.events[%q].github.repo: must be in owner/repo format", eventType))
	}
	if g.AutoCloseAfterDays != 0 {
		l.warn("notifier: github.auto_close_after_days is deprecated and ignored; HelixObs does not close issues",
			"event", eventType, "repo", g.Repo)
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
		OnRecurrenceAfterClose: recurrence,
	}, nil
}
