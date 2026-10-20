// Package notifier dispatches messaging and SCM notifications for helix.* entity events.
//
// Backends are registered at startup via RegisterMessaging / RegisterSCM.
// The interceptor sends events to the Notifier's channel after writing them to DB.
// For each event: SCM backends (GitHub, etc.) run first with a WaitGroup so issue URLs
// are collected, then messaging backends (Slack, etc.) fire with those URLs included.
// All dispatch happens in a goroutine so the event loop never blocks on slow API calls.
package notifier

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"sort"
	"strings"
	"sync"
	"text/template"
	"time"

	"github.com/HelixObs/herald/internal/metrics"
	"github.com/HelixObs/herald/internal/notifier/config"
	"github.com/HelixObs/herald/internal/notifier/fingerprint"
	"github.com/HelixObs/herald/internal/notifier/silence"
)

// maxConcurrentDispatches caps concurrent outbound goroutines per backend type.
const maxConcurrentDispatches = 20

// staleOverrideAfter is how long an override's operation can go without an
// event before the herald warns that the override may be stale -- typically
// because the operation was renamed in pipeline code.
const staleOverrideAfter = 7 * 24 * time.Hour

// Event is sent by the interceptor for every helix.* span event.
type Event struct {
	InstrumentID string
	EntityID     string
	EventName    string
	Stage        string
	Operation    string // name of the span that emitted this event — the operation that failed
	Message      string
	TimestampNs  int64
	TraceID      string // hex-encoded trace ID of the span that emitted this event
	Metadata     map[string]string
}

// Notifier wires config, silence, and registered backends into a dispatch loop.
type Notifier struct {
	ch             chan Event
	cfg            *config.Loader
	silence        *silence.Store
	messaging      map[string]MessagingBackend
	scm            map[string]SCMBackend
	metrics        *metrics.Metrics
	uiBase         string
	grafana        string
	sem            map[string]chan struct{} // per "type" and "type_scm"
	digestInterval time.Duration
	fpLocks        sync.Map // fingerprint → *sync.Mutex; serialises concurrent createIssue for same fp

	startedAt   time.Time
	opSeen      sync.Map        // instrument|operation → time.Time of the last event
	staleWarned map[string]bool // instrument|operation already warned; only touched by Start's goroutine
}

// New creates a Notifier. Backends must be registered via RegisterMessaging / RegisterSCM
// before Start is called.
func New(
	cfg *config.Loader,
	sl *silence.Store,
	m *metrics.Metrics,
	bufSize int,
	uiBaseURL, grafanaURL string,
) *Notifier {
	return &Notifier{
		ch:             make(chan Event, bufSize),
		cfg:            cfg,
		silence:        sl,
		messaging:      make(map[string]MessagingBackend),
		scm:            make(map[string]SCMBackend),
		metrics:        m,
		uiBase:         strings.TrimRight(uiBaseURL, "/"),
		grafana:        strings.TrimRight(grafanaURL, "/"),
		sem:            make(map[string]chan struct{}),
		digestInterval: 30 * time.Second,
		startedAt:      time.Now(),
		staleWarned:    make(map[string]bool),
	}
}

// SetDigestInterval overrides the default 30-second flush ticker. Intended for tests.
func (n *Notifier) SetDigestInterval(d time.Duration) { n.digestInterval = d }

// RegisterMessaging registers a messaging backend (e.g. "slack", "discord").
// Call before Start.
func (n *Notifier) RegisterMessaging(name string, b MessagingBackend) {
	n.messaging[name] = b
	n.sem[name] = make(chan struct{}, maxConcurrentDispatches)
}

// RegisterSCM registers an SCM backend (e.g. "github", "gitlab").
// Call before Start.
func (n *Notifier) RegisterSCM(name string, b SCMBackend) {
	n.scm[name] = b
	n.sem[name+"_scm"] = make(chan struct{}, maxConcurrentDispatches)
}

// Send queues an event for dispatch. Non-blocking — drops if channel is full.
func (n *Notifier) Send(e Event) {
	select {
	case n.ch <- e:
	default:
		n.metrics.NotificationChannelDropsTotal.Inc()
		slog.Warn("notifier: channel full, dropping event",
			"instrument", e.InstrumentID, "event", e.EventName)
	}
}

// Start runs the dispatch loop until ctx is cancelled.
func (n *Notifier) Start(ctx context.Context) {
	digestTicker := time.NewTicker(n.digestInterval)
	defer digestTicker.Stop()
	staleTicker := time.NewTicker(time.Hour)
	defer staleTicker.Stop()

	for {
		select {
		case e := <-n.ch:
			go n.dispatch(ctx, e) // goroutine so the loop never blocks on slow API calls
		case <-digestTicker.C:
			n.flushDigests(ctx)
		case now := <-staleTicker.C:
			n.warnStaleOverrides(now)
		case <-ctx.Done():
			return
		}
	}
}

func (n *Notifier) dispatch(ctx context.Context, e Event) {
	reg := n.cfg.Get()
	instCfg, ok := reg[e.InstrumentID]
	if !ok {
		return
	}

	evCfg, ok := instCfg.Events[e.EventName]
	if !ok {
		return
	}

	n.seenOperation(e.InstrumentID, e.Operation)

	// The first override matching the operation (and stage, if it sets one)
	// supplies messaging, SCM and playbook; anything it left unset was
	// already inherited from the event at config load.
	messaging, scm, playbook := evCfg.Resolve(e.Operation, e.Stage)

	fp := fingerprint.Compute(e.InstrumentID, e.EventName, e.Message, e.Operation, e.Stage)

	if n.silence.IsSilenced(ctx, e.InstrumentID, e.EventName, fp) {
		n.metrics.NotificationsSuppressedTotal.WithLabelValues(e.InstrumentID, "silence", e.EventName).Inc()
		return
	}

	inspectorURL := fmt.Sprintf("%s/entity/%s", n.uiBase, e.EntityID)
	// Deep-links straight into the failing trace (not just the entity's own creation
	// trace, which entity-inspector defaults to and which can be a different trace
	// entirely for errors raised during a later operation).
	grafanaInspectorURL := fmt.Sprintf(
		"%s/d/helix-entity-inspector/entity-inspector?orgId=1&from=now-1h&to=now&timezone=browser&var-entity_id=%s&var-active_trace_id=%s",
		n.grafana, url.QueryEscape(e.EntityID), url.QueryEscape(e.TraceID),
	)
	errEntitiesURL := fmt.Sprintf("%s/d/helix-error-entities", n.grafana)
	notificationsURL := fmt.Sprintf("%s/notifications?instrument_id=%s&fingerprint=%s",
		n.uiBase, url.QueryEscape(e.InstrumentID), fp)

	// SCM backends (GitHub, etc.) run first. We wait for all of them so that
	// messaging backends (Slack, etc.) can include the issue URLs in their messages.
	var (
		urlMu     sync.Mutex
		issueURLs []string
		wg        sync.WaitGroup
	)

	for _, sc := range scm {
		sc := sc
		backend, ok := n.scm[sc.Type]
		if !ok {
			slog.Warn("notifier: no SCM backend registered", "type", sc.Type)
			continue
		}
		sem := n.sem[sc.Type+"_scm"]
		select {
		case sem <- struct{}{}:
			wg.Add(1)
			go func() {
				defer func() { <-sem }()
				defer wg.Done()
				if u := n.doSCM(ctx, e, sc, backend, fp, inspectorURL, notificationsURL, playbook); u != "" {
					urlMu.Lock()
					issueURLs = append(issueURLs, u)
					urlMu.Unlock()
				}
			}()
		default:
			n.metrics.NotificationChannelDropsTotal.Inc()
			slog.Warn("notifier: SCM semaphore full, dropping",
				"type", sc.Type, "instrument", e.InstrumentID, "event", e.EventName)
		}
	}

	wg.Wait() // block until all SCM goroutines have finished and URLs are collected

	for _, mc := range messaging {
		mc := mc
		backend, ok := n.messaging[mc.Type]
		if !ok {
			slog.Warn("notifier: no messaging backend registered", "type", mc.Type)
			continue
		}
		sem := n.sem[mc.Type]
		select {
		case sem <- struct{}{}:
			go func() {
				defer func() { <-sem }()
				n.doMessaging(ctx, e, mc, backend, fp, grafanaInspectorURL, errEntitiesURL, notificationsURL, playbook, issueURLs)
			}()
		default:
			n.metrics.NotificationChannelDropsTotal.Inc()
			slog.Warn("notifier: messaging semaphore full, dropping",
				"type", mc.Type, "instrument", e.InstrumentID, "event", e.EventName)
		}
	}
}

func (n *Notifier) doMessaging(ctx context.Context, e Event, mc config.MessagingCall,
	backend MessagingBackend, fp, inspectorURL, errEntitiesURL, notificationsURL string, playbook config.Playbook, issueURLs []string) {

	msg := n.buildMsg(e, mc.MessageTemplate, inspectorURL, errEntitiesURL, notificationsURL, issueURLs)
	msg.PlaybookURL, msg.PlaybookFromBase = playbook.URL, playbook.Generated

	start := time.Now()
	sent, err := backend.Send(ctx, mc.Destination, fp, msg, mc.SampleWindowSeconds, mc.MaxPerWindow)
	dur := time.Since(start)
	if err != nil {
		n.metrics.NotificationErrorsTotal.WithLabelValues(e.InstrumentID, mc.Type).Inc()
		slog.Warn("notifier: messaging send failed",
			"type", mc.Type, "instrument", e.InstrumentID, "event", e.EventName, "error", err)
		return
	}
	if sent {
		n.metrics.NotificationsSentTotal.WithLabelValues(e.InstrumentID, mc.Type, e.EventName).Inc()
		n.metrics.NotificationSendDuration.WithLabelValues(mc.Type).Observe(dur.Seconds())
	} else {
		n.metrics.NotificationsSuppressedTotal.WithLabelValues(e.InstrumentID, mc.Type, e.EventName).Inc()
	}
}

func (n *Notifier) seenOperation(instrumentID, operation string) {
	if operation != "" {
		n.opSeen.Store(instrumentID+"|"+operation, time.Now())
	}
}

// warnStaleOverrides logs a warning, once, for each configured override whose
// operation hasn't produced an event in staleOverrideAfter (counting from
// herald start). It warns again only if the operation recovers and then goes
// quiet again.
func (n *Notifier) warnStaleOverrides(now time.Time) {
	for instrumentID, instCfg := range n.cfg.Get() {
		for eventName, evCfg := range instCfg.Events {
			for _, o := range evCfg.Overrides {
				key := instrumentID + "|" + o.Operation
				last := n.startedAt
				if v, ok := n.opSeen.Load(key); ok {
					last = v.(time.Time)
				}
				if now.Sub(last) < staleOverrideAfter {
					delete(n.staleWarned, key)
					continue
				}
				if n.staleWarned[key] {
					continue
				}
				n.staleWarned[key] = true
				slog.Warn("notifier: override matches an operation with no events in 7 days; was it renamed?",
					"instrument", instrumentID, "event", eventName, "operation", o.Operation,
					"last_seen", last.UTC().Format(time.RFC3339))
			}
		}
	}
}

func (n *Notifier) fpLock(fp string) *sync.Mutex {
	v, _ := n.fpLocks.LoadOrStore(fp, &sync.Mutex{})
	return v.(*sync.Mutex)
}

// doSCM dispatches to one SCM backend and returns the resulting issue URL (empty on error).
func (n *Notifier) doSCM(ctx context.Context, e Event, sc config.SCMCall,
	backend SCMBackend, fp, inspectorURL, notificationsURL string, playbook config.Playbook) string {

	// Serialise concurrent dispatches for the same fingerprint. Without this, a burst
	// of N events with identical messages (e.g. queued spans flushed after reconnect)
	// each see nil from FindNotificationIssue and all call createIssue, producing N
	// duplicate GitHub issues.
	mu := n.fpLock(fp)
	mu.Lock()
	defer mu.Unlock()

	title := fmt.Sprintf("[%s] %s", e.InstrumentID, truncate(e.Message, 80))
	if e.Operation != "" {
		title = fmt.Sprintf("[%s] %s (%s)", e.InstrumentID, truncate(e.Message, 60), e.Operation)
	}

	start := time.Now()
	issueURL, err := backend.Dispatch(ctx, SCMParams{
		Token:            sc.Token,
		Repo:             sc.Repo,
		Labels:           sc.Labels,
		Title:            title,
		EntityID:         e.EntityID,
		InspectorURL:     inspectorURL,
		InspectorBase:    fmt.Sprintf("%s/entity", n.uiBase),
		EventName:        e.EventName,
		Message:          e.Message,
		Operation:        e.Operation,
		Stage:            e.Stage,
		PlaybookURL:      playbook.URL,
		PlaybookFromBase: playbook.Generated,
		Timestamp:        time.Unix(0, e.TimestampNs),
		Fingerprint:      fp,
		InstrumentID:     e.InstrumentID,
		OnRecurrence:     sc.OnRecurrenceAfterClose,
		NotificationsURL: notificationsURL,
	})
	dur := time.Since(start)
	if err != nil {
		n.metrics.NotificationErrorsTotal.WithLabelValues(e.InstrumentID, sc.Type).Inc()
		slog.Warn("notifier: SCM dispatch failed",
			"type", sc.Type, "instrument", e.InstrumentID, "event", e.EventName, "error", err)
		return ""
	}
	n.metrics.NotificationsSentTotal.WithLabelValues(e.InstrumentID, sc.Type, e.EventName).Inc()
	n.metrics.NotificationSendDuration.WithLabelValues(sc.Type).Observe(dur.Seconds())
	return issueURL
}

func (n *Notifier) flushDigests(ctx context.Context) {
	reg := n.cfg.Get()
	for _, instCfg := range reg {
		for _, evCfg := range instCfg.Events {
			n.flushMessaging(ctx, evCfg.Messaging)
			for _, o := range evCfg.Overrides {
				n.flushMessaging(ctx, o.Messaging)
			}
		}
	}
}

func (n *Notifier) flushMessaging(ctx context.Context, calls []config.MessagingCall) {
	for _, mc := range calls {
		backend, ok := n.messaging[mc.Type]
		if !ok {
			continue
		}
		backend.FlushDigests(ctx, mc.Destination, mc.SampleWindowSeconds)
	}
}

// ── Message builder ───────────────────────────────────────────────────────────

// buildMsg constructs a Message for messaging backends.
// issueURLs contains GitHub/GitLab issue URLs already created for this event.
func (n *Notifier) buildMsg(e Event, messageTemplate, inspectorURL, errEntitiesURL, notificationsURL string, issueURLs []string) Message {
	text := n.buildText(e, messageTemplate, inspectorURL, errEntitiesURL)
	return Message{
		Text:             text,
		InstrumentID:     e.InstrumentID,
		EventName:        e.EventName,
		EntityID:         e.EntityID,
		Body:             e.Message,
		Stage:            e.Stage,
		Operation:        e.Operation,
		InspectorURL:     inspectorURL,
		ErrDashURL:       errEntitiesURL,
		NotificationsURL: notificationsURL,
		IssueURLs:        issueURLs,
		Metadata:         e.Metadata,
	}
}

// buildText renders a plain-text representation of the event (used as digest fallback).
func (n *Notifier) buildText(e Event, messageTemplate, inspectorURL, errEntitiesURL string) string {
	if messageTemplate != "" {
		t, err := template.New("").Parse(messageTemplate)
		if err == nil {
			var buf bytes.Buffer
			if err := t.Execute(&buf, e.Metadata); err == nil {
				return fmt.Sprintf("[%s] %s\nEntity: %s\n🔍 Inspect: %s",
					e.InstrumentID, buf.String(), e.EntityID, inspectorURL)
			}
		}
		slog.Warn("notifier: message_template render failed, using default",
			"instrument", e.InstrumentID, "event", e.EventName)
	}

	var sb strings.Builder
	fmt.Fprintf(&sb, "[%s] %s", e.InstrumentID, e.EventName)
	if e.Message != "" {
		fmt.Fprintf(&sb, "\n%s", e.Message)
	}
	if e.Stage != "" {
		fmt.Fprintf(&sb, "  (stage: %s)", e.Stage)
	}
	// Append extra metadata fields (sorted for deterministic output).
	if len(e.Metadata) > 0 {
		keys := make([]string, 0, len(e.Metadata))
		for k := range e.Metadata {
			// skip fields already promoted to dedicated Event fields
			if k == "message" || k == "stage" || k == "exception.message" {
				continue
			}
			keys = append(keys, k)
		}
		sort.Strings(keys)
		var pairs []string
		for _, k := range keys {
			pairs = append(pairs, fmt.Sprintf("%s: %s", k, e.Metadata[k]))
		}
		if len(pairs) > 0 {
			fmt.Fprintf(&sb, "\n%s", strings.Join(pairs, " · "))
		}
	}
	if e.Operation != "" {
		fmt.Fprintf(&sb, "\nOperation: %s", e.Operation)
	}
	fmt.Fprintf(&sb, "\nEntity: %s\n🔍 Inspect: %s\n📊 Error Entities: %s",
		e.EntityID, inspectorURL, errEntitiesURL)
	return sb.String()
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n-1] + "…"
}
