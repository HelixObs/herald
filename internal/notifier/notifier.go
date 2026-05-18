// Package notifier dispatches messaging and SCM notifications for helix.* entity events.
//
// Backends are registered at startup via RegisterMessaging / RegisterSCM.
// The interceptor sends events to the Notifier's channel after writing them to DB.
// A single goroutine drains the channel, applies silence rules, rate limits, and
// dispatches to each configured backend concurrently per event.
package notifier

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"strings"
	"text/template"
	"time"

	"github.com/HelixObs/gateway/internal/metrics"
	"github.com/HelixObs/gateway/internal/notifier/config"
	"github.com/HelixObs/gateway/internal/notifier/fingerprint"
	"github.com/HelixObs/gateway/internal/notifier/silence"
)

// maxConcurrentDispatches caps concurrent outbound goroutines per backend type.
const maxConcurrentDispatches = 20

// Event is sent by the interceptor for every helix.* span event.
type Event struct {
	InstrumentID string
	EntityID     string
	EventName    string
	Stage        string
	Message      string
	TimestampNs  int64
	Metadata     map[string]string
}

// Notifier wires config, silence, and registered backends into a dispatch loop.
type Notifier struct {
	ch        chan Event
	cfg       *config.Loader
	silence   *silence.Store
	messaging map[string]MessagingBackend
	scm       map[string]SCMBackend
	metrics   *metrics.Metrics
	uiBase    string
	grafana   string
	sem       map[string]chan struct{} // per "type" and "type_scm"
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
		ch:        make(chan Event, bufSize),
		cfg:       cfg,
		silence:   sl,
		messaging: make(map[string]MessagingBackend),
		scm:       make(map[string]SCMBackend),
		metrics:   m,
		uiBase:    strings.TrimRight(uiBaseURL, "/"),
		grafana:   strings.TrimRight(grafanaURL, "/"),
		sem:       make(map[string]chan struct{}),
	}
}

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
	digestTicker := time.NewTicker(30 * time.Second)
	defer digestTicker.Stop()

	for {
		select {
		case e := <-n.ch:
			n.dispatch(ctx, e)
		case <-digestTicker.C:
			n.flushDigests(ctx)
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

	fp := fingerprint.Compute(e.InstrumentID, e.EventName, e.Message, e.Stage)

	if n.silence.IsSilenced(ctx, e.InstrumentID, e.EventName, fp) {
		n.metrics.NotificationsSuppressedTotal.WithLabelValues(e.InstrumentID, "silence", e.EventName).Inc()
		return
	}

	inspectorURL := fmt.Sprintf("%s/entity/%s", n.uiBase, e.EntityID)
	errEntitiesURL := fmt.Sprintf("%s/d/helix-error-entities", n.grafana)

	for _, mc := range evCfg.Messaging {
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
				n.doMessaging(ctx, e, mc, backend, fp, inspectorURL, errEntitiesURL)
			}()
		default:
			n.metrics.NotificationChannelDropsTotal.Inc()
			slog.Warn("notifier: messaging semaphore full, dropping",
				"type", mc.Type, "instrument", e.InstrumentID, "event", e.EventName)
		}
	}

	for _, sc := range evCfg.SCM {
		sc := sc
		backend, ok := n.scm[sc.Type]
		if !ok {
			slog.Warn("notifier: no SCM backend registered", "type", sc.Type)
			continue
		}
		sem := n.sem[sc.Type+"_scm"]
		select {
		case sem <- struct{}{}:
			go func() {
				defer func() { <-sem }()
				n.doSCM(ctx, e, sc, backend, fp, inspectorURL)
			}()
		default:
			n.metrics.NotificationChannelDropsTotal.Inc()
			slog.Warn("notifier: SCM semaphore full, dropping",
				"type", sc.Type, "instrument", e.InstrumentID, "event", e.EventName)
		}
	}
}

func (n *Notifier) doMessaging(ctx context.Context, e Event, mc config.MessagingCall,
	backend MessagingBackend, fp, inspectorURL, errEntitiesURL string) {

	tmpl := mc.MessageTemplate
	if tmpl == "" {
		tmpl = "" // falls through to default builder
	}
	msg := n.buildMessage(e, tmpl, inspectorURL, errEntitiesURL)

	sent, err := backend.Send(ctx, mc.Destination, fp, msg, mc.SampleWindowSeconds, mc.MaxPerWindow)
	if err != nil {
		n.metrics.NotificationErrorsTotal.WithLabelValues(e.InstrumentID, mc.Type).Inc()
		slog.Warn("notifier: messaging send failed",
			"type", mc.Type, "instrument", e.InstrumentID, "event", e.EventName, "error", err)
		return
	}
	if sent {
		n.metrics.NotificationsSentTotal.WithLabelValues(e.InstrumentID, mc.Type, e.EventName).Inc()
	} else {
		n.metrics.NotificationsSuppressedTotal.WithLabelValues(e.InstrumentID, mc.Type, e.EventName).Inc()
	}
}

func (n *Notifier) doSCM(ctx context.Context, e Event, sc config.SCMCall,
	backend SCMBackend, fp, inspectorURL string) {

	title := fmt.Sprintf("[%s] %s", e.InstrumentID, truncate(e.Message, 80))
	if e.Stage != "" {
		title = fmt.Sprintf("[%s] %s (stage: %s)", e.InstrumentID, truncate(e.Message, 60), e.Stage)
	}

	start := time.Now()
	err := backend.Dispatch(ctx, SCMParams{
		Token:         sc.Token,
		Repo:          sc.Repo,
		Labels:        sc.Labels,
		Title:         title,
		EntityID:      e.EntityID,
		InspectorURL:  inspectorURL,
		InspectorBase: fmt.Sprintf("%s/entity", n.uiBase),
		EventName:     e.EventName,
		Message:       e.Message,
		Stage:         e.Stage,
		Timestamp:     time.Unix(0, e.TimestampNs),
		Fingerprint:   fp,
		InstrumentID:  e.InstrumentID,
		OnRecurrence:  sc.OnRecurrenceAfterClose,
	})
	dur := time.Since(start)
	if err != nil {
		n.metrics.NotificationErrorsTotal.WithLabelValues(e.InstrumentID, sc.Type).Inc()
		slog.Warn("notifier: SCM dispatch failed",
			"type", sc.Type, "instrument", e.InstrumentID, "event", e.EventName, "error", err)
		return
	}
	n.metrics.NotificationsSentTotal.WithLabelValues(e.InstrumentID, sc.Type, e.EventName).Inc()
	n.metrics.NotificationSendDuration.WithLabelValues(sc.Type).Observe(dur.Seconds())
}

func (n *Notifier) flushDigests(ctx context.Context) {
	reg := n.cfg.Get()
	for _, instCfg := range reg {
		for _, evCfg := range instCfg.Events {
			for _, mc := range evCfg.Messaging {
				backend, ok := n.messaging[mc.Type]
				if !ok {
					continue
				}
				backend.FlushDigests(ctx, mc.Destination, mc.SampleWindowSeconds)
			}
		}
	}
}

// ── Message builder ───────────────────────────────────────────────────────────

func (n *Notifier) buildMessage(e Event, messageTemplate, inspectorURL, errEntitiesURL string) string {
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
	if len(e.Metadata) > 0 && e.EventName != "helix.error" {
		var pairs []string
		for k, v := range e.Metadata {
			pairs = append(pairs, fmt.Sprintf("%s: %s", k, v))
		}
		fmt.Fprintf(&sb, "\n%s", strings.Join(pairs, " · "))
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
