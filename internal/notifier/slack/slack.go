// Package slack sends Slack webhook notifications with per-fingerprint rate limiting.
package slack

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/HelixObs/herald/internal/notifier"
)

// Client sends messages to a Slack webhook with exponential-backoff retry.
type Client struct {
	httpClient *http.Client
	mu         sync.Mutex
	windows    map[string]*windowState // fingerprint → state
}

type windowState struct {
	windowStart time.Time
	count       int
	suppressed  int
	lastMsg     notifier.Message // last suppressed message, used for digest block kit
}

// New creates a Slack client.
func New() *Client {
	return &Client{
		httpClient: &http.Client{Timeout: 10 * time.Second},
		windows:    make(map[string]*windowState),
	}
}

// Send sends msg to webhookURL if the rate limit allows.
// Returns true if the message was dispatched, false if suppressed within the window.
// windowSecs is the sample window; maxPerWindow is the max sends before suppression.
func (c *Client) Send(ctx context.Context, webhookURL, fingerprint string, msg notifier.Message, windowSecs, maxPerWindow int) (bool, error) {
	c.mu.Lock()
	ws, ok := c.windows[fingerprint]
	now := time.Now()

	if !ok || now.Sub(ws.windowStart) >= time.Duration(windowSecs)*time.Second {
		// New window — flush digest for old window if there were suppressions.
		if ok && ws.suppressed > 0 {
			body := buildDigestBlockKit(ws.suppressed, windowSecs, fingerprint, ws.lastMsg)
			c.mu.Unlock()
			if err := c.post(ctx, webhookURL, body); err != nil {
				slog.Warn("slack: digest send failed", "fingerprint", fingerprint, "error", err)
			}
			c.mu.Lock()
		}
		c.windows[fingerprint] = &windowState{windowStart: now, count: 1, lastMsg: msg}
		c.mu.Unlock()
		return true, c.sendBlocks(ctx, webhookURL, fingerprint, msg)
	}

	ws.count++
	if ws.count > maxPerWindow {
		ws.suppressed++
		ws.lastMsg = msg
		c.mu.Unlock()
		return false, nil
	}
	c.mu.Unlock()
	return true, c.sendBlocks(ctx, webhookURL, fingerprint, msg)
}

// FlushDigests sweeps expired windows and sends any pending digest messages.
// Call this from a background ticker.
func (c *Client) FlushDigests(ctx context.Context, webhookURL string, windowSecs int) {
	c.mu.Lock()
	type pending struct {
		fp         string
		suppressed int
		lastMsg    notifier.Message
	}
	var toFlush []pending
	now := time.Now()
	for fp, ws := range c.windows {
		if ws.suppressed > 0 && now.Sub(ws.windowStart) >= time.Duration(windowSecs)*time.Second {
			toFlush = append(toFlush, pending{fp, ws.suppressed, ws.lastMsg})
			delete(c.windows, fp)
		}
	}
	c.mu.Unlock()

	for _, f := range toFlush {
		body := buildDigestBlockKit(f.suppressed, windowSecs, f.fp, f.lastMsg)
		if err := c.post(ctx, webhookURL, body); err != nil {
			slog.Warn("slack: digest flush failed", "fingerprint", f.fp, "error", err)
		}
	}
}

// buildDigestBlockKit renders a Block Kit summary for suppressed occurrences.
func buildDigestBlockKit(suppressed, windowSecs int, fp string, last notifier.Message) []byte {
	header := fmt.Sprintf(":rotating_light:  [%s] %s", last.InstrumentID, last.EventName)
	mainText := fmt.Sprintf(":zzz: *%d more occurrence(s) suppressed* in the last %ds\nLast: %s",
		suppressed, windowSecs, escMrkdwn(last.Body))

	fpShort := fp
	if len(fp) > 8 {
		fpShort = fp[:8]
	}

	elements := []any{
		map[string]any{
			"type":  "button",
			"text":  map[string]any{"type": "plain_text", "text": ":mag: Inspect Entity", "emoji": true},
			"url":   last.InspectorURL,
			"style": "primary",
		},
		map[string]any{
			"type": "button",
			"text": map[string]any{"type": "plain_text", "text": ":bar_chart: Error Dashboard", "emoji": true},
			"url":  last.ErrDashURL,
		},
	}
	if last.NotificationsURL != "" {
		elements = append(elements, map[string]any{
			"type": "button",
			"text": map[string]any{"type": "plain_text", "text": ":no_bell: Manage Silences", "emoji": true},
			"url":  last.NotificationsURL,
		})
	}

	var ctxParts []string
	ctxParts = append(ctxParts, fmt.Sprintf("Fingerprint: `%s`", fpShort))
	ctxParts = append(ctxParts, "HelixObs")

	blocks := []any{
		map[string]any{
			"type": "header",
			"text": map[string]any{"type": "plain_text", "text": header, "emoji": true},
		},
		map[string]any{
			"type": "section",
			"text": map[string]any{"type": "mrkdwn", "text": mainText},
		},
		map[string]any{
			"type": "section",
			"fields": []any{
				map[string]any{"type": "mrkdwn", "text": fmt.Sprintf(":id:  *Entity*\n`%s`", last.EntityID)},
				map[string]any{"type": "mrkdwn", "text": fmt.Sprintf(":telescope:  *Instrument*\n%s", last.InstrumentID)},
			},
		},
		map[string]any{"type": "actions", "elements": elements},
		map[string]any{
			"type": "context",
			"elements": []any{
				map[string]any{"type": "mrkdwn", "text": strings.Join(ctxParts, "  •  ")},
			},
		},
	}

	payload, _ := json.Marshal(map[string]any{"blocks": blocks})
	return payload
}

// sendBlocks posts a Slack Block Kit message built from msg.
func (c *Client) sendBlocks(ctx context.Context, webhookURL, fingerprint string, msg notifier.Message) error {
	body := buildBlockKit(fingerprint, msg)
	return c.post(ctx, webhookURL, body)
}

// buildBlockKit renders Slack Block Kit JSON for msg (variant 2 layout).
//
// Layout:
//   - Header: ":rotating_light: [INST] event.name"
//   - Section: bold error body + stage/metadata fields inline
//   - Fields: Entity ID + Instrument
//   - Actions: Inspect Entity (primary), Error Dashboard
//   - Context: GitHub issue link(s) + fingerprint short + HelixObs tag
func buildBlockKit(fp string, msg notifier.Message) []byte {
	header := fmt.Sprintf(":rotating_light:  [%s] %s", msg.InstrumentID, msg.EventName)

	// Main text: bold body, then stage and extra metadata inline.
	mainText := "*" + escMrkdwn(msg.Body) + "*"
	var extras []string
	if msg.Stage != "" {
		extras = append(extras, fmt.Sprintf(":label: Stage: `%s`", msg.Stage))
	}
	// Append sorted metadata keys (skip "message" — it duplicates Body).
	if len(msg.Metadata) > 0 {
		keys := make([]string, 0, len(msg.Metadata))
		for k := range msg.Metadata {
			// skip fields already promoted to dedicated message fields
			if k == "message" || k == "stage" || k == "exception.message" {
				continue
			}
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			extras = append(extras, fmt.Sprintf(":gear: %s: `%s`", k, msg.Metadata[k]))
		}
	}
	if len(extras) > 0 {
		mainText += "\n" + strings.Join(extras, "   ")
	}

	// Context bar: GitHub issue links + short fingerprint + brand.
	var ctxParts []string
	for _, u := range msg.IssueURLs {
		ctxParts = append(ctxParts, fmt.Sprintf(":link: <%s|GitHub Issue>", u))
	}
	fpShort := fp
	if len(fp) > 8 {
		fpShort = fp[:8]
	}
	ctxParts = append(ctxParts, fmt.Sprintf("Fingerprint: `%s`", fpShort))
	ctxParts = append(ctxParts, "HelixObs")

	actionElements := []any{
		map[string]any{
			"type":  "button",
			"text":  map[string]any{"type": "plain_text", "text": ":mag: Inspect Entity", "emoji": true},
			"url":   msg.InspectorURL,
			"style": "primary",
		},
		map[string]any{
			"type": "button",
			"text": map[string]any{"type": "plain_text", "text": ":bar_chart: Error Dashboard", "emoji": true},
			"url":  msg.ErrDashURL,
		},
	}
	if msg.NotificationsURL != "" {
		actionElements = append(actionElements, map[string]any{
			"type": "button",
			"text": map[string]any{"type": "plain_text", "text": ":no_bell: Manage Silences", "emoji": true},
			"url":  msg.NotificationsURL,
		})
	}

	blocks := []any{
		map[string]any{
			"type": "header",
			"text": map[string]any{"type": "plain_text", "text": header, "emoji": true},
		},
		map[string]any{
			"type": "section",
			"text": map[string]any{"type": "mrkdwn", "text": mainText},
		},
		map[string]any{
			"type": "section",
			"fields": []any{
				map[string]any{"type": "mrkdwn", "text": fmt.Sprintf(":id:  *Entity*\n`%s`", msg.EntityID)},
				map[string]any{"type": "mrkdwn", "text": fmt.Sprintf(":telescope:  *Instrument*\n%s", msg.InstrumentID)},
			},
		},
		map[string]any{
			"type":     "actions",
			"elements": actionElements,
		},
		map[string]any{
			"type": "context",
			"elements": []any{
				map[string]any{"type": "mrkdwn", "text": strings.Join(ctxParts, "  •  ")},
			},
		},
	}

	payload, _ := json.Marshal(map[string]any{"blocks": blocks})
	return payload
}

// escMrkdwn escapes characters that Slack mrkdwn treats as special.
func escMrkdwn(s string) string {
	s = strings.ReplaceAll(s, "&", "&amp;")
	s = strings.ReplaceAll(s, "<", "&lt;")
	s = strings.ReplaceAll(s, ">", "&gt;")
	return s
}

// post sends body to webhookURL with exponential backoff (up to 5 attempts).
func (c *Client) post(ctx context.Context, webhookURL string, body []byte) error {
	var lastErr error
	for attempt := 0; attempt < 5; attempt++ {
		if attempt > 0 {
			wait := time.Duration(1<<attempt) * time.Second // 2s, 4s, 8s, 16s
			select {
			case <-time.After(wait):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, webhookURL, bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := c.httpClient.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			return nil
		}
		lastErr = fmt.Errorf("slack webhook returned %d", resp.StatusCode)
		if resp.StatusCode < 500 {
			break // 4xx — don't retry
		}
	}
	return lastErr
}
