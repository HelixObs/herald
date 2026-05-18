// Package slack sends Slack webhook notifications with per-fingerprint rate limiting.
package slack

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"
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
	lastMsg     string // last message text for digest
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
func (c *Client) Send(ctx context.Context, webhookURL, fingerprint, msg string, windowSecs, maxPerWindow int) (bool, error) {
	c.mu.Lock()
	ws, ok := c.windows[fingerprint]
	now := time.Now()

	if !ok || now.Sub(ws.windowStart) >= time.Duration(windowSecs)*time.Second {
		// New window — flush digest for old window if there were suppressions.
		if ok && ws.suppressed > 0 {
			digestMsg := fmt.Sprintf("%d more occurrence(s) suppressed in the last %ds. Last message: %s",
				ws.suppressed, windowSecs, ws.lastMsg)
			c.mu.Unlock()
			if err := c.send(ctx, webhookURL, digestMsg); err != nil {
				slog.Warn("slack: digest send failed", "fingerprint", fingerprint, "error", err)
			}
			c.mu.Lock()
		}
		c.windows[fingerprint] = &windowState{windowStart: now, count: 1, lastMsg: msg}
		c.mu.Unlock()
		return true, c.send(ctx, webhookURL, msg)
	}

	ws.count++
	if ws.count > maxPerWindow {
		ws.suppressed++
		ws.lastMsg = msg
		c.mu.Unlock()
		return false, nil
	}
	c.mu.Unlock()
	return true, c.send(ctx, webhookURL, msg)
}

// FlushDigests sweeps expired windows and sends any pending digest messages.
// Call this from a background ticker.
func (c *Client) FlushDigests(ctx context.Context, webhookURL string, windowSecs int) {
	c.mu.Lock()
	var toFlush []struct {
		fingerprint string
		suppressed  int
		lastMsg     string
	}
	now := time.Now()
	for fp, ws := range c.windows {
		if ws.suppressed > 0 && now.Sub(ws.windowStart) >= time.Duration(windowSecs)*time.Second {
			toFlush = append(toFlush, struct {
				fingerprint string
				suppressed  int
				lastMsg     string
			}{fp, ws.suppressed, ws.lastMsg})
			delete(c.windows, fp)
		}
	}
	c.mu.Unlock()

	for _, f := range toFlush {
		msg := fmt.Sprintf("%d more occurrence(s) suppressed in the last %ds. Last: %s",
			f.suppressed, windowSecs, f.lastMsg)
		if err := c.send(ctx, webhookURL, msg); err != nil {
			slog.Warn("slack: digest flush failed", "fingerprint", f.fingerprint, "error", err)
		}
	}
}

// send posts msg to webhookURL with exponential backoff (up to 5 attempts).
func (c *Client) send(ctx context.Context, webhookURL, msg string) error {
	body, _ := json.Marshal(map[string]string{"text": msg})
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
