package auth

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"gopkg.in/yaml.v3"
)

// InstrumentAuth is the auth block from an instrument YAML file.
type InstrumentAuth struct {
	Type       string `yaml:"type"`         // "secret" | "token_introspection"
	APIKeyHash string `yaml:"api_key_hash"` // sha256:<hex>  (type=secret)
	VerifyURL  string `yaml:"verify_url"`   // https://...  (type=token_introspection)
}

type instrumentYAML struct {
	InstrumentID string         `yaml:"instrument_id"`
	Auth         InstrumentAuth `yaml:"auth"`
}

// ConfigStore hot-reloads auth configs from instrument YAML files in a directory.
// It reads the same files as the notifier config loader — no duplication on disk.
type ConfigStore struct {
	dir     string
	mu      sync.RWMutex
	configs map[string]InstrumentAuth // instrument_id → auth config
}

func NewConfigStore(dir string) *ConfigStore {
	cs := &ConfigStore{dir: dir, configs: map[string]InstrumentAuth{}}
	cs.load()
	return cs
}

// Start begins periodic hot-reload. Runs until ctx is cancelled.
func (cs *ConfigStore) Start(ctx context.Context, interval time.Duration) {
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				cs.load()
			}
		}
	}()
}

// Get returns the auth config for an instrument, and whether it was found.
func (cs *ConfigStore) Get(instrumentID string) (InstrumentAuth, bool) {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	a, ok := cs.configs[instrumentID]
	return a, ok
}

func (cs *ConfigStore) load() {
	entries, err := os.ReadDir(cs.dir)
	if err != nil {
		return
	}
	m := make(map[string]InstrumentAuth, len(entries))
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".yml") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(cs.dir, e.Name()))
		if err != nil {
			continue
		}
		var y instrumentYAML
		if err := yaml.Unmarshal(data, &y); err != nil || y.InstrumentID == "" {
			continue
		}
		if y.Auth.Type != "" {
			m[y.InstrumentID] = y.Auth
		}
	}
	cs.mu.Lock()
	cs.configs = m
	cs.mu.Unlock()
}
