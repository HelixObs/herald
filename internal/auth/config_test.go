package auth_test

import (
	"os"
	"testing"

	"github.com/HelixObs/herald/internal/auth"
)

func TestConfigStore_LoadsSecretBackend(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(dir+"/chime.yml", []byte(`
instrument_id: CHIMEFRB
auth:
  type: secret
  api_key_hash: sha256:aabbccdd00112233aabbccdd00112233aabbccdd00112233aabbccdd00112233
`), 0o600)

	cs := auth.NewConfigStore(dir)
	cfg, ok := cs.Get("CHIMEFRB")
	if !ok {
		t.Fatal("expected config to be found for CHIMEFRB")
	}
	if cfg.Type != "secret" {
		t.Errorf("expected type=secret, got %q", cfg.Type)
	}
	if cfg.APIKeyHash == "" {
		t.Error("expected non-empty api_key_hash")
	}
}

func TestConfigStore_LoadsTokenIntrospectionBackend(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(dir+"/inst.yml", []byte(`
instrument_id: INST
auth:
  type: token_introspection
  verify_url: https://example.com/verify
`), 0o600)

	cs := auth.NewConfigStore(dir)
	cfg, ok := cs.Get("INST")
	if !ok {
		t.Fatal("expected config for INST")
	}
	if cfg.Type != "token_introspection" {
		t.Errorf("expected type=token_introspection, got %q", cfg.Type)
	}
	if cfg.VerifyURL != "https://example.com/verify" {
		t.Errorf("expected verify_url to be set, got %q", cfg.VerifyURL)
	}
}

func TestConfigStore_MissingInstrumentReturnsNotFound(t *testing.T) {
	cs := auth.NewConfigStore(t.TempDir())
	_, ok := cs.Get("NOBODY")
	if ok {
		t.Error("expected Get() to return false for unknown instrument")
	}
}

func TestConfigStore_IgnoresNonYAMLFiles(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(dir+"/README.md", []byte("not yaml"), 0o600)
	os.WriteFile(dir+"/notes.txt", []byte("notes"), 0o600)

	cs := auth.NewConfigStore(dir)
	// No instrument should be loaded from non-YAML files.
	if _, ok := cs.Get("README"); ok {
		t.Error("expected non-YAML files to be ignored")
	}
}

func TestConfigStore_IgnoresInstrumentWithNoAuthBlock(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(dir+"/inst.yml", []byte(`
instrument_id: NOAUTH
notifications:
  slack_webhook_env: SLACK_WEBHOOK
`), 0o600)

	cs := auth.NewConfigStore(dir)
	_, ok := cs.Get("NOAUTH")
	if ok {
		t.Error("expected instrument with no auth block to not appear in config store")
	}
}

func TestConfigStore_EmptyDirReturnsNoConfigs(t *testing.T) {
	cs := auth.NewConfigStore(t.TempDir())
	if _, ok := cs.Get("ANY"); ok {
		t.Error("expected no configs in empty dir")
	}
}
