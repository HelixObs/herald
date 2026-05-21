package auth_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/HelixObs/herald/internal/auth"
)

// ── Issuer ────────────────────────────────────────────────────────────────────

func TestIssuer_EnabledWhenKeysSet(t *testing.T) {
	is := auth.NewIssuer([]string{"secret"})
	if !is.Enabled() {
		t.Error("expected Enabled() = true when keys are set")
	}
}

func TestIssuer_DisabledWhenNoKeys(t *testing.T) {
	is := auth.NewIssuer([]string{})
	if is.Enabled() {
		t.Error("expected Enabled() = false when no keys")
	}
}

func TestIssuer_DisabledWhenOnlyEmptyString(t *testing.T) {
	is := auth.NewIssuer([]string{""})
	if is.Enabled() {
		t.Error("expected Enabled() = false for empty-string key")
	}
}

func TestIssuer_IssueAndValidate(t *testing.T) {
	is := auth.NewIssuer([]string{"signing-key"})
	tok, err := is.Issue("CHIMEFRB")
	if err != nil {
		t.Fatalf("Issue() error: %v", err)
	}
	subject, err := is.Validate(tok)
	if err != nil {
		t.Fatalf("Validate() error: %v", err)
	}
	if subject != "CHIMEFRB" {
		t.Errorf("expected subject=CHIMEFRB, got %q", subject)
	}
}

func TestIssuer_ValidateMultiKey(t *testing.T) {
	// Old tokens signed with "old-key" remain valid when "new-key" is added.
	old := auth.NewIssuer([]string{"old-key"})
	tok, err := old.Issue("INST")
	if err != nil {
		t.Fatalf("Issue() error: %v", err)
	}

	rotated := auth.NewIssuer([]string{"new-key", "old-key"})
	subject, err := rotated.Validate(tok)
	if err != nil {
		t.Fatalf("rotated Validate() rejected old token: %v", err)
	}
	if subject != "INST" {
		t.Errorf("expected subject=INST, got %q", subject)
	}
}

func TestIssuer_ValidateRejectsWrongKey(t *testing.T) {
	is := auth.NewIssuer([]string{"key-a"})
	tok, err := is.Issue("INST")
	if err != nil {
		t.Fatalf("Issue() error: %v", err)
	}
	other := auth.NewIssuer([]string{"key-b"})
	if _, err := other.Validate(tok); err == nil {
		t.Error("expected Validate() to reject token signed with a different key")
	}
}

func TestIssuer_ValidateRejectsGarbage(t *testing.T) {
	is := auth.NewIssuer([]string{"key"})
	if _, err := is.Validate("not.a.jwt"); err == nil {
		t.Error("expected Validate() to reject malformed token")
	}
}

// ── SecretBackend ─────────────────────────────────────────────────────────────

func hashOf(s string) string {
	h := sha256.Sum256([]byte(s))
	return "sha256:" + hex.EncodeToString(h[:])
}

func TestSecretBackend_ValidCredential(t *testing.T) {
	sb, err := auth.NewSecretBackend(hashOf("super-secret"))
	if err != nil {
		t.Fatalf("NewSecretBackend() error: %v", err)
	}
	if err := sb.Authenticate(context.Background(), "super-secret"); err != nil {
		t.Errorf("expected valid credential to pass: %v", err)
	}
}

func TestSecretBackend_InvalidCredential(t *testing.T) {
	sb, err := auth.NewSecretBackend(hashOf("super-secret"))
	if err != nil {
		t.Fatalf("NewSecretBackend() error: %v", err)
	}
	if err := sb.Authenticate(context.Background(), "wrong-secret"); err == nil {
		t.Error("expected invalid credential to be rejected")
	}
}

func TestSecretBackend_MissingPrefix(t *testing.T) {
	_, err := auth.NewSecretBackend("deadbeef1234")
	if err == nil {
		t.Error("expected error when hash is missing 'sha256:' prefix")
	}
}

func TestSecretBackend_InvalidHex(t *testing.T) {
	_, err := auth.NewSecretBackend("sha256:notvalidhex")
	if err == nil {
		t.Error("expected error for invalid hex in hash")
	}
}

func TestSecretBackend_WrongLength(t *testing.T) {
	_, err := auth.NewSecretBackend("sha256:deadbeef")
	if err == nil {
		t.Error("expected error when hash is not 64 hex chars (32 bytes)")
	}
}

// ── TokenIntrospectionBackend ─────────────────────────────────────────────────

func TestTokenIntrospectionBackend_ValidToken(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer my-token" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	tb := auth.NewTokenIntrospectionBackend(srv.URL)
	if err := tb.Authenticate(context.Background(), "my-token"); err != nil {
		t.Errorf("expected valid token to pass: %v", err)
	}
}

func TestTokenIntrospectionBackend_InvalidToken(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
	}))
	defer srv.Close()

	tb := auth.NewTokenIntrospectionBackend(srv.URL)
	if err := tb.Authenticate(context.Background(), "bad-token"); err == nil {
		t.Error("expected invalid token to be rejected")
	}
}

func TestTokenIntrospectionBackend_NetworkError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	srv.Close() // close immediately so the connection is refused

	tb := auth.NewTokenIntrospectionBackend(srv.URL)
	if err := tb.Authenticate(context.Background(), "token"); err == nil {
		t.Error("expected network error to be returned")
	}
}

// ── BackendFor ────────────────────────────────────────────────────────────────

func TestBackendFor_SecretType(t *testing.T) {
	cfg := auth.InstrumentAuth{Type: "secret", APIKeyHash: hashOf("key")}
	b, err := auth.BackendFor(cfg)
	if err != nil {
		t.Fatalf("BackendFor(secret) error: %v", err)
	}
	if b == nil {
		t.Error("expected non-nil backend")
	}
}

func TestBackendFor_TokenIntrospectionType(t *testing.T) {
	cfg := auth.InstrumentAuth{Type: "token_introspection", VerifyURL: "https://example.com/verify"}
	b, err := auth.BackendFor(cfg)
	if err != nil {
		t.Fatalf("BackendFor(token_introspection) error: %v", err)
	}
	if b == nil {
		t.Error("expected non-nil backend")
	}
}

func TestBackendFor_TokenIntrospectionMissingURL(t *testing.T) {
	cfg := auth.InstrumentAuth{Type: "token_introspection"}
	if _, err := auth.BackendFor(cfg); err == nil {
		t.Error("expected error when verify_url is missing")
	}
}

func TestBackendFor_UnknownType(t *testing.T) {
	cfg := auth.InstrumentAuth{Type: "oauth2"}
	if _, err := auth.BackendFor(cfg); err == nil {
		t.Error("expected error for unknown auth type")
	}
}
