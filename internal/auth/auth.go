// Package auth implements JWT issuance and pluggable per-instrument auth backends.
package auth

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// Backend authenticates an incoming credential for a specific instrument.
type Backend interface {
	Authenticate(ctx context.Context, credential string) error
}

// Issuer creates and validates HelixObs JWTs signed with HMAC-SHA256.
// Multi-key: the first key is used for signing; all keys are tried for verification
// (enables zero-downtime rotation). Empty keys slice = auth disabled (dev mode).
type Issuer struct {
	keys [][]byte
}

// NewIssuer creates an Issuer from a list of raw secret strings.
func NewIssuer(secrets []string) *Issuer {
	keys := make([][]byte, 0, len(secrets))
	for _, s := range secrets {
		if s != "" {
			keys = append(keys, []byte(s))
		}
	}
	return &Issuer{keys: keys}
}

// Enabled reports whether auth enforcement is active (i.e. at least one key is set).
func (is *Issuer) Enabled() bool {
	return len(is.keys) > 0
}

// Issue returns a signed 24h JWT with the instrument ID as the subject.
func (is *Issuer) Issue(instrumentID string) (string, error) {
	claims := jwt.RegisteredClaims{
		Issuer:    "helixobs",
		Subject:   instrumentID,
		IssuedAt:  jwt.NewNumericDate(time.Now()),
		ExpiresAt: jwt.NewNumericDate(time.Now().Add(24 * time.Hour)),
	}
	return jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(is.keys[0])
}

// Validate verifies a HelixObs JWT and returns the instrument ID (subject claim).
// All configured keys are tried so tokens issued before a rotation remain valid.
func (is *Issuer) Validate(tokenStr string) (string, error) {
	var lastErr error
	for _, key := range is.keys {
		k := key
		token, err := jwt.ParseWithClaims(tokenStr, &jwt.RegisteredClaims{},
			func(t *jwt.Token) (interface{}, error) {
				if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
					return nil, fmt.Errorf("unexpected signing method: %v", t.Header["alg"])
				}
				return k, nil
			},
			jwt.WithIssuer("helixobs"),
			jwt.WithExpirationRequired(),
		)
		if err != nil {
			lastErr = err
			continue
		}
		if claims, ok := token.Claims.(*jwt.RegisteredClaims); ok && token.Valid {
			return claims.Subject, nil
		}
	}
	return "", lastErr
}

// SecretBackend validates a registration secret via constant-time SHA-256 comparison.
// The expected hash is stored as "sha256:<hex>" in the instrument YAML so the plaintext
// secret never needs to be committed.
type SecretBackend struct {
	expectedHash [32]byte
}

// NewSecretBackend parses "sha256:<64-hex-chars>" and returns a SecretBackend.
func NewSecretBackend(apiKeyHash string) (*SecretBackend, error) {
	after, ok := strings.CutPrefix(apiKeyHash, "sha256:")
	if !ok {
		return nil, fmt.Errorf("api_key_hash must start with 'sha256:'")
	}
	b, err := hex.DecodeString(after)
	if err != nil || len(b) != 32 {
		return nil, fmt.Errorf("api_key_hash: must be a 64-char hex string after 'sha256:'")
	}
	sb := &SecretBackend{}
	copy(sb.expectedHash[:], b)
	return sb, nil
}

func (sb *SecretBackend) Authenticate(_ context.Context, credential string) error {
	got := sha256.Sum256([]byte(credential))
	if !hmac.Equal(got[:], sb.expectedHash[:]) {
		return fmt.Errorf("invalid credential")
	}
	return nil
}

// TokenIntrospectionBackend delegates validation to a remote /verify endpoint.
// Used for instruments that already have their own JWT infrastructure (e.g. CHIME
// via frb-master). The credential is sent as a Bearer token to the verify URL;
// HTTP 200 = valid.
type TokenIntrospectionBackend struct {
	verifyURL string
	client    *http.Client
}

func NewTokenIntrospectionBackend(verifyURL string) *TokenIntrospectionBackend {
	return &TokenIntrospectionBackend{
		verifyURL: verifyURL,
		client:    &http.Client{Timeout: 5 * time.Second},
	}
}

func (tb *TokenIntrospectionBackend) Authenticate(ctx context.Context, credential string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, tb.verifyURL, nil)
	if err != nil {
		return fmt.Errorf("build verify request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+credential)
	resp, err := tb.client.Do(req)
	if err != nil {
		return fmt.Errorf("verify request: %w", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("verify returned %d", resp.StatusCode)
	}
	return nil
}

// BackendFor constructs the Backend described by an InstrumentAuth config.
func BackendFor(a InstrumentAuth) (Backend, error) {
	switch a.Type {
	case "secret":
		return NewSecretBackend(a.APIKeyHash)
	case "token_introspection":
		if a.VerifyURL == "" {
			return nil, fmt.Errorf("token_introspection requires verify_url")
		}
		return NewTokenIntrospectionBackend(a.VerifyURL), nil
	default:
		return nil, fmt.Errorf("unknown auth type: %q", a.Type)
	}
}
