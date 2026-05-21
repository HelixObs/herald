package auth_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/HelixObs/herald/internal/auth"
	"golang.org/x/net/context"
)

// ── BearerAuth (HTTP middleware) ──────────────────────────────────────────────

func TestBearerAuth_PassthroughWhenDisabled(t *testing.T) {
	issuer := auth.NewIssuer([]string{}) // no keys = disabled
	called := false
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { called = true })

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	auth.BearerAuth(issuer, next).ServeHTTP(rec, req)

	if !called {
		t.Error("expected next handler to be called when auth is disabled")
	}
}

func TestBearerAuth_AllowsValidToken(t *testing.T) {
	issuer := auth.NewIssuer([]string{"key"})
	tok, _ := issuer.Issue("INST")

	called := false
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { called = true })

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	rec := httptest.NewRecorder()
	auth.BearerAuth(issuer, next).ServeHTTP(rec, req)

	if !called {
		t.Error("expected next handler to be called with valid token")
	}
	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
}

func TestBearerAuth_RejectsMissingToken(t *testing.T) {
	issuer := auth.NewIssuer([]string{"key"})
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("next should not be called")
	})

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	auth.BearerAuth(issuer, next).ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d", rec.Code)
	}
}

func TestBearerAuth_RejectsInvalidToken(t *testing.T) {
	issuer := auth.NewIssuer([]string{"key"})
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("next should not be called")
	})

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer not-a-valid-jwt")
	rec := httptest.NewRecorder()
	auth.BearerAuth(issuer, next).ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d", rec.Code)
	}
}

func TestBearerAuth_RejectsBearerWithNoToken(t *testing.T) {
	issuer := auth.NewIssuer([]string{"key"})
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("next should not be called")
	})

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer ") // space but no token
	rec := httptest.NewRecorder()
	auth.BearerAuth(issuer, next).ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d", rec.Code)
	}
}

// ── UnaryInterceptor (gRPC) ───────────────────────────────────────────────────

func callInterceptor(issuer *auth.Issuer, ctx context.Context) error {
	interceptor := auth.UnaryInterceptor(issuer)
	_, err := interceptor(ctx, nil, nil, func(ctx context.Context, req any) (any, error) {
		return nil, nil
	})
	return err
}

func TestUnaryInterceptor_PassthroughWhenDisabled(t *testing.T) {
	issuer := auth.NewIssuer([]string{})
	if err := callInterceptor(issuer, context.Background()); err != nil {
		t.Errorf("expected no error when auth disabled, got: %v", err)
	}
}

func TestUnaryInterceptor_AllowsValidToken(t *testing.T) {
	issuer := auth.NewIssuer([]string{"key"})
	tok, _ := issuer.Issue("INST")

	md := metadata.Pairs("authorization", "Bearer "+tok)
	ctx := metadata.NewIncomingContext(context.Background(), md)

	if err := callInterceptor(issuer, ctx); err != nil {
		t.Errorf("expected no error with valid token, got: %v", err)
	}
}

func TestUnaryInterceptor_RejectsMissingMetadata(t *testing.T) {
	issuer := auth.NewIssuer([]string{"key"})
	err := callInterceptor(issuer, context.Background()) // no metadata attached
	if err == nil {
		t.Error("expected error when metadata is missing")
	}
	if status.Code(err) != codes.Unauthenticated {
		t.Errorf("expected Unauthenticated, got %v", status.Code(err))
	}
}

func TestUnaryInterceptor_RejectsMissingAuthHeader(t *testing.T) {
	issuer := auth.NewIssuer([]string{"key"})
	md := metadata.Pairs("content-type", "application/grpc")
	ctx := metadata.NewIncomingContext(context.Background(), md)

	err := callInterceptor(issuer, ctx)
	if err == nil {
		t.Error("expected error when authorization header is absent")
	}
	if status.Code(err) != codes.Unauthenticated {
		t.Errorf("expected Unauthenticated, got %v", status.Code(err))
	}
}

func TestUnaryInterceptor_RejectsInvalidToken(t *testing.T) {
	issuer := auth.NewIssuer([]string{"key"})
	md := metadata.Pairs("authorization", "Bearer garbage-token")
	ctx := metadata.NewIncomingContext(context.Background(), md)

	err := callInterceptor(issuer, ctx)
	if err == nil {
		t.Error("expected error for invalid token")
	}
	if status.Code(err) != codes.Unauthenticated {
		t.Errorf("expected Unauthenticated, got %v", status.Code(err))
	}
}

func TestUnaryInterceptor_RejectsNonBearerScheme(t *testing.T) {
	issuer := auth.NewIssuer([]string{"key"})
	tok, _ := issuer.Issue("INST")
	md := metadata.Pairs("authorization", "Basic "+tok) // wrong scheme
	ctx := metadata.NewIncomingContext(context.Background(), md)

	err := callInterceptor(issuer, ctx)
	if err == nil {
		t.Error("expected error for non-Bearer auth scheme")
	}
}
