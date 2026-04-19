package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestInternalMuxProtectsCronTickWhenTokenConfigured(t *testing.T) {
	mux := NewInternalMux(time.Second, func(context.Context) error { return nil }, "expected-token", func(w http.ResponseWriter, r *http.Request) {})

	req := httptest.NewRequest(http.MethodPost, "/system/cron/tick", nil)
	rec := httptest.NewRecorder()

	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for missing internal token, got %d", rec.Code)
	}
}

func TestInternalMuxAllowsHealthAndMetricsWithoutToken(t *testing.T) {
	mux := NewInternalMux(time.Second, func(context.Context) error { return nil }, "expected-token", func(w http.ResponseWriter, r *http.Request) {})

	for _, tc := range []struct {
		method string
		path   string
	}{
		{method: http.MethodGet, path: "/livez"},
		{method: http.MethodGet, path: "/readyz"},
		{method: http.MethodGet, path: "/metrics"},
	} {
		req := httptest.NewRequest(tc.method, tc.path, nil)
		rec := httptest.NewRecorder()

		mux.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("%s %s: expected 200, got %d", tc.method, tc.path, rec.Code)
		}
	}
}

func TestInternalMuxPassesAuthorizedCronTickRequests(t *testing.T) {
	mux := NewInternalMux(time.Second, func(context.Context) error { return nil }, "expected-token", func(w http.ResponseWriter, r *http.Request) {})

	req := httptest.NewRequest(http.MethodPost, "/system/cron/tick", nil)
	req.Header.Set("X-Internal-Token", "expected-token")
	rec := httptest.NewRecorder()

	mux.ServeHTTP(rec, req)

	if rec.Code == http.StatusUnauthorized {
		t.Fatalf("expected authorized request to pass auth guard, got %d", rec.Code)
	}
}
