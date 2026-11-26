package localsetup

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"syscall"
	"testing"

	"github.com/block/mysql"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/stretchr/testify/require"
)

func TestConnectionErrorsNeverExposeDSN(t *testing.T) {
	for _, engine := range []string{"mysql", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			secret := "private-password-do-not-print"
			dsn := "postgres://user:" + secret + "@%bad"
			err := CheckConnection(t.Context(), engine, dsn)
			require.Error(t, err)
			require.NotContains(t, err.Error(), secret)
			_, err = DiscoverNamespaces(t.Context(), engine, dsn)
			require.Error(t, err)
			require.NotContains(t, err.Error(), secret)
			require.Error(t, CheckConnection(t.Context(), engine, ""))
			_, err = DiscoverNamespaces(t.Context(), engine, "")
			require.Error(t, err)
		})
	}
}

func TestConnectionErrorRetainsCause(t *testing.T) {
	cause := errors.New("driver detail with private connection material")
	err := &setupConnectionError{message: "Check the connection", cause: cause}
	require.ErrorIs(t, err, cause)
	require.EqualError(t, err, "Check the connection: connection could not be verified")
}

// A Vitess target is addressed over HTTP rather than a database connection, so
// its check establishes that a healthy PlanetScale-compatible API answers at
// the endpoint before anything is registered against it.
func TestVitessConnectionChecksThePlanetScaleEndpoint(t *testing.T) {
	healthy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/health", r.URL.Path)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(healthy.Close)
	require.NoError(t, CheckConnection(t.Context(), "vitess", healthy.URL))
	require.NoError(t, CheckConnection(t.Context(), "vitess", healthy.URL+"/"))

	unhealthy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	t.Cleanup(unhealthy.Close)
	require.ErrorContains(t, CheckConnection(t.Context(), "vitess", unhealthy.URL), "not as a healthy PlanetScale-compatible API")

	unreachable := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	endpoint := unreachable.URL
	unreachable.Close()
	require.ErrorContains(t, CheckConnection(t.Context(), "vitess", endpoint), "couldn’t reach that endpoint")

	for _, malformed := range []string{"localhost:8080", "mysql://user@localhost:3306/shop", "://nope"} {
		require.ErrorContains(t, CheckConnection(t.Context(), "vitess", malformed), "http://host:port", malformed)
	}
	require.ErrorContains(t, CheckConnection(t.Context(), "vitess", ""), "set a connection variable")
}

func TestConnectionFailureCategories(t *testing.T) {
	for _, tc := range []struct {
		cause error
		want  string
	}{
		{context.DeadlineExceeded, "timed out"},
		{context.Canceled, "cancelled"},
		{&pgconn.PgError{Code: "28P01", Message: "private-password"}, "database error 28P01"},
		{&pgconn.PgError{Code: "28?01", Message: "private-password"}, "database rejected the connection"},
		{&pgconn.PgError{Code: "private", Message: "private-password"}, "connection could not be verified"},
		{syscall.ECONNREFUSED, "connection refused"},
		{&net.DNSError{Name: "private-host", Err: "private-detail"}, "hostname could not be resolved"},
		{&mysql.MySQLError{Number: 1045, Message: "private-password"}, "database error 1045"},
	} {
		err := &setupConnectionError{message: "Check connection", cause: tc.cause}
		require.ErrorIs(t, err, tc.cause)
		require.ErrorContains(t, err, tc.want)
		require.NotContains(t, err.Error(), "private")
	}
}
