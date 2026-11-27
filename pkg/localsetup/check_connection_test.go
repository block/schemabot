package localsetup

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
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

// Each failure SchemaBot can identify renders as one line naming the cause
// and the fix, with no generic prefix and no driver text. Any other failure
// keeps the caller's generic line with its category or server code.
func TestConnectionFailureCategories(t *testing.T) {
	refusedTLS := connectErrorFor(t, "require", fmt.Errorf("tls error: %w", errors.New(pgxRefusedTLS)))
	refusedTLSOnOneAddress := connectErrorFor(t, "require", errors.Join(syscall.ECONNREFUSED, errors.New(pgxRefusedTLS)))
	refusedTLSWithPlaintextAllowed := connectErrorFor(t, "prefer", errors.New(pgxRefusedTLS))
	for _, tc := range []struct {
		name  string
		cause error
		want  string
	}{
		{"timeout", context.DeadlineExceeded, "the connection check timed out; check the host and port, and that your network can reach the server"},
		{"cancelled", context.Canceled, "the connection check was cancelled"},
		{"wrong password", &pgconn.PgError{Code: "28P01", Message: "private-password"}, "the server rejected the password for that user; check the user name and password in the connection string"},
		{"user not allowed", &pgconn.PgError{Code: "28000", Message: "private-detail"}, "the server does not accept that user from this client; check the user name, and that the server's client authentication rules allow it"},
		{"missing database", &pgconn.PgError{Code: "3D000", Message: "private-detail"}, "that database does not exist on the server; check the database name in the connection string"},
		{"other server code", &pgconn.PgError{Code: "53300", Message: "private-detail"}, "Check connection: database error 53300"},
		{"malformed server code", &pgconn.PgError{Code: "28?01", Message: "private-password"}, "Check connection: database rejected the connection"},
		{"unreadable server code", &pgconn.PgError{Code: "private", Message: "private-password"}, "Check connection: connection could not be verified"},
		{"refused", syscall.ECONNREFUSED, "the server refused the connection; check that the database is running and that the host and port are right"},
		{"unresolvable host", &net.DNSError{Name: "private-host", Err: "private-detail"}, "the database hostname could not be resolved; check the host in the connection string and your network connection"},
		{"no TLS", refusedTLS, "the server does not offer TLS, which the connection's sslmode requires; connect to an endpoint that offers TLS, or change sslmode in the connection string"},
		{"no TLS on one of several addresses", refusedTLSOnOneAddress, "the server does not offer TLS"},
		{"no TLS when sslmode allows plaintext", refusedTLSWithPlaintextAllowed, "Check connection: connection could not be verified"},
		{"server answer beats a refused address", errors.Join(syscall.ECONNREFUSED, &pgconn.PgError{Code: "28P01"}), "the server rejected the password"},
		{"mysql access denied", &mysql.MySQLError{Number: 1045, Message: "private-password"}, "the server denied access for that user and password; check them in the connection string"},
		{"mysql missing database", &mysql.MySQLError{Number: 1049, Message: "private-detail"}, "that database does not exist on the server; check the database name in the connection string"},
		{"mysql no TLS", fmt.Errorf("connect: %w", mysql.ErrNoTLS), "the server does not offer TLS, which the connection's tls setting requires; connect to an endpoint that offers TLS, or change tls in the connection string"},
		{"other mysql error", &mysql.MySQLError{Number: 1040, Message: "private-detail"}, "Check connection: database error 1040"},
		{"unknown", errors.New("private-detail"), "Check connection: connection could not be verified"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := connectionCheckError("Check connection", tc.cause)
			require.ErrorIs(t, err, tc.cause)
			require.True(t, strings.HasPrefix(err.Error(), tc.want), "got %q", err.Error())
			require.NotContains(t, err.Error(), "private")
		})
	}
}

// A step after the connection is proven keeps its own line even when its
// cause is one the connection check would name: a timeout creating the
// storage database or an interrupted catalog read says nothing about the
// host, port, or credentials.
func TestLaterStepsKeepTheirOwnLine(t *testing.T) {
	for _, tc := range []struct {
		name    string
		message string
		cause   error
		want    string
	}{
		{"create database timed out", "could not create the schemabot database", context.DeadlineExceeded, "timed out"},
		{"discovery interrupted", "namespace discovery was interrupted; try again", context.Canceled, "cancelled"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := &setupConnectionError{message: tc.message, cause: tc.cause}
			require.ErrorIs(t, err, tc.cause)
			require.Equal(t, tc.message+": "+tc.want, err.Error())
			require.NotContains(t, err.Error(), "connection check")
		})
	}
	require.Equal(t, "could not create the schemabot database: timed out", integratedStorageCreationError(context.DeadlineExceeded).Error())
}

// connectErrorFor returns the connect error pgx gives for a connection with
// the given sslmode whose every attempt fails with cause, without a network:
// the dial itself returns cause.
func connectErrorFor(t *testing.T, sslmode string, cause error) error {
	t.Helper()
	config, err := pgconn.ParseConfig("postgres://app@127.0.0.1:5432/shop?sslmode=" + sslmode)
	require.NoError(t, err)
	config.DialFunc = func(context.Context, string, string) (net.Conn, error) {
		return nil, cause
	}
	_, err = pgconn.ConnectConfig(t.Context(), config)
	var connectError *pgconn.ConnectError
	require.ErrorAs(t, err, &connectError)
	return err
}
