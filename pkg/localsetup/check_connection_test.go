package localsetup

import (
	"context"
	"errors"
	"net"
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
