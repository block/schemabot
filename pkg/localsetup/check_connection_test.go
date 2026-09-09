package localsetup

import (
	"context"
	"net"
	"syscall"
	"testing"

	"github.com/block/mysql"

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
			require.Error(t, CheckConnection(t.Context(), engine, ""))
		})
	}
}

func TestConnectionFailureCategories(t *testing.T) {
	for _, tc := range []struct {
		cause error
		want  string
	}{
		{context.DeadlineExceeded, "timed out"},
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
