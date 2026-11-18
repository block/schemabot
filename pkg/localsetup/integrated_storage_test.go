package localsetup

import (
	"net"
	"syscall"
	"testing"

	"github.com/block/mysql"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
)

func TestIntegratedStorageCreationErrors(t *testing.T) {
	for _, tt := range []struct {
		name     string
		cause    error
		want     string
		guidance bool
	}{
		{"mysql exists", &mysql.MySQLError{Number: 1007, Message: "private-secret"}, "already exists, possibly from an earlier setup attempt", true},
		{"postgres exists", &pgconn.PgError{Code: "42P04", Message: "private-secret"}, "if it is yours, choose standalone", true},
		{"mysql denied", &mysql.MySQLError{Number: 1044, Message: "private-secret"}, "this user cannot create", true},
		{"postgres denied", &pgconn.PgError{Code: "42501", Message: "private-secret"}, "this user cannot create", true},
		{"refused", syscall.ECONNREFUSED, "connection refused", false},
		{"DNS", &net.DNSError{Name: "private-secret", Err: "private-secret"}, "hostname could not be resolved", false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := integratedStorageCreationError(tt.cause)
			require.ErrorIs(t, err, tt.cause)
			require.ErrorContains(t, err, tt.want)
			require.NotContains(t, err.Error(), "private-secret")
			if !tt.guidance {
				require.NotContains(t, err.Error(), "standalone")
				require.NotContains(t, err.Error(), "already exists")
			}
		})
	}
}
