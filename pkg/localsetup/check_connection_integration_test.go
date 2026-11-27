//go:build integration

package localsetup

import (
	"net"
	"net/url"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/testutil"
)

// A first-time user runs init against a PostgreSQL server and gets one part
// of the connection string wrong. Each mistake produces a line that names
// what went wrong and what to change, never the generic "check the address,
// credentials, and network" line, a bare SQLSTATE, or the password.
func TestCheckConnectionNamesEachPostgresFailure(t *testing.T) {
	const password = "private-wrong-password"
	dsn, _ := testutil.StartPostgres(t, "connection_failures_test")
	base, err := url.Parse(dsn)
	require.NoError(t, err)
	with := func(change func(u *url.URL)) string {
		u := *base
		change(&u)
		return u.String()
	}

	// A port that was just listening and is now closed refuses connections.
	var listenConfig net.ListenConfig
	listener, err := listenConfig.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	closedPort := strconv.Itoa(listener.Addr().(*net.TCPAddr).Port)
	require.NoError(t, listener.Close())

	for _, tc := range []struct {
		name string
		dsn  string
		want string
	}{
		{
			name: "wrong password",
			dsn:  with(func(u *url.URL) { u.User = url.UserPassword("schemabot", password) }),
			want: `the server rejected the password for user "schemabot"; check the user name and password in the connection string`,
		},
		{
			name: "missing database",
			dsn:  with(func(u *url.URL) { u.Path = "/no_such_database" }),
			want: `database "no_such_database" does not exist on the server; check the database name in the connection string`,
		},
		{
			name: "sslmode=require against a server without TLS",
			dsn: with(func(u *url.URL) {
				query := u.Query()
				query.Set("sslmode", "require")
				u.RawQuery = query.Encode()
			}),
			want: "the server does not offer TLS, which the connection's sslmode requires; connect to an endpoint that offers TLS, or change sslmode in the connection string",
		},
		{
			name: "nothing listening",
			dsn:  with(func(u *url.URL) { u.Host = net.JoinHostPort("127.0.0.1", closedPort) }),
			want: "the server refused the connection; check that the database is running and that the host and port are right",
		},
		{
			name: "unresolvable host",
			dsn:  with(func(u *url.URL) { u.Host = net.JoinHostPort("schemabot-test.invalid", base.Port()) }),
			want: "the database hostname could not be resolved; check the host in the connection string and your network connection",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := CheckConnection(t.Context(), "postgres", tc.dsn)
			require.Error(t, err)
			assert.Equal(t, tc.want, err.Error())
			assert.NotContains(t, err.Error(), password)
		})
	}

	require.NoError(t, CheckConnection(t.Context(), "postgres", dsn), "the unchanged connection string connects")
}
