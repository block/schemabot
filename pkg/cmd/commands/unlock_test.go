package commands

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/cmd/client"
)

func TestUnlockCmdCanonicalizesKeysForAlternateTypeHint(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodDelete && r.URL.Path == "/api/locks":
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			_, err := w.Write([]byte(`{"error":"lock not found"}`))
			require.NoError(t, err)
		case r.Method == http.MethodGet && r.URL.Path == "/api/locks":
			w.Header().Set("Content-Type", "application/json")
			_, err := w.Write([]byte(`{"locks":[{"database":"games","database_type":"vitess","owner":"org/repo#2"}]}`))
			require.NoError(t, err)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)

	cmd := UnlockCmd{Database: "GAMES", Type: "MYSQL"}
	var runErr error
	output := captureStdout(func() {
		runErr = cmd.Run(&Globals{Endpoint: server.URL})
	})

	require.NoError(t, runErr)
	assert.Equal(t, "games", cmd.Database)
	assert.Equal(t, "mysql", cmd.Type)
	assert.Contains(t, stripAnsi(output), "a vitess lock exists for this database")
}

func TestLockUnderOtherType(t *testing.T) {
	locks := []*client.LockInfo{
		{Database: "orders", DatabaseType: "mysql", Owner: "org/repo#1"},
		{Database: "games", DatabaseType: "vitess", Owner: "org/repo#2"},
	}

	t.Run("finds lock held under another type", func(t *testing.T) {
		lock := lockUnderOtherType(locks, "games", "mysql")
		require.NotNil(t, lock)
		assert.Equal(t, "games", lock.Database)
		assert.Equal(t, "vitess", lock.DatabaseType)
		assert.Equal(t, "org/repo#2", lock.Owner)
	})

	t.Run("ignores lock held under the requested type", func(t *testing.T) {
		assert.Nil(t, lockUnderOtherType(locks, "orders", "mysql"))
	})

	t.Run("ignores locks on other databases", func(t *testing.T) {
		assert.Nil(t, lockUnderOtherType(locks, "payments", "mysql"))
	})

	t.Run("no locks", func(t *testing.T) {
		assert.Nil(t, lockUnderOtherType(nil, "games", "mysql"))
	})
}
