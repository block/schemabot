package commands

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/cmd/client"
)

// An operator who types the database and type flags in a different case than
// the stored lock still gets the alternate-type hint, spelled with the lock's
// canonical identity, on both the owned and the forced release paths.
func TestUnlockCmdCanonicalizesKeysForAlternateTypeHint(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodDelete && r.URL.Path == "/api/locks":
			w.WriteHeader(http.StatusNotFound)
			_, err := w.Write([]byte(`{"error":"lock not found"}`))
			assert.NoError(t, err)
		case r.Method == http.MethodGet && r.URL.Path == "/api/locks/games/mysql":
			w.WriteHeader(http.StatusNotFound)
			_, err := w.Write([]byte(`{"error":"lock not found"}`))
			assert.NoError(t, err)
		case r.Method == http.MethodGet && r.URL.Path == "/api/locks":
			_, err := w.Write([]byte(`{"locks":[{"database":"games","database_type":"vitess","owner":"org/repo#2"}]}`))
			assert.NoError(t, err)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)

	for name, force := range map[string]bool{"owned release": false, "force release": true} {
		t.Run(name, func(t *testing.T) {
			cmd := UnlockCmd{Database: "GAMES", Type: "MYSQL", Force: force}
			var runErr error
			output := captureStdout(func() {
				runErr = cmd.Run(&Globals{Endpoint: server.URL})
			})

			require.NoError(t, runErr)
			assert.Contains(t, stripAnsi(output),
				"No lock found for games (mysql), but a vitess lock exists for this database.")
			assert.Contains(t, stripAnsi(output), "unlock -d games -t vitess")
		})
	}
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

	t.Run("folds the request before comparing", func(t *testing.T) {
		lock := lockUnderOtherType(locks, "GAMES", "MYSQL")
		require.NotNil(t, lock)
		assert.Equal(t, "org/repo#2", lock.Owner)
		assert.Nil(t, lockUnderOtherType(locks, "GAMES", "VITESS"))
	})

	t.Run("folds a stored lock spelled in another case", func(t *testing.T) {
		stranded := []*client.LockInfo{{Database: "GAMES", DatabaseType: "VITESS", Owner: "org/repo#3"}}
		lock := lockUnderOtherType(stranded, "games", "mysql")
		require.NotNil(t, lock)
		assert.Equal(t, "org/repo#3", lock.Owner)
		assert.Nil(t, lockUnderOtherType(stranded, "games", "vitess"))
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
