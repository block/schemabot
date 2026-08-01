package commands

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/cmd/client"
)

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
