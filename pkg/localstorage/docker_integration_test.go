//go:build integration

package localstorage

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/block/mysql"
	"github.com/block/schemabot/pkg/localdocker"
	"github.com/block/schemabot/pkg/mysqlconn"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/require"
)

func TestManagedStoragePersistsAcrossRestart(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	dir := t.TempDir()
	require.NoError(t, os.Chmod(dir, 0700))
	sum := sha256.Sum256([]byte(dir))
	name := fmt.Sprintf("schemabot-state-%x", sum[:8])
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), 30*time.Second)
		defer cancel()
		if _, err := localdocker.Run(cleanup, nil, "rm", "-f", name); err != nil {
			t.Log(err)
		}
		if _, err := localdocker.Run(cleanup, nil, "volume", "rm", name+"-data"); err != nil {
			t.Log(err)
		}
	})
	ref, err := Prepare(ctx, dir)
	require.NoError(t, err)
	data, err := os.ReadFile(strings.TrimPrefix(ref, "file:"))
	require.NoError(t, err)
	db, err := mysqlconn.Open(string(data))
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	require.NoError(t, db.PingContext(ctx))
	_, err = db.ExecContext(ctx, "CREATE TABLE history_test (id INT PRIMARY KEY)")
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, "INSERT INTO history_test VALUES (42)")
	require.NoError(t, err)
	_, err = localdocker.Run(ctx, nil, "stop", name)
	require.NoError(t, err)
	cfg, err := mysql.ParseDSN(string(data))
	require.NoError(t, err)
	occupied, err := new(net.ListenConfig).Listen(ctx, "tcp4", cfg.Addr)
	require.NoError(t, err)
	require.Error(t, Resume(ctx, dir))
	require.NoError(t, occupied.Close())
	require.NoError(t, Resume(ctx, dir))
	var id int
	require.NoError(t, db.QueryRowContext(ctx, "SELECT id FROM history_test").Scan(&id))
	require.Equal(t, 42, id)
	again, err := Prepare(ctx, dir)
	require.NoError(t, err)
	require.Equal(t, ref, again)
	require.ErrorContains(t, ValidateReference(dir, "env:OTHER"), "owns local Docker storage")
	raw, err := os.ReadFile(filepath.Join(dir, "docker-storage.json"))
	require.NoError(t, err)
	var saved record
	require.NoError(t, json.Unmarshal(raw, &saved))
	_, err = localdocker.Run(ctx, nil, "rm", "-f", name)
	require.NoError(t, err)
	require.ErrorContains(t, Resume(ctx, dir), "missing or unavailable")
	_, err = Prepare(ctx, dir)
	require.Error(t, err)
	// A missing container must not be replaced, even during another init.
	found, err := localdocker.Run(ctx, nil, "container", "ls", "-a", "--filter", "name=^/"+name+"$", "--format", "{{.ID}}")
	require.NoError(t, err)
	require.Empty(t, strings.TrimSpace(string(found)))
}
