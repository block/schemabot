package sqlstore

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/changeevents"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

type applyEventReadConn struct {
	stubConn
	noopTx
	cancelWrite context.CancelFunc
	committed   bool
	reads       int
}

func (c *applyEventReadConn) Connect(context.Context) (driver.Conn, error) { return c, nil }
func (*applyEventReadConn) Driver() driver.Driver                          { return stubDriver{} }

func (c *applyEventReadConn) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) {
	return c, nil
}

func (c *applyEventReadConn) Commit() error {
	c.committed = true
	return nil
}

func (c *applyEventReadConn) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	c.reads++
	if !c.committed {
		// Simulate a slow event-only read using up the update's deadline.
		c.cancelWrite()
	}
	return nil, context.DeadlineExceeded
}

func TestFailedApplyCommitsBeforeReadingChangeEventData(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	conn := &applyEventReadConn{
		stubConn:    stubConn{execResult: driver.RowsAffected(1)},
		cancelWrite: cancel,
	}
	db := sql.OpenDB(conn)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	logger := slog.New(slog.DiscardHandler)
	sender, err := changeevents.New([]changeevents.Destination{{
		URL: "http://localhost", Environments: []string{"staging"},
		Events: []string{changeevents.ApplyFinished}, Body: "{}",
	}}, logger)
	require.NoError(t, err)
	t.Cleanup(sender.Close)

	store := NewMySQL(db, storage.WithChangeEvents(sender, nil, logger))
	apply := &storage.Apply{ID: 1, Environment: "staging", State: state.Apply.Failed}
	require.NoError(t, store.Applies().Update(ctx, apply))
	require.True(t, conn.committed)
	require.Equal(t, 1, conn.reads, "the recorder attempted its read after commit")
}

type blockedApplyEventReadConn struct {
	applyEventReadConn
	reading chan struct{}
	release <-chan struct{}
}

func (c *blockedApplyEventReadConn) Connect(context.Context) (driver.Conn, error) { return c, nil }

func (c *blockedApplyEventReadConn) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	close(c.reading)
	<-c.release
	return nil, context.DeadlineExceeded
}

func TestReapedApplyFinishDoesNotWaitForEventData(t *testing.T) {
	release := make(chan struct{})
	conn := &blockedApplyEventReadConn{reading: make(chan struct{}), release: release}
	db := sql.OpenDB(conn)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	defer close(release)

	logger := slog.New(slog.DiscardHandler)
	sender, err := changeevents.New([]changeevents.Destination{{
		URL: "http://localhost", Environments: []string{"staging"},
		Events: []string{changeevents.ApplyFinished}, Body: "{}",
	}}, logger)
	require.NoError(t, err)
	t.Cleanup(sender.Close)

	store := NewMySQL(db, storage.WithChangeEvents(sender, nil, logger))
	returned := make(chan struct{})
	go func() {
		defer close(returned)
		store.changeEvents.offerReapedApplyFinish(&storage.Apply{ID: 1, Environment: "staging", State: state.Apply.Failed})
	}()

	deadline := time.After(5 * time.Second)
	select {
	case <-conn.reading:
	case <-deadline:
		t.Fatal("notification worker did not start the event lookup")
	}
	select {
	case <-returned:
	case <-deadline:
		t.Fatal("the reaper's event offer blocked on notification storage")
	}
}
