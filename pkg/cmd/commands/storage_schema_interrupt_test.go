package commands

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/cmd/cliname"
)

// unreachableStorageDSN names a PostgreSQL storage database on a port nothing
// listens on, so a direct convergence fails at its first connection.
const unreachableStorageDSN = "postgres://schemabot:secret@127.0.0.1:1/schemabot?sslmode=disable"

// An operator presses Ctrl-C while `storage apply` waits on the server's
// convergence. The server carries the convergence on regardless (AV-13), so the
// command says so on stderr and exits with the interrupt status rather than
// reporting a convergence that failed.
func TestStorageApplyThroughAPI_InterruptExitsInterrupted(t *testing.T) {
	server, held := heldStorageApplyServer(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	stderr, err := convergeThroughAPI(t, ctx, server, func() {
		awaitSignal(t, held, "the storage apply request to reach the server")
		cancel()
	})

	require.ErrorIs(t, err, ErrSilent)
	require.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, ExitInterrupted, ExitCodeFor(err))
	assert.Equal(t, fmt.Sprintf(storageApplyInterruptedThroughAPI+"\n", cliname.Name()), stderr)
}

// A convergence the server answers with an error is a failure: it exits 1 and
// carries the server's error, with no interrupt notice.
func TestStorageApplyThroughAPI_ServerFailureExitsOne(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/api/storage/schema/apply", r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_, err := io.WriteString(w, `{"error":"storage bootstrap lock is held by another instance"}`)
		assert.NoError(t, err)
	}))
	t.Cleanup(server.Close)

	stderr, err := convergeThroughAPI(t, t.Context(), server, func() {})

	require.Error(t, err)
	assert.NotErrorIs(t, err, ErrSilent)
	assert.Contains(t, err.Error(), "storage bootstrap lock is held by another instance")
	assert.Equal(t, 1, ExitCodeFor(err))
	assert.Empty(t, stderr)
}

// A deadline on the command's context is not the operator stopping it, so the
// request it cut off is a failure that exits 1 with its error.
func TestStorageApplyThroughAPI_DeadlineExitsOne(t *testing.T) {
	server, held := heldStorageApplyServer(t)
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()

	stderr, err := convergeThroughAPI(t, ctx, server, func() {
		awaitSignal(t, held, "the storage apply request to reach the server")
	})

	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.NotErrorIs(t, err, ErrSilent)
	assert.Equal(t, 1, ExitCodeFor(err))
	assert.Empty(t, stderr)
}

// An operator presses Ctrl-C during a direct convergence. The convergence
// releases what it was doing (AV-13), so the command says what that leaves and
// exits with the interrupt status.
func TestStorageApplyDirect_InterruptExitsInterrupted(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	stderr, err := convergeDirect(t, ctx)

	require.ErrorIs(t, err, ErrSilent)
	require.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, ExitInterrupted, ExitCodeFor(err))
	assert.Contains(t, stderr, fmt.Sprintf(storageApplyInterruptedDirect+"\n", cliname.Name()))
}

// A direct convergence that cannot reach its database is a failure: it exits 1
// with the error naming where the DSN came from, and prints no interrupt notice.
func TestStorageApplyDirect_FailureExitsOne(t *testing.T) {
	stderr, err := convergeDirect(t, t.Context())

	require.Error(t, err)
	assert.NotErrorIs(t, err, ErrSilent)
	assert.NotErrorIs(t, err, context.Canceled)
	assert.Contains(t, err.Error(), "converge storage schema on the database from --dsn flag")
	assert.Equal(t, 1, ExitCodeFor(err))
	assert.NotContains(t, stderr, "Storage apply interrupted")
}

// heldStorageApplyServer answers the storage apply endpoint by holding each
// request open until the client gives up on it, and reports each arrival.
func heldStorageApplyServer(t *testing.T) (*httptest.Server, <-chan struct{}) {
	t.Helper()
	hold := newHoldingHandler(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/api/storage/schema/apply", r.URL.Path)
		// The server notices the client hanging up only once the body is read.
		_, err := io.Copy(io.Discard, r.Body)
		assert.NoError(t, err)
		hold.ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)
	return server, hold.inFlight
}

// convergeThroughAPI runs an unattended convergence against server, calls
// during while it is running, and returns its stderr and the error it ended with.
func convergeThroughAPI(t *testing.T, ctx context.Context, server *httptest.Server, during func()) (string, error) {
	t.Helper()
	cmd := &StorageApplyCmd{AutoApprove: true}
	done := make(chan error, 1)
	var err error
	stderr := captureStderr(t, func() {
		go func() {
			_, _, convergeErr := cmd.converge(ctx, &Globals{Endpoint: server.URL}, &cmd.storageSchemaTargetFlags, nil)
			done <- convergeErr
		}()
		during()
		err = awaitSignal(t, done, "the storage apply to end")
	})
	return stderr, err
}

// convergeDirect runs an unattended direct convergence of the unreachable
// storage database and returns its stderr and the error it ended with.
func convergeDirect(t *testing.T, ctx context.Context) (string, error) {
	t.Helper()
	cmd := &StorageApplyCmd{storageSchemaTargetFlags: storageSchemaTargetFlags{DSN: unreachableStorageDSN}, AutoApprove: true}
	var err error
	stderr := captureStderr(t, func() {
		_, _, err = cmd.converge(ctx, &Globals{Version: "v1.4.0"}, &cmd.storageSchemaTargetFlags, nil)
	})
	return stderr, err
}
