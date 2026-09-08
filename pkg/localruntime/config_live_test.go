//go:build darwin || linux

package localruntime

import (
	"bytes"
	"context"
	"net/http"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// Concurrent callers append configuration through the same live host. CAS
// retries preserve both additions and keep the original process generation.
func TestLiveConfigSerializesConcurrentAdditions(t *testing.T) {
	m := testManager(t, "ready")
	ctx, cancel := context.WithTimeout(t.Context(), testDeadline)
	defer cancel()
	before, err := m.Ensure(ctx)
	require.NoError(t, err)
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for _, suffix := range []string{"-one", "-two"} {
		wg.Go(func() {
			_, err := m.UpdateConfig(func(data []byte) ([]byte, error) { return append(data, []byte(suffix)...), nil }, map[string]map[string]string{})
			errs <- err
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	data, err := ReadPrivate(filepath.Join(m.Dir, "runtime.yaml"))
	require.NoError(t, err)
	require.Contains(t, string(data), "-one")
	require.Contains(t, string(data), "-two")
	after, err := m.Ensure(ctx)
	require.NoError(t, err)
	require.Equal(t, before.Generation, after.Generation)
	require.Equal(t, before.PID, after.PID)
	_, err = m.UpdateConfig(func([]byte) ([]byte, error) { return []byte("reject"), nil }, map[string]map[string]string{})
	require.Error(t, err)
	preserved, err := ReadPrivate(filepath.Join(m.Dir, "runtime.yaml"))
	require.NoError(t, err)
	require.Equal(t, data, preserved)
}

// A signature for one body cannot authorize different configuration bytes.
func TestLiveConfigRejectsTamperedBody(t *testing.T) {
	m := testManager(t, "ready")
	ctx, cancel := context.WithTimeout(t.Context(), testDeadline)
	defer cancel()
	c, err := m.Ensure(ctx)
	require.NoError(t, err)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.Control+"/config", bytes.NewBufferString("tampered"))
	require.NoError(t, err)
	nonce, err := randomID()
	require.NoError(t, err)
	req.Header.Set("X-Runtime-Generation", c.Generation)
	req.Header.Set("X-Runtime-Nonce", nonce)
	req.Header.Set("X-Runtime-Body", digest([]byte("original")))
	req.Header.Set("X-Runtime-Signature", signature(c.Token, requestMessage(req)))
	resp, err := controlClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	saved, err := ReadPrivate(filepath.Join(m.Dir, "runtime.yaml"))
	require.NoError(t, err)
	require.Equal(t, "ready", string(saved))
}
