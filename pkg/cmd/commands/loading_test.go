package commands

import (
	"bytes"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type notifyingWriter struct {
	buf     bytes.Buffer
	wrote   chan struct{}
	once    sync.Once
	message string
}

func (w *notifyingWriter) Write(p []byte) (int, error) {
	n, err := w.buf.Write(p)
	if strings.Contains(string(p), w.message) {
		w.once.Do(func() { close(w.wrote) })
	}
	return n, err
}

func (w *notifyingWriter) String() string {
	return w.buf.String()
}

func TestWithLoadingShowsAndClearsSpinner(t *testing.T) {
	writer := &notifyingWriter{wrote: make(chan struct{}), message: "Loading schema change status..."}
	withTestLoadingSpinner(t, writer)

	err := withLoading("Loading schema change status...", true, func() error {
		select {
		case <-writer.wrote:
			return nil
		case <-time.After(time.Second):
			return errors.New("spinner did not render")
		}
	})

	require.NoError(t, err)
	assert.Contains(t, writer.String(), "Loading schema change status...")
	assert.Contains(t, writer.String(), "\r\033[2K")
}

func TestWithLoadingReturnsCommandError(t *testing.T) {
	writer := &notifyingWriter{wrote: make(chan struct{}), message: "Loading locks..."}
	withTestLoadingSpinner(t, writer)
	wantErr := errors.New("server unavailable")

	err := withLoading("Loading locks...", true, func() error {
		select {
		case <-writer.wrote:
			return wantErr
		case <-time.After(time.Second):
			return errors.New("spinner did not render")
		}
	})

	require.ErrorIs(t, err, wantErr)
	assert.Contains(t, writer.String(), "Loading locks...")
	assert.Contains(t, writer.String(), "\r\033[2K")
}

func TestWithLoadingDisabledIsSilent(t *testing.T) {
	var out bytes.Buffer
	withTestLoadingSpinner(t, &out)

	err := withLoading("Loading logs...", false, func() error { return nil })

	require.NoError(t, err)
	assert.Empty(t, out.String())
}

func TestWithLoadingNonTerminalIsSilent(t *testing.T) {
	var out bytes.Buffer
	withTestLoadingSpinner(t, &out)
	loadingSpinnerTerminal = func() bool { return false }

	err := withLoading("Loading schema change progress...", true, func() error { return nil })

	require.NoError(t, err)
	assert.Empty(t, out.String())
}

func withTestLoadingSpinner(t *testing.T, writer interface{ Write([]byte) (int, error) }) {
	t.Helper()
	originalDelay := loadingSpinnerDelay
	originalInterval := loadingSpinnerInterval
	originalWriter := loadingSpinnerWriter
	originalTerminal := loadingSpinnerTerminal

	loadingSpinnerDelay = 0
	loadingSpinnerInterval = time.Hour
	loadingSpinnerWriter = writer
	loadingSpinnerTerminal = func() bool { return true }

	t.Cleanup(func() {
		loadingSpinnerDelay = originalDelay
		loadingSpinnerInterval = originalInterval
		loadingSpinnerWriter = originalWriter
		loadingSpinnerTerminal = originalTerminal
	})
}
