package localscale

import (
	"io"
	"log/slog"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/require"
)

func TestCloseBranchProxiesPreservesEdgeProxies(t *testing.T) {
	s := &Server{
		logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
		proxies: make(map[string]*branchProxy),
	}
	edge := newTestBranchProxy(t)
	branch := newTestBranchProxy(t)
	s.proxies["edge:org/database"] = edge
	s.proxies["feature-branch"] = branch

	s.closeBranchProxies()

	require.Contains(t, s.proxies, "edge:org/database")
	require.NotContains(t, s.proxies, "feature-branch")
}

func TestBranchProxyCloseClosesTrackedClientConnections(t *testing.T) {
	p := newTestBranchProxy(t)
	client, server := net.Pipe()
	t.Cleanup(func() {
		utils.CloseAndLog(client)
		utils.CloseAndLog(server)
	})
	untrack, ok := p.trackClientConn(server)
	require.True(t, ok)
	t.Cleanup(untrack)

	errCh := make(chan error, 1)
	go func() {
		buf := make([]byte, 1)
		_, err := client.Read(buf)
		errCh <- err
	}()

	p.closeClientConns()

	select {
	case err := <-errCh:
		require.Error(t, err)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for proxy close to close client connection")
	}
}

func TestBranchProxyRejectsNewClientConnectionsAfterCloseStarts(t *testing.T) {
	p := newTestBranchProxy(t)
	client, server := net.Pipe()
	t.Cleanup(func() {
		utils.CloseAndLog(client)
		utils.CloseAndLog(server)
	})

	p.beginClose()
	untrack, ok := p.trackClientConn(server)

	require.False(t, ok)
	untrack()
}

func TestTrackProxyDoesNotReleaseOldPortBeforeCloseCompletes(t *testing.T) {
	port1 := freeTCPPort(t)
	port2 := freeTCPPort(t)
	require.NotEqual(t, port1, port2)

	s := &Server{
		logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
		proxies:   make(map[string]*branchProxy),
		proxyHost: "127.0.0.1",
		portAlloc: newTestPortAllocator(port1, port2),
	}

	listenAddr, err := s.proxyListenAddr()
	require.NoError(t, err)
	old := newTestBranchProxyAtAddr(t, listenAddr)
	s.trackProxy("feature-branch", old)

	var releaseOld sync.Once
	old.connWg.Add(1)
	releaseOldConn := func() {
		releaseOld.Do(func() {
			old.connWg.Done()
		})
	}
	t.Cleanup(releaseOldConn)

	replacementAddr, err := s.proxyListenAddr()
	require.NoError(t, err)
	replacement := newTestBranchProxyAtAddr(t, replacementAddr)
	s.trackProxy("feature-branch", replacement)

	_, err = s.portAlloc.acquire()
	require.Error(t, err, "old proxy port must not be reusable until the listener has closed")

	releaseOldConn()
	s.wg.Wait()
}

func TestPortAllocatorIgnoresDuplicateRelease(t *testing.T) {
	port1 := freeTCPPort(t)
	port2 := freeTCPPort(t)
	require.NotEqual(t, port1, port2)

	alloc := newTestPortAllocator(port1, port2)
	acquired, err := alloc.acquire()
	require.NoError(t, err)
	require.Equal(t, port1, acquired)

	alloc.release(acquired)
	alloc.release(acquired)

	next, err := alloc.acquire()
	require.NoError(t, err)
	require.Equal(t, port2, next)
	next, err = alloc.acquire()
	require.NoError(t, err)
	require.Equal(t, port1, next)
	_, err = alloc.acquire()
	require.Error(t, err)
}

func TestNewBranchProxyWithRetrySkipsOccupiedPort(t *testing.T) {
	port1 := freeTCPPort(t)
	port2 := freeTCPPort(t)
	require.NotEqual(t, port1, port2)

	held, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port1)))
	require.NoError(t, err)
	defer utils.CloseAndLog(held)

	s := &Server{
		logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
		proxyHost: "127.0.0.1",
		portAlloc: newTestPortAllocator(port1, port2),
	}

	proxy, err := s.newBranchProxyWithRetry(t.Context(), "root@tcp(127.0.0.1:1)/", "", nil, nil)
	require.NoError(t, err)
	defer utils.CloseAndLog(proxy)

	_, portStr, err := net.SplitHostPort(proxy.Addr())
	require.NoError(t, err)
	require.Equal(t, strconv.Itoa(port2), portStr)

	_, err = s.portAlloc.acquire()
	require.Error(t, err, "occupied port should stay out of the free pool")
}

func newTestBranchProxy(t *testing.T) *branchProxy {
	t.Helper()
	return newTestBranchProxyAtAddr(t, "127.0.0.1:0")
}

func newTestBranchProxyAtAddr(t *testing.T, listenAddr string) *branchProxy {
	t.Helper()
	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", listenAddr)
	require.NoError(t, err)

	p := &branchProxy{
		listener: ln,
		logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
		done:     make(chan struct{}),
		conns:    make(map[net.Conn]struct{}),
	}
	go p.serve()

	var closeOnce sync.Once
	t.Cleanup(func() {
		closeOnce.Do(func() {
			utils.CloseAndLog(p)
		})
	})
	return p
}

func freeTCPPort(t *testing.T) int {
	t.Helper()
	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer utils.CloseAndLog(ln)

	_, portStr, err := net.SplitHostPort(ln.Addr().String())
	require.NoError(t, err)
	port, err := strconv.Atoi(portStr)
	require.NoError(t, err)
	return port
}

func newTestPortAllocator(ports ...int) *portAllocator {
	return &portAllocator{
		free:  append([]int(nil), ports...),
		inUse: make(map[int]struct{}, len(ports)),
	}
}
