//go:build integration

package spirit

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/block/mysql"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
)

// cutoverCallTimeout bounds one Cutover call, so a proxy that stalls fails the
// test instead of hanging it.
const cutoverCallTimeout = 30 * time.Second

// A cutover whose connection breaks while the sentinel drop is in flight cannot
// tell whether the drop ran, so it must report an unknown outcome rather than a
// failure: a failed cutover would be recorded as refused even when Spirit is
// already swapping the tables in. The connection can break before the drop
// reaches the server, leaving the sentinel in place, or after the drop ran but
// before its reply arrived, leaving the sentinel gone. Both look the same to the
// caller and both are reported as unknown. With no break, the same cutover is
// accepted and the sentinel is gone.
func TestEngine_CutoverReportsAnUnknownOutcomeWhenTheConnectionBreaksDuringTheSentinelDrop(t *testing.T) {
	tests := []struct {
		name             string
		fault            sentinelDropFault
		sentinelRemains  bool
		wantUnknownError bool
	}{
		{name: "no break accepts the cutover", fault: sentinelDropFaultNone, sentinelRemains: false},
		{name: "a break before the drop reaches the server is an unknown outcome", fault: sentinelDropFaultBeforeDrop, sentinelRemains: true, wantUnknownError: true},
		{name: "a break after the drop ran but before its reply is an unknown outcome", fault: sentinelDropFaultAfterDrop, sentinelRemains: false, wantUnknownError: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dsn, db := setupTestMySQL(t)
			cleanupTables(t, db)

			_, err := db.ExecContext(t.Context(), "CREATE TABLE `_spirit_sentinel` (id int NOT NULL PRIMARY KEY)")
			require.NoError(t, err, "create sentinel")
			t.Cleanup(func() {
				// t.Context() is already cancelled when cleanup runs.
				ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), 30*time.Second)
				defer cancel()
				_, err := db.ExecContext(ctx, "DROP TABLE IF EXISTS `_spirit_sentinel`")
				assert.NoError(t, err, "drop sentinel")
			})

			proxy := startSentinelDropProxy(t, dsn, tt.fault)

			ctx, cancel := context.WithTimeout(t.Context(), cutoverCallTimeout)
			defer cancel()
			result, err := New(Config{}).Cutover(ctx, &engine.ControlRequest{
				Database:    testDatabase,
				Credentials: &engine.Credentials{DSN: proxy.dsn},
			})

			select {
			case <-proxy.sawDrop:
			default:
				t.Fatal("the sentinel drop never passed through the proxy, so the test did not exercise it")
			}

			if tt.wantUnknownError {
				require.Error(t, err, "Cutover() over a broken connection")
				assert.Nil(t, result, "a cutover with an unknown outcome returns no result")
				assert.True(t, proxy.severed.Load(), "the proxy broke the connection during the sentinel drop")
				assert.True(t, engine.IsOutcomeUnknown(err),
					"a broken connection during the sentinel drop must be an unknown outcome, not a failure: %v", err)
				assert.ErrorIs(t, err, mysql.ErrInvalidConn, "the broken connection stays reachable for triage")
				assert.Contains(t, err.Error(), "drop sentinel table")
			} else {
				require.NoError(t, err, "Cutover()")
				require.NotNil(t, result, "Cutover() result")
				assert.True(t, result.Accepted, "cutover not accepted: %s", result.Message)
				assert.False(t, proxy.severed.Load(), "the proxy left the connection intact")
			}

			assert.Equal(t, tt.sentinelRemains, tableExists(t, db, deferredCutoverSentinelTable),
				"whether the sentinel drop reached the database")
		})
	}
}

// sentinelDropFault says where the proxy breaks the connection relative to the
// sentinel drop.
type sentinelDropFault int

const (
	// sentinelDropFaultNone forwards every packet unchanged.
	sentinelDropFaultNone sentinelDropFault = iota
	// sentinelDropFaultBeforeDrop closes the connection instead of forwarding
	// the drop, so the server never runs it.
	sentinelDropFaultBeforeDrop
	// sentinelDropFaultAfterDrop forwards the drop, then closes the connection
	// instead of forwarding the server's reply, so the drop ran but the client
	// never hears about it.
	sentinelDropFaultAfterDrop
)

// comQuery is the MySQL protocol command byte for a text query.
const comQuery = 0x03

// sentinelDropProxy is a TCP proxy in front of the test MySQL server that
// reads the MySQL packet framing in both directions, so it can break the
// connection at a specific point of the sentinel drop without timing guesses.
type sentinelDropProxy struct {
	dsn     string
	sawDrop chan struct{}
	severed atomic.Bool

	upstream string
	fault    sentinelDropFault
	sawOnce  sync.Once
	mu       sync.Mutex
	conns    []net.Conn
}

// startSentinelDropProxy starts a proxy to the server dsn names and returns it
// with a DSN that connects through it. The proxy and every connection through
// it are closed when the test ends.
func startSentinelDropProxy(t *testing.T, dsn string, fault sentinelDropFault) *sentinelDropProxy {
	t.Helper()

	cfg, err := mysql.ParseDSN(dsn)
	require.NoError(t, err, "parse test DSN")

	var listenConfig net.ListenConfig
	listener, err := listenConfig.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err, "listen for proxy")

	p := &sentinelDropProxy{
		sawDrop:  make(chan struct{}),
		upstream: cfg.Addr,
		fault:    fault,
	}
	cfg.Addr = listener.Addr().String()
	p.dsn = cfg.FormatDSN()

	var wg sync.WaitGroup
	wg.Go(func() {
		for {
			client, err := listener.Accept()
			if err != nil {
				t.Logf("proxy stopped accepting connections: %v", err)
				return
			}
			wg.Go(func() { p.serve(t, client) })
		}
	})
	t.Cleanup(func() {
		utils.CloseAndLog(listener)
		p.mu.Lock()
		for _, conn := range p.conns {
			// Each connection may already be closed by its own pumps.
			_ = conn.Close()
		}
		p.mu.Unlock()
		wg.Wait()
	})
	return p
}

// serve relays one client connection to the server, breaking it at the
// sentinel drop as the proxy's fault says.
func (p *sentinelDropProxy) serve(t *testing.T, client net.Conn) {
	var dialer net.Dialer
	server, err := dialer.DialContext(t.Context(), "tcp", p.upstream)
	if err != nil {
		t.Logf("proxy could not reach the server at %s, closing the client connection: %v", p.upstream, err)
		utils.CloseAndLog(client)
		return
	}
	p.mu.Lock()
	p.conns = append(p.conns, client, server)
	p.mu.Unlock()

	var closeOnce sync.Once
	closeBoth := func() {
		closeOnce.Do(func() {
			// The peer may have closed either side already.
			_ = client.Close()
			_ = server.Close()
		})
	}
	sever := func() {
		p.severed.Store(true)
		closeBoth()
	}
	// Set before the drop is forwarded, so it is visible by the time the
	// server's reply to the drop can arrive.
	var swallowReply atomic.Bool

	var wg sync.WaitGroup
	wg.Go(func() {
		defer closeBoth()
		for {
			packet, err := readMySQLPacket(client)
			if err != nil {
				return
			}
			if isSentinelDrop(packet) {
				p.sawOnce.Do(func() { close(p.sawDrop) })
				switch p.fault {
				case sentinelDropFaultBeforeDrop:
					sever()
					return
				case sentinelDropFaultAfterDrop:
					swallowReply.Store(true)
				case sentinelDropFaultNone:
				}
			}
			if _, err := server.Write(packet); err != nil {
				return
			}
		}
	})
	wg.Go(func() {
		defer closeBoth()
		for {
			packet, err := readMySQLPacket(server)
			if err != nil {
				return
			}
			if swallowReply.Load() {
				sever()
				return
			}
			if _, err := client.Write(packet); err != nil {
				return
			}
		}
	})
	wg.Wait()
}

// readMySQLPacket reads one MySQL protocol packet, header included.
func readMySQLPacket(r io.Reader) ([]byte, error) {
	header := make([]byte, 4)
	if _, err := io.ReadFull(r, header); err != nil {
		return nil, err
	}
	length := int(header[0]) | int(header[1])<<8 | int(header[2])<<16
	packet := make([]byte, 4+length)
	copy(packet, header)
	if _, err := io.ReadFull(r, packet[4:]); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, io.ErrUnexpectedEOF
		}
		return nil, err
	}
	return packet, nil
}

// isSentinelDrop reports whether packet is the text query that drops Spirit's
// cutover sentinel.
func isSentinelDrop(packet []byte) bool {
	if len(packet) < 5 || packet[4] != comQuery {
		return false
	}
	query := packet[5:]
	return bytes.Contains(query, []byte("DROP TABLE")) && bytes.Contains(query, []byte(deferredCutoverSentinelTable))
}
