package localscale

// Branch Proxy
//
// In production PlanetScale, each branch is a fully isolated Vitess cluster with its
// own hostname. Passwords.Create() returns an access_host_url pointing to the branch's
// dedicated MySQL endpoint.
//
// LocalScale has a single mysqld shared by all branches. Branch data is stored in
// namespaced databases (branch_{name}_{keyspace}). This MySQL proxy bridges the gap:
// each branch gets its own port with a go-mysql protocol server that rewrites database
// names (e.g., "testapp" → "branch_X_testapp") and forwards queries to vtgate.
//
// Flow: branch creation snapshots schema into branch databases → password creation
// starts a proxy on a random port → engine connects via proxy address → proxy rewrites
// database via USE statement → queries hit the correct branch database.

import (
	"context"
	"crypto/tls"
	"database/sql"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"

	"github.com/block/spirit/pkg/utils"
	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/server"
)

// defaultMySQLServerOnce lazily initializes a shared go-mysql Server for
// non-TLS branch proxies. NewDefaultServer() generates an RSA CA + cert
// on every call, which is CPU-intensive and causes timeouts under load.
var (
	defaultMySQLServerOnce sync.Once
	defaultMySQLServerInst *server.Server
)

func defaultMySQLServer() *server.Server {
	defaultMySQLServerOnce.Do(func() {
		defaultMySQLServerInst = server.NewDefaultServer()
	})
	return defaultMySQLServerInst
}

// portAllocator manages a pool of ports from a fixed range.
// Used to give branch proxies predictable ports that can be exposed in Docker.
type portAllocator struct {
	mu   sync.Mutex
	free []int
}

func newPortAllocator(start, end int) *portAllocator {
	ports := make([]int, 0, end-start+1)
	for p := start; p <= end; p++ {
		ports = append(ports, p)
	}
	return &portAllocator{free: ports}
}

func (a *portAllocator) acquire() (int, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.free) == 0 {
		return 0, fmt.Errorf("proxy port range exhausted")
	}
	port := a.free[0]
	a.free = a.free[1:]
	return port, nil
}

func (a *portAllocator) release(port int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.free = append(a.free, port)
}

// branchProxy is a MySQL protocol proxy that gives a branch its own network endpoint.
// It uses go-mysql to handle the MySQL protocol and rewrites database names from
// keyspaces (e.g., "testapp") to branch databases (e.g., "branch_foo_testapp").
//
// This matches production PlanetScale where each branch has a dedicated hostname.
// Multiple branches can run concurrently on different ports.
type branchProxy struct {
	listener    net.Listener
	upstreamDSN string            // vtgate MySQL DSN base (e.g., "root@tcp(host:port)/")
	dbMap       map[string]string // keyspace name → branch database name
	mysqlServer *server.Server
	logger      *slog.Logger
	done        chan struct{}
	connWg      sync.WaitGroup // tracks in-flight connections
}

func newBranchProxy(ctx context.Context, listenAddr, upstreamDSN string, branchName string, keyspaces []string, logger *slog.Logger, tlsCfg *tls.Config) (*branchProxy, error) {
	lc := &net.ListenConfig{
		Control: setReuseAddr,
	}
	ln, err := lc.Listen(ctx, "tcp", listenAddr)
	if err != nil {
		return nil, fmt.Errorf("listen on %s: %w", listenAddr, err)
	}

	// Build the database name mapping. Empty branchName = identity mapping
	// (used for "main" branch where vtgate uses real keyspace names).
	var dbMap map[string]string
	if branchName == "" {
		dbMap = nil // nil map = no rewriting (passthrough)
	} else {
		dbMap = make(map[string]string, len(keyspaces))
		for _, ks := range keyspaces {
			dbMap[ks] = branchDBName(branchName, ks)
		}
	}

	// Configure the go-mysql protocol server. When TLS is configured, use
	// NewServer with the TLS config so the server advertises TLS capability
	// and performs STARTTLS during the MySQL handshake.
	//
	// Without TLS, use a shared default server to avoid generating RSA
	// keys on every proxy creation (NewDefaultServer generates a CA + cert
	// per call, which is CPU-intensive under load).
	var mysqlSrv *server.Server
	if tlsCfg != nil {
		mysqlSrv = server.NewServer("8.0.35-localscale", 255, "mysql_native_password", nil, tlsCfg)
	} else {
		mysqlSrv = defaultMySQLServer()
	}

	p := &branchProxy{
		listener:    ln,
		upstreamDSN: upstreamDSN,
		dbMap:       dbMap,
		mysqlServer: mysqlSrv,
		logger:      logger,
		done:        make(chan struct{}),
	}
	go p.serve()
	return p, nil
}

// Addr returns the proxy's listen address (e.g., "127.0.0.1:54321").
func (p *branchProxy) Addr() string {
	return p.listener.Addr().String()
}

// Close shuts down the proxy listener, waits for serve to exit, and drains
// in-flight connections.
func (p *branchProxy) Close() error {
	err := p.listener.Close()
	<-p.done
	p.connWg.Wait()
	return err
}

func (p *branchProxy) serve() {
	defer close(p.done)
	for {
		client, err := p.listener.Accept()
		if err != nil {
			return
		}
		p.connWg.Go(func() {
			p.handleConn(client)
		})
	}
}

func (p *branchProxy) handleConn(clientConn net.Conn) {
	// Open a dedicated upstream connection for this client.
	upstreamDB, err := sql.Open("mysql", p.upstreamDSN)
	if err != nil {
		p.logger.Error("proxy: open upstream", "error", err)
		utils.CloseAndLog(clientConn)
		return
	}
	upstreamDB.SetMaxOpenConns(1)
	defer utils.CloseAndLog(upstreamDB)

	conn, err := upstreamDB.Conn(context.Background())
	if err != nil {
		p.logger.Error("proxy: acquire upstream conn", "error", err)
		utils.CloseAndLog(clientConn)
		return
	}
	defer utils.CloseAndLog(conn)

	handler := &branchHandler{
		conn:   conn,
		dbMap:  p.dbMap,
		logger: p.logger,
	}

	// go-mysql handles the full MySQL protocol: greeting, auth, command loop.
	// Accept both root (vtgate) and vt_dba_tcp (managed mysqld) with no password,
	// matching the credentials returned by handleCreateBranchPassword.
	credProvider := server.NewInMemoryProvider()
	credProvider.AddUser("root", "")
	credProvider.AddUser(managedMySQLTCPUser, "")

	mysqlConn, err := p.mysqlServer.NewCustomizedConn(clientConn, credProvider, handler)
	if err != nil {
		p.logger.Debug("proxy: handshake failed", "error", err)
		// go-mysql already closed clientConn on handshake failure
		return
	}

	for {
		if err := mysqlConn.HandleCommand(); err != nil {
			return
		}
	}
}

// branchHandler implements go-mysql server.Handler, forwarding all queries to
// the upstream vtgate connection with database name rewriting.
type branchHandler struct {
	conn   *sql.Conn
	dbMap  map[string]string
	logger *slog.Logger
}

var _ server.Handler = (*branchHandler)(nil)

func (h *branchHandler) UseDB(db string) error {
	target := db
	if h.dbMap != nil {
		mapped, found := h.dbMap[db]
		if !found {
			return fmt.Errorf("unknown keyspace %q for this branch (available: %v)", db, mapKeys(h.dbMap))
		}
		target = mapped
	}
	_, err := h.conn.ExecContext(context.Background(), "USE "+quoteIdentifier(target))
	if err != nil {
		return fmt.Errorf("USE %s: %w", target, err)
	}
	return nil
}

func mapKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

func (h *branchHandler) HandleQuery(query string) (*gomysql.Result, error) {
	ctx := context.Background()

	// Intercept USE statements and route through UseDB for database name rewriting.
	trimmed := strings.TrimSpace(query)
	if len(trimmed) > 4 && strings.EqualFold(trimmed[:4], "USE ") {
		db := strings.TrimSpace(trimmed[4:])
		db = strings.Trim(db, "`")
		if err := h.UseDB(db); err != nil {
			return nil, err
		}
		return &gomysql.Result{}, nil
	}

	// Route SELECT/SHOW to QueryContext, everything else to ExecContext.
	if isReadQuery(query) {
		return h.handleReadQuery(ctx, query)
	}

	result, err := h.conn.ExecContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("proxy exec %s: %w", query, err)
	}
	affected, _ := result.RowsAffected()
	insertID, _ := result.LastInsertId()
	return &gomysql.Result{
		AffectedRows: uint64(affected),
		InsertId:     uint64(insertID),
	}, nil
}

func (h *branchHandler) handleReadQuery(ctx context.Context, query string) (*gomysql.Result, error) {
	rows, err := h.conn.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("proxy query %s: %w", query, err)
	}
	defer utils.CloseAndLog(rows)

	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("proxy columns %s: %w", query, err)
	}

	var resultRows [][]any
	for rows.Next() {
		values := make([]any, len(columns))
		ptrs := make([]any, len(columns))
		for i := range values {
			ptrs[i] = &values[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, fmt.Errorf("proxy scan %s: %w", query, err)
		}
		// Convert []byte to string — database/sql returns most MySQL types as
		// []byte, but go-mysql's BuildSimpleTextResultset expects string values.
		row := make([]any, len(values))
		for i, v := range values {
			if b, ok := v.([]byte); ok {
				row[i] = string(b)
			} else {
				row[i] = v
			}
		}
		resultRows = append(resultRows, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("proxy iterate %s: %w", query, err)
	}

	resultset, err := gomysql.BuildSimpleTextResultset(columns, resultRows)
	if err != nil {
		return nil, fmt.Errorf("proxy build resultset %s: %w", query, err)
	}
	return &gomysql.Result{Resultset: resultset}, nil
}

func (h *branchHandler) HandleFieldList(table string, wildcard string) ([]*gomysql.Field, error) {
	return nil, fmt.Errorf("COM_FIELD_LIST not supported")
}

func (h *branchHandler) HandleStmtPrepare(query string) (int, int, any, error) {
	return 0, 0, nil, fmt.Errorf("prepared statements not supported")
}

func (h *branchHandler) HandleStmtExecute(context any, query string, args []any) (*gomysql.Result, error) {
	return nil, fmt.Errorf("prepared statements not supported")
}

func (h *branchHandler) HandleStmtClose(context any) error {
	return nil
}

func (h *branchHandler) HandleOtherCommand(cmd byte, data []byte) error {
	switch cmd {
	case gomysql.COM_PING:
		return nil
	case gomysql.COM_INIT_DB:
		if len(data) > 0 {
			return h.UseDB(string(data))
		}
		return nil
	default:
		return fmt.Errorf("command %d not supported", cmd)
	}
}

// isReadQuery returns true if the query is a SELECT, SHOW, DESCRIBE, EXPLAIN, or WITH (CTE).
// Skips leading whitespace and SQL comments (/* ... */) to find the first keyword.
func isReadQuery(query string) bool {
	i := 0
	for i < len(query) {
		// Skip whitespace
		if query[i] == ' ' || query[i] == '\t' || query[i] == '\n' || query[i] == '\r' {
			i++
			continue
		}
		// Skip C-style comments (/* ... */)
		if i+1 < len(query) && query[i] == '/' && query[i+1] == '*' {
			end := strings.Index(query[i+2:], "*/")
			if end == -1 {
				return false
			}
			i += end + 4
			continue
		}
		break
	}
	if i >= len(query) {
		return false
	}

	rest := strings.ToUpper(query[i:])
	return strings.HasPrefix(rest, "SELECT") ||
		strings.HasPrefix(rest, "SHOW") ||
		strings.HasPrefix(rest, "DESCRIBE") ||
		strings.HasPrefix(rest, "EXPLAIN") ||
		strings.HasPrefix(rest, "WITH")
}
