// Package localscale implements a fake PlanetScale HTTP API server backed by
// managed vtcombo clusters (vttest.LocalCluster). It enables integration testing
// of the PlanetScale engine against real Vitess online DDL, using the real
// PlanetScale Go SDK client.
//
// Architecture:
//
//	PlanetScale Engine (real code)
//	  → PSClient / psClientWrapper (real SDK)
//	      → HTTP requests to PlanetScale API
//	          → LocalScale HTTP Server (this package)
//	              → vtctldclient gRPC (for DDL operations)
//	              → vtgate MySQL (for schema queries)
//	                  → vtcombo (real Vitess, in-process)
package localscale

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/utils"
	_ "github.com/go-sql-driver/mysql"

	"github.com/block/schemabot/pkg/ddl"
	localscaleschema "github.com/block/schemabot/pkg/localscale/schema"
	"github.com/block/schemabot/pkg/state"

	"google.golang.org/protobuf/encoding/protojson"

	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	_ "vitess.io/vitess/go/vt/vtctl/grpcvtctldclient"
	"vitess.io/vitess/go/vt/vtctl/vtctldclient"
)

// vschemaMarshaler serializes VSchema protobuf messages using snake_case field
// names (e.g. "column_vindexes") to match the canonical Vitess JSON format.
var vschemaMarshaler = protojson.MarshalOptions{UseProtoNames: true}

// backendKey identifies a (org, database) pair for backend resolution.
type backendKey struct{ org, database string }

// databaseBackend holds connections to a single vtcombo instance for one database.
type databaseBackend struct {
	vtctld           vtctldclient.VtctldClient
	vtgateMySQLAddr  string
	vtgateDBs        map[string]*sql.DB // keyspace -> vtgate DB connection
	unscopedVtgateDB *sql.DB            // vtgate connection without a default keyspace (for shard-targeted queries)
	mysqlDSNBase     string             // DSN prefix for branch database connections (no database name)
	mysqlTCPAddr     string             // MySQL TCP address (host:port) for branch proxy upstream (managed mode only)
	managed          *managedCluster    // non-nil if this backend was started by LocalScale
	shardCounts      map[string]int     // keyspace → number of shards (from config)
	requireApproval  bool               // reject deploys unless approved (per-database setting)
}

const (
	// defaultRevertWindowDuration is how long the revert window stays open after a deploy completes.
	defaultRevertWindowDuration = 30 * time.Minute

	// defaultProcessorTickInterval is how often the background state processor polls for active deploy requests.
	defaultProcessorTickInterval = 500 * time.Millisecond
)

// Server is a fake PlanetScale HTTP API server backed by one or more vtcombo instances.
type Server struct {
	backends             map[backendKey]*databaseBackend // (org, database) -> backend
	metadataDB           *sql.DB
	httpServer           *httptest.Server // used in test mode (ListenAddr == "")
	standaloneServer     *http.Server     // used in standalone mode (ListenAddr != "")
	baseURL              string
	logger               *slog.Logger
	processorCancel      context.CancelFunc
	processorDone        chan struct{}
	wg                   sync.WaitGroup // tracks background goroutines
	revertWindowDuration time.Duration  // how long the revert window stays open after deploy completes
	defaultThrottleRatio float64        // default throttle ratio applied to new deploys (0.0 = no throttle, max 0.95)

	// proxies maps branch name → TCP proxy. Each proxy gives a branch its own
	// network endpoint by rewriting MySQL database names in the handshake.
	proxies            map[string]*branchProxy
	proxyMu            sync.Mutex
	proxyHost          string         // bind host for proxy listeners
	proxyAdvertiseHost string         // host in password responses (may differ from bind host)
	portAlloc          *portAllocator // nil when using OS-assigned ports
	proxyPortMap       map[int]int    // internal container port → external host port; populated by the testcontainer's POST /admin/proxy-port-map endpoint after container startup

	// edgeAddrs maps "org/database" → edge proxy listen address for vtgate MySQL.
	// In production PlanetScale, this is the "Edge" — the public MySQL endpoint
	// that routes queries to the correct Vitess cluster.
	edgeAddrs map[string]string

	// tlsBundle holds the self-signed CA and server certs for branch proxy TLS.
	// When set, branch proxies require TLS connections, matching PlanetScale.
	tlsBundle *TLSBundle

	// shutdownCtx is cancelled when Close() is called, allowing background
	// goroutines with artificial delays to exit promptly during shutdown.
	shutdownCtx    context.Context
	shutdownCancel context.CancelFunc

	// Artificial delays for realistic demo rendering.
	branchCreationDelay   time.Duration
	deployRequestDelay    time.Duration
	processorTickInterval time.Duration
}

// OrgConfig holds the databases for a single organization.
type OrgConfig struct {
	Databases map[string]DatabaseConfig // database name -> backend config
}

// DatabaseConfig holds connection parameters for a single database (one vtcombo instance).
type DatabaseConfig struct {
	Keyspaces       []KeyspaceConfig // keyspaces in this database
	RequireApproval bool             // require approval before deploying (rejects deploys with "must be approved")
}

// KeyspaceConfig describes a keyspace and its sharding.
type KeyspaceConfig struct {
	Name   string // keyspace name
	Shards int    // number of shards (defaults to 1)
}

// Config holds the connection parameters for the LocalScale server.
type Config struct {
	Orgs                 map[string]OrgConfig // org name -> databases
	ListenAddr           string               // When set, listen on this address instead of using httptest (e.g., ":8080")
	RevertWindowDuration time.Duration        // How long to keep the revert window open after deploy completes (default 30m)
	DefaultThrottleRatio float64              // Default throttle ratio for new deploys (0.0 = no throttle, max 0.95; default 0)
	// Branch proxy settings. In standalone mode (ListenAddr set), these auto-default
	// to 0.0.0.0:19100-19199. In test mode (ListenAddr empty), 127.0.0.1:0 (OS-assigned).
	// The advertise host in password responses is derived from the request Host header,
	// so Docker compose users don't need any proxy config.
	ProxyHost          string // Bind host for branch proxies (default: "0.0.0.0" standalone, "127.0.0.1" test)
	ProxyAdvertiseHost string // Override host in password responses (default: derived from request Host header)
	ProxyPortRange     [2]int // [start, end] port range (default: [19100, 19199] standalone, [0,0] = OS-assigned test)
	// BranchTLSMode controls TLS on branch proxy endpoints, matching PlanetScale behavior.
	//   - "none" (default): plain TCP
	//   - "tls": server-side TLS with self-signed certs (matches vanilla PlanetScale)
	//   - "mtls": mutual TLS — server cert + client cert required
	// The CA cert path is available via TLSCACertPath() for client config.
	// Client cert/key paths are available via TLSClientCertPath()/TLSClientKeyPath() for mTLS.
	BranchTLSMode string
	// Artificial delays for realistic demo rendering. Zero means no delay.
	BranchCreationDelay   time.Duration // Delay before branch snapshot starts
	DeployRequestDelay    time.Duration // Delay before deploy request diff computation starts
	ProcessorTickInterval time.Duration // How often the state processor polls (default 500ms)
	Logger                *slog.Logger
}

// New creates a new LocalScale server and starts it on a random port.
func New(ctx context.Context, cfg Config) (*Server, error) {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	// Start managed clusters for all databases in parallel.
	managedClusters, err := startManagedClusters(ctx, cfg.Orgs, cfg.Logger)
	if err != nil {
		return nil, fmt.Errorf("start managed clusters: %w", err)
	}

	// Cleanup on failure: close backends, metadata DB, and managed clusters.
	// Set success=true at the end to skip cleanup on the happy path.
	backends := make(map[backendKey]*databaseBackend)
	var metadataDB *sql.DB
	success := false
	defer func() {
		if !success {
			if metadataDB != nil {
				utils.CloseAndLog(metadataDB)
			}
			closeBackends(backends)
			teardownManagedClusters(managedClusters, cfg.Logger)
		}
	}()

	// Connect to each (org, database) backend using managed cluster addresses.
	cfg.Logger.Info("connecting to managed backends", "count", len(managedClusters))
	for orgName, orgCfg := range cfg.Orgs {
		for dbName, dbCfg := range orgCfg.Databases {
			key := backendKey{orgName, dbName}
			mc := managedClusters[key]
			if mc == nil {
				return nil, fmt.Errorf("no managed cluster for %s/%s", orgName, dbName)
			}

			vtctld, err := vtctldclient.New(ctx, "grpc", mc.grpcAddr)
			if err != nil {
				return nil, fmt.Errorf("connect to vtctld for %s/%s: %w", orgName, dbName, err)
			}

			// Build shard count map from config.
			shardCounts := make(map[string]int, len(dbCfg.Keyspaces))
			for _, ks := range dbCfg.Keyspaces {
				shards := ks.Shards
				if shards == 0 {
					shards = 1
				}
				shardCounts[ks.Name] = shards
			}

			vtgateDBs := make(map[string]*sql.DB)
			for _, ks := range dbCfg.Keyspaces {
				dsn := fmt.Sprintf("root@tcp(%s)/%s", mc.vtgateMySQLAddr, ks.Name)
				db, err := sql.Open("mysql", dsn)
				if err != nil {
					closeDatabaseBackend(vtctld, vtgateDBs)
					return nil, fmt.Errorf("connect to vtgate keyspace %s (%s/%s): %w", ks.Name, orgName, dbName, err)
				}
				if err := db.PingContext(ctx); err != nil {
					utils.CloseAndLog(db)
					closeDatabaseBackend(vtctld, vtgateDBs)
					return nil, fmt.Errorf("ping vtgate keyspace %s (%s/%s): %w", ks.Name, orgName, dbName, err)
				}
				vtgateDBs[ks.Name] = db
			}

			// Create unscoped vtgate DB pool (no default keyspace) for shard-targeted connections.
			unscopedDB, err := sql.Open("mysql", fmt.Sprintf("root@tcp(%s)/", mc.vtgateMySQLAddr))
			if err != nil {
				closeDatabaseBackend(vtctld, vtgateDBs)
				return nil, fmt.Errorf("connect unscoped vtgate for %s/%s: %w", orgName, dbName, err)
			}
			if err := unscopedDB.PingContext(ctx); err != nil {
				utils.CloseAndLog(unscopedDB)
				closeDatabaseBackend(vtctld, vtgateDBs)
				return nil, fmt.Errorf("ping unscoped vtgate for %s/%s: %w", orgName, dbName, err)
			}

			backends[key] = &databaseBackend{
				vtctld:           vtctld,
				vtgateMySQLAddr:  mc.vtgateMySQLAddr,
				vtgateDBs:        vtgateDBs,
				unscopedVtgateDB: unscopedDB,
				mysqlDSNBase:     mc.mysqlDSNBase,
				mysqlTCPAddr:     mc.mysqlTCPAddr,
				managed:          mc,
				shardCounts:      shardCounts,
				requireApproval:  dbCfg.RequireApproval,
			}
		}
	}

	// Connect to metadata MySQL using the first managed cluster's mysqld (sorted
	// by key for deterministic selection across runs).
	sortedMCKeys := make([]backendKey, 0, len(managedClusters))
	for key := range managedClusters {
		sortedMCKeys = append(sortedMCKeys, key)
	}
	slices.SortFunc(sortedMCKeys, func(a, b backendKey) int {
		if a.org != b.org {
			return strings.Compare(a.org, b.org)
		}
		return strings.Compare(a.database, b.database)
	})
	var firstMC *managedCluster
	if len(sortedMCKeys) > 0 {
		firstMC = managedClusters[sortedMCKeys[0]]
	}
	if firstMC == nil {
		return nil, fmt.Errorf("at least one database must be configured")
	}
	metadataDB, _, err = createManagedMetadataDB(ctx, firstMC.mysqlDSNBase)
	if err != nil {
		return nil, fmt.Errorf("create managed metadata db: %w", err)
	}

	proxyHost := cfg.ProxyHost
	proxyPortRange := cfg.ProxyPortRange
	if cfg.ListenAddr != "" {
		// Standalone mode (Docker): default to bind-all and a fixed port range
		// so branch proxies are reachable from outside the container.
		if proxyHost == "" {
			proxyHost = "0.0.0.0"
		}
		if proxyPortRange == [2]int{} {
			proxyPortRange = [2]int{19100, 19199}
		}
	}
	if proxyHost == "" {
		proxyHost = "127.0.0.1"
	}
	proxyAdvertiseHost := cfg.ProxyAdvertiseHost // empty = derive from request Host header
	var pa *portAllocator
	if proxyPortRange[0] > 0 && proxyPortRange[1] >= proxyPortRange[0] {
		pa = newPortAllocator(proxyPortRange[0], proxyPortRange[1])
	}

	if cfg.RevertWindowDuration < 0 {
		return nil, fmt.Errorf("revert_window_duration must be non-negative, got %v", cfg.RevertWindowDuration)
	}
	revertWindow := cfg.RevertWindowDuration
	if revertWindow == 0 {
		revertWindow = defaultRevertWindowDuration
	}
	if cfg.DefaultThrottleRatio < 0 || cfg.DefaultThrottleRatio > 0.95 {
		return nil, fmt.Errorf("default_throttle_ratio must be between 0.0 and 0.95, got %f", cfg.DefaultThrottleRatio)
	}

	shutdownCtx, shutdownCancel := context.WithCancel(context.Background())
	s := &Server{
		backends:              backends,
		metadataDB:            metadataDB,
		logger:                cfg.Logger,
		revertWindowDuration:  revertWindow,
		defaultThrottleRatio:  cfg.DefaultThrottleRatio,
		branchCreationDelay:   cfg.BranchCreationDelay,
		deployRequestDelay:    cfg.DeployRequestDelay,
		processorTickInterval: cfg.ProcessorTickInterval,
		proxies:               make(map[string]*branchProxy),
		proxyHost:             proxyHost,
		proxyAdvertiseHost:    proxyAdvertiseHost,
		portAlloc:             pa,
		shutdownCtx:           shutdownCtx,
		shutdownCancel:        shutdownCancel,
	}

	// Generate TLS certificates for branch proxies when TLS is enabled.
	tlsMode := cfg.BranchTLSMode
	if tlsMode == "" {
		tlsMode = "none"
	}
	if tlsMode != "none" {
		tlsDir, err := os.MkdirTemp("", "localscale-tls-*")
		if err != nil {
			s.Close()
			return nil, fmt.Errorf("create TLS temp dir: %w", err)
		}
		bundle, err := generateTLSBundle(tlsDir, tlsMode == "mtls")
		if err != nil {
			s.Close()
			return nil, fmt.Errorf("generate TLS bundle: %w", err)
		}
		s.tlsBundle = bundle
		cfg.Logger.Info("generated TLS certificates for branch proxies", "mode", tlsMode, "ca", bundle.CAPath)
	}

	// Create metadata tables
	cfg.Logger.Info("initializing metadata schema")
	if err := s.initMetadata(ctx); err != nil {
		s.Close()
		return nil, fmt.Errorf("init metadata: %w", err)
	}

	// Create edge proxies for each org/database. In production PlanetScale, the
	// "Edge" is the public MySQL endpoint that routes queries to the correct Vitess
	// cluster. Here we create a TCP proxy for each backend that forwards to vtgate
	// MySQL, giving external consumers (SchemaBot engine, mysql CLI) a fixed address.
	// Sorted by key for deterministic port assignment.
	s.edgeAddrs = make(map[string]string, len(backends))
	sortedKeys := make([]backendKey, 0, len(backends))
	for key := range backends {
		sortedKeys = append(sortedKeys, key)
	}
	slices.SortFunc(sortedKeys, func(a, b backendKey) int {
		if a.org != b.org {
			return strings.Compare(a.org, b.org)
		}
		return strings.Compare(a.database, b.database)
	})
	for _, key := range sortedKeys {
		backend := backends[key]
		listenAddr, err := s.proxyListenAddr()
		if err != nil {
			s.Close()
			return nil, fmt.Errorf("allocate edge port for %s/%s: %w", key.org, key.database, err)
		}
		// Empty branch name + nil keyspaces = pure passthrough (no DB name rewriting).
		edgeDSN := fmt.Sprintf("root@tcp(%s)/", backend.vtgateMySQLAddr)
		proxy, err := newBranchProxy(ctx, listenAddr, edgeDSN, "", nil, cfg.Logger, nil)
		if err != nil {
			s.Close()
			return nil, fmt.Errorf("start edge proxy for %s/%s: %w", key.org, key.database, err)
		}
		edgeName := fmt.Sprintf("edge:%s/%s", key.org, key.database)
		s.trackProxy(edgeName, proxy)
		addrKey := key.org + "/" + key.database
		s.edgeAddrs[addrKey] = proxy.Addr()
		s.logger.Info("edge proxy ready",
			"org", key.org,
			"database", key.database,
			"listen_addr", proxy.Addr(),
		)
	}

	// Start background state machine processor
	processorCtx, processorCancel := context.WithCancel(context.Background())
	s.processorCancel = processorCancel
	s.processorDone = make(chan struct{})
	go s.runStateProcessor(processorCtx)

	// Start HTTP server
	mux := http.NewServeMux()
	s.registerRoutes(mux)

	if cfg.ListenAddr != "" {
		// Standalone mode: listen on a real address
		ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", cfg.ListenAddr)
		if err != nil {
			s.Close()
			return nil, fmt.Errorf("listen on %s: %w", cfg.ListenAddr, err)
		}
		s.standaloneServer = &http.Server{Handler: mux}
		// Normalize the base URL to use localhost instead of [::] (IPv6 unspecified).
		addr := ln.Addr().String()
		if host, port, err := net.SplitHostPort(addr); err == nil && (host == "::" || host == "0.0.0.0" || host == "") {
			addr = "localhost:" + port
		}
		s.baseURL = fmt.Sprintf("http://%s", addr)
		go func() { _ = s.standaloneServer.Serve(ln) }()
	} else {
		// Test mode: use httptest for random port
		s.httpServer = httptest.NewServer(mux)
		s.baseURL = s.httpServer.URL
	}

	// Wait for Vitess's online DDL executor to be ready in each keyspace.
	// The executor requires per-shard sidecar databases to be initialized before
	// it can process migrations. By waiting here, /health only returns 200 after
	// DDL submissions will succeed.
	if err := s.waitForOnlineDDLReady(ctx); err != nil {
		s.Close()
		return nil, fmt.Errorf("wait for online DDL readiness: %w", err)
	}

	cfg.Logger.Info("localscale server started", "url", s.baseURL)
	success = true
	return s, nil
}

// URL returns the base URL of the fake API server for use with ps.WithBaseURL().
// TLSCACertPath returns the path to the CA certificate PEM file for branch
// proxy TLS verification. Clients use this to verify the server certificate.
func (s *Server) TLSCACertPath() string {
	if s.tlsBundle == nil {
		return ""
	}
	return s.tlsBundle.CAPath
}

// TLSClientCertPath returns the path to the client certificate PEM file.
// Only populated in mTLS mode.
func (s *Server) TLSClientCertPath() string {
	if s.tlsBundle == nil {
		return ""
	}
	return s.tlsBundle.ClientCertPath
}

// TLSClientKeyPath returns the path to the client private key PEM file.
// Only populated in mTLS mode.
func (s *Server) TLSClientKeyPath() string {
	if s.tlsBundle == nil {
		return ""
	}
	return s.tlsBundle.ClientKeyPath
}

func (s *Server) URL() string {
	return s.baseURL
}

// backendFor resolves the backend for a given (org, database) pair.
func (s *Server) backendFor(org, database string) (*databaseBackend, error) {
	if b, ok := s.backends[backendKey{org, database}]; ok {
		return b, nil
	}
	return nil, fmt.Errorf("unknown database: %s/%s", org, database)
}

// applyVSchemaInternal applies a VSchema using a resolved backend.
func (s *Server) applyVSchemaInternal(ctx context.Context, backend *databaseBackend, keyspace string, vschemaJSON []byte) error {
	var ks vschemapb.Keyspace
	if err := protojson.Unmarshal(vschemaJSON, &ks); err != nil {
		return fmt.Errorf("parse vschema JSON for %s: %w", keyspace, err)
	}

	resp, err := backend.vtctld.ApplyVSchema(ctx, &vtctldatapb.ApplyVSchemaRequest{
		Keyspace: keyspace,
		VSchema:  &ks,
	})
	if err != nil {
		return fmt.Errorf("apply vschema to %s: %w", keyspace, err)
	}

	s.logger.Info("applied vschema", "keyspace", keyspace, "sharded", resp.VSchema.GetSharded())
	return nil
}

// ResetState cancels all running Vitess migrations, waits for them to reach
// terminal state, and truncates metadata tables. This is used by tests to clean
// up stale state from previous test runs (since vtcombo persists data).
func (s *Server) ResetState(ctx context.Context) error {
	// Cancel all running Vitess migrations across all backends.
	// Uses shard-targeted connections because ALTER VITESS_MIGRATION CANCEL ALL
	// fails on keyspace-scoped connections for multi-shard keyspaces.
	for _, backend := range s.backends {
		for keyspace := range backend.vtgateDBs {
			if err := s.forEachShard(ctx, backend, keyspace, func(conn *sql.Conn) error {
				if _, err := conn.ExecContext(ctx, "ALTER VITESS_MIGRATION CANCEL ALL"); err != nil {
					return fmt.Errorf("cancel all vitess migrations: %w", err)
				}
				return nil
			}); err != nil {
				s.logger.Warn("cancel all migrations failed", "keyspace", keyspace, "error", err)
			}
		}
	}
	// Wait for all migrations to reach terminal state so table locks are released.
	// Uses shard-targeted connections for the same reason as above.
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	timer := time.NewTimer(30 * time.Second)
	defer timer.Stop()
	for {
		allTerminal := true
		for _, backend := range s.backends {
			for keyspace := range backend.vtgateDBs {
				_ = s.forEachShard(ctx, backend, keyspace, func(conn *sql.Conn) error {
					rows, err := conn.QueryContext(ctx, "SHOW VITESS_MIGRATIONS")
					if err != nil {
						s.logger.Warn("show migrations failed", "keyspace", keyspace, "error", err)
						return nil // non-fatal, continue checking other shards
					}
					rowMaps, err := scanDynamicRows(rows)
					utils.CloseAndLog(rows)
					if err != nil {
						s.logger.Warn("scan migrations failed", "keyspace", keyspace, "error", err)
						return nil
					}
					for _, colMap := range rowMaps {
						status := colMap["migration_status"]
						switch status {
						case state.Vitess.Complete, state.Vitess.Failed, state.Vitess.Cancelled:
							// terminal
						default:
							allTerminal = false
						}
					}
					return nil
				})
			}
		}
		if allTerminal {
			break
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("waiting for migrations to cancel: %w", ctx.Err())
		case <-timer.C:
			return fmt.Errorf("timed out waiting for migrations to reach terminal state")
		case <-ticker.C:
		}
	}
	// Shut down all branch proxies.
	s.closeAllProxies()

	// Drop all branch databases (branch_*) from the metadata MySQL.
	// Branch databases persist across container reuse and contain stale schema
	// from previous test runs (e.g., columns added by ALTER TABLE tests).
	rows, err := s.metadataDB.QueryContext(ctx,
		"SELECT SCHEMA_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME LIKE 'branch\\_%'")
	if err == nil {
		var dbNames []string
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err == nil {
				dbNames = append(dbNames, name)
			}
		}
		utils.CloseAndLog(rows)
		for _, name := range dbNames {
			if _, err := s.metadataDB.ExecContext(ctx, "DROP DATABASE IF EXISTS "+quoteIdentifier(name)); err != nil {
				s.logger.Warn("drop branch database failed", "database", name, "error", err)
			}
		}
	}

	// Truncate tables to reset state and auto-increment numbering.
	for _, table := range []string{"localscale_deploy_requests", "localscale_branches"} {
		if _, err := s.metadataDB.ExecContext(ctx, "TRUNCATE TABLE "+table); err != nil {
			return fmt.Errorf("truncate %s: %w", table, err)
		}
	}
	// Re-insert default main branch.
	if _, err := s.metadataDB.ExecContext(ctx,
		"INSERT IGNORE INTO localscale_branches (org, database_name, name, parent_branch, ready) VALUES ('', '', 'main', '', TRUE)"); err != nil {
		return fmt.Errorf("insert default branch: %w", err)
	}
	return nil
}

// proxyListenAddr returns the next bind address for a branch proxy.
// When a port range is configured, it allocates from the range.
// Otherwise, it returns host:0 for OS-assigned random ports.
func (s *Server) proxyListenAddr() (string, error) {
	if s.portAlloc != nil {
		port, err := s.portAlloc.acquire()
		if err != nil {
			return "", fmt.Errorf("acquire proxy port: %w", err)
		}
		return net.JoinHostPort(s.proxyHost, strconv.Itoa(port)), nil
	}
	return net.JoinHostPort(s.proxyHost, "0"), nil
}

// proxyAdvertiseAddr returns the address clients should use to connect to a proxy.
// When proxyAdvertiseHost is set explicitly, it uses that. Otherwise, it derives
// the host from the HTTP request's Host header — so if a client reaches the API at
// "http://localscale:8080", the proxy returns "localscale:19100". This means Docker
// compose users don't need any proxy config; it just works.
//
// When a proxy port map is configured (via POST /admin/proxy-port-map), internal
// container ports are translated to external host ports. This supports testcontainers
// where dynamic port mapping means internal port 19100 might map to host port 54321.
func (s *Server) proxyAdvertiseAddr(proxy *branchProxy, r *http.Request) string {
	addr := proxy.Addr()
	_, proxyPort, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}

	// Translate internal → external port if a port map is configured.
	s.proxyMu.Lock()
	if s.proxyPortMap != nil {
		if internalPort, err := strconv.Atoi(proxyPort); err == nil {
			if externalPort, ok := s.proxyPortMap[internalPort]; ok {
				proxyPort = strconv.Itoa(externalPort)
			}
		}
	}
	s.proxyMu.Unlock()

	// Explicit override takes priority.
	if s.proxyAdvertiseHost != "" {
		return net.JoinHostPort(s.proxyAdvertiseHost, proxyPort)
	}

	// Derive from the request Host header (e.g., "localscale:8080" → "localscale").
	if r != nil {
		reqHost := r.Host
		if h, _, err := net.SplitHostPort(reqHost); err == nil {
			reqHost = h
		}
		if reqHost != "" {
			return net.JoinHostPort(reqHost, proxyPort)
		}
	}

	return addr
}

// releaseProxyPort returns a proxy's port to the allocator pool, if applicable.
func (s *Server) releaseProxyPort(proxy *branchProxy) {
	if s.portAlloc == nil {
		return
	}
	_, portStr, err := net.SplitHostPort(proxy.Addr())
	if err != nil {
		return
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return
	}
	s.portAlloc.release(port)
}

// trackProxy registers a TCP proxy for a branch, closing any existing one.
// The old proxy is closed in a background goroutine to avoid blocking the
// caller (e.g., the password HTTP handler) while connections drain.
func (s *Server) trackProxy(branch string, proxy *branchProxy) {
	s.proxyMu.Lock()
	old := s.proxies[branch]
	if old != nil {
		s.releaseProxyPort(old)
	}
	s.proxies[branch] = proxy
	s.proxyMu.Unlock()

	if old != nil {
		s.wg.Go(func() { utils.CloseAndLog(old) })
	}
}

// closeProxy shuts down and removes the TCP proxy for a branch.
func (s *Server) closeProxy(branch string) {
	s.proxyMu.Lock()
	defer s.proxyMu.Unlock()
	if p, ok := s.proxies[branch]; ok {
		s.releaseProxyPort(p)
		utils.CloseAndLog(p)
		delete(s.proxies, branch)
	}
}

// closeAllProxies shuts down all branch TCP proxies.
func (s *Server) closeAllProxies() {
	s.proxyMu.Lock()
	defer s.proxyMu.Unlock()
	for name, p := range s.proxies {
		s.releaseProxyPort(p)
		utils.CloseAndLog(p)
		delete(s.proxies, name)
	}
}

// Close shuts down the HTTP server and all connections.
func (s *Server) Close() {
	if s.processorCancel != nil {
		s.processorCancel()
		<-s.processorDone
	}
	s.shutdownCancel()
	s.wg.Wait()
	s.closeAllProxies()
	if s.standaloneServer != nil {
		utils.CloseAndLog(s.standaloneServer)
	}
	if s.httpServer != nil {
		s.httpServer.Close()
	}
	closeBackends(s.backends)
	if s.metadataDB != nil {
		utils.CloseAndLog(s.metadataDB)
	}
}

func closeBackends(backends map[backendKey]*databaseBackend) {
	for _, backend := range backends {
		closeDatabaseBackend(backend.vtctld, backend.vtgateDBs)
		if backend.unscopedVtgateDB != nil {
			utils.CloseAndLog(backend.unscopedVtgateDB)
		}
		// Managed clusters are torn down after all connections are closed.
		if backend.managed != nil {
			_ = backend.managed.cluster.TearDown()
		}
	}
}

func closeDatabaseBackend(vtctld vtctldclient.VtctldClient, vtgateDBs map[string]*sql.DB) {
	if vtctld != nil {
		utils.CloseAndLog(vtctld)
	}
	for _, db := range vtgateDBs {
		utils.CloseAndLog(db)
	}
}

// teardownManagedClusters tears down all managed clusters (used during error cleanup in New()).
func teardownManagedClusters(clusters map[backendKey]*managedCluster, logger *slog.Logger) {
	for key, mc := range clusters {
		if err := mc.cluster.TearDown(); err != nil {
			logger.Warn("teardown managed cluster", "org", key.org, "database", key.database, "error", err)
		}
	}
}

// initMetadata ensures the metadata schema is up-to-date by diffing the embedded
// SQL files against the live database and applying any needed DDL. This follows
// the same declarative pattern as SchemaBot's EnsureSchema.
func (s *Server) initMetadata(ctx context.Context) error {
	// Read desired schema from embedded SQL files.
	entries, err := localscaleschema.FS.ReadDir(".")
	if err != nil {
		return fmt.Errorf("read schema directory: %w", err)
	}
	var desired []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		content, err := localscaleschema.FS.ReadFile(entry.Name())
		if err != nil {
			return fmt.Errorf("read schema file %s: %w", entry.Name(), err)
		}
		desired = append(desired, string(content))
	}

	// Read current schema from metadata database.
	current, err := table.LoadSchemaFromDB(ctx, s.metadataDB, table.WithoutUnderscoreTables)
	if err != nil {
		return fmt.Errorf("read current schema: %w", err)
	}
	var currentStmts []string
	for _, t := range current {
		currentStmts = append(currentStmts, t.Schema)
	}

	// Diff current vs desired and apply any DDL.
	differ := ddl.NewDiffer()
	result, err := differ.DiffStatements(currentStmts, desired)
	if err != nil {
		return fmt.Errorf("diff metadata schema: %w", err)
	}
	for _, stmt := range result.Statements {
		if _, err := s.metadataDB.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("apply metadata DDL: %w\nstatement: %s", err, stmt)
		}
		s.logger.Info("applied metadata schema change", "stmt", stmt[:min(len(stmt), 80)])
	}

	// Insert default main branch (idempotent).
	if _, err := s.metadataDB.ExecContext(ctx,
		"INSERT IGNORE INTO localscale_branches (org, database_name, name, parent_branch, ready) VALUES ('', '', 'main', '', TRUE)"); err != nil {
		return fmt.Errorf("insert default branch: %w", err)
	}
	return nil
}

// handleGetEdges returns the edge proxy addresses for each org/database.
// The edge is the public MySQL endpoint (vtgate) for each database.
// Response: {"localscale-production/testapp-vitess": "0.0.0.0:19100", ...}
func (s *Server) handleGetEdges(w http.ResponseWriter, _ *http.Request) error {
	s.writeJSON(w, s.edgeAddrs)
	return nil
}

// handleProxyPortMap sets the internal → external port mapping for branch proxies.
// This is used by testcontainers where dynamic port mapping means internal container
// ports (e.g. 19100) map to different host ports (e.g. 54321). The password API uses
// this mapping to return correct access_host_url values.
func (s *Server) handleProxyPortMap(w http.ResponseWriter, r *http.Request) error {
	var portMap map[int]int
	if err := s.decodeJSON(r, &portMap); err != nil {
		return err
	}
	s.proxyMu.Lock()
	s.proxyPortMap = portMap
	s.proxyMu.Unlock()
	s.logger.Info("proxy port map configured", "mappings", len(portMap))
	s.writeJSON(w, map[string]any{"ok": true, "mappings": len(portMap)})
	return nil
}

// handleSeedDDL executes DDL statements directly against vtgate using
// SET @@ddl_strategy (default 'direct'). This bypasses branches and deploys,
// providing a fast way to set up initial schema for tests.
//
// Optional fields:
//   - strategy: DDL strategy (default "direct"). Use "vitess --prefer-instant-ddl ..."
//     for online DDL warmup where SET @@ddl_strategy must be on the same connection.
//   - migration_context: migration context for online DDL tracking.
func (s *Server) handleSeedDDL(w http.ResponseWriter, r *http.Request) error {
	var body struct {
		Org              string   `json:"org"`
		Database         string   `json:"database"`
		Keyspace         string   `json:"keyspace"`
		Statements       []string `json:"statements"`
		Strategy         string   `json:"strategy"`
		MigrationContext string   `json:"migration_context"`
	}
	if err := s.decodeJSON(r, &body); err != nil {
		return err
	}

	backend, err := s.backendFor(body.Org, body.Database)
	if err != nil {
		return newHTTPError(http.StatusNotFound, "%v", err)
	}
	db, ok := backend.vtgateDBs[body.Keyspace]
	if !ok {
		return newHTTPError(http.StatusNotFound, "keyspace not found: %s", body.Keyspace)
	}

	strategy := body.Strategy
	if strategy == "" {
		strategy = "direct"
	}
	if err := validateSessionString(strategy); err != nil {
		return newHTTPError(http.StatusBadRequest, "invalid strategy: %v", err)
	}

	conn, err := db.Conn(r.Context())
	if err != nil {
		return newHTTPError(http.StatusInternalServerError, "get connection: %v", err)
	}
	defer utils.CloseAndLog(conn)

	// SET @@variable doesn't support prepared statement placeholders (?).
	// Input is validated by validateSessionString above (rejects quotes/backslash).
	if _, err := conn.ExecContext(r.Context(), fmt.Sprintf("SET @@ddl_strategy='%s'", strategy)); err != nil {
		return newHTTPError(http.StatusInternalServerError, "set ddl_strategy: %v", err)
	}

	if body.MigrationContext != "" {
		if err := validateSessionString(body.MigrationContext); err != nil {
			return newHTTPError(http.StatusBadRequest, "invalid migration_context: %v", err)
		}
		if _, err := conn.ExecContext(r.Context(), fmt.Sprintf("SET @@migration_context='%s'", body.MigrationContext)); err != nil {
			return newHTTPError(http.StatusInternalServerError, "set migration_context: %v", err)
		}
	}

	for _, stmt := range body.Statements {
		if err := sanitizeDDL(stmt); err != nil {
			return newHTTPError(http.StatusBadRequest, "invalid DDL: %v", err)
		}
		if _, err := conn.ExecContext(r.Context(), stmt); err != nil {
			return newHTTPError(http.StatusInternalServerError, "execute DDL: %v\nstatement: %s", err, stmt)
		}
	}

	s.writeJSON(w, map[string]any{"ok": true, "executed": len(body.Statements)})
	return nil
}

// handleSeedVSchema applies a VSchema (as JSON) to a keyspace via vtctldclient gRPC.
func (s *Server) handleSeedVSchema(w http.ResponseWriter, r *http.Request) error {
	var body struct {
		Org      string          `json:"org"`
		Database string          `json:"database"`
		Keyspace string          `json:"keyspace"`
		VSchema  json.RawMessage `json:"vschema"`
	}
	if err := s.decodeJSON(r, &body); err != nil {
		return err
	}

	backend, err := s.backendFor(body.Org, body.Database)
	if err != nil {
		return newHTTPError(http.StatusNotFound, "%v", err)
	}

	if err := s.applyVSchemaInternal(r.Context(), backend, body.Keyspace, body.VSchema); err != nil {
		return newHTTPError(http.StatusInternalServerError, "apply vschema: %v", err)
	}
	s.writeJSON(w, map[string]any{"ok": true})
	return nil
}

// handleVtgateExec executes a SQL query against vtgate for a given keyspace.
func (s *Server) handleVtgateExec(w http.ResponseWriter, r *http.Request) error {
	var body struct {
		Org      string `json:"org"`
		Database string `json:"database"`
		Keyspace string `json:"keyspace"`
		Query    string `json:"query"`
		Args     []any  `json:"args"`
	}
	if err := s.decodeJSON(r, &body); err != nil {
		return err
	}

	backend, err := s.backendFor(body.Org, body.Database)
	if err != nil {
		return newHTTPError(http.StatusNotFound, "%v", err)
	}
	db, ok := backend.vtgateDBs[body.Keyspace]
	if !ok {
		return newHTTPError(http.StatusNotFound, "keyspace not found: %s", body.Keyspace)
	}

	result, err := executeQuery(r.Context(), db, body.Query, body.Args...)
	if err != nil {
		return newHTTPError(http.StatusInternalServerError, "execute query: %v", err)
	}
	s.writeJSON(w, result)
	return nil
}

// handleMetadataQuery executes a SQL query against the metadata database.
func (s *Server) handleMetadataQuery(w http.ResponseWriter, r *http.Request) error {
	var body struct {
		Query string `json:"query"`
		Args  []any  `json:"args"`
	}
	if err := s.decodeJSON(r, &body); err != nil {
		return err
	}

	result, err := executeQuery(r.Context(), s.metadataDB, body.Query, body.Args...)
	if err != nil {
		return newHTTPError(http.StatusInternalServerError, "execute query: %v", err)
	}
	s.writeJSON(w, result)
	return nil
}

// handleResetState resets all LocalScale state (cancel migrations, truncate metadata).
func (s *Server) handleResetState(w http.ResponseWriter, r *http.Request) error {
	if err := s.ResetState(r.Context()); err != nil {
		return newHTTPError(http.StatusInternalServerError, "reset state: %v", err)
	}
	s.writeJSON(w, map[string]any{"ok": true})
	return nil
}

// handleBranchDBQuery executes a SQL query against a branch database.
func (s *Server) handleBranchDBQuery(w http.ResponseWriter, r *http.Request) error {
	var body struct {
		Branch   string `json:"branch"`
		Keyspace string `json:"keyspace"`
		Query    string `json:"query"`
	}
	if err := s.decodeJSON(r, &body); err != nil {
		return err
	}

	db, err := s.openBranchDB(r.Context(), body.Branch, body.Keyspace)
	if err != nil {
		return newHTTPError(http.StatusNotFound, "open branch db: %v", err)
	}
	defer utils.CloseAndLog(db)

	result, err := executeQuery(r.Context(), db, body.Query)
	if err != nil {
		return newHTTPError(http.StatusInternalServerError, "execute query: %v", err)
	}
	s.writeJSON(w, result)
	return nil
}

// handleGetTLSCerts returns the TLS certificate paths and contents for branch
// proxy connections. Used by integration tests to configure RegisterMTLS.
func (s *Server) handleGetTLSCerts(w http.ResponseWriter, _ *http.Request) error {
	if s.tlsBundle == nil {
		return newHTTPError(http.StatusNotFound, "TLS not enabled (BranchTLSMode is none)")
	}
	resp := map[string]string{
		"ca_cert_path": s.tlsBundle.CAPath,
	}
	if s.tlsBundle.ClientCertPath != "" {
		resp["client_cert_path"] = s.tlsBundle.ClientCertPath
		resp["client_key_path"] = s.tlsBundle.ClientKeyPath
	}
	// Include cert contents so container clients don't need volume mounts.
	if ca, err := os.ReadFile(s.tlsBundle.CAPath); err == nil {
		resp["ca_cert"] = string(ca)
	}
	if s.tlsBundle.ClientCertPath != "" {
		if cert, err := os.ReadFile(s.tlsBundle.ClientCertPath); err == nil {
			resp["client_cert"] = string(cert)
		}
		if key, err := os.ReadFile(s.tlsBundle.ClientKeyPath); err == nil {
			resp["client_key"] = string(key)
		}
	}
	s.writeJSON(w, resp)
	return nil
}

// handleError wraps an error-returning handler so it can be used with http.ServeMux.
// If the handler returns an *httpError, the response uses its status code; otherwise 500.
func (s *Server) handleError(fn func(http.ResponseWriter, *http.Request) error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := fn(w, r); err != nil {
			s.writeHTTPError(w, err)
		}
	}
}

func (s *Server) registerRoutes(mux *http.ServeMux) {
	// Health endpoint
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	// Admin endpoints
	mux.HandleFunc("GET /admin/edges", s.handleError(s.handleGetEdges))
	mux.HandleFunc("POST /admin/proxy-port-map", s.handleError(s.handleProxyPortMap))
	mux.HandleFunc("POST /admin/seed-ddl", s.handleError(s.handleSeedDDL))
	mux.HandleFunc("POST /admin/seed-vschema", s.handleError(s.handleSeedVSchema))
	mux.HandleFunc("POST /admin/vtgate-exec", s.handleError(s.handleVtgateExec))
	mux.HandleFunc("POST /admin/metadata-query", s.handleError(s.handleMetadataQuery))
	mux.HandleFunc("POST /admin/reset-state", s.handleError(s.handleResetState))
	mux.HandleFunc("POST /admin/branch-db-query", s.handleError(s.handleBranchDBQuery))
	mux.HandleFunc("GET /admin/tls-certs", s.handleError(s.handleGetTLSCerts))

	// Keyspace and schema endpoints
	mux.HandleFunc("GET /v1/organizations/{org}/databases/{db}/branches/{branch}/keyspaces", s.handleError(s.handleListKeyspaces))
	mux.HandleFunc("GET /v1/organizations/{org}/databases/{db}/branches/{branch}/schema", s.handleError(s.handleGetBranchSchema))
	mux.HandleFunc("GET /v1/organizations/{org}/databases/{db}/branches/{branch}/vschema", s.handleError(s.handleGetBranchVSchema))
	mux.HandleFunc("GET /v1/organizations/{org}/databases/{db}/branches/{branch}/keyspaces/{keyspace}/vschema", s.handleError(s.handleGetKeyspaceVSchema))
	mux.HandleFunc("PATCH /v1/organizations/{org}/databases/{db}/branches/{branch}/keyspaces/{keyspace}/vschema", s.handleError(s.handleUpdateKeyspaceVSchema))

	// Branch endpoints
	mux.HandleFunc("GET /v1/organizations/{org}/databases/{db}/branches/{branch}", s.handleError(s.handleGetBranch))
	mux.HandleFunc("POST /v1/organizations/{org}/databases/{db}/branches", s.handleError(s.handleCreateBranch))
	mux.HandleFunc("POST /v1/organizations/{org}/databases/{db}/branches/{branch}/passwords", s.handleError(s.handleCreateBranchPassword))

	// Apply DDL/VSchema to a branch (used by PlanetScale engine before CreateDeployRequest)
	mux.HandleFunc("POST /v1/organizations/{org}/databases/{db}/branches/{branch}/schema", s.handleError(s.handleApplyBranchSchema))

	// Deploy request action endpoints (registered before the GET for {number})
	mux.HandleFunc("POST /v1/organizations/{org}/databases/{db}/deploy-requests/{number}/deploy", s.handleError(s.handleDeployDeployRequest))
	mux.HandleFunc("POST /v1/organizations/{org}/databases/{db}/deploy-requests/{number}/cancel", s.handleError(s.handleCancelDeployRequest))
	mux.HandleFunc("POST /v1/organizations/{org}/databases/{db}/deploy-requests/{number}/apply-deploy", s.handleError(s.handleApplyDeployRequest))
	mux.HandleFunc("POST /v1/organizations/{org}/databases/{db}/deploy-requests/{number}/revert", s.handleError(s.handleRevertDeployRequest))
	mux.HandleFunc("POST /v1/organizations/{org}/databases/{db}/deploy-requests/{number}/skip-revert", s.handleError(s.handleSkipRevertDeployRequest))
	mux.HandleFunc("PUT /v1/organizations/{org}/databases/{db}/deploy-requests/{number}/throttle", s.handleError(s.handleThrottleDeployRequest))

	// Deploy request CRUD endpoints
	mux.HandleFunc("GET /v1/organizations/{org}/databases/{db}/deploy-requests/{number}", s.handleError(s.handleGetDeployRequest))
	mux.HandleFunc("GET /v1/organizations/{org}/databases/{db}/deploy-requests", s.handleError(s.handleListDeployRequests))
	mux.HandleFunc("POST /v1/organizations/{org}/databases/{db}/deploy-requests", s.handleError(s.handleCreateDeployRequest))

	// Catch-all for unimplemented PlanetScale API endpoints.
	mux.HandleFunc("/v1/", func(w http.ResponseWriter, r *http.Request) {
		s.writeError(w, http.StatusNotImplemented, "endpoint not implemented: %s %s", r.Method, r.URL.Path)
	})
}
