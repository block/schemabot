package localscale

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"

	"github.com/block/spirit/pkg/utils"

	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/vttest"

	vttestpb "vitess.io/vitess/go/vt/proto/vttest"
)

// managedMySQLTCPUser is the MySQL user created for TCP access to the managed
// cluster's mysqld. vttest's init_db.sql only grants vt_dba@'localhost' (socket-only),
// so we create this user for branch proxy upstream connections.
const managedMySQLTCPUser = "vt_dba_tcp"

// managedCluster wraps a vttest.LocalCluster that was started by LocalScale.
// It holds the extracted addresses needed to connect to the cluster.
type managedCluster struct {
	cluster         *vttest.LocalCluster
	grpcAddr        string // vtctld gRPC address (host:port)
	vtgateMySQLAddr string // vtgate MySQL protocol address (host:port)
	mysqlDSNBase    string // direct mysqld DSN prefix for branch DBs (user@unix(socket)/)
	mysqlTCPAddr    string // direct mysqld TCP address (host:port) for branch proxy upstream
}

// ensureVTROOT sets up the VTROOT environment for vtcombo. It auto-detects the
// root directory and extracts Vitess config files from the Go module cache if needed.
//
// VTROOT must contain:
//   - config/ directory with init_db.sql and mycnf templates
//
// Detection order:
//  1. $VTROOT if already set
//  2. Directory containing vtcombo binary on $PATH
//  3. Current working directory (creates config/ if missing)
func ensureVTROOT(ctx context.Context, logger *slog.Logger) error {
	if vtroot := os.Getenv("VTROOT"); vtroot != "" {
		logger.Info("using VTROOT from environment", "vtroot", vtroot)
		return nil
	}

	// Try to find vtcombo and derive VTROOT from its location.
	if vtcomboPath, err := exec.LookPath("vtcombo"); err == nil {
		vtroot := filepath.Dir(filepath.Dir(vtcomboPath)) // bin/vtcombo → parent
		if _, err := os.Stat(filepath.Join(vtroot, "config")); err == nil {
			if err := os.Setenv("VTROOT", vtroot); err != nil {
				return fmt.Errorf("set VTROOT: %w", err)
			}
			logger.Info("auto-detected VTROOT from vtcombo path", "vtroot", vtroot)
			return nil
		}
	}

	// Fall back to current working directory.
	cwd, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("get working directory: %w", err)
	}

	// Extract config from Go module cache if config/ doesn't exist.
	configDir := filepath.Join(cwd, "config")
	if _, err := os.Stat(configDir); os.IsNotExist(err) {
		if err := extractVitessConfig(ctx, configDir, logger); err != nil {
			return fmt.Errorf("extract vitess config: %w", err)
		}
	}

	if err := os.Setenv("VTROOT", cwd); err != nil {
		return fmt.Errorf("set VTROOT: %w", err)
	}
	logger.Info("set VTROOT to working directory", "vtroot", cwd)
	return nil
}

// extractVitessConfig copies Vitess runtime config files from the Go module
// cache into the target directory. Uses `go list -m` to find the module path.
func extractVitessConfig(ctx context.Context, targetDir string, logger *slog.Logger) error {
	// Find Vitess module directory in the Go module cache.
	out, err := exec.CommandContext(ctx, "go", "list", "-m", "-f", "{{.Dir}}", "vitess.io/vitess").Output()
	if err != nil {
		return fmt.Errorf("find vitess module: %w", err)
	}
	vitessDir := filepath.Join(string(out[:len(out)-1]), "config") // trim newline

	if _, err := os.Stat(vitessDir); err != nil {
		return fmt.Errorf("vitess config not found at %s: %w", vitessDir, err)
	}

	// Copy config directory and fix permissions (module cache files are read-only).
	if err := exec.CommandContext(ctx, "cp", "-r", vitessDir, targetDir).Run(); err != nil {
		return fmt.Errorf("copy vitess config: %w", err)
	}
	_ = exec.CommandContext(ctx, "chmod", "-R", "u+w", targetDir).Run()

	// Remove Go-only files not needed at runtime.
	_ = os.Remove(filepath.Join(targetDir, "embed.go"))
	_ = os.Remove(filepath.Join(targetDir, "gomysql.pc.tmpl"))

	logger.Info("extracted vitess config from module cache", "target", targetDir)
	return nil
}

// startManagedCluster builds a VTTestTopology from the given keyspaces, starts
// a vttest.LocalCluster, and returns the extracted connection addresses.
//
// The cluster runs vtcombo in a single process with an embedded mysqld — the
// same code path as `vttestserver`. Flags are set to match the docker-compose
// vttestserver configuration:
//
//   - PerShardSidecar: each shard gets its own _vt_{ks}_{shard} sidecar DB
//   - MigrationCheckInterval: 5s (fast online DDL polling; default is 60s)
//   - EnableOnlineDDL: true
//   - ForeignKeyMode: "disallow" (matches PlanetScale behavior)
func startManagedCluster(ctx context.Context, keyspaces []KeyspaceConfig, logger *slog.Logger) (*managedCluster, error) {
	// Ensure VTROOT is set and Vitess config files exist.
	if err := ensureVTROOT(ctx, logger); err != nil {
		return nil, fmt.Errorf("ensure VTROOT: %w", err)
	}

	// Build VTTestTopology from keyspace configs.
	topo := &vttestpb.VTTestTopology{}
	for _, ks := range keyspaces {
		shards := ks.Shards
		if shards == 0 {
			shards = 1
		}
		ksTopo := &vttestpb.Keyspace{
			Name:   ks.Name,
			Shards: buildShards(shards),
		}
		topo.Keyspaces = append(topo.Keyspaces, ksTopo)
	}

	cluster := &vttest.LocalCluster{
		Config: vttest.Config{
			Topology: topo,

			// Match vttestserver docker-compose flags exactly:
			ForeignKeyMode:         "disallow",
			EnableOnlineDDL:        true,
			EnableDirectDDL:        true,
			MigrationCheckInterval: 5 * time.Second,
			PerShardSidecar:        true,

			// Bind to localhost only (not 0.0.0.0 since we're in-process).
			VtComboBindAddress: "127.0.0.1",
			MySQLBindHost:      "127.0.0.1",
		},
	}

	logger.Info("starting managed vttest cluster", "keyspaces", len(keyspaces))
	start := time.Now()

	if err := cluster.Setup(); err != nil {
		return nil, fmt.Errorf("vttest cluster setup: %w", err)
	}

	// Extract connection addresses from the running cluster.
	grpcPort := cluster.GrpcPort()
	grpcAddr := fmt.Sprintf("127.0.0.1:%d", grpcPort)

	// vtgate MySQL port = basePort + 3 (vtcombo_mysql_port).
	vtgateMySQLPort := cluster.Env.PortForProtocol("vtcombo_mysql_port", "")
	vtgateMySQLAddr := fmt.Sprintf("127.0.0.1:%d", vtgateMySQLPort)

	// Direct mysqld connection via Unix socket for internal operations (branch DB creation, metadata).
	mysqlParams := cluster.MySQLConnParams()
	mysqlDSNBase := fmt.Sprintf("%s@unix(%s)/", mysqlParams.Uname, mysqlParams.UnixSocket)

	// Also extract the TCP address for branch proxy upstream connections.
	tcpParams := cluster.MySQLTCPConnParams()
	mysqlTCPAddr := fmt.Sprintf("%s:%d", tcpParams.Host, tcpParams.Port)
	if err := createManagedTCPUser(mysqlDSNBase, logger); err != nil {
		logger.Warn("create managed TCP user", "error", err)
	}

	elapsed := time.Since(start)
	logger.Info("managed vttest cluster ready",
		"grpc_addr", grpcAddr,
		"vtgate_mysql_addr", vtgateMySQLAddr,
		"mysql_dsn_base", mysqlDSNBase,
		"startup_time", elapsed.Round(time.Millisecond),
	)

	return &managedCluster{
		cluster:         cluster,
		grpcAddr:        grpcAddr,
		vtgateMySQLAddr: vtgateMySQLAddr,
		mysqlDSNBase:    mysqlDSNBase,
		mysqlTCPAddr:    mysqlTCPAddr,
	}, nil
}

// createManagedTCPUser creates the managedMySQLTCPUser user with full privileges
// on the managed cluster's mysqld. vttest's init_db.sql only grants vt_dba@'localhost'
// (socket-only), so we need a separate user for TCP access (branch proxy upstream).
func createManagedTCPUser(mysqlDSNBase string, logger *slog.Logger) error {
	db, err := sql.Open("mysql", mysqlDSNBase)
	if err != nil {
		return fmt.Errorf("connect to mysqld: %w", err)
	}
	defer utils.CloseAndLog(db)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if _, err := db.ExecContext(ctx, fmt.Sprintf("CREATE USER IF NOT EXISTS '%s'@'%%'", managedMySQLTCPUser)); err != nil {
		return fmt.Errorf("create user: %w", err)
	}
	// Full privileges for test-only managed mysqld — not for production use.
	if _, err := db.ExecContext(ctx, fmt.Sprintf("GRANT ALL ON *.* TO '%s'@'%%'", managedMySQLTCPUser)); err != nil {
		return fmt.Errorf("grant privileges: %w", err)
	}
	logger.Info("created managed TCP user", "user", managedMySQLTCPUser)
	return nil
}

// buildShards generates shard names for a given count using Vitess's standard
// shard range generation. 1 shard → ["0"] (unsharded), 2 → ["-80", "80-"], etc.
func buildShards(count int) []*vttestpb.Shard {
	if count <= 1 {
		return []*vttestpb.Shard{{Name: "0"}}
	}
	ranges, err := key.GenerateShardRanges(count, 0)
	if err != nil {
		// Invalid count — fall back to unsharded.
		return []*vttestpb.Shard{{Name: "0"}}
	}
	shards := make([]*vttestpb.Shard, len(ranges))
	for i, r := range ranges {
		shards[i] = &vttestpb.Shard{Name: r}
	}
	return shards
}

// startManagedClusters starts all managed clusters in parallel for fast startup.
// Returns a map from backendKey to managedCluster. If any cluster fails to start,
// all successfully started clusters are torn down and the first error is returned.
func startManagedClusters(
	ctx context.Context,
	orgs map[string]OrgConfig,
	logger *slog.Logger,
) (map[backendKey]*managedCluster, error) {
	type result struct {
		key     backendKey
		cluster *managedCluster
		err     error
	}

	// Collect all databases to start managed clusters for.
	var items []struct {
		key backendKey
		cfg DatabaseConfig
	}
	for orgName, orgCfg := range orgs {
		for dbName, dbCfg := range orgCfg.Databases {
			items = append(items, struct {
				key backendKey
				cfg DatabaseConfig
			}{
				key: backendKey{orgName, dbName},
				cfg: dbCfg,
			})
		}
	}

	if len(items) == 0 {
		return nil, nil
	}

	logger.Info("starting managed clusters in parallel", "count", len(items))
	start := time.Now()

	// Start all clusters in parallel.
	results := make(chan result, len(items))
	var wg sync.WaitGroup
	for _, item := range items {
		wg.Go(func() {
			mc, err := startManagedCluster(ctx, item.cfg.Keyspaces, logger.With("org", item.key.org, "database", item.key.database))
			results <- result{key: item.key, cluster: mc, err: err}
		})
	}
	wg.Wait()
	close(results)

	// Collect results.
	clusters := make(map[backendKey]*managedCluster, len(items))
	var firstErr error
	for r := range results {
		if r.err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("start managed cluster %s/%s: %w", r.key.org, r.key.database, r.err)
			}
			continue
		}
		clusters[r.key] = r.cluster
	}

	// If any failed, tear down all successful clusters.
	if firstErr != nil {
		for _, mc := range clusters {
			if err := mc.cluster.TearDown(); err != nil {
				logger.Warn("teardown managed cluster after error", "error", err)
			}
		}
		return nil, firstErr
	}

	logger.Info("all managed clusters ready",
		"count", len(clusters),
		"total_startup_time", time.Since(start).Round(time.Millisecond),
	)
	return clusters, nil
}

// createManagedMetadataDB creates the `localscale` database on the given mysqld
// and returns a *sql.DB connected to it.
func createManagedMetadataDB(ctx context.Context, mysqlDSNBase string) (*sql.DB, string, error) {
	// Connect without database to create it.
	rootDB, err := sql.Open("mysql", mysqlDSNBase)
	if err != nil {
		return nil, "", fmt.Errorf("connect to mysqld for metadata: %w", err)
	}
	defer utils.CloseAndLog(rootDB)

	if err := rootDB.PingContext(ctx); err != nil {
		return nil, "", fmt.Errorf("ping mysqld for metadata: %w", err)
	}

	if _, err := rootDB.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS `localscale`"); err != nil {
		return nil, "", fmt.Errorf("create localscale database: %w", err)
	}

	// Connect to the localscale database.
	dsn := mysqlDSNBase + "localscale"
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, "", fmt.Errorf("connect to localscale database: %w", err)
	}
	if err := db.PingContext(ctx); err != nil {
		utils.CloseAndLog(db)
		return nil, "", fmt.Errorf("ping localscale database: %w", err)
	}

	return db, mysqlDSNBase, nil
}
