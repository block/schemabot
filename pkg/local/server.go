package local

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"database/sql"

	_ "github.com/go-sql-driver/mysql"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/spirit/pkg/utils"

	"gopkg.in/yaml.v3"
)

const (
	// StorageDatabase is the database created on the local MySQL server
	// for SchemaBot's internal state (plans, applies, locks, tasks).
	StorageDatabase = "_schemabot"

	defaultServerPort = "18080"
	defaultMySQLAddr  = "127.0.0.1:3306"
)

// GetServerPort returns the local HTTP server port.
// Override with SCHEMABOT_LOCAL_PORT for testing or non-standard setups.
func GetServerPort() string {
	if p := os.Getenv("SCHEMABOT_LOCAL_PORT"); p != "" {
		return p
	}
	return defaultServerPort
}

// GetStorageDSN returns the DSN for the local storage database.
// Override with SCHEMABOT_LOCAL_STORAGE_DSN for non-standard MySQL setups
// (e.g., different port, password, or auth).
func GetStorageDSN() string {
	if dsn := os.Getenv("SCHEMABOT_LOCAL_STORAGE_DSN"); dsn != "" {
		return dsn
	}
	return "root@tcp(" + defaultMySQLAddr + ")/" + StorageDatabase + "?parseTime=true"
}

// getServerDSN returns the DSN for bootstrapping (no database selected).
func getServerDSN() string {
	if dsn := os.Getenv("SCHEMABOT_LOCAL_STORAGE_DSN"); dsn != "" {
		// Strip database name from the override DSN for bootstrap
		return stripDatabaseFromDSN(dsn)
	}
	return "root@tcp(" + defaultMySQLAddr + ")/"
}

// stripDatabaseFromDSN removes the database name from a DSN, keeping auth and params.
func stripDatabaseFromDSN(dsn string) string {
	// DSN format: user:pass@tcp(host:port)/dbname?params
	slashIdx := strings.LastIndex(dsn, "/")
	if slashIdx < 0 {
		return dsn
	}
	// Keep everything before the slash + the slash, drop dbname but keep ?params
	rest := dsn[slashIdx+1:]
	if qIdx := strings.Index(rest, "?"); qIdx >= 0 {
		return dsn[:slashIdx+1] + rest[qIdx:]
	}
	return dsn[:slashIdx+1]
}

// EnsureRunning checks if the local server is running and starts it if not.
// Returns the endpoint URL.
//
// Server lifecycle:
//   - Auto-start: first CLI command that needs an endpoint forks `schemabot serve`
//     as a background daemon (detached, PID file, log file).
//   - Reuse: subsequent commands check the health endpoint and reuse the running server.
//   - Stale recovery: if the port is bound but health check fails, the stale
//     process is killed before starting a new one.
//   - Explicit stop: `schemabot local stop` sends SIGTERM and waits for shutdown.
//   - Binary upgrade: stop the old server, next command auto-starts with new binary.
//   - Machine reboot: server dies, next command auto-starts fresh.
//
// Storage lives on the developer's local MySQL (_schemabot database),
// fully decoupled from the target database.
func EnsureRunning(ctx context.Context, database string, dbConfig LocalDatabase) (string, error) {
	port := GetServerPort()
	endpoint := "http://127.0.0.1:" + port

	if isHealthy(ctx, endpoint) {
		slog.Debug("local server already running")
		return endpoint, nil
	}

	// Port bound but not healthy — kill the stale process
	if findProcessOnPort(port) != 0 {
		killStaleServer()
	}

	// Verify local MySQL is reachable before attempting anything.
	if err := checkLocalMySQL(ctx); err != nil {
		return "", err
	}

	// Bootstrap _schemabot database on the local MySQL server.
	if err := bootstrapStorage(ctx); err != nil {
		return "", err
	}

	// Generate server config file
	configPath, err := writeLocalServerConfig(database, dbConfig)
	if err != nil {
		return "", fmt.Errorf("generate server config: %w", err)
	}

	// Fork schemabot serve as a background process
	if err := startDaemon(configPath); err != nil {
		return "", fmt.Errorf("start local server: %w", err)
	}

	// Wait for server to be healthy
	if err := waitForHealthy(ctx, endpoint, 15*time.Second); err != nil {
		return "", fmt.Errorf("local server not ready: %w", err)
	}

	slog.Debug("local server started", "endpoint", endpoint, "pid", readPID())
	return endpoint, nil
}

// Stop stops the local background server. It tries the PID file first,
// then falls back to finding the process by port if the PID file is missing.
func Stop() error {
	pid := readPID()

	// If no PID file, try to find the process by port
	if pid == 0 {
		pid = findProcessOnPort(GetServerPort())
		if pid == 0 {
			return fmt.Errorf("no local server running")
		}
		slog.Debug("found orphan server process by port", "pid", pid)
	}

	process, err := os.FindProcess(pid)
	if err != nil {
		removePIDFile()
		return fmt.Errorf("find process %d: %w", pid, err)
	}

	if err := process.Signal(syscall.SIGTERM); err != nil {
		removePIDFile()
		return fmt.Errorf("stop process %d: %w", pid, err)
	}

	// Wait for graceful shutdown
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if !isProcessRunning(pid) {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	removePIDFile()
	return nil
}

// IsRunning returns true if the local server is running.
func IsRunning(ctx context.Context) bool {
	return isHealthy(ctx, "http://127.0.0.1:"+GetServerPort())
}

// killStaleServer kills any process holding the server port that isn't
// responding to health checks. Called before starting a new server.
func killStaleServer() {
	pid := findProcessOnPort(GetServerPort())
	if pid == 0 {
		return
	}
	slog.Debug("killing stale server process", "pid", pid)
	if process, err := os.FindProcess(pid); err == nil {
		if err := process.Signal(syscall.SIGTERM); err != nil {
			slog.Debug("failed to kill stale process", "pid", pid, "error", err)
		}
	}
	time.Sleep(500 * time.Millisecond)
	removePIDFile()
}

// findProcessOnPort returns the PID of the process listening on the given port,
// or 0 if no process is found. Uses lsof (macOS/Linux only).
func findProcessOnPort(port string) int {
	out, err := exec.CommandContext(context.Background(), "lsof", "-ti", "tcp:"+port).Output()
	if err != nil {
		return 0
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(out)))
	if err != nil {
		return 0
	}
	return pid
}

// isProcessRunning checks if a process with the given PID exists.
func isProcessRunning(pid int) bool {
	process, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	err = process.Signal(syscall.Signal(0))
	return err == nil
}

// checkLocalMySQL verifies that MySQL is running on the configured address
// and accessible with the storage DSN.
func checkLocalMySQL(ctx context.Context) error {
	serverDSN := getServerDSN()
	db, err := sql.Open("mysql", serverDSN)
	if err != nil {
		return fmt.Errorf("local mode requires MySQL on localhost:3306 — is MySQL running?\n\n  Install: https://dev.mysql.com/downloads/ or `brew install mysql`\n  Start:   mysql.server start")
	}
	defer utils.CloseAndLog(db)
	if err := db.PingContext(ctx); err != nil {
		if strings.Contains(err.Error(), "Access denied") {
			return fmt.Errorf("local mode cannot connect to MySQL: %w\n\nSchemaBot connects as root with no password for local storage.\nCheck your MySQL user configuration or grant access", err)
		}
		return fmt.Errorf("local mode requires MySQL on localhost:3306 — is MySQL running?\n\n  Install: https://dev.mysql.com/downloads/ or `brew install mysql`\n  Start:   mysql.server start")
	}
	return nil
}

// bootstrapStorage creates the _schemabot database on the local MySQL server
// and runs EnsureSchema to bootstrap the storage tables.
func bootstrapStorage(ctx context.Context) error {
	db, err := sql.Open("mysql", getServerDSN())
	if err != nil {
		return fmt.Errorf("open MySQL server: %w", err)
	}
	defer utils.CloseAndLog(db)

	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("connect to MySQL server: %w", err)
	}

	if _, err := db.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS `"+StorageDatabase+"`"); err != nil {
		return fmt.Errorf("create storage database: %w", err)
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	if err := api.EnsureSchema(GetStorageDSN(), logger); err != nil {
		return fmt.Errorf("bootstrap storage schema: %w", err)
	}

	return nil
}

// startDaemon forks the current binary as `schemabot serve` in the background.
func startDaemon(configPath string) error {
	dir, err := schemabotDir()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return fmt.Errorf("create config directory: %w", err)
	}

	logPath := filepath.Join(dir, "server.log")
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		return fmt.Errorf("open log file: %w", err)
	}

	// Use context.Background because the child process must outlive the parent CLI command.
	cmd := exec.CommandContext(context.Background(), os.Args[0], "serve")
	cmd.Env = append(os.Environ(),
		"SCHEMABOT_CONFIG_FILE="+configPath,
		"PORT="+GetServerPort(),
		"LOG_LEVEL=warn",
	)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}

	if err := cmd.Start(); err != nil {
		utils.CloseAndLog(logFile)
		return fmt.Errorf("start server process: %w", err)
	}

	// Write PID file
	pidPath := filepath.Join(dir, "server.pid")
	if err := os.WriteFile(pidPath, []byte(strconv.Itoa(cmd.Process.Pid)), 0600); err != nil {
		slog.Warn("failed to write PID file", "error", err)
	}

	// Detach — don't wait for the child process
	if err := cmd.Process.Release(); err != nil {
		slog.Debug("release process", "error", err)
	}
	utils.CloseAndLog(logFile)

	return nil
}

// writeLocalServerConfig generates a server config YAML and writes it to
// ~/.schemabot/local-server.yaml.
func writeLocalServerConfig(database string, dbConfig LocalDatabase) (string, error) {
	envConfigs := make(map[string]serverEnvConfig, len(dbConfig.Environments))
	for envName, env := range dbConfig.Environments {
		envConfigs[envName] = serverEnvConfig{
			DSN:            env.DSN,
			Organization:   env.Organization,
			TokenSecretRef: env.Token,
		}
	}

	cfg := serverConfig{
		Storage: serverStorageConfig{
			DSN: GetStorageDSN(),
		},
		Databases: map[string]serverDatabaseConfig{
			database: {
				Type:         dbConfig.Type,
				Environments: envConfigs,
			},
		},
	}

	data, err := yaml.Marshal(cfg)
	if err != nil {
		return "", fmt.Errorf("marshal config: %w", err)
	}

	dir, err := schemabotDir()
	if err != nil {
		return "", err
	}
	path := filepath.Join(dir, "local-server.yaml")
	if err := os.WriteFile(path, data, 0600); err != nil {
		return "", fmt.Errorf("write config: %w", err)
	}
	return path, nil
}

// Server config types for YAML generation.
type serverConfig struct {
	Storage   serverStorageConfig             `yaml:"storage"`
	Databases map[string]serverDatabaseConfig `yaml:"databases"`
}

type serverStorageConfig struct {
	DSN string `yaml:"dsn"`
}

type serverDatabaseConfig struct {
	Type         string                     `yaml:"type"`
	Environments map[string]serverEnvConfig `yaml:"environments"`
}

type serverEnvConfig struct {
	DSN            string `yaml:"dsn,omitempty"`
	Organization   string `yaml:"organization,omitempty"`
	TokenSecretRef string `yaml:"token_secret_ref,omitempty"`
}

// Health check

func isHealthy(ctx context.Context, endpoint string) bool {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint+"/health", nil)
	if err != nil {
		return false
	}
	client := &http.Client{Timeout: 1 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return false
	}
	resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

func waitForHealthy(ctx context.Context, endpoint string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if isHealthy(ctx, endpoint) {
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("timeout waiting for %s", endpoint)
}

// DropStorage drops the _schemabot database from the local MySQL server.
func DropStorage() error {
	db, err := sql.Open("mysql", getServerDSN())
	if err != nil {
		return fmt.Errorf("connect to local MySQL: %w", err)
	}
	defer utils.CloseAndLog(db)
	if _, err := db.ExecContext(context.Background(), "DROP DATABASE IF EXISTS `"+StorageDatabase+"`"); err != nil {
		return fmt.Errorf("drop database: %w", err)
	}
	return nil
}

// ReadPID returns the PID from the PID file, or 0 if not found.
func ReadPID() int {
	return readPID()
}

// PID file management

func readPID() int {
	dir, err := schemabotDir()
	if err != nil {
		return 0
	}
	data, err := os.ReadFile(filepath.Join(dir, "server.pid"))
	if err != nil {
		return 0
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		return 0
	}
	return pid
}

func removePIDFile() {
	dir, err := schemabotDir()
	if err != nil {
		return
	}
	if err := os.Remove(filepath.Join(dir, "server.pid")); err != nil && !os.IsNotExist(err) {
		slog.Debug("remove PID file", "error", err)
	}
}
