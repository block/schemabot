package localscale

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/docker/go-connections/nat"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	defaultImage          = "localscale:latest"
	defaultAPIPort        = "8080"
	defaultProxyPortStart = 19100
	defaultProxyPortEnd   = 19150
)

// LocalScaleContainer wraps a testcontainers.Container with LocalScale-specific helpers.
type LocalScaleContainer struct {
	testcontainers.Container
	url string // base URL for the PlanetScale-compatible API
}

// URL returns the base URL for the LocalScale API (e.g., "http://localhost:54321").
func (c *LocalScaleContainer) URL() string {
	return c.url
}

// NewTestHelper creates a LocalScaleContainer from an in-process *Server, without
// Docker. All helper methods (SchemaDir, SeedDDL, VtgateExec, ResetState, etc.)
// work because they only use HTTP against the server's URL.
//
// Use this when running LocalScale in-process via localscale.New() instead of
// RunContainer():
//
//	server, _ := localscale.New(ctx, cfg)
//	helper := localscale.NewTestHelper(server.URL())
//	helper.SchemaDir(ctx, org, db, "testdata/schema")
func NewTestHelper(url string) *LocalScaleContainer {
	return &LocalScaleContainer{url: url}
}

// ContainerConfig configures the LocalScale testcontainer.
type ContainerConfig struct {
	Orgs           map[string]ContainerOrgConfig `json:"organizations"`
	ListenAddr     string                        `json:"listen_addr"`
	ProxyHost      string                        `json:"proxy_host"`
	ProxyPortStart int                           `json:"proxy_port_start"`
	ProxyPortEnd   int                           `json:"proxy_port_end"`

	// Reuse keeps the container running between test invocations for fast iteration.
	// When true, RunContainer reuses an existing container (by name) and automatically
	// calls ResetState to clean up stale data from previous runs. Callers should NOT
	// call Terminate() on a reused container.
	Reuse bool `json:"-"`

	// ContainerName sets the Docker container name for reuse. Defaults to "localscale-test".
	// Only used when Reuse is true.
	ContainerName string `json:"-"`
}

// ContainerOrgConfig holds databases for an org in container config.
type ContainerOrgConfig struct {
	Databases map[string]ContainerDatabaseConfig `json:"databases"`
}

// ContainerDatabaseConfig holds keyspaces for a database in container config.
type ContainerDatabaseConfig struct {
	Keyspaces []ContainerKeyspaceConfig `json:"keyspaces"`
}

// ContainerKeyspaceConfig describes a keyspace in container config.
type ContainerKeyspaceConfig struct {
	Name   string `json:"name"`
	Shards int    `json:"shards"`
}

// RunContainer starts a LocalScale Docker container and returns a ready-to-use
// LocalScaleContainer with the API URL configured. It handles port exposure,
// config injection, and proxy port mapping automatically.
//
// Set cfg.Reuse = true for fast iteration — the container persists between
// test runs and ResetState is called automatically to clean up stale data.
//
// Optional ContainerCustomizer opts allow further overrides to the container request.
func RunContainer(ctx context.Context, cfg ContainerConfig, opts ...testcontainers.ContainerCustomizer) (*LocalScaleContainer, error) {
	// Handle built-in reuse mode.
	if cfg.Reuse {
		name := cfg.ContainerName
		if name == "" {
			name = "localscale-test"
		}
		opts = append(opts, testcontainers.CustomizeRequest(testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{Name: name},
			Reuse:            true,
		}))
	}
	// Apply defaults.
	if cfg.ListenAddr == "" {
		cfg.ListenAddr = ":" + defaultAPIPort
	}
	if cfg.ProxyHost == "" {
		cfg.ProxyHost = "0.0.0.0"
	}
	if cfg.ProxyPortStart == 0 {
		cfg.ProxyPortStart = defaultProxyPortStart
	}
	if cfg.ProxyPortEnd == 0 {
		cfg.ProxyPortEnd = defaultProxyPortEnd
	}

	// Build exposed ports list.
	exposedPorts := []string{defaultAPIPort + "/tcp"}
	for p := cfg.ProxyPortStart; p <= cfg.ProxyPortEnd; p++ {
		exposedPorts = append(exposedPorts, fmt.Sprintf("%d/tcp", p))
	}

	// Serialize config to JSON.
	configJSON, err := json.Marshal(cfg)
	if err != nil {
		return nil, fmt.Errorf("marshal localscale config: %w", err)
	}

	req := testcontainers.ContainerRequest{
		Image:        defaultImage,
		ExposedPorts: exposedPorts,
		Env: map[string]string{
			"CONFIG_FILE": "/etc/localscale/config.json",
		},
		Files: []testcontainers.ContainerFile{
			{
				Reader:            bytes.NewReader(configJSON),
				ContainerFilePath: "/etc/localscale/config.json",
				FileMode:          0644,
			},
		},
		WaitingFor: wait.ForHTTP("/health").
			WithPort(defaultAPIPort).
			WithStartupTimeout(3 * time.Minute),
	}

	genReq := testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	}

	// Apply custom options (e.g., Reuse, container name).
	for _, opt := range opts {
		if err := opt.Customize(&genReq); err != nil {
			return nil, fmt.Errorf("apply container option: %w", err)
		}
	}

	ctr, err := testcontainers.GenericContainer(ctx, genReq)
	if err != nil {
		return nil, fmt.Errorf("start localscale container: %w", err)
	}

	// Cleanup on failure: terminate the container if any setup step fails.
	// Skip termination if Reuse is enabled (container should persist).
	success := false
	defer func() {
		if !success && !genReq.Reuse {
			_ = ctr.Terminate(ctx)
		}
	}()

	// Resolve the API URL.
	host, err := ctr.Host(ctx)
	if err != nil {
		return nil, fmt.Errorf("get container host: %w", err)
	}
	apiPort, err := ctr.MappedPort(ctx, nat.Port(defaultAPIPort))
	if err != nil {
		return nil, fmt.Errorf("get API port: %w", err)
	}
	baseURL := fmt.Sprintf("http://%s:%s", host, apiPort.Port())

	// Register proxy port mappings so the password API returns correct external ports.
	portMap := make(map[int]int)
	for p := cfg.ProxyPortStart; p <= cfg.ProxyPortEnd; p++ {
		mapped, err := ctr.MappedPort(ctx, nat.Port(fmt.Sprintf("%d", p)))
		if err != nil {
			continue
		}
		portMap[p] = mapped.Int()
	}
	if len(portMap) > 0 {
		if err := postProxyPortMap(ctx, baseURL, portMap); err != nil {
			return nil, fmt.Errorf("register proxy port map: %w", err)
		}
	}

	success = true
	lsc := &LocalScaleContainer{Container: ctr, url: baseURL}

	// Automatically clean up stale state when reusing a container.
	if cfg.Reuse {
		if err := lsc.ResetState(ctx); err != nil {
			return nil, fmt.Errorf("reset state on reused container: %w", err)
		}
	}

	return lsc, nil
}

// SeedDDL executes DDL statements directly against vtgate (bypassing branches/deploys).
// This is the recommended way to set up initial schema before running tests.
func (c *LocalScaleContainer) SeedDDL(ctx context.Context, org, database, keyspace string, stmts ...string) error {
	return c.SeedDDLWithStrategy(ctx, org, database, keyspace, "", "", stmts...)
}

// SeedDDLWithStrategy executes DDL statements against vtgate with a specific DDL strategy
// and migration context. Use for online DDL warmup where SET @@ddl_strategy must be on
// the same connection as the DDL.
func (c *LocalScaleContainer) SeedDDLWithStrategy(ctx context.Context, org, database, keyspace, strategy, migrationContext string, stmts ...string) error {
	body := map[string]any{
		"org":        org,
		"database":   database,
		"keyspace":   keyspace,
		"statements": stmts,
	}
	if strategy != "" {
		body["strategy"] = strategy
	}
	if migrationContext != "" {
		body["migration_context"] = migrationContext
	}
	return c.postAdmin(ctx, "/admin/seed-ddl", body)
}

// SeedVSchema applies a VSchema (as JSON) to a keyspace via vtctldclient gRPC.
func (c *LocalScaleContainer) SeedVSchema(ctx context.Context, org, database, keyspace string, vschema []byte) error {
	body := map[string]any{
		"org":      org,
		"database": database,
		"keyspace": keyspace,
		"vschema":  json.RawMessage(vschema),
	}
	return c.postAdmin(ctx, "/admin/seed-vschema", body)
}

// SchemaDir seeds schema from a directory structure where each subdirectory is a
// keyspace containing .sql files (DDL) and an optional vschema.json. VSchema is applied
// first so vtgate knows how to route queries before tables are created.
//
// Directory layout:
//
//	schemaDir/
//	  testapp/
//	    users.sql
//	    vschema.json   (optional)
//	  testapp_sharded/
//	    orders.sql
//	    products.sql
//	    vschema.json
func (c *LocalScaleContainer) SchemaDir(ctx context.Context, org, database, schemaDir string) error {
	entries, err := os.ReadDir(schemaDir)
	if err != nil {
		return fmt.Errorf("read schema dir %s: %w", schemaDir, err)
	}

	// First pass: apply VSchema for all keyspaces (routing must be set before DDL).
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		keyspace := entry.Name()
		vschemaPath := filepath.Join(schemaDir, keyspace, "vschema.json")
		vschemaData, err := os.ReadFile(vschemaPath)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return fmt.Errorf("read vschema for %s: %w", keyspace, err)
		}
		if err := c.SeedVSchema(ctx, org, database, keyspace, vschemaData); err != nil {
			return fmt.Errorf("apply %s vschema: %w", keyspace, err)
		}
	}

	// Second pass: execute DDL for all keyspaces.
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		keyspace := entry.Name()
		files, err := os.ReadDir(filepath.Join(schemaDir, keyspace))
		if err != nil {
			return fmt.Errorf("read keyspace dir %s: %w", keyspace, err)
		}
		var stmts []string
		for _, f := range files {
			if f.IsDir() || filepath.Ext(f.Name()) != ".sql" {
				continue
			}
			data, err := os.ReadFile(filepath.Join(schemaDir, keyspace, f.Name()))
			if err != nil {
				return fmt.Errorf("read %s/%s: %w", keyspace, f.Name(), err)
			}
			stmts = append(stmts, strings.TrimSpace(string(data)))
		}
		if len(stmts) > 0 {
			if err := c.SeedDDL(ctx, org, database, keyspace, stmts...); err != nil {
				return fmt.Errorf("seed %s DDL: %w", keyspace, err)
			}
		}
	}

	return nil
}

// VtgateExec executes a SQL query against vtgate for a given keyspace and returns the result.
func (c *LocalScaleContainer) VtgateExec(ctx context.Context, org, database, keyspace, query string, args ...any) (*QueryResult, error) {
	body := map[string]any{
		"org":      org,
		"database": database,
		"keyspace": keyspace,
		"query":    query,
	}
	if len(args) > 0 {
		body["args"] = args
	}
	return c.postAdminResult(ctx, "/admin/vtgate-exec", body)
}

// MetadataQuery executes a SQL query against the metadata database.
func (c *LocalScaleContainer) MetadataQuery(ctx context.Context, query string, args ...any) (*QueryResult, error) {
	body := map[string]any{
		"query": query,
	}
	if len(args) > 0 {
		body["args"] = args
	}
	return c.postAdminResult(ctx, "/admin/metadata-query", body)
}

// ResetState cancels all running Vitess migrations, waits for terminal state,
// and truncates metadata tables.
func (c *LocalScaleContainer) ResetState(ctx context.Context) error {
	return c.postAdmin(ctx, "/admin/reset-state", nil)
}

// BranchDBQuery executes a SQL query against a branch database.
func (c *LocalScaleContainer) BranchDBQuery(ctx context.Context, branch, keyspace, query string) (*QueryResult, error) {
	body := map[string]any{
		"branch":   branch,
		"keyspace": keyspace,
		"query":    query,
	}
	return c.postAdminResult(ctx, "/admin/branch-db-query", body)
}

// postAdmin sends a POST request to an admin endpoint and checks for success.
func (c *LocalScaleContainer) postAdmin(ctx context.Context, path string, body any) error {
	var bodyReader *bytes.Reader
	if body != nil {
		bodyJSON, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("marshal request body for %s: %w", path, err)
		}
		bodyReader = bytes.NewReader(bodyJSON)
	} else {
		bodyReader = bytes.NewReader([]byte("{}"))
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url+path, bodyReader)
	if err != nil {
		return fmt.Errorf("create request for %s: %w", path, err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("POST %s: %w", path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		var errBody map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&errBody)
		return fmt.Errorf("%s returned %d: %v", path, resp.StatusCode, errBody["message"])
	}
	return nil
}

// postAdminResult sends a POST request to an admin endpoint and decodes a QueryResult.
func (c *LocalScaleContainer) postAdminResult(ctx context.Context, path string, body any) (*QueryResult, error) {
	bodyJSON, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("marshal request body for %s: %w", path, err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url+path, bytes.NewReader(bodyJSON))
	if err != nil {
		return nil, fmt.Errorf("create request for %s: %w", path, err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("POST %s: %w", path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		var errBody map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&errBody)
		return nil, fmt.Errorf("%s returned %d: %v", path, resp.StatusCode, errBody["message"])
	}
	var result QueryResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode response from %s: %w", path, err)
	}
	return &result, nil
}

// postProxyPortMap sends the internal->external port mapping to the LocalScale admin endpoint.
func postProxyPortMap(ctx context.Context, baseURL string, portMap map[int]int) error {
	body, err := json.Marshal(portMap)
	if err != nil {
		return fmt.Errorf("marshal proxy port map: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/admin/proxy-port-map", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create proxy-port-map request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("POST proxy-port-map: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("proxy-port-map returned %d", resp.StatusCode)
	}
	return nil
}
