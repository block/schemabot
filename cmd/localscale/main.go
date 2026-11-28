// Binary localscale runs the LocalScale fake PlanetScale API server as a
// standalone HTTP service backed by managed Vitess clusters (vttest.LocalCluster).
//
// Configuration is via a JSON config file specified by CONFIG_FILE env var.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/block/schemabot/pkg/localscale"
)

// configFile is the JSON config file format for the localscale binary.
type configFile struct {
	Organizations map[string]struct {
		Databases map[string]struct {
			Keyspaces []struct {
				Name   string `json:"name"`
				Shards int    `json:"shards"`
			} `json:"keyspaces"`
			RequireApproval bool  `json:"require_approval,omitempty"`
			SafeMigrations  *bool `json:"safe_migrations,omitempty"`
		} `json:"databases"`
	} `json:"organizations"`
	ListenAddr           string  `json:"listen_addr"`
	SchemaDir            string  `json:"schema_dir"`
	RevertWindowDuration string  `json:"revert_window_duration,omitempty"`
	DefaultThrottleRatio float64 `json:"default_throttle_ratio,omitempty"`
	ProxyHost            string  `json:"proxy_host,omitempty"`
	ProxyAdvertiseHost   string  `json:"proxy_advertise_host,omitempty"`
	ProxyPortStart       int     `json:"proxy_port_start,omitempty"`
	ProxyPortEnd         int     `json:"proxy_port_end,omitempty"`
	// ProxyAdvertisePortStart is the host port proxy_port_start is published
	// on, when the range is published as one block at an offset.
	ProxyAdvertisePortStart int    `json:"proxy_advertise_port_start,omitempty"`
	BranchTLSMode           string `json:"branch_tls_mode,omitempty"`
	BranchCreationDelay     string `json:"branch_creation_delay,omitempty"`
	DeployRequestDelay      string `json:"deploy_request_delay,omitempty"`
	ProcessorTickInterval   string `json:"processor_tick_interval,omitempty"`
}

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	configPath := os.Getenv("CONFIG_FILE")
	if configPath == "" {
		logger.Error("CONFIG_FILE env var is required")
		os.Exit(1)
	}

	data, err := os.ReadFile(configPath)
	if err != nil {
		logger.Error("read config file", "path", configPath, "error", err)
		os.Exit(1)
	}

	var cf configFile
	if err := json.Unmarshal(data, &cf); err != nil {
		logger.Error("parse config file", "path", configPath, "error", err)
		os.Exit(1)
	}

	orgs := make(map[string]localscale.OrgConfig, len(cf.Organizations))
	for name, org := range cf.Organizations {
		databases := make(map[string]localscale.DatabaseConfig, len(org.Databases))
		for dbName, db := range org.Databases {
			var keyspaces []localscale.KeyspaceConfig
			for _, ks := range db.Keyspaces {
				shards := ks.Shards
				if shards == 0 {
					shards = 1
				}
				keyspaces = append(keyspaces, localscale.KeyspaceConfig{
					Name:   ks.Name,
					Shards: shards,
				})
			}
			databases[dbName] = localscale.DatabaseConfig{
				Keyspaces:       keyspaces,
				RequireApproval: db.RequireApproval,
				SafeMigrations:  db.SafeMigrations,
			}
		}
		orgs[name] = localscale.OrgConfig{Databases: databases}
	}

	revertWindow := parseDuration(cf.RevertWindowDuration, "revert_window_duration")
	branchCreationDelay := parseDuration(cf.BranchCreationDelay, "branch_creation_delay")
	deployRequestDelay := parseDuration(cf.DeployRequestDelay, "deploy_request_delay")
	processorTickInterval := parseDuration(cf.ProcessorTickInterval, "processor_tick_interval")

	ctx := context.Background()

	server, err := localscale.New(ctx, localscale.Config{
		Orgs:                    orgs,
		ListenAddr:              cf.ListenAddr,
		RevertWindowDuration:    revertWindow,
		DefaultThrottleRatio:    cf.DefaultThrottleRatio,
		ProxyHost:               cf.ProxyHost,
		ProxyAdvertiseHost:      cf.ProxyAdvertiseHost,
		BranchTLSMode:           cf.BranchTLSMode,
		ProxyPortRange:          [2]int{cf.ProxyPortStart, cf.ProxyPortEnd},
		ProxyAdvertisePortStart: cf.ProxyAdvertisePortStart,
		BranchCreationDelay:     branchCreationDelay,
		DeployRequestDelay:      deployRequestDelay,
		ProcessorTickInterval:   processorTickInterval,
		Logger:                  logger,
		// Seeding runs inside New, before the server reports healthy, so a
		// dependent waiting on /health never starts against a cluster whose
		// tables have not been created yet.
		Seed: func(ctx context.Context, baseURL string) error {
			if cf.SchemaDir == "" {
				logger.Info("no schema_dir configured, skipping schema seeding")
				return nil
			}
			logger.Info("seeding schema from schema_dir", "schema_dir", cf.SchemaDir)
			if err := seedSchemaDir(ctx, baseURL, orgs, cf.SchemaDir, logger); err != nil {
				return err
			}
			logger.Info("schema seeding complete")
			return nil
		},
	})
	if err != nil {
		logger.Error("failed to start localscale", "schema_dir", cf.SchemaDir, "error", err)
		os.Exit(1)
	}

	logger.Info("localscale running", "url", server.URL(), "orgs", len(orgs))

	// Block until signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	logger.Info("shutting down")
	server.Close()
}

// seedSchemaDir walks schemaDir, treating each subdirectory as a keyspace that
// holds an optional vschema.json and any number of .sql files, and seeds both
// into every org and database the server was configured with.
//
// VSchema goes first for every keyspace, then the tables: vtgate has to know
// how to route a keyspace's queries before anything creates tables in it, and a
// keyspace's vindexes can reference a table in another keyspace.
func seedSchemaDir(ctx context.Context, baseURL string, orgs map[string]localscale.OrgConfig, schemaDir string, logger *slog.Logger) error {
	entries, err := os.ReadDir(schemaDir)
	if err != nil {
		return fmt.Errorf("read schema dir %s: %w", schemaDir, err)
	}
	var keyspaces []string
	for _, entry := range entries {
		// Stat the entry rather than trust it: os.ReadDir reports a symlink to
		// a keyspace directory as a non-directory, so a schema_dir assembled
		// out of links would be skipped entirely and seed nothing.
		path := filepath.Join(schemaDir, entry.Name())
		info, err := os.Stat(path)
		if err != nil {
			return fmt.Errorf("stat schema dir entry %s: %w", path, err)
		}
		if !info.IsDir() {
			logger.Debug("skipping non-keyspace entry in schema_dir", "schema_dir", schemaDir, "entry", entry.Name())
			continue
		}
		keyspaces = append(keyspaces, entry.Name())
	}

	// A keyspace directory that matches no configured database seeds nothing.
	// That is how a misspelled directory name presents, and seeding would
	// otherwise finish "complete" with the cluster empty, so say so at a level
	// the binary actually prints.
	for _, keyspace := range keyspaces {
		if !anyDatabaseHasKeyspace(orgs, keyspace) {
			logger.Warn("keyspace directory matches no configured database, nothing will be seeded from it",
				"schema_dir", schemaDir, "keyspace", keyspace)
		}
	}

	for _, keyspace := range keyspaces {
		vschemaPath := filepath.Join(schemaDir, keyspace, "vschema.json")
		data, err := os.ReadFile(vschemaPath)
		if err != nil {
			if os.IsNotExist(err) {
				logger.Debug("keyspace has no vschema.json", "keyspace", keyspace)
				continue
			}
			return fmt.Errorf("read vschema for %s: %w", keyspace, err)
		}
		for orgName, orgCfg := range orgs {
			for dbName, dbCfg := range orgCfg.Databases {
				if !databaseHasKeyspace(dbCfg, keyspace) {
					logger.Debug("database does not declare this keyspace, skipping vschema",
						"org", orgName, "database", dbName, "keyspace", keyspace)
					continue
				}
				body := map[string]any{
					"org":      orgName,
					"database": dbName,
					"keyspace": keyspace,
					"vschema":  json.RawMessage(data),
				}
				if err := postAdmin(ctx, baseURL, "/admin/seed-vschema", body); err != nil {
					return fmt.Errorf("apply vschema for %s (%s/%s): %w", keyspace, orgName, dbName, err)
				}
				logger.Info("applied vschema", "org", orgName, "database", dbName, "keyspace", keyspace)
			}
		}
	}

	for _, keyspace := range keyspaces {
		stmts, err := keyspaceDDL(filepath.Join(schemaDir, keyspace))
		if err != nil {
			return fmt.Errorf("read DDL for %s: %w", keyspace, err)
		}
		if len(stmts) == 0 {
			logger.Debug("keyspace has no .sql files", "keyspace", keyspace)
			continue
		}
		for orgName, orgCfg := range orgs {
			for dbName, dbCfg := range orgCfg.Databases {
				if !databaseHasKeyspace(dbCfg, keyspace) {
					logger.Debug("database does not declare this keyspace, skipping DDL",
						"org", orgName, "database", dbName, "keyspace", keyspace)
					continue
				}
				body := map[string]any{
					"org":        orgName,
					"database":   dbName,
					"keyspace":   keyspace,
					"statements": stmts,
				}
				if err := postAdmin(ctx, baseURL, "/admin/seed-ddl", body); err != nil {
					return fmt.Errorf("seed DDL for %s (%s/%s): %w", keyspace, orgName, dbName, err)
				}
				logger.Info("seeded tables", "org", orgName, "database", dbName, "keyspace", keyspace, "statements", len(stmts))
			}
		}
	}

	return nil
}

// adminRequestTimeout bounds a single seed request. Seeding submits a
// keyspace's whole DDL batch in one call, so it is given more room than the
// package's own admin calls, which carry a single operation each.
const adminRequestTimeout = 30 * time.Second

// anyDatabaseHasKeyspace reports whether any configured database declares the
// named keyspace, so a keyspace directory that matches none can be surfaced.
func anyDatabaseHasKeyspace(orgs map[string]localscale.OrgConfig, keyspace string) bool {
	for _, orgCfg := range orgs {
		for _, dbCfg := range orgCfg.Databases {
			if databaseHasKeyspace(dbCfg, keyspace) {
				return true
			}
		}
	}
	return false
}

// databaseHasKeyspace reports whether a database declares the named keyspace.
// A schema_dir is shared across every database, but each one declares only the
// keyspaces it holds, so seeding a keyspace into a database that does not have
// it is the configuration's answer rather than an error to surface.
func databaseHasKeyspace(db localscale.DatabaseConfig, keyspace string) bool {
	for _, ks := range db.Keyspaces {
		if ks.Name == keyspace {
			return true
		}
	}
	return false
}

// keyspaceDDL reads a keyspace directory's .sql files in name order, so a
// schema whose statements depend on each other can fix their order by naming.
// Each file is submitted as one statement, so a file holding two of them fails
// the seed loudly rather than being split here: splitting SQL by hand is what
// the real-parser rule exists to prevent.
func keyspaceDDL(dir string) ([]string, error) {
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var stmts []string
	for _, f := range files {
		if f.IsDir() || filepath.Ext(f.Name()) != ".sql" {
			continue
		}
		data, err := os.ReadFile(filepath.Join(dir, f.Name()))
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", f.Name(), err)
		}
		if stmt := strings.TrimSpace(string(data)); stmt != "" {
			stmts = append(stmts, stmt)
		}
	}
	return stmts, nil
}

func postAdmin(ctx context.Context, baseURL, path string, body map[string]any) error {
	payload, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("encode %s request: %w", path, err)
	}
	// A seed statement waiting on a lock would otherwise hold startup open
	// with no deadline to cancel it, since the admin endpoints run the SQL on
	// the request's own context.
	reqCtx, cancel := context.WithTimeout(ctx, adminRequestTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, baseURL+path, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("create %s request: %w", path, err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		detail, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return fmt.Errorf("%s returned HTTP %d: %s", path, resp.StatusCode, strings.TrimSpace(string(detail)))
	}
	return nil
}

func parseDuration(s, name string) time.Duration {
	if s == "" {
		return 0
	}
	duration, err := time.ParseDuration(s)
	if err != nil {
		slog.Error("invalid config value", "field", name, "value", s, "error", err)
		os.Exit(1)
	}
	return duration
}
