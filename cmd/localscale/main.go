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
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
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
	BranchCreationDelay  string  `json:"branch_creation_delay,omitempty"`
	DeployRequestDelay   string  `json:"deploy_request_delay,omitempty"`
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
				Keyspaces: keyspaces,
			}
		}
		orgs[name] = localscale.OrgConfig{Databases: databases}
	}

	revertWindow := parseDuration(cf.RevertWindowDuration, "revert_window_duration")
	branchCreationDelay := parseDuration(cf.BranchCreationDelay, "branch_creation_delay")
	deployRequestDelay := parseDuration(cf.DeployRequestDelay, "deploy_request_delay")

	ctx := context.Background()

	server, err := localscale.New(ctx, localscale.Config{
		Orgs:                 orgs,
		ListenAddr:           cf.ListenAddr,
		RevertWindowDuration: revertWindow,
		DefaultThrottleRatio: cf.DefaultThrottleRatio,
		ProxyHost:            cf.ProxyHost,
		ProxyAdvertiseHost:   cf.ProxyAdvertiseHost,
		ProxyPortRange:       [2]int{cf.ProxyPortStart, cf.ProxyPortEnd},
		BranchCreationDelay:  branchCreationDelay,
		DeployRequestDelay:   deployRequestDelay,
		Logger:               logger,
	})
	if err != nil {
		logger.Error("failed to start localscale", "error", err)
		os.Exit(1)
	}

	// Apply VSchema files from schema_dir if set
	if cf.SchemaDir != "" {
		if err := applyVSchemas(ctx, server.URL(), orgs, cf.SchemaDir, logger); err != nil {
			logger.Error("failed to apply vschemas", "error", err)
			os.Exit(1)
		}
	}

	logger.Info("localscale running", "url", server.URL(), "orgs", len(orgs))

	// Block until signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	logger.Info("shutting down")
	server.Close()
}

// applyVSchemas walks schemaDir looking for keyspace subdirectories containing
// vschema.json files and applies them to all orgs/databases via the admin HTTP endpoint.
func applyVSchemas(ctx context.Context, baseURL string, orgs map[string]localscale.OrgConfig, schemaDir string, logger *slog.Logger) error {
	entries, err := os.ReadDir(schemaDir)
	if err != nil {
		return fmt.Errorf("read schema dir %s: %w", schemaDir, err)
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		keyspace := entry.Name()
		vschemaPath := filepath.Join(schemaDir, keyspace, "vschema.json")
		data, err := os.ReadFile(vschemaPath)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return fmt.Errorf("read vschema for %s: %w", keyspace, err)
		}

		for orgName, orgCfg := range orgs {
			for dbName := range orgCfg.Databases {
				body := map[string]any{
					"org":      orgName,
					"database": dbName,
					"keyspace": keyspace,
					"vschema":  json.RawMessage(data),
				}
				bodyJSON, _ := json.Marshal(body)
				req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/admin/seed-vschema", bytes.NewReader(bodyJSON))
				if err != nil {
					return fmt.Errorf("create request for %s (%s/%s): %w", keyspace, orgName, dbName, err)
				}
				req.Header.Set("Content-Type", "application/json")
				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					return fmt.Errorf("apply vschema for %s (%s/%s): %w", keyspace, orgName, dbName, err)
				}
				_ = resp.Body.Close()
				if resp.StatusCode != http.StatusOK {
					return fmt.Errorf("apply vschema for %s (%s/%s): HTTP %d", keyspace, orgName, dbName, resp.StatusCode)
				}
				logger.Info("applied vschema", "org", orgName, "database", dbName, "keyspace", keyspace)
			}
		}
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
