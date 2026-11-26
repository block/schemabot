package localdemo

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

// localscaleRepository holds the LocalScale image: a PlanetScale-compatible API
// over an embedded Vitess cluster, so a sample needs no PlanetScale account.
const localscaleRepository = "ghcr.io/block/schemabot/localscale"

// sampleOrganization and sampleVitessDatabase name the organization and
// database the sample serves. They are the PlanetScale coordinates the engine
// addresses, not a DSN.
const (
	sampleOrganization   = "schemabot-sample"
	sampleVitessDatabase = "shop"
)

// sampleVitessToken is the service token LocalScale accepts. LocalScale
// authenticates no one, so this is a placeholder in PlanetScale's name:value
// form rather than a credential: it never reaches a real PlanetScale account.
const sampleVitessToken = "localscale:localscale"

// SampleShards is the shard count the sample keyspace is split across. A
// sharded keyspace is the point of a Vitess sample: it is what makes an apply
// report per-shard progress, which a single-shard sample would not show.
const SampleShards = 2

// releaseVersion matches the versions the release workflow publishes an image
// for. A development build carries something else (a commit, "dev", or an
// empty string), and the sample then follows the default branch.
var releaseVersion = regexp.MustCompile(`^v\d+\.\d+\.\d+$`)

// localscaleImage pins the sample to the image published for this CLI's own
// release, so a change to the API the engine speaks reaches both sides at once.
// A development build has no published image of its own and follows the default
// branch instead.
func localscaleImage(version string) string {
	tag := "latest"
	if releaseVersion.MatchString(version) {
		tag = version
	}
	return localscaleRepository + ":" + tag
}

// ensureVitess starts or reuses this project's Vitess sample. It is two
// containers: LocalScale, which answers the PlanetScale API against an embedded
// Vitess cluster, and a MySQL server holding SchemaBot's own state. LocalScale
// publishes only its API and branch-proxy ports, so SchemaBot's state cannot
// live inside it.
func ensureVitess(ctx context.Context, project, version string, report func(string)) (Database, error) {
	report("Checking Docker")
	if err := requireLocalDocker(ctx); err != nil {
		return Database{}, err
	}
	storage, err := ensureSQL(ctx, sqlSample{
		project: project, engine: "mysql", label: "vitess-storage",
		seed: "CREATE DATABASE schemabot;\n", seedStage: "Preparing SchemaBot's own database",
	}, report)
	if err != nil {
		return Database{}, fmt.Errorf("prepare the sample's state database: %w", err)
	}

	name, err := sampleName(project, "vitess")
	if err != nil {
		return Database{}, err
	}
	image := localscaleImage(version)
	// Listing distinguishes an absent container from a broken Docker connection.
	listed, err := run(ctx, nil, "container", "ls", "-a", "--filter", "name=^/"+name+"$", "--format", "{{.ID}}")
	if err != nil {
		return Database{}, err
	}
	if strings.TrimSpace(string(listed)) == "" {
		report("Preparing " + image + " (downloading if needed)")
		if err = ensureImage(ctx, image); err != nil {
			return Database{}, err
		}
		hostPort, err := reserveHostPort(ctx)
		if err != nil {
			return Database{}, err
		}
		args := []string{
			"create", "--name", name,
			"--label", ownerLabel + "=" + name,
			"--label", "com.block.schemabot.engine=vitess",
			"--restart", "unless-stopped",
			"-p", fmt.Sprintf("127.0.0.1:%d:8080", hostPort),
			"-e", "CONFIG_FILE=" + localscaleConfigPath,
			image, "localscale",
		}
		if _, err = run(ctx, nil, args...); err != nil {
			return Database{}, err
		}
	}
	c, err := inspect(ctx, name)
	if err != nil {
		return Database{}, err
	}
	if c.Config.Labels[ownerLabel] != name || c.Config.Labels["com.block.schemabot.engine"] != "vitess" {
		return Database{}, fmt.Errorf("container %s is not this project's sample; refusing to use it", name)
	}
	if c.State.Status == "created" {
		report("Preparing sample keyspace and tables")
		if err = copyVitessSample(ctx, name); err != nil {
			return Database{}, err
		}
	}
	if !c.State.Running {
		report("Starting your sample cluster")
		if _, err = run(ctx, nil, "start", name); err != nil {
			if c.State.Status == "created" {
				return Database{}, fmt.Errorf("start new sample %s: %w; resolve the Docker error and retry setup", name, err)
			}
			return Database{}, fmt.Errorf("start sample %s: %w; if its port is in use, stop the process using that port and retry docker start %s; keep the container to preserve your data and saved connection", name, err, name)
		}
	}
	c, err = inspect(ctx, name)
	if err != nil {
		return Database{}, err
	}
	bindings := c.NetworkSettings.Ports["8080/tcp"]
	if len(bindings) != 1 || bindings[0].HostIP != "127.0.0.1" {
		return Database{}, fmt.Errorf("sample %s must publish one loopback-only port", name)
	}
	result := Database{
		Name:         name,
		APIURL:       "http://" + net.JoinHostPort("127.0.0.1", bindings[0].HostPort),
		Organization: sampleOrganization,
		Token:        sampleVitessToken,
		StorageDSN:   storage.StorageDSN,
		Namespace:    sampleVitessDatabase,
	}
	// A Vitess target is addressed through the PlanetScale API rather than a
	// DSN, so the engine is given the endpoint where a SQL engine is given a
	// connection string.
	result.DSN = result.APIURL

	report("Waiting for the cluster to come up")
	if err := waitForLocalscale(ctx, name, result.APIURL); err != nil {
		return Database{}, err
	}
	return result, nil
}

// localscaleConfigPath and localscaleSchemaDir are where the sample's
// configuration and seed schema are placed inside the container. Both are
// copied in rather than bind-mounted, so the sample survives the project
// directory moving and leaves nothing behind on the host.
const (
	localscaleConfigPath = "/vt/localscale.json"
	localscaleSchemaDir  = "/vt/sample-schema"
)

// copyVitessSample writes the sample's LocalScale configuration and seed schema
// into a created-but-not-yet-started container, so its first boot comes up with
// the keyspace and tables already in place.
func copyVitessSample(ctx context.Context, name string) error {
	dir, err := os.MkdirTemp("", "schemabot-sample-")
	if err != nil {
		return err
	}
	defer func() {
		if err := os.RemoveAll(dir); err != nil {
			slog.Warn("remove sample seed directory", "error", err)
		}
	}()
	config, err := json.MarshalIndent(localscaleConfig(), "", "  ")
	if err != nil {
		return fmt.Errorf("encode sample cluster configuration: %w", err)
	}
	configPath := filepath.Join(dir, "localscale.json")
	if err := os.WriteFile(configPath, config, 0644); err != nil {
		return err
	}
	if _, err := run(ctx, nil, "cp", configPath, name+":"+localscaleConfigPath); err != nil {
		return err
	}
	keyspace := filepath.Join(dir, "schema", sampleVitessDatabase)
	if err := os.MkdirAll(keyspace, 0755); err != nil {
		return err
	}
	for file, content := range vitessSeed() {
		if err := os.WriteFile(filepath.Join(keyspace, file), []byte(content), 0644); err != nil {
			return err
		}
	}
	if _, err := run(ctx, nil, "cp", filepath.Join(dir, "schema"), name+":"+localscaleSchemaDir); err != nil {
		return err
	}
	return nil
}

// localscaleConfig is the sample's LocalScale configuration: one organization
// holding one sharded database, with the delays a real PlanetScale deploy has
// shortened to what a local demo can wait for.
func localscaleConfig() map[string]any {
	return map[string]any{
		"organizations": map[string]any{
			sampleOrganization: map[string]any{
				"databases": map[string]any{
					sampleVitessDatabase: map[string]any{
						"keyspaces": []map[string]any{
							{"name": sampleVitessDatabase, "shards": SampleShards},
						},
					},
				},
			},
		},
		"listen_addr":            ":8080",
		"schema_dir":             localscaleSchemaDir,
		"revert_window_duration": "30m",
		"branch_creation_delay":  "1s",
		"deploy_request_delay":   "1s",
	}
}

// vitessSeed is the sample's starting schema: the same two tables the SQL
// samples seed, sharded on the column each is looked up by, so the keyspace has
// a vindex and an apply reports progress per shard.
func vitessSeed() map[string]string {
	return map[string]string{
		"vschema.json": `{
  "sharded": true,
  "vindexes": {
    "hash": {"type": "hash"}
  },
  "tables": {
    "customers": {"column_vindexes": [{"column": "id", "name": "hash"}]},
    "orders": {"column_vindexes": [{"column": "customer_id", "name": "hash"}]}
  }
}
`,
		"customers.sql": "CREATE TABLE customers (\n" +
			"  id bigint unsigned NOT NULL,\n" +
			"  email varchar(255) NOT NULL,\n" +
			"  PRIMARY KEY (id)\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci\n",
		"orders.sql": "CREATE TABLE orders (\n" +
			"  id bigint unsigned NOT NULL,\n" +
			"  customer_id bigint unsigned NOT NULL,\n" +
			"  status varchar(32) NOT NULL DEFAULT 'pending',\n" +
			"  PRIMARY KEY (id)\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci\n",
	}
}

// localscaleStartupTimeout bounds the wait for the embedded Vitess cluster.
// vtcombo starts its own mysqld and one tablet per shard, so it takes longer to
// answer than a single database server does.
const localscaleStartupTimeout = 5 * time.Minute

// waitForLocalscale blocks until the sample answers its health endpoint. It
// fails rather than waiting forever, and names the container so the operator
// can read why it did not come up.
func waitForLocalscale(ctx context.Context, name, apiURL string) error {
	readyCtx, cancel := context.WithTimeout(ctx, localscaleStartupTimeout)
	defer cancel()
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	var probeErr error
	for {
		probeErr = localscaleHealthy(readyCtx, apiURL)
		if probeErr == nil {
			return nil
		}
		select {
		case <-readyCtx.Done():
			return fmt.Errorf("sample %s did not become ready; inspect it with docker logs %s: %w", name, name, probeErr)
		case <-ticker.C:
		}
	}
}

func localscaleHealthy(ctx context.Context, apiURL string) error {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiURL+"/health", nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("health endpoint returned HTTP %d", resp.StatusCode)
	}
	return nil
}
