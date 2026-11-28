package localdemo

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
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

// sampleProxyPortStart and sampleProxyPorts are the proxy ports LocalScale
// allocates inside the container, and how many of them the sample publishes.
// LocalScale serves two things on this range: the keyspace's edge proxy, which
// is the vtgate endpoint, and one proxy per deploy branch, which is where an
// apply runs its DDL. A sample that published only its API would plan fine and
// fail every apply at "ping branch".
const (
	sampleProxyPortStart = 19100
	sampleProxyPorts     = 32
)

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
// publishes only its API and proxy ports, so SchemaBot's state cannot live
// inside it. The cluster itself is rebuilt from the seed schema whenever the
// container restarts, so the sample is a demonstration rather than a database
// to keep work in.
func ensureVitess(ctx context.Context, project, version string, progress Progress) (Database, error) {
	progress.step("Checking Docker")
	endpoint, err := requireLocalDocker(ctx)
	if err != nil {
		return Database{}, err
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
	needsCluster := strings.TrimSpace(string(listed)) == ""
	needsImage := false
	if needsCluster {
		present, err := imagePresent(ctx, image)
		if err != nil {
			return Database{}, err
		}
		needsImage = !present
	}

	// The cluster image and the state database are independent: one is
	// downloaded, the other is a second container that starts and seeds itself.
	// Download while the state database comes up, so the state database costs
	// nothing on the run where the download dominates setup.
	var pulled chan error
	cancelPull := context.CancelFunc(func() {})
	pullProgress := &gatedPull{report: progress.pull}
	if needsImage {
		var pullCtx context.Context
		pullCtx, cancelPull = context.WithCancel(ctx)
		defer cancelPull()
		pulled = make(chan error, 1)
		go func() { pulled <- pullImage(pullCtx, endpoint, image, pullProgress.observe) }()
	}

	storage, err := ensureSQL(ctx, sqlSample{
		project: project, engine: "mysql", label: "vitess-storage",
		seed:        "CREATE DATABASE schemabot;\n",
		createStage: "Creating SchemaBot's own database",
		seedStage:   "Preparing SchemaBot's own database",
	}, progress)
	if err != nil {
		return Database{}, errors.Join(
			fmt.Errorf("prepare the sample's state database: %w", err),
			abandonPull(pulled, cancelPull),
		)
	}

	if needsImage {
		// The step is reported here rather than when the download starts, so
		// the printed steps follow the state database's in the order setup
		// waits on them, and the download's progress shows only under its own
		// step.
		progress.step("Downloading " + image)
		pullProgress.show()
		if err = <-pulled; err != nil {
			return Database{}, err
		}
	}
	if needsCluster {
		// The API port and the proxy ports are reserved as one block, so the
		// proxy ports can never include the API port. The proxy ports are
		// published at an offset, so two projects can each run a sample without
		// either taking the fixed ports the compose stack binds.
		hostPort, err := reserveHostPortRange(ctx, sampleProxyPorts+1)
		if err != nil {
			return Database{}, err
		}
		proxyBase := hostPort + 1
		args := []string{
			"create", "--name", name,
			"--label", ownerLabel + "=" + name,
			"--label", "com.block.schemabot.engine=vitess",
			"--restart", "unless-stopped",
			"-p", fmt.Sprintf("127.0.0.1:%d:8080", hostPort),
			"-p", fmt.Sprintf("127.0.0.1:%d-%d:%d-%d", proxyBase, proxyBase+sampleProxyPorts-1,
				sampleProxyPortStart, sampleProxyPortStart+sampleProxyPorts-1),
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
		progress.step("Preparing sample keyspace and tables")
		proxyBase, err := sampleProxyBase(c)
		if err != nil {
			return Database{}, fmt.Errorf("read the proxy ports of new sample %s: %w", name, err)
		}
		if err = copyVitessSample(ctx, name, proxyBase); err != nil {
			return Database{}, err
		}
	}
	if !c.State.Running {
		progress.step("Starting your sample cluster")
		if _, err = run(ctx, nil, "start", name); err != nil {
			if c.State.Status == "created" {
				// A container that never started holds no data. Removing it
				// lets the retry reserve fresh ports instead of restarting the
				// same bindings that just failed.
				if _, rmErr := run(ctx, nil, "rm", name); rmErr != nil {
					return Database{}, fmt.Errorf("start new sample %s: %w; remove it with docker rm %s and retry setup (removing it failed: %w)", name, err, name, rmErr)
				}
				return Database{}, fmt.Errorf("start new sample %s: %w; resolve the Docker error and retry setup", name, err)
			}
			return Database{}, fmt.Errorf("start sample %s: %w; if its ports are in use, stop the process holding them and retry docker start %s; keep the container to preserve its saved connection", name, err, name)
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
	proxyPorts, err := sampleProxyPortMap(c)
	if err != nil {
		return Database{}, fmt.Errorf("read the published proxy ports of sample %s: %w", name, err)
	}
	result := Database{
		Name:         name,
		APIURL:       "http://" + net.JoinHostPort("127.0.0.1", bindings[0].HostPort),
		Organization: sampleOrganization,
		Token:        sampleVitessToken,
		StorageDSN:   storage.StorageDSN,
		Namespace:    sampleVitessDatabase,
	}

	progress.step("Waiting for the cluster to come up")
	if err := waitForLocalscale(ctx, name, result.APIURL); err != nil {
		return Database{}, err
	}
	// The sample's configuration already advertises each proxy port at its
	// published host port, from the moment the server starts, so a container
	// that Docker restarts on its own still hands out reachable addresses.
	// Posting the map read from Docker also covers an image that predates that
	// setting.
	if err := postSampleProxyPortMap(ctx, result.APIURL, proxyPorts); err != nil {
		return Database{}, err
	}
	// The database's edge proxy is its vtgate endpoint. The engine reads shards
	// and schema over it, which is what gives an apply its per-shard progress;
	// the DDL itself runs over the deploy branch's own proxy.
	result.DSN, err = sampleVtgateDSN(ctx, result.APIURL, proxyPorts)
	if err != nil {
		return Database{}, err
	}
	return result, nil
}

// sampleProxyBase reads the host port the container was created to publish its
// first proxy port on.
func sampleProxyBase(c container) (int, error) {
	bindings := c.HostConfig.PortBindings[strconv.Itoa(sampleProxyPortStart)+"/tcp"]
	if len(bindings) != 1 || bindings[0].HostIP != "127.0.0.1" {
		return 0, fmt.Errorf("proxy port %d must be published on one loopback-only port", sampleProxyPortStart)
	}
	port, err := strconv.Atoi(bindings[0].HostPort)
	if err != nil {
		return 0, fmt.Errorf("proxy port %d is published on %q: %w", sampleProxyPortStart, bindings[0].HostPort, err)
	}
	return port, nil
}

// abandonPull settles the cluster image download when setup stops before it
// needs the image. A download that already failed returns its own error, so
// both causes reach the user at once instead of the second only on the retry.
// One still running is cancelled and waited for, so no docker pull outlives
// setup; the error it then returns is the cancellation this function caused,
// which says nothing about the image, so it is not reported.
func abandonPull(pulled <-chan error, cancel context.CancelFunc) error {
	if pulled == nil {
		return nil
	}
	select {
	case err := <-pulled:
		return err
	default:
	}
	cancel()
	<-pulled
	return nil
}

// sampleProxyPortMap reads what host port each of the container's proxy ports
// is published on. Every port in the range must be there: a missing one would
// leave the API advertising an address only the container can reach.
func sampleProxyPortMap(c container) (map[int]int, error) {
	ports := make(map[int]int, sampleProxyPorts)
	for internal := sampleProxyPortStart; internal < sampleProxyPortStart+sampleProxyPorts; internal++ {
		bindings := c.NetworkSettings.Ports[strconv.Itoa(internal)+"/tcp"]
		if len(bindings) != 1 || bindings[0].HostIP != "127.0.0.1" {
			return nil, fmt.Errorf("proxy port %d is not published on loopback alone", internal)
		}
		host, err := strconv.Atoi(bindings[0].HostPort)
		if err != nil {
			return nil, fmt.Errorf("read the host port published for proxy port %d: %w", internal, err)
		}
		ports[internal] = host
	}
	return ports, nil
}

// postSampleProxyPortMap tells LocalScale which host port each of its proxy
// ports is published on, so the addresses it hands out are ones the engine can
// dial from the host.
func postSampleProxyPortMap(ctx context.Context, apiURL string, ports map[int]int) error {
	body, err := json.Marshal(ports)
	if err != nil {
		return fmt.Errorf("encode the sample's proxy port map: %w", err)
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, apiURL+"/admin/proxy-port-map", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("register the sample's proxy port map: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("register the sample's proxy port map: the cluster answered HTTP %d", resp.StatusCode)
	}
	return nil
}

// sampleVtgateDSN reads the sample database's edge proxy and returns it as a
// MySQL DSN on the host, so the engine can query vtgate for shards and schema.
func sampleVtgateDSN(ctx context.Context, apiURL string, ports map[int]int) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiURL+"/admin/edges", nil)
	if err != nil {
		return "", err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("read the sample's vtgate endpoint: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("read the sample's vtgate endpoint: the cluster answered HTTP %d", resp.StatusCode)
	}
	var edges map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&edges); err != nil {
		return "", fmt.Errorf("decode the sample's vtgate endpoint: %w", err)
	}
	database := sampleOrganization + "/" + sampleVitessDatabase
	addr, ok := edges[database]
	if !ok {
		return "", fmt.Errorf("the sample serves no vtgate endpoint for %s", database)
	}
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "", fmt.Errorf("read the sample's vtgate address %q: %w", addr, err)
	}
	internal, err := strconv.Atoi(port)
	if err != nil {
		return "", fmt.Errorf("read the sample's vtgate port %q: %w", port, err)
	}
	host, ok := ports[internal]
	if !ok {
		return "", fmt.Errorf("the sample's vtgate listens on port %d, outside the range it publishes", internal)
	}
	// The DSN names the keyspace, both because vtgate addresses a keyspace as
	// the database and because an environment's DSN has to name one.
	return fmt.Sprintf("root@tcp(%s)/%s", net.JoinHostPort("127.0.0.1", strconv.Itoa(host)), sampleVitessDatabase), nil
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
func copyVitessSample(ctx context.Context, name string, proxyBase int) error {
	dir, err := os.MkdirTemp("", "schemabot-sample-")
	if err != nil {
		return err
	}
	defer func() {
		if err := os.RemoveAll(dir); err != nil {
			slog.Warn("remove sample seed directory", "error", err)
		}
	}()
	config, err := json.MarshalIndent(localscaleConfig(proxyBase), "", "  ")
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
// shortened to what a local demo can wait for. proxyBase is the host port the
// first proxy port is published on.
func localscaleConfig(proxyBase int) map[string]any {
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
		"listen_addr":                ":8080",
		"schema_dir":                 localscaleSchemaDir,
		"proxy_advertise_host":       "127.0.0.1",
		"proxy_port_start":           sampleProxyPortStart,
		"proxy_port_end":             sampleProxyPortStart + sampleProxyPorts - 1,
		"proxy_advertise_port_start": proxyBase,
		"revert_window_duration":     "30m",
		"branch_creation_delay":      "1s",
		"deploy_request_delay":       "1s",
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
