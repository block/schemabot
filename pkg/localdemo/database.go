// Package localdemo provisions disposable sample databases for first-time users.
// It never connects to or modifies a database supplied by the user.
package localdemo

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/block/mysql"
	"github.com/block/schemabot/pkg/mysqlconn"
	"github.com/block/schemabot/pkg/postgresconn"
	"github.com/block/spirit/pkg/utils"
)

// NamePrefix identifies project-owned sample runtimes and containers.
const NamePrefix = "schemabot-sample-"

const ownerLabel = "com.block.schemabot.sample"

// Database describes a provisioned sample and how to reach it. A SQL sample
// carries a DSN; a Vitess sample carries the PlanetScale-compatible endpoint
// its engine addresses instead, along with the organization and token that
// endpoint expects.
type Database struct {
	Name       string
	DSN        string
	StorageDSN string
	Namespace  string

	// Set for a Vitess sample only. APIURL is the PlanetScale-compatible
	// endpoint, Organization the organization holding the sample database, and
	// Token the service token credential in PlanetScale's name:value form.
	APIURL       string
	Organization string
	Token        string
}

type container struct {
	ID     string
	Config struct {
		Labels map[string]string
		Env    []string
	}
	State struct {
		Running bool
		Status  string
	}
	NetworkSettings struct {
		Ports map[string][]struct{ HostIP, HostPort string }
	}
	// HostConfig holds the bindings the container was created with, which
	// Docker reports before the container first starts.
	HostConfig struct {
		PortBindings map[string][]struct{ HostIP, HostPort string }
	}
}

// Ensure starts or reuses a sample owned by this project and engine. Docker keeps
// its data across restarts; retrying setup never re-seeds an existing database.
func Ensure(ctx context.Context, project, engine, version string, progress Progress) (Database, error) {
	switch engine {
	case "mysql", "postgres":
		return ensureSQL(ctx, sqlSample{
			project: project, engine: engine, label: engine,
			seed:        seedSQL(engine),
			createStage: "Creating your sample database",
			seedStage:   "Preparing sample tables",
		}, progress)
	case "vitess":
		return ensureVitess(ctx, project, version, progress)
	}
	return Database{}, fmt.Errorf("sample databases support mysql, postgres, or vitess")
}

// sqlSample describes one SQL container to provision. engine picks the image
// and connection-string shape; label is the container's identity, which differs
// from the engine when the container is one component of a larger sample rather
// than the sample itself.
type sqlSample struct {
	project string
	engine  string
	label   string
	seed    string
	// createStage and seedStage are the steps shown while the container is
	// created and while its seed is copied in.
	createStage string
	seedStage   string
}

// ensureSQL provisions one SQL container: the engine's own server, seeded on
// first creation and reused on every later run.
func ensureSQL(ctx context.Context, sample sqlSample, progress Progress) (Database, error) {
	engine := sample.engine
	name, err := sampleName(sample.project, sample.label)
	if err != nil {
		return Database{}, err
	}
	image, port, passwordKey := "mysql:8.4", "3306/tcp", "MYSQL_ROOT_PASSWORD"
	env := []string{"MYSQL_DATABASE=shop"}
	if engine == "postgres" {
		image, port, passwordKey = "postgres:17", "5432/tcp", "POSTGRES_PASSWORD"
		env = []string{"POSTGRES_DB=shop"}
	}
	progress.step("Checking Docker")
	endpoint, err := requireLocalDocker(ctx)
	if err != nil {
		return Database{}, err
	}
	// Listing distinguishes an absent container from a broken Docker connection.
	listed, err := run(ctx, nil, "container", "ls", "-a", "--filter", "name=^/"+name+"$", "--format", "{{.ID}}")
	if err != nil {
		return Database{}, err
	}
	if strings.TrimSpace(string(listed)) == "" {
		present, err := imagePresent(ctx, image)
		if err != nil {
			return Database{}, err
		}
		if !present {
			progress.step("Downloading " + image)
			if err = pullImage(ctx, endpoint, image, progress.pull); err != nil {
				return Database{}, err
			}
		}
		progress.step(sample.createStage)
		password := make([]byte, 24)
		if _, err = rand.Read(password); err != nil {
			return Database{}, err
		}
		hostPort, err := reserveHostPort(ctx)
		if err != nil {
			return Database{}, err
		}
		keyValue := passwordKey + "=" + hex.EncodeToString(password)
		args := []string{"create", "--name", name, "--label", ownerLabel + "=" + name, "--label", "com.block.schemabot.engine=" + sample.label, "--restart", "unless-stopped", "-p", fmt.Sprintf("127.0.0.1:%d:%s", hostPort, strings.TrimSuffix(port, "/tcp")), "-e", passwordKey}
		for _, v := range env {
			args = append(args, "-e", v)
		}
		args = append(args, image)
		if _, err = run(ctx, []string{keyValue}, args...); err != nil {
			return Database{}, err
		}
	}
	c, err := inspect(ctx, name)
	if err != nil {
		return Database{}, err
	}
	if c.Config.Labels[ownerLabel] != name || c.Config.Labels["com.block.schemabot.engine"] != sample.label {
		return Database{}, fmt.Errorf("container %s is not this project's sample; refusing to use it", name)
	}
	if c.State.Status == "created" {
		progress.step(sample.seedStage)
		dir, err := os.MkdirTemp("", "schemabot-sample-")
		if err != nil {
			return Database{}, err
		}
		defer func() {
			if err := os.RemoveAll(dir); err != nil {
				slog.Warn("remove sample seed directory", "error", err)
			}
		}()
		seed := filepath.Join(dir, "sample.sql")
		if err = os.WriteFile(seed, []byte(sample.seed), 0644); err != nil {
			return Database{}, err
		}
		if _, err = run(ctx, nil, "cp", seed, name+":/docker-entrypoint-initdb.d/01-sample.sql"); err != nil {
			return Database{}, err
		}
	}
	if !c.State.Running {
		progress.step("Starting your sample database")
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
	bindings := c.NetworkSettings.Ports[port]
	if len(bindings) != 1 || bindings[0].HostIP != "127.0.0.1" {
		return Database{}, fmt.Errorf("sample %s must publish one loopback-only port", name)
	}
	password := ""
	for _, entry := range c.Config.Env {
		if v, ok := strings.CutPrefix(entry, passwordKey+"="); ok {
			password = v
		}
	}
	if password == "" {
		return Database{}, fmt.Errorf("sample %s has no database credential", name)
	}
	address := net.JoinHostPort("127.0.0.1", bindings[0].HostPort)
	result := Database{Name: name, Namespace: "shop"}
	connection := func(database string) string {
		if engine == "postgres" {
			u := url.URL{Scheme: "postgres", Host: address, User: url.UserPassword("postgres", password), Path: "/" + database, RawQuery: "sslmode=disable"}
			return u.String()
		}
		cfg := mysql.NewConfig()
		cfg.User = "root"
		cfg.Passwd = password
		cfg.Net = "tcp"
		cfg.Addr = address
		cfg.DBName = database
		cfg.ParseTime = true
		return cfg.FormatDSN()
	}
	result.DSN = connection("shop")
	result.StorageDSN = connection("schemabot")
	if engine == "postgres" {
		result.Namespace = "public"
	}
	progress.step("Waiting for the database to accept connections")
	readyCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	var probeErr error
	for {
		probeErr = containerReady(readyCtx, name, engine)
		if probeErr == nil {
			probeErr = ready(readyCtx, engine, result.StorageDSN)
		}
		if probeErr == nil {
			return result, nil
		}
		select {
		case <-readyCtx.Done():
			return Database{}, fmt.Errorf("sample %s did not become ready; inspect it with docker logs %s: %w", name, name, errors.Join(readyCtx.Err(), probeErr))
		case <-ticker.C:
		}
	}
}

// requireLocalDocker refuses a remote Docker endpoint before anything is
// created. A sample publishes its ports on loopback and is reached there, so a
// remote daemon would produce a container nothing on this machine can connect
// to.
func requireLocalDocker(ctx context.Context) (string, error) {
	endpoint := os.Getenv("DOCKER_HOST")
	if endpoint == "" || os.Getenv("DOCKER_CONTEXT") != "" {
		value, err := run(ctx, nil, "context", "inspect", "--format", "{{.Endpoints.docker.Host}}")
		if err != nil {
			return "", err
		}
		endpoint = strings.TrimSpace(string(value))
	}
	if !localDockerEndpoint(endpoint) {
		return "", fmt.Errorf("sample setup needs a local Docker context; your current Docker endpoint is remote")
	}
	if _, err := run(ctx, nil, "info", "--format", "{{.ServerVersion}}"); err != nil {
		return "", fmt.Errorf("start Docker, then try sample setup again: %w", err)
	}
	return endpoint, nil
}

// imagePresent reports whether the daemon already holds image. An image it
// holds is never downloaded again, so repeated setup on a machine that is
// offline still works.
func imagePresent(ctx context.Context, image string) (bool, error) {
	images, err := run(ctx, nil, "image", "ls", "--quiet", image)
	if err != nil {
		return false, fmt.Errorf("inspect sample image %s: %w", image, err)
	}
	return strings.TrimSpace(string(images)) != "", nil
}

// reserveHostPort picks a free loopback port and releases it for Docker to
// claim. Pinning the port keeps a saved connection valid across stop, start,
// and daemon restarts. Docker claims it after the listener closes; losing that
// race fails startup rather than binding something else.
func reserveHostPort(ctx context.Context) (int, error) {
	listener, err := new(net.ListenConfig).Listen(ctx, "tcp4", "127.0.0.1:0")
	if err != nil {
		return 0, fmt.Errorf("choose sample port: %w", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	if err := listener.Close(); err != nil {
		return 0, err
	}
	return port, nil
}

// reserveHostPortRange picks a block of count consecutive free loopback ports.
// The kernel assigns ports one at a time, so a block is found by holding one
// and testing the rest beside it; a block that cannot be completed is retried
// from a different starting port rather than returned part-formed.
func reserveHostPortRange(ctx context.Context, count int) (int, error) {
	const attempts = 20
	var lastErr error
	for range attempts {
		start, err := reserveHostPort(ctx)
		if err != nil {
			return 0, err
		}
		if start+count-1 > 65535 {
			lastErr = fmt.Errorf("port %d leaves no room for %d ports", start, count)
			continue
		}
		held := make([]net.Listener, 0, count)
		var cfg net.ListenConfig
		for port := start; port < start+count; port++ {
			listener, err := cfg.Listen(ctx, "tcp4", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)))
			if err != nil {
				lastErr = err
				break
			}
			held = append(held, listener)
		}
		taken := len(held)
		for _, listener := range held {
			if err := listener.Close(); err != nil {
				return 0, err
			}
		}
		if taken == count {
			return start, nil
		}
	}
	return 0, fmt.Errorf("choose %d consecutive sample ports: %w", count, lastErr)
}

func ready(ctx context.Context, engine, dsn string) error {
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	var db *sql.DB
	var err error
	if engine == "mysql" {
		db, err = mysqlconn.Open(dsn)
	} else {
		db, err = postgresconn.Open(dsn)
	}
	if err != nil {
		return err
	}
	defer utils.CloseAndLog(db)
	return db.PingContext(ctx)
}

func inspect(ctx context.Context, name string) (container, error) {
	b, err := run(ctx, nil, "inspect", name)
	if err != nil {
		return container{}, err
	}
	var values []container
	if err = json.Unmarshal(b, &values); err != nil {
		return container{}, fmt.Errorf("read sample container: %w", err)
	}
	if len(values) != 1 {
		return container{}, fmt.Errorf("expected one sample container")
	}
	return values[0], nil
}

func run(ctx context.Context, env []string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, "docker", args...)
	cmd.Env = append(os.Environ(), env...)
	output, err := cmd.Output()
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			detail := strings.TrimSpace(string(exitErr.Stderr))
			// Docker may repeat environment arguments in a diagnostic. Never expose credentials.
			for _, entry := range env {
				if _, value, ok := strings.Cut(entry, "="); ok && value != "" {
					detail = strings.ReplaceAll(detail, value, "[redacted]")
				}
			}
			detail = strings.Map(func(r rune) rune {
				if r < 32 && r != '\n' && r != '\t' {
					return -1
				}
				return r
			}, detail)
			if len(detail) > 2048 {
				end := 2048
				for !utf8.RuneStart(detail[end]) {
					end--
				}
				detail = detail[:end] + "…"
			}
			if detail != "" {
				return nil, fmt.Errorf("docker %s: %s: %w", args[0], detail, err)
			}
		}
		return nil, fmt.Errorf("docker %s failed; check Docker and retry: %w", args[0], err)
	}
	return output, nil
}

func seedSQL(engine string) string {
	if engine == "postgres" {
		return `CREATE DATABASE schemabot;
CREATE TABLE customers (id bigint PRIMARY KEY, email varchar(255) NOT NULL);
CREATE TABLE orders (id bigint PRIMARY KEY, customer_id bigint NOT NULL, status varchar(32) NOT NULL DEFAULT 'pending');
INSERT INTO customers VALUES (1, 'alex@example.com');
INSERT INTO orders VALUES (1, 1, 'pending');
`
	}
	return "CREATE DATABASE schemabot;\nUSE shop;\n" + `CREATE TABLE customers (id bigint unsigned NOT NULL, email varchar(255) NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
CREATE TABLE orders (id bigint unsigned NOT NULL, customer_id bigint unsigned NOT NULL, status varchar(32) NOT NULL DEFAULT 'pending', PRIMARY KEY (id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
INSERT INTO customers VALUES (1, 'alex@example.com');
INSERT INTO orders VALUES (1, 1, 'pending');
`
}

func localDockerEndpoint(endpoint string) bool {
	if strings.HasPrefix(endpoint, "unix://") || strings.HasPrefix(endpoint, "npipe://") {
		return true
	}
	u, err := url.Parse(endpoint)
	return err == nil && u.Scheme == "tcp" && (u.Hostname() == "localhost" || u.Hostname() == "127.0.0.1" || u.Hostname() == "::1")
}

// Probe inside the container until first-boot initialization finishes. Docker's
// published port can accept a TCP connection before MySQL can greet a client.
func containerReady(ctx context.Context, name, engine string) error {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	args := []string{"exec", name, "pg_isready", "-q", "-h", "127.0.0.1", "-U", "postgres", "-d", "schemabot"}
	if engine == "mysql" {
		args = []string{"exec", name, "sh", "-c", `MYSQL_PWD="$MYSQL_ROOT_PASSWORD" mysql -uroot -h127.0.0.1 -Dschemabot -Nse 'SELECT 1'`}
	}
	_, err := run(ctx, nil, args...)
	return err
}

func sampleName(project, engine string) (string, error) {
	project, err := filepath.Abs(project)
	if err != nil {
		return "", err
	}
	project, err = filepath.EvalSymlinks(project)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256([]byte(project + "\x00" + engine))
	return fmt.Sprintf("%s%x", NamePrefix, sum[:6]), nil
}
