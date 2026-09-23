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
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/block/mysql"
	"github.com/block/schemabot/pkg/mysqlconn"
	"github.com/block/schemabot/pkg/postgresconn"
	"github.com/block/spirit/pkg/utils"
)

const ownerLabel = "com.block.schemabot.sample"

type Database struct {
	Name       string
	DSN        string
	StorageDSN string
	Namespace  string
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
}

// Ensure starts or reuses a sample owned by this project and engine. Docker keeps
// its data across restarts; retrying setup never re-seeds an existing database.
func Ensure(ctx context.Context, project, engine string) (Database, error) {
	if engine != "mysql" && engine != "postgres" {
		return Database{}, fmt.Errorf("sample databases support mysql or postgres")
	}
	project, err := filepath.Abs(project)
	if err != nil {
		return Database{}, err
	}
	project, err = filepath.EvalSymlinks(project)
	if err != nil {
		return Database{}, err
	}
	sum := sha256.Sum256([]byte(project + "\x00" + engine))
	name := fmt.Sprintf("schemabot-sample-%x", sum[:6])
	image, port, passwordKey := "mysql:8.4", "3306/tcp", "MYSQL_ROOT_PASSWORD"
	env := []string{"MYSQL_DATABASE=shop"}
	if engine == "postgres" {
		image, port, passwordKey = "postgres:17", "5432/tcp", "POSTGRES_PASSWORD"
		env = []string{"POSTGRES_DB=shop"}
	}
	endpoint := os.Getenv("DOCKER_HOST")
	if endpoint == "" || os.Getenv("DOCKER_CONTEXT") != "" {
		value, err := run(ctx, nil, "context", "inspect", "--format", "{{.Endpoints.docker.Host}}")
		if err != nil {
			return Database{}, err
		}
		endpoint = strings.TrimSpace(string(value))
	}
	if !localDockerEndpoint(endpoint) {
		return Database{}, fmt.Errorf("sample setup needs a local Docker context; your current Docker endpoint is remote")
	}
	if _, err = run(ctx, nil, "info", "--format", "{{.ServerVersion}}"); err != nil {
		return Database{}, fmt.Errorf("start Docker, then try sample setup again: %w", err)
	}
	// Listing distinguishes an absent container from a broken Docker connection.
	listed, err := run(ctx, nil, "container", "ls", "-a", "--filter", "name=^/"+name+"$", "--format", "{{.ID}}")
	if err != nil {
		return Database{}, err
	}
	if strings.TrimSpace(string(listed)) == "" {
		password := make([]byte, 24)
		if _, err = rand.Read(password); err != nil {
			return Database{}, err
		}
		keyValue := passwordKey + "=" + hex.EncodeToString(password)
		args := []string{"create", "--name", name, "--label", ownerLabel + "=" + name, "--label", "com.block.schemabot.engine=" + engine, "--restart", "unless-stopped", "-p", "127.0.0.1::" + strings.TrimSuffix(port, "/tcp"), "-e", passwordKey}
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
	if c.Config.Labels[ownerLabel] != name || c.Config.Labels["com.block.schemabot.engine"] != engine {
		return Database{}, fmt.Errorf("container %s is not this project's sample; refusing to use it", name)
	}
	if c.State.Status == "created" {
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
		if err = os.WriteFile(seed, []byte(seedSQL(engine)), 0644); err != nil {
			return Database{}, err
		}
		if _, err = run(ctx, nil, "cp", seed, name+":/docker-entrypoint-initdb.d/01-sample.sql"); err != nil {
			return Database{}, err
		}
	}
	if !c.State.Running {
		if _, err = run(ctx, nil, "start", name); err != nil {
			return Database{}, err
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
	readyCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		if containerReady(readyCtx, name, engine) && ready(readyCtx, engine, result.StorageDSN) == nil {
			return result, nil
		}
		select {
		case <-readyCtx.Done():
			return Database{}, fmt.Errorf("sample %s did not become ready; inspect it with docker logs %s: %w", name, name, readyCtx.Err())
		case <-ticker.C:
		}
	}
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
func containerReady(ctx context.Context, name, engine string) bool {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	args := []string{"exec", name, "pg_isready", "-q", "-h", "127.0.0.1", "-U", "postgres", "-d", "schemabot"}
	if engine == "mysql" {
		args = []string{"exec", name, "sh", "-c", `MYSQL_PWD="$MYSQL_ROOT_PASSWORD" mysql -uroot -h127.0.0.1 -Dschemabot -Nse 'SELECT 1'`}
	}
	_, err := run(ctx, nil, args...)
	return err == nil
}
