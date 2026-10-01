// Package localstorage manages an explicitly selected local state database.
// It never provisions or modifies the application's database.
package localstorage

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"os"

	"path/filepath"
	"strings"
	"time"

	"github.com/block/mysql"
	"github.com/block/schemabot/pkg/localdocker"
	"github.com/block/schemabot/pkg/localruntime"
	"github.com/block/schemabot/pkg/mysqlconn"
	"github.com/block/spirit/pkg/utils"
)

const ownerLabel = "com.block.schemabot.storage"
const image = "mysql:8.4"

type record struct{ Name, ID, Volume, DSN string }
type container struct {
	ID     string
	Config struct {
		Labels map[string]string
		Env    []string
	}
	State           struct{ Running bool }
	Mounts          []struct{ Name, Destination string }
	NetworkSettings struct {
		Ports map[string][]struct{ HostIP, HostPort string }
	}
}

// Prepare creates storage only on explicit initialization. The persisted record
// makes later invocations resume the same container rather than bootstrap anew.
func Prepare(ctx context.Context, dir string, progress ...func(string)) (string, error) {
	report := func(stage string) {
		for _, notify := range progress {
			if notify != nil {
				notify(stage)
			}
		}
	}
	report("Checking Docker")
	if err := os.MkdirAll(dir, 0700); err != nil {
		return "", err
	}
	info, err := os.Lstat(dir)
	if err != nil {
		return "", err
	}
	if !info.IsDir() || info.Mode().Perm()&0077 != 0 {
		return "", fmt.Errorf("runtime directory must be private")
	}
	path := filepath.Join(dir, "docker-storage.json")
	if _, err := localruntime.ReadPrivate(path); err == nil {
		if err := Resume(ctx, dir); err != nil {
			return "", err
		}
		return "file:" + filepath.Join(dir, "docker-storage.dsn"), nil
	} else if !os.IsNotExist(err) {
		return "", err
	}
	// Do not add a new authority beside an already registered runtime.
	if _, err := os.Stat(filepath.Join(dir, "runtime.yaml")); err == nil {
		return "", fmt.Errorf("this runtime already has storage; use its existing connection or choose a new --runtime")
	} else if !os.IsNotExist(err) {
		return "", err
	}
	if err := localdocker.Check(ctx); err != nil {
		return "", err
	}
	sum := sha256.Sum256([]byte(dir))
	name := fmt.Sprintf("schemabot-state-%x", sum[:8])
	volume := name + "-data"
	// Credentials are saved before Docker creation, so retries use the same secret.
	passwordPath := filepath.Join(dir, "docker-storage.password")
	secret := make([]byte, 24)
	if _, err := rand.Read(secret); err != nil {
		return "", err
	}
	if err := writeOnce(passwordPath, []byte(hex.EncodeToString(secret))); err != nil {
		return "", err
	}
	password, err := localruntime.ReadPrivate(passwordPath)
	if err != nil {
		return "", err
	}
	volumes, err := localdocker.Run(ctx, nil, "volume", "ls", "--filter", "name=^"+volume+"$", "--format", "{{.Name}}")
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(string(volumes)) == "" {
		if _, err := localdocker.Run(ctx, nil, "volume", "create", "--label", ownerLabel+"="+name, volume); err != nil {
			return "", err
		}
	}
	label, err := localdocker.Run(ctx, nil, "volume", "inspect", "--format", "{{index .Labels \""+ownerLabel+"\"}}", volume)
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(string(label)) != name {
		return "", fmt.Errorf("volume %s is not owned by this runtime", volume)
	}
	listed, err := localdocker.Run(ctx, nil, "container", "ls", "-a", "--filter", "name=^/"+name+"$", "--format", "{{.ID}}")
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(string(listed)) == "" {
		report("Preparing " + image + " (downloading if needed)")
		if err := localdocker.EnsureImage(ctx, image); err != nil {
			return "", err
		}
		listener, err := new(net.ListenConfig).Listen(ctx, "tcp4", "127.0.0.1:0")
		if err != nil {
			return "", err
		}
		port := listener.Addr().(*net.TCPAddr).Port
		if err := listener.Close(); err != nil {
			return "", err
		}
		_, err = localdocker.Run(ctx, []string{"MYSQL_ROOT_PASSWORD=" + string(password)}, "create", "--name", name, "--label", ownerLabel+"="+name, "--restart", "unless-stopped", "-p", fmt.Sprintf("127.0.0.1:%d:3306", port), "--mount", "type=volume,source="+volume+",target=/var/lib/mysql", "-e", "MYSQL_ROOT_PASSWORD", "-e", "MYSQL_DATABASE=schemabot", image)
		if err != nil {
			return "", err
		}
	}
	c, err := inspect(ctx, name)
	if err != nil {
		return "", err
	}
	if err := owned(c, name, volume); err != nil {
		return "", err
	}
	if !c.State.Running {
		report("Starting your local state database")
		if _, err := localdocker.Run(ctx, nil, "start", name); err != nil {
			return "", err
		}
	}
	c, err = inspect(ctx, name)
	if err != nil {
		return "", err
	}
	address, err := endpoint(c)
	if err != nil {
		return "", err
	}
	cfg := mysql.NewConfig()
	cfg.User = "root"
	cfg.Passwd = string(password)
	cfg.Net = "tcp"
	cfg.Addr = address
	cfg.DBName = "schemabot"
	cfg.ParseTime = true
	dsn := cfg.FormatDSN()
	report("Waiting for the state database to accept connections")
	if err := ready(ctx, name, dsn); err != nil {
		return "", err
	}
	dsnPath := filepath.Join(dir, "docker-storage.dsn")
	if err := writeOnce(dsnPath, []byte(dsn)); err != nil {
		return "", err
	}
	saved, err := localruntime.ReadPrivate(dsnPath)
	if err != nil {
		return "", err
	}
	if string(saved) != dsn {
		return "", fmt.Errorf("local storage connection changed; existing state was preserved")
	}
	data, err := json.Marshal(record{Name: name, ID: c.ID, Volume: volume, DSN: dsnPath})
	if err != nil {
		return "", err
	}
	if err := writeOnce(path, data); err != nil {
		return "", err
	}
	return "file:" + dsnPath, nil
}

// Resume never creates a container, volume, or database. Missing state requires
// recovery by the user, never an automatic replacement with an empty database.
func Resume(ctx context.Context, dir string) error {
	data, err := localruntime.ReadPrivate(filepath.Join(dir, "docker-storage.json"))
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	var r record
	if json.Unmarshal(data, &r) != nil || r.Name == "" || r.ID == "" || r.DSN != filepath.Join(dir, "docker-storage.dsn") {
		return fmt.Errorf("invalid local storage registration")
	}
	if err := localdocker.Check(ctx); err != nil {
		return err
	}
	label, err := localdocker.Run(ctx, nil, "volume", "inspect", "--format", "{{index .Labels \""+ownerLabel+"\"}}", r.Volume)
	if err != nil {
		return fmt.Errorf("local state volume is missing; restore it before retrying: %w", err)
	}
	if strings.TrimSpace(string(label)) != r.Name {
		return fmt.Errorf("local state volume ownership changed")
	}
	c, err := inspect(ctx, r.ID)
	if err != nil {
		return fmt.Errorf("local state database is missing or unavailable; restore its container and volume before retrying: %w", err)
	}
	if err := owned(c, r.Name, r.Volume); err != nil {
		return err
	}
	if !c.State.Running {
		if _, err := localdocker.Run(ctx, nil, "start", r.ID); err != nil {
			return err
		}
	}
	c, err = inspect(ctx, r.ID)
	if err != nil {
		return err
	}
	address, err := endpoint(c)
	if err != nil {
		return err
	}
	dsn, err := localruntime.ReadPrivate(r.DSN)
	if err != nil {
		return err
	}
	cfg, err := mysql.ParseDSN(string(dsn))
	if err != nil || cfg.Addr != address {
		return fmt.Errorf("local storage endpoint changed; refusing to redirect state")
	}
	return ready(ctx, r.ID, string(dsn))
}
func owned(c container, name, volume string) error {
	if c.Config.Labels[ownerLabel] != name {
		return fmt.Errorf("container is not owned by this runtime")
	}
	for _, m := range c.Mounts {
		if m.Destination == "/var/lib/mysql" && m.Name == volume {
			return nil
		}
	}
	return fmt.Errorf("local storage volume does not match its registration")
}
func endpoint(c container) (string, error) {
	p := c.NetworkSettings.Ports["3306/tcp"]
	if len(p) != 1 || p[0].HostIP != "127.0.0.1" || p[0].HostPort == "" {
		return "", fmt.Errorf("local storage requires one loopback-only port")
	}
	return net.JoinHostPort("127.0.0.1", p[0].HostPort), nil
}
func inspect(ctx context.Context, id string) (container, error) {
	b, err := localdocker.Run(ctx, nil, "inspect", id)
	if err != nil {
		return container{}, err
	}
	var list []container
	if json.Unmarshal(b, &list) != nil || len(list) != 1 {
		return container{}, fmt.Errorf("cannot read local storage container")
	}
	return list[0], nil
}
func ready(ctx context.Context, name, dsn string) error {
	ctx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	db, err := mysqlconn.Open(dsn)
	if err != nil {
		return fmt.Errorf("cannot open local state connection")
	}
	defer utils.CloseAndLog(db)
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		probe, cancel := context.WithTimeout(ctx, time.Second)
		err = localdocker.ProbeDatabase(probe, name, "mysql")
		if err == nil {
			err = db.PingContext(probe)
		}
		cancel()
		if err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("local state database did not become ready; check Docker and retry: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}
func writeOnce(path string, data []byte) error {
	f, err := os.CreateTemp(filepath.Dir(path), ".storage-*")
	if err != nil {
		return err
	}
	defer func() {
		if err := os.Remove(f.Name()); err != nil {
			slog.Warn("remove temporary storage file", "error", err)
		}
	}()
	if _, err := f.Write(data); err != nil {
		utils.CloseAndLog(f)
		return err
	}
	if err := f.Sync(); err != nil {
		utils.CloseAndLog(f)
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Link(f.Name(), path); err != nil && !os.IsExist(err) {
		return err
	}
	return nil
}

// ValidateReference prevents a setup retry from abandoning managed durable state.
func ValidateReference(dir, ref string) error {
	_, err := localruntime.ReadPrivate(filepath.Join(dir, "docker-storage.json"))
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if ref != "file:"+filepath.Join(dir, "docker-storage.dsn") {
		return fmt.Errorf("this runtime owns local Docker storage; use --local-storage or choose a new --runtime")
	}
	return nil
}
