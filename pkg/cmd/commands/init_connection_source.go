package commands

import (
	"crypto/sha256"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/block/mysql"
	"github.com/block/schemabot/pkg/localruntime"
	"github.com/block/schemabot/pkg/secrets"
)

func validInitConnectionReference(ref string) bool {
	return initVariable.MatchString(ref) || (strings.HasPrefix(ref, "file:") && filepath.IsAbs(strings.TrimPrefix(ref, "file:")))
}

func resolveInitConnection(ref string) (string, error) {
	if !validInitConnectionReference(ref) {
		return "", fmt.Errorf("use env:VARIABLE or file:/absolute/path to a file containing the connection string")
	}
	dsn, err := secrets.Resolve(ref, "")
	if err != nil {
		return "", fmt.Errorf("could not read the connection reference; check the file path and permissions")
	}
	if strings.TrimSpace(dsn) == "" {
		return "", fmt.Errorf("this connection is empty; choose another source or paste a connection string here")
	}
	return dsn, nil
}

func normalizeInitConnection(engine, value string) (string, error) {
	if engine == "mysql" && strings.HasPrefix(value, "mysql://") {
		u, err := url.Parse(value)
		if err != nil || u.Hostname() == "" || u.User == nil || u.Fragment != "" {
			return "", fmt.Errorf("check the MySQL connection string format")
		}
		port := u.Port()
		if port == "" {
			port = "3306"
		}
		cfg := mysql.NewConfig()
		cfg.User = u.User.Username()
		cfg.Passwd, _ = u.User.Password()
		cfg.Net = "tcp"
		cfg.Addr = net.JoinHostPort(u.Hostname(), port)
		cfg.DBName = strings.TrimPrefix(u.Path, "/")
		value = cfg.FormatDSN()
		if u.RawQuery != "" {
			value += "?" + u.RawQuery
		}
	}
	return value, nil
}

// Persist only after the final review. Stable identities make retries reuse the
// same reference; a changed secret cannot overwrite an existing runtime's file.
func saveInitConnection(runtime, database, environment, purpose, dsn string) (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	parent := filepath.Join(home, ".schemabot")
	if err := os.MkdirAll(parent, 0700); err != nil {
		return "", err
	}
	info, err := os.Lstat(parent)
	if err != nil {
		return "", err
	}
	if !info.IsDir() {
		return "", fmt.Errorf("credential parent must be a directory, not a symlink")
	}
	dir := filepath.Join(parent, "credentials")
	if err := os.Mkdir(dir, 0700); err != nil && !os.IsExist(err) {
		return "", err
	}
	info, err = os.Lstat(dir)
	if err != nil {
		return "", err
	}
	if !info.IsDir() || info.Mode().Perm()&0077 != 0 {
		return "", fmt.Errorf("credential directory must be private (0700) and not a symlink")
	}
	id := sha256.Sum256([]byte(runtime + "\x00" + database + "\x00" + environment + "\x00" + purpose))
	path := filepath.Join(dir, fmt.Sprintf("%x.dsn", id[:16]))
	f, err := os.CreateTemp(dir, ".connection-*")
	if err != nil {
		return "", err
	}
	defer func() {
		if err := os.Remove(f.Name()); err != nil {
			slog.Debug("remove staged connection file", "error", err)
		}
	}()
	_, writeErr := f.WriteString(dsn)
	if writeErr == nil {
		writeErr = f.Sync()
	}
	closeErr := f.Close()
	if writeErr != nil {
		return "", fmt.Errorf("save private connection: %w", writeErr)
	}
	if closeErr != nil {
		return "", closeErr
	}
	// Publish complete content without replacing another setup's credential.
	err = os.Link(f.Name(), path)
	if os.IsExist(err) {
		saved, readErr := localruntime.ReadPrivate(path)
		if readErr != nil {
			return "", fmt.Errorf("could not read existing private connection file")
		}
		if string(saved) != dsn {
			return "", fmt.Errorf("a different connection is already saved for this runtime and database; use a different runtime or an explicit connection reference")
		}
		return "file:" + path, nil
	}
	if err != nil {
		return "", err
	}
	return "file:" + path, nil
}
