// Package localdocker provides Docker operations for local SchemaBot databases.
package localdocker

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"time"
	"unicode/utf8"
)

// Run executes Docker and redacts supplied credentials from diagnostics.
func Run(ctx context.Context, env []string, args ...string) ([]byte, error) {
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

// IsLocalEndpoint reports whether published loopback ports belong to this computer.
func IsLocalEndpoint(endpoint string) bool {
	if strings.HasPrefix(endpoint, "unix://") || strings.HasPrefix(endpoint, "npipe://") {
		return true
	}
	u, err := url.Parse(endpoint)
	return err == nil && u.Scheme == "tcp" && (u.Hostname() == "localhost" || u.Hostname() == "127.0.0.1" || u.Hostname() == "::1")
}

// Check verifies a local Docker daemon is available.
func Check(ctx context.Context) error {
	endpoint := os.Getenv("DOCKER_HOST")
	if endpoint == "" || os.Getenv("DOCKER_CONTEXT") != "" {
		value, err := Run(ctx, nil, "context", "inspect", "--format", "{{.Endpoints.docker.Host}}")
		if err != nil {
			return err
		}
		endpoint = strings.TrimSpace(string(value))
	}
	if !IsLocalEndpoint(endpoint) {
		return fmt.Errorf("setup needs a local Docker context; your current Docker endpoint is remote")
	}
	_, err := Run(ctx, nil, "info", "--format", "{{.ServerVersion}}")
	return err
}

// EnsureImage downloads an absent image before allocating its host port.
func EnsureImage(ctx context.Context, image string) error {
	images, err := Run(ctx, nil, "image", "ls", "--quiet", image)
	if err != nil {
		return fmt.Errorf("inspect image %s: %w", image, err)
	}
	if strings.TrimSpace(string(images)) == "" {
		if _, err := Run(ctx, nil, "pull", image); err != nil {
			return fmt.Errorf("pull image %s: %w", image, err)
		}
	}
	return nil
}

// Probe inside the container until first-boot initialization finishes. Docker's
// published port can accept a TCP connection before MySQL can greet a client.
func ProbeDatabase(ctx context.Context, name, engine string) error {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	args := []string{"exec", name, "pg_isready", "-q", "-h", "127.0.0.1", "-U", "postgres", "-d", "schemabot"}
	if engine == "mysql" {
		args = []string{"exec", name, "sh", "-c", `MYSQL_PWD="$MYSQL_ROOT_PASSWORD" mysql -uroot -h127.0.0.1 -Dschemabot -Nse 'SELECT 1'`}
	}
	_, err := Run(ctx, nil, args...)
	return err
}
