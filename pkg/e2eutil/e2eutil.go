// Package e2eutil provides shared test utilities for SchemaBot e2e tests.
// This package is intended for use by both the SchemaBot repo and downstream
// consumers that need to build the CLI, run commands, and set up schema directories.
//
// This package has zero non-stdlib dependencies — import it freely.
package e2eutil

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"testing"
)

// BuildCLI builds the SchemaBot CLI binary from pkg and returns the binary path.
// The moduleDir should be the root of a Go module containing the schemabot dependency
// (or the schemabot repo itself). The pkg argument is the Go package to build
// (e.g., "github.com/block/schemabot/pkg/cmd" or "./pkg/cmd").
func BuildCLI(t *testing.T, moduleDir, pkg string) string {
	t.Helper()
	binDir := t.TempDir()
	binPath := filepath.Join(binDir, "schemabot")

	cmd := exec.CommandContext(t.Context(), "go", "build", "-o", binPath, pkg)
	cmd.Dir = moduleDir
	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		t.Fatalf("build schemabot CLI: %v: %s", err, stderr.String())
	}
	return binPath
}

// CLIFinder locates or builds the SchemaBot CLI binary, caching the result
// across calls via sync.Once. Use a single package-level var per test package.
type CLIFinder struct {
	once     sync.Once
	binary   string
	buildErr error
}

// FindOrBuild checks prebuiltPaths first (validated with --help), then
// falls back to `go build` of pkg in moduleDir. The binary is cached for
// the lifetime of the process.
func (f *CLIFinder) FindOrBuild(t *testing.T, moduleDir, pkg string, prebuiltPaths ...string) string {
	t.Helper()

	f.once.Do(func() {
		// Try pre-built binaries first.
		for _, p := range prebuiltPaths {
			if _, err := os.Stat(p); err != nil {
				continue
			}
			if exec.CommandContext(context.Background(), p, "--help").Run() == nil { //nolint:gosec,usetesting // paths are test-controlled; no *testing.T in sync.Once
				f.binary = p
				log.Printf("Using pre-built binary: %s", p)
				return
			}
		}

		// Fall back to go build.
		binDir, err := os.MkdirTemp("", "schemabot-e2e-cli-*") //nolint:usetesting // shared across tests via sync.Once
		if err != nil {
			f.buildErr = fmt.Errorf("create temp dir: %w", err)
			return
		}
		f.binary = filepath.Join(binDir, "schemabot")

		log.Printf("Building CLI binary to %s...", f.binary)
		cmd := exec.CommandContext(context.Background(), "go", "build", "-o", f.binary, pkg) //nolint:usetesting // shared across tests via sync.Once
		cmd.Dir = moduleDir
		var stderr bytes.Buffer
		cmd.Stderr = &stderr

		if err := cmd.Run(); err != nil {
			f.buildErr = fmt.Errorf("build schemabot CLI: %w: %s", err, stderr.String())
			return
		}
		log.Printf("CLI binary built successfully")
	})

	if f.buildErr != nil {
		t.Fatalf("build CLI: %v", f.buildErr)
	}
	return f.binary
}

// RunCLI runs the CLI binary in dir and returns combined stdout+stderr.
// Fails the test on non-zero exit.
func RunCLI(t *testing.T, binPath, dir string, args ...string) string {
	t.Helper()
	out, err := RunCLIWithError(binPath, dir, args...)
	if err != nil {
		t.Fatalf("CLI command failed: %v\nOutput: %s", err, out)
	}
	return out
}

// RunCLIWithError runs the CLI binary in dir and returns combined stdout+stderr and any error.
func RunCLIWithError(binPath, dir string, args ...string) (string, error) {
	cmd := exec.CommandContext(context.Background(), binPath, args...)
	cmd.Dir = dir
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	return stdout.String() + stderr.String(), err
}

type schemaDirConfig struct {
	environments map[string]string // env name → target
}

// SchemaDirOption configures optional fields for WriteSchemaDir.
type SchemaDirOption func(*schemaDirConfig)

// WithEnvironments adds per-environment targets to the generated schemabot.yaml.
// The map keys are environment names (e.g., "staging", "production") and values
// are target identifiers (e.g., DSIDs).
func WithEnvironments(envs map[string]string) SchemaDirOption {
	return func(c *schemaDirConfig) {
		c.environments = envs
	}
}

// WriteSchemaDir creates a temporary directory with a schemabot.yaml config and
// the provided SQL files. Returns the directory path.
func WriteSchemaDir(t *testing.T, database, dbType string, sqlFiles map[string]string, opts ...SchemaDirOption) string {
	t.Helper()

	cfg := &schemaDirConfig{}
	for _, opt := range opts {
		opt(cfg)
	}

	dir := t.TempDir()
	var b strings.Builder
	fmt.Fprintf(&b, "database: %s\ntype: %s\n", database, dbType)
	if len(cfg.environments) > 0 {
		envNames := make([]string, 0, len(cfg.environments))
		for env := range cfg.environments {
			envNames = append(envNames, env)
		}
		sort.Strings(envNames)
		b.WriteString("environments:\n")
		for _, env := range envNames {
			fmt.Fprintf(&b, "  %s:\n    target: %s\n", env, cfg.environments[env])
		}
	}
	if err := os.WriteFile(filepath.Join(dir, "schemabot.yaml"), []byte(b.String()), 0644); err != nil {
		t.Fatalf("write schemabot.yaml: %v", err)
	}
	for name, content := range sqlFiles {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
	return dir
}

var applyIDRe = regexp.MustCompile(`apply-[a-f0-9]+`)

// ParseApplyID extracts an apply ID (e.g., "apply-abc12345") from CLI output.
func ParseApplyID(t *testing.T, output string) string {
	t.Helper()
	match := applyIDRe.FindString(StripANSI(output))
	if match == "" {
		t.Fatalf("no apply ID found in output:\n%s", output)
	}
	return match
}

var ansiRe = regexp.MustCompile(`\x1b\[[0-9;]*m`)

// StripANSI removes ANSI escape codes from a string.
func StripANSI(s string) string {
	return ansiRe.ReplaceAllString(s, "")
}

// AssertContains checks that output contains the expected substring after
// stripping ANSI codes. Fails the test with a descriptive error if not found.
func AssertContains(t *testing.T, output, expected string) {
	t.Helper()
	stripped := StripANSI(output)
	if !strings.Contains(stripped, expected) {
		t.Errorf("expected output to contain %q, got:\n%s", expected, output)
	}
}

// AssertNotContains checks that output does NOT contain the unexpected substring
// after stripping ANSI codes.
func AssertNotContains(t *testing.T, output, unexpected string) {
	t.Helper()
	stripped := StripANSI(output)
	if strings.Contains(stripped, unexpected) {
		t.Errorf("expected output to NOT contain %q, got:\n%s", unexpected, output)
	}
}

// WriteFile writes content to a file, failing the test on error.
func WriteFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("write file %s: %v", path, err)
	}
}

// NewSchemaDirForDB creates a temp directory with a schemabot.yaml for the given database.
func NewSchemaDirForDB(t *testing.T, dbName string) string {
	t.Helper()
	dir := t.TempDir()
	WriteFile(t, filepath.Join(dir, "schemabot.yaml"), fmt.Sprintf("database: %s\ntype: mysql\n", dbName))
	return dir
}

// RunCLIInDir runs a CLI command in a specific directory using t.Context() and
// returns combined stdout+stderr. Fails the test on non-zero exit.
func RunCLIInDir(t *testing.T, binPath, dir string, args ...string) string {
	t.Helper()
	out, err := RunCLIWithErrorInDir(t, binPath, dir, args...)
	if err != nil {
		t.Fatalf("CLI command failed: %v\nOutput: %s", err, out)
	}
	return out
}

// RunCLIWithErrorInDir runs a CLI command in a specific directory using t.Context()
// and returns combined stdout+stderr and any error.
func RunCLIWithErrorInDir(t *testing.T, binPath, dir string, args ...string) (string, error) {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), binPath, args...)
	cmd.Dir = dir
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	return stdout.String() + stderr.String(), err
}
