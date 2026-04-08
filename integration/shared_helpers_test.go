//go:build integration

package integration

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// =============================================================================
// CLI Execution Helpers (shared across all e2e test suites)
// =============================================================================

// runCLIInDir runs a CLI command in a specific directory and returns combined
// stdout+stderr. Fails the test on non-zero exit.
func runCLIInDir(t *testing.T, binPath, dir string, args ...string) string {
	t.Helper()
	out, err := runCLIWithErrorInDir(t, binPath, dir, args...)
	require.NoErrorf(t, err, "CLI command failed\nOutput: %s", out)
	return out
}

// runCLIWithErrorInDir runs a CLI command in a specific directory and returns
// combined stdout+stderr and any error. Uses t.Context() so the subprocess is
// killed if the test times out.
func runCLIWithErrorInDir(t *testing.T, binPath, dir string, args ...string) (string, error) {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), binPath, args...)
	cmd.Dir = dir
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	output := stdout.String() + stderr.String()
	return output, err
}

// =============================================================================
// Schema Dir Helpers
// =============================================================================

// newSchemaDirForDB creates a temp directory with a schemabot.yaml for the given database.
func newSchemaDirForDB(t *testing.T, dbName string) string {
	t.Helper()
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "schemabot.yaml"), fmt.Sprintf("database: %s\ntype: mysql\n", dbName))
	return dir
}

// =============================================================================
// File Helpers
// =============================================================================

// writeFile writes content to a file, failing the test on error.
func writeFile(t *testing.T, path, content string) {
	t.Helper()
	err := os.WriteFile(path, []byte(content), 0644)
	require.NoErrorf(t, err, "write file %s", path)
}

// =============================================================================
// Assertion Helpers
// =============================================================================

var ansiRe = regexp.MustCompile(`\x1b\[[0-9;]*m`)

// stripANSI removes ANSI escape codes from a string.
func stripANSI(s string) string {
	return ansiRe.ReplaceAllString(s, "")
}

// assertContains checks that output contains the expected substring after
// stripping ANSI codes.
func assertContains(t *testing.T, output, expected string) {
	t.Helper()
	stripped := stripANSI(output)
	assert.Contains(t, stripped, expected, "expected output to contain %q, got:\n%s", expected, output)
}
