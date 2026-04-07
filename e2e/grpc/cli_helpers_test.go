//go:build e2e

package grpc

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sync"
	"testing"

	"github.com/block/schemabot/pkg/e2eutil"
	"github.com/stretchr/testify/require"
)

// =============================================================================
// CLI Binary Building
// =============================================================================

var (
	grpcCLIBinary     string
	grpcCLIBinaryOnce sync.Once
	grpcCLIBuildErr   error
)

// grpcCLIBuildOrFind finds bin/schemabot (pre-built by Makefile) or falls back to go build.
func grpcCLIBuildOrFind(t *testing.T) string {
	t.Helper()

	grpcCLIBinaryOnce.Do(func() {
		wd, err := os.Getwd()
		if err != nil {
			grpcCLIBuildErr = fmt.Errorf("getwd: %w", err)
			return
		}
		moduleRoot, err := findModuleRootFrom(wd)
		if err != nil {
			grpcCLIBuildErr = err
			return
		}

		// Check for pre-built binary
		prebuilt := filepath.Join(moduleRoot, "bin", "schemabot")
		if _, err := os.Stat(prebuilt); err == nil {
			grpcCLIBinary = prebuilt
			return
		}

		// Fall back to go build
		binDir := t.TempDir()
		grpcCLIBinary = filepath.Join(binDir, "schemabot")

		cmd := exec.CommandContext(t.Context(), "go", "build", "-o", grpcCLIBinary, "./pkg/cmd")
		cmd.Dir = moduleRoot
		var stderr bytes.Buffer
		cmd.Stderr = &stderr

		if err := cmd.Run(); err != nil {
			grpcCLIBuildErr = fmt.Errorf("build schemabot: %w: %s", err, stderr.String())
			return
		}
	})

	require.NoError(t, grpcCLIBuildErr, "build CLI")
	return grpcCLIBinary
}

// findModuleRootFrom walks up from dir until it finds a go.mod file.
func findModuleRootFrom(start string) (string, error) {
	dir := start
	for {
		_, err := os.Stat(filepath.Join(dir, "go.mod"))
		if err == nil {
			return dir, nil
		}
		if !os.IsNotExist(err) {
			return "", fmt.Errorf("stat %s/go.mod: %w", dir, err)
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("could not find go.mod in any parent of %s", start)
		}
		dir = parent
	}
}

// =============================================================================
// Output Parsing Helpers
// =============================================================================

// parseApplyID extracts an apply ID (e.g., "apply-abc12345") from CLI output.
func parseApplyID(t *testing.T, output string) string {
	t.Helper()
	re := regexp.MustCompile(`apply-[a-f0-9]+`)
	match := re.FindString(output)
	require.NotEmptyf(t, match, "no apply ID found in output:\n%s", output)
	return match
}

// =============================================================================
// Schema Directory Helper
// =============================================================================

// grpcCLISchemaDir creates a temp directory with schemabot.yaml and SQL files.
func grpcCLISchemaDir(t *testing.T, files map[string]string) string {
	t.Helper()
	dir := e2eutil.NewSchemaDirForDB(t, "testapp")
	for name, content := range files {
		e2eutil.WriteFile(t, filepath.Join(dir, name), content)
	}
	return dir
}
