package testutil

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// ryukRef matches any Ryuk image reference, whatever tag it carries, so a
// stale pin is caught rather than silently skipped.
var ryukRef = regexp.MustCompile(`testcontainers/ryuk:[^\s"'\\]+`)

// reaperImageDecl matches the constant testcontainers-go starts Ryuk from. Its
// exported alias is deprecated, so the test reads the source of the module
// version go.mod pins instead.
var reaperImageDecl = regexp.MustCompile(`ReaperDefaultImage\s*=\s*"([^"]+)"`)

// CI pre-pulls the Ryuk image so a registry hiccup is retried before tests
// start. A stale tag still pulls fine, so the step stays green while the tests
// fetch the real tag themselves with no retry. Assert the workflow pins the
// tag the pinned testcontainers-go starts, so a dependency bump fails here.
func TestRyukPrepullMatchesTestcontainers(t *testing.T) {
	root := moduleRoot(t)

	out, err := exec.CommandContext(t.Context(), "go", "list", "-m", "-f", "{{.Dir}}",
		"github.com/testcontainers/testcontainers-go").Output()
	require.NoError(t, err, "locate the testcontainers-go module source")

	config, err := os.ReadFile(filepath.Join(strings.TrimSpace(string(out)), "internal", "config", "config.go"))
	require.NoError(t, err)
	match := reaperImageDecl.FindSubmatch(config)
	require.NotNil(t, match, "testcontainers-go no longer declares ReaperDefaultImage in internal/config")
	want := string(match[1])

	contents, err := os.ReadFile(filepath.Join(root, ".github", "workflows", "test.yaml"))
	require.NoError(t, err)

	refs := ryukRef.FindAllString(string(contents), -1)
	require.NotEmpty(t, refs, "the workflow no longer pre-pulls a Ryuk image")

	for _, ref := range refs {
		require.Equal(t, want, ref,
			"the workflow pre-pulls %s but testcontainers-go starts %s -- bump the pre-pull lists", ref, want)
	}
}
