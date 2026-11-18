package testutil

import (
	"os"
	"path/filepath"
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"
)

// miniStackRef matches any MiniStack image reference, whatever tag it carries,
// so a stale pin is caught rather than silently skipped.
var miniStackRef = regexp.MustCompile(`ministackorg/ministack:[^\s"']+`)

// The MiniStack tag lives in three places: MiniStackImage here, the in-cluster
// manifest, and the image list CI pre-pulls. Nothing in the build ties them
// together, and a mismatch is close to invisible -- a stale pre-pull tag only
// makes CI quietly pull a second image, and a stale manifest tag leaves the two
// test layers emulating AWS on different releases, which is the drift this
// helper exists to remove. Assert they agree so a partial bump fails here
// instead of surfacing as a test that passes against the wrong emulator.
func TestMiniStackImagePinsAgree(t *testing.T) {
	root := moduleRoot(t)

	for _, path := range []string{
		filepath.Join("e2e", "k8s", "etre-stack.yaml"),
		filepath.Join(".github", "workflows", "test.yaml"),
	} {
		t.Run(path, func(t *testing.T) {
			contents, err := os.ReadFile(filepath.Join(root, path))
			require.NoError(t, err)

			refs := miniStackRef.FindAllString(string(contents), -1)
			require.NotEmpty(t, refs, "%s no longer references a MiniStack image", path)

			for _, ref := range refs {
				require.Equal(t, MiniStackImage, ref,
					"%s pins %s but testutil.MiniStackImage is %s -- bump every pin together",
					path, ref, MiniStackImage)
			}
		})
	}
}

// moduleRoot walks up from the working directory until it finds a go.mod file.
func moduleRoot(t *testing.T) string {
	t.Helper()

	dir, err := os.Getwd()
	require.NoError(t, err)

	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}

		parent := filepath.Dir(dir)
		require.NotEqual(t, dir, parent, "no go.mod found above the working directory")
		dir = parent
	}
}
