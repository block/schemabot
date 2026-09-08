package templates

import (
	"os"
	"testing"

	"github.com/block/schemabot/pkg/ui"
)

// TestMain pins hyperlink emission off so rendering tests are deterministic
// regardless of whether the test binary's stdout is a terminal. Tests that
// exercise the hyperlinked path enable it explicitly and restore it.
func TestMain(m *testing.M) {
	ui.Hyperlinks = false
	os.Exit(m.Run())
}
