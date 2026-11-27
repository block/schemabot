package templates

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/cmd/cliname"
	"github.com/block/schemabot/pkg/glyph"
)

// lineAttack is text that acts on a terminal when it is printed as it arrived:
// it sets the window title, clears the screen, returns to the start of the
// line, starts a new line, opens a control sequence with a C1 rune and
// reverses the text after it. lineAttackCleaned is what is left of it in a
// value that is printed on one line.
const (
	lineAttack        = "\x1b]0;owned\x07\x1b[2J\r\n\u009b31m\u202e"
	lineAttackCleaned = "]0;owned[2J31m"
)

// assertNoTerminalControls fails when output still carries one of the runes
// lineAttack acts through. The lock templates print no color of their own, so
// any escape in their output came from a value.
func assertNoTerminalControls(t *testing.T, output string) {
	t.Helper()
	for _, control := range []string{"\x1b", "\x07", "\r", "\u009b", "\u202e"} {
		assert.NotContains(t, output, control)
	}
}

// A lock's owner and repository are whatever the caller that took the lock
// sent, and the lock list prints every lock the server holds. An operator who
// lists locks, or whose apply or unlock is refused by someone else's lock, sees
// those values with their control runes removed and on the line they belong
// to.
func TestLockTemplatesDropTerminalEscapes(t *testing.T) {
	heldSince := time.Now()

	t.Run("the lock list", func(t *testing.T) {
		output := captureStdout(t, func() {
			WriteLocksList([]LockData{{
				Database:     "orders" + lineAttack,
				DatabaseType: "mysql" + lineAttack,
				Owner:        "cli:jdoe" + lineAttack,
				Repository:   "acme/shop" + lineAttack,
				PullRequest:  412,
				CreatedAt:    heldSince,
			}})
		})

		assertNoTerminalControls(t, output)
		assert.Contains(t, output, "  1. orders"+lineAttackCleaned+" (mysql"+lineAttackCleaned+")\n"+
			"     Owner: cli:jdoe"+lineAttackCleaned+"\n"+
			"     Since: just now\n"+
			"     PR:    acme/shop"+lineAttackCleaned+"#412\n")
	})

	t.Run("an apply refused by another owner's lock", func(t *testing.T) {
		output := captureStdout(t, func() {
			WriteLockConflict(LockConflictData{
				Database:     "orders",
				DatabaseType: "mysql",
				Owner:        "cli:jdoe" + lineAttack,
				Repository:   "acme/shop" + lineAttack,
				PullRequest:  412,
				CreatedAt:    heldSince,
			})
		})

		assertNoTerminalControls(t, output)
		assert.Contains(t, output, "│  Locked by:  cli:jdoe"+lineAttackCleaned)
		assert.Contains(t, output, "│  PR:         acme/shop"+lineAttackCleaned+"#412")
		assert.Equal(t, 4, strings.Count(output, "\n│"), "each of the four values stays on its own row of the box")
	})

	t.Run("a forced release names the previous owner", func(t *testing.T) {
		output := captureStdout(t, func() {
			WriteLockForceReleased("orders", "mysql", "cli:jdoe"+lineAttack)
		})

		assert.Equal(t, glyph.Attention+"  Force released lock for orders (mysql)\n"+
			"   Previous owner: cli:jdoe"+lineAttackCleaned+"\n", output)
	})

	t.Run("the warning before a forced command takes another owner's lock", func(t *testing.T) {
		output := captureStdout(t, func() {
			WriteLockTakeoverWarning(LockConflictData{
				Database:     "orders",
				DatabaseType: "mysql",
				Owner:        "cli:jdoe" + lineAttack,
				Repository:   "acme/shop" + lineAttack,
				PullRequest:  412,
				CreatedAt:    heldSince,
			}, UnlockTakeoverConsequence)
		})

		assert.Equal(t, "\n"+glyph.Attention+"  The lock on orders (mysql) belongs to another owner\n"+
			"   Held by: cli:jdoe"+lineAttackCleaned+"\n"+
			"   Since:   just now\n"+
			"   PR:      acme/shop"+lineAttackCleaned+"#412\n"+
			"   Releasing it removes their claim on this database.\n", output)
	})

	t.Run("the warning names a PR's lock once when the PR owns it", func(t *testing.T) {
		output := captureStdout(t, func() {
			WriteLockTakeoverWarning(LockConflictData{
				Database:     "orders",
				DatabaseType: "mysql",
				Owner:        "acme/shop#412",
				Repository:   "acme/shop",
				PullRequest:  412,
				CreatedAt:    heldSince,
			}, ApplyTakeoverConsequence)
		})

		assert.Equal(t, "\n"+glyph.Attention+"  The lock on orders (mysql) belongs to another owner\n"+
			"   Held by: acme/shop#412\n"+
			"   Since:   just now\n"+
			"   Applying with --force releases it and takes the lock for you.\n", output)
	})

	t.Run("an unlock refused because someone else owns the lock", func(t *testing.T) {
		output := captureStdout(t, func() {
			WriteUnlockNotOwned("orders", "mysql", "cli:jdoe"+lineAttack)
		})

		assertNoTerminalControls(t, output)
		assert.Contains(t, output, "  Current owner: cli:jdoe"+lineAttackCleaned+"\n")
	})

	t.Run("a lock found under another database type", func(t *testing.T) {
		output := captureStdout(t, func() {
			WriteLockExistsUnderOtherType("orders", "mysql", "vitess"+lineAttack)
		})

		assert.Equal(t, "No lock found for orders (mysql), but a vitess"+lineAttackCleaned+" lock exists for this database.\n"+
			"Release it with: "+cliname.Command("unlock", "-d", "orders", "-t", "vitess"+lineAttackCleaned)+"\n", output)
	})

	t.Run("a failed check for locks under other types reports the error on one line", func(t *testing.T) {
		output := captureStdout(t, func() {
			WriteLockTypeScanFailed(errors.New("list locks: bad gateway\x1b[2J\r\n  retry the unlock"))
		})

		assert.Equal(t, "Could not check for locks under other database types: list locks: bad gateway[2J retry the unlock\n", output)
	})
}
