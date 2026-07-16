package webhook

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBaseDriftDowngradeReason(t *testing.T) {
	t.Run("lists changed files under the base ref", func(t *testing.T) {
		reason := baseDriftDowngradeReason("main", []string{"schema/orders.sql", "schema/users.sql"})
		assert.Contains(t, reason, "`main`")
		assert.Contains(t, reason, "apply-confirm")
		assert.Contains(t, reason, "- `schema/orders.sql`")
		assert.Contains(t, reason, "- `schema/users.sql`")
	})

	t.Run("omits the file list when none were resolved", func(t *testing.T) {
		reason := baseDriftDowngradeReason("main", nil)
		assert.Contains(t, reason, "`main`")
		assert.NotContains(t, reason, "Changed on")
	})

	t.Run("caps the displayed file list", func(t *testing.T) {
		files := make([]string, 0, maxBaseDriftFilesShown+5)
		for range cap(files) {
			files = append(files, "schema/table.sql")
		}
		reason := baseDriftDowngradeReason("main", files)
		assert.Equal(t, maxBaseDriftFilesShown, strings.Count(reason, "- `schema/table.sql`"))
		assert.Contains(t, reason, "…and 5 more")
	})

	t.Run("falls back to a generic base name when the ref is empty", func(t *testing.T) {
		reason := baseDriftDowngradeReason("", nil)
		assert.Contains(t, reason, "the base branch")
	})
}

func TestBaseDriftUnknownDowngradeReason(t *testing.T) {
	assert.Contains(t, baseDriftUnknownDowngradeReason("main"), "could not verify")
	assert.Contains(t, baseDriftUnknownDowngradeReason("main"), "`main`")
	assert.Contains(t, baseDriftUnknownDowngradeReason(""), "the base branch")
}
