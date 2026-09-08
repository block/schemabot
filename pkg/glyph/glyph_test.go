package glyph_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/glyph"
	"github.com/block/schemabot/pkg/ui"
)

func severityGlyphs() map[string]string {
	return map[string]string{
		"Escalation": glyph.Escalation,
		"Refused":    glyph.Refused,
		"Failed":     glyph.Failed,
		"Attention":  glyph.Attention,
		"Info":       glyph.Info,
	}
}

// TestSeverityGlyphsOccupyTwoCells pins every severity glyph at two terminal
// cells, so a padded CLI column can hold any mix of them without misaligning.
func TestSeverityGlyphsOccupyTwoCells(t *testing.T) {
	for name, g := range severityGlyphs() {
		assert.Equal(t, 2, ui.VisibleWidth(g), "glyph %s (%q) must occupy two terminal cells", name, g)
	}
}

// TestSeverityGlyphsAreDistinct pins one meaning per glyph: no two severities
// may render the same symbol.
func TestSeverityGlyphsAreDistinct(t *testing.T) {
	seen := map[string]string{}
	for name, g := range severityGlyphs() {
		if prior, dup := seen[g]; dup {
			t.Errorf("glyph %q is shared by %s and %s; each severity needs its own symbol", g, prior, name)
		}
		seen[g] = name
	}
}
