package glyph_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/glyph"
	"github.com/block/schemabot/pkg/ui"
)

// vocabulary is every glyph the package exports, severity or not: the width
// and distinctness rules hold for the whole vocabulary, since any member can
// render beside any other.
func vocabulary() map[string]string {
	return map[string]string{
		"Escalation": glyph.Escalation,
		"Refused":    glyph.Refused,
		"Failed":     glyph.Failed,
		"Attention":  glyph.Attention,
		"Info":       glyph.Info,
		"Docs":       glyph.Docs,
	}
}

// TestGlyphsOccupyTwoCells pins every glyph at two terminal cells, so a
// padded CLI column can hold any mix of them without misaligning.
func TestGlyphsOccupyTwoCells(t *testing.T) {
	for name, g := range vocabulary() {
		assert.Equal(t, 2, ui.VisibleWidth(g), "glyph %s (%q) must occupy two terminal cells", name, g)
	}
}

// TestGlyphsAreDistinct pins one meaning per glyph: no two meanings may
// render the same symbol.
func TestGlyphsAreDistinct(t *testing.T) {
	seen := map[string]string{}
	for name, g := range vocabulary() {
		if prior, dup := seen[g]; dup {
			t.Errorf("glyph %q is shared by %s and %s; each meaning needs its own symbol", g, prior, name)
		}
		seen[g] = name
	}
}
