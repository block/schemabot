package tern

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/storage"
)

// Stop terminality is engine-specific: a Vitess (PlanetScale) stop cancels the
// deploy request permanently, while a MySQL (Spirit) stop pauses for resume.
func TestStopTerminatesChange(t *testing.T) {
	assert.True(t, stopTerminatesChange(storage.DatabaseTypeVitess),
		"a Vitess stop cancels the deploy request permanently")
	assert.False(t, stopTerminatesChange(storage.DatabaseTypeMySQL),
		"a MySQL stop pauses at a checkpoint and is resumable")
}
