package ui

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestFreezeUntil(t *testing.T) {
	assert.Equal(t, "until lifted", FreezeUntil(nil))
	end := time.Date(2026, 11, 22, 18, 30, 0, 0, time.FixedZone("PST", -8*60*60))
	assert.Equal(t, "until 2026-11-23T02:30:00Z", FreezeUntil(&end), "the end is shown in UTC")
}
