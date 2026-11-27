package presentation

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// A cancelled table reads by how far it got and by who stopped it: a copy
// the cancel cut short names its progress, a started change reads as
// cancelled, and one that never began reads as not run when its apply's
// failure stopped it, and as cancelled before it started when an operator
// did.
func TestReadCancelledTask(t *testing.T) {
	tests := []struct {
		name        string
		facts       TableFacts
		began       bool
		applyFailed bool
		want        CancelledTask
	}{
		{"a copy the cancel cut short", TableFacts{PercentComplete: 40, RowsCopied: 400, RowsTotal: 1000}, true, false, CancelledTask{Label: "Cancelled at 40.00%"}},
		{"a copy cut short when the apply failed", TableFacts{PercentComplete: 40, RowsCopied: 400, RowsTotal: 1000}, true, true, CancelledTask{Label: "Cancelled at 40.00%"}},
		{"a change that began", TableFacts{}, true, false, CancelledTask{Label: "Cancelled"}},
		{"a change the failure kept from starting", TableFacts{}, false, true, CancelledTask{Label: "Not run", NotRun: true}},
		{"a change the cancel kept from starting", TableFacts{}, false, false, CancelledTask{Label: "Cancelled (not started)"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, ReadCancelledTask(tt.facts, tt.began, tt.applyFailed))
		})
	}
}
