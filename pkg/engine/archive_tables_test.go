package engine

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// The reason is printed on the plan and returned by the plan API, so readers
// and callers see this exact text.
func TestExemptReasonArchiveNamingIsTheTextAPlanCarries(t *testing.T) {
	assert.Equal(t, "archive naming", ExemptReasonArchiveNaming)
}
