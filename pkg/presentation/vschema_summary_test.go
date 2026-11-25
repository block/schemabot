package presentation

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

// A keyspace's first VSchema adds every table at once. The description lists
// the first tables and counts the rest, so the list stays readable and still
// says how much it left out.
func TestVSchemaSummaryLinesCountsWhatItLeavesOut(t *testing.T) {
	short := []string{"adds table `orders`", "adds table `refunds`"}
	assert.Equal(t, short, VSchemaSummaryLines(short))

	long := make([]string, 25)
	for i := range long {
		long[i] = fmt.Sprintf("adds table `t%02d`", i)
	}
	got := VSchemaSummaryLines(long)
	assert.Len(t, got, 20)
	assert.Equal(t, "adds table `t18`", got[18])
	assert.Equal(t, "and 6 more changes, shown in the diff", got[19])
}
