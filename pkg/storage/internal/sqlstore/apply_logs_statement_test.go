package sqlstore

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// The retention count and the purge select the same applies: both render the
// one eligibility test, with the settled states, the database-clock cutoff and
// the lease gate, on both dialects.
func TestSettledApplyLogCountMatchesPurgeEligibility(t *testing.T) {
	for _, d := range []Dialect{MySQLDialect{}, PostgresDialect{}} {
		eligible, args := settledApplyLogEligibility(d, 30*24*time.Hour)
		assert.Equal(t, strings.Count(eligible, "?"), len(args), "one bound state per placeholder")
		assert.Contains(t, eligible, "a.state IN (")
		assert.Contains(t, eligible, "a.completed_at < ")
		assert.Contains(t, eligible, "a.lease_owner = ''")
		assert.Contains(t, eligible, "lease_holder.lease_owner <> ''")

		count := settledApplyLogCountQuery(eligible)
		page := settledApplyLogPageQuery(eligible)
		assert.Contains(t, count, "WHERE "+eligible)
		assert.Contains(t, page, "WHERE "+eligible)
		assert.NotContains(t, count, "LIMIT", "the count covers every eligible entry")
	}
}
