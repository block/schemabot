package spirit

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A drop_tables entry is dropped only when the target shows it went the quiet
// period without a write. The server keeps a table's last write time in
// memory, so a table with none is quiet only if the server itself has been up
// for the whole period; on a server that started more recently the answer is
// unknown and the plan is refused, with how long to wait.
func TestRefuseDropsNotQuiet(t *testing.T) {
	const quietPeriod = 24 * time.Hour
	written := func(table string, age time.Duration) dropTableWrite {
		return dropTableWrite{table: table, known: true, age: age}
	}
	unknown := func(table string) dropTableWrite { return dropTableWrite{table: table} }

	tests := []struct {
		name      string
		writes    []dropTableWrite
		uptime    time.Duration
		wantError string
	}{
		{
			name:   "no listed table exists",
			uptime: time.Minute,
		},
		{
			name:   "last written exactly the quiet period ago",
			writes: []dropTableWrite{written("_jobs_old", quietPeriod)},
			uptime: 30 * 24 * time.Hour,
		},
		{
			name:      "last written one second inside the quiet period",
			writes:    []dropTableWrite{written("_jobs_old", quietPeriod-time.Second)},
			uptime:    30 * 24 * time.Hour,
			wantError: `drop_tables entries were written in the last 1d, so something may still be using them, such as an online schema change another tool is running: "_jobs_old" (23h 59m ago). Plan again once they have gone 1d without a write, or remove them from drop_tables`,
		},
		{
			name:   "no write time on a server up for exactly the quiet period",
			writes: []dropTableWrite{unknown("_jobs_old")},
			uptime: quietPeriod,
		},
		{
			name:      "no write time on a server that started inside the quiet period",
			writes:    []dropTableWrite{unknown("_jobs_old"), written("_long_idle", 90*24*time.Hour), unknown("_other_old")},
			uptime:    2*time.Hour + 10*time.Minute,
			wantError: `SchemaBot cannot tell whether drop_tables entries "_jobs_old", "_other_old" were written in the last 1d: the server has no write time for them and started 2h 10m ago, so it has no record of a write before that. Plan again in 21h 50m, or remove them from drop_tables`,
		},
		{
			name:      "a recent write is reported ahead of an unknown write time",
			writes:    []dropTableWrite{unknown("_jobs_old"), written("_executions_new", 3*time.Second)},
			uptime:    time.Hour,
			wantError: `"_executions_new" (3s ago)`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := refuseDropsNotQuiet(tc.writes, tc.uptime, quietPeriod)
			if tc.wantError == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantError)
		})
	}
}
