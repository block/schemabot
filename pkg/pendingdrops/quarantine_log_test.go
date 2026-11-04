package pendingdrops

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A record without an apply identifier could never be read back by the apply
// that made it, so it is refused before the target is touched.
func TestRecordQuarantinesRequiresAnApplyIdentifier(t *testing.T) {
	moved := []QuarantinedTable{{SchemaName: "app", TableName: "orders", QuarantineSchema: Database, QuarantineTable: TableName("app", "orders", time.Now())}}
	err := RecordQuarantines(t.Context(), nil, "", moved, time.Now())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "apply identifier is required")
}

// A server with lower_case_table_names set lists a quarantined copy under its
// folded name, while the quarantine log keeps the casing the quarantine chose.
// The copy still exists, so its record must stay; only records of copies that
// are gone and past retention are pruned.
func TestPrunableLogRecordsMatchesCopiesCaseInsensitively(t *testing.T) {
	now := time.Now()
	cutoff := now.Add(-DefaultRetention)
	old := now.Add(-8 * 24 * time.Hour)
	liveMixedCase := TableName("app", "Orders", old)
	goneExpired := TableName("app", "legacy", old)
	goneFresh := TableName("app", "moving", now)

	remaining := []string{TableName("app", "orders", old)}

	assert.Equal(t, []string{goneExpired},
		prunableLogRecords([]string{liveMixedCase, goneExpired, goneFresh}, remaining, cutoff))
}
