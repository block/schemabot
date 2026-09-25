package ui

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPlanCountsCountsATableOncePerOperation(t *testing.T) {
	var counts PlanCounts
	counts.AddTable("app", "alter", "users")
	counts.AddTable("app", "alter", "users")
	counts.AddTable("app", "alter", "orders")
	counts.AddTable("app", "create", "users")
	counts.AddTable("app", "unknown", "audit")
	counts.AddOther()

	assert.Equal(t, 1, counts.Created)
	assert.Equal(t, 2, counts.Altered)
	assert.Zero(t, counts.Dropped)
	assert.Equal(t, 2, counts.Other)
	assert.Equal(t, []string{"**1** table to create", "**2** tables to alter", "2 other DDL statements"}, PlanSummaryParts(counts, 6, true))
	assert.Equal(t, []string{"1 table to create", "2 tables to alter", "2 other DDL statements"}, PlanSummaryParts(counts, 6, false))
}

// An index build changes its table's schema, so it shares the table's alter
// bucket: an ALTER and a CREATE INDEX on one table are one table to alter, and
// an index on a table nothing else touches is a table to alter on its own. An
// index drop names no table in every dialect that emits one, so it stays an
// other statement.
func TestPlanCountsCountsAnIndexBuildAsAnAlterOfItsTable(t *testing.T) {
	var counts PlanCounts
	counts.AddTable("app", "create", "invoices")
	counts.AddTable("app", "alter", "orders")
	counts.AddTable("app", "alter", "users")
	counts.AddTable("app", "create_index", "events")
	counts.AddTable("app", "create_index", "orders")
	counts.AddTable("app", "drop_index", "")

	assert.Equal(t, 1, counts.Created)
	assert.Equal(t, 3, counts.Altered)
	assert.Zero(t, counts.Dropped)
	assert.Equal(t, 1, counts.Other)
	assert.Equal(t, []string{"1 table to create", "3 tables to alter", "1 other DDL statement"}, PlanSummaryParts(counts, 6, false))
}

func TestPlanCountsKeepsEqualNamesInDifferentNamespacesApart(t *testing.T) {
	var counts PlanCounts
	counts.AddTable("billing", "drop", "events")
	counts.AddTable("audit", "drop", "events")
	counts.AddTable("audit", "drop", "events")

	assert.Equal(t, 2, counts.Dropped)
	assert.Equal(t, []string{"2 tables to drop"}, PlanSummaryParts(counts, 3, false))
}

func TestPlanCountsCountsNamelessChangesWithoutMerging(t *testing.T) {
	var counts PlanCounts
	counts.AddTable("app", "alter", "")
	counts.AddTable("app", "alter", "")
	counts.AddTable("app", "alter", "orders")
	counts.AddTable("app", "alter", "orders")

	assert.Equal(t, 3, counts.Altered)
}

func TestPlanSummaryPartsNamesOtherStatementsOnlyBesideTypedCounts(t *testing.T) {
	var onlyOther PlanCounts
	onlyOther.AddOther()
	onlyOther.AddOther()
	assert.Equal(t, []string{"2 DDL statements"}, PlanSummaryParts(onlyOther, 2, true))

	assert.Equal(t, []string{"3 DDL statements"}, PlanSummaryParts(PlanCounts{}, 3, false))
	assert.Empty(t, PlanSummaryParts(PlanCounts{}, 0, false))
}
