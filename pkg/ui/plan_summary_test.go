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

// Index builds and drops on existing tables are named in their own clauses,
// one per statement: an ALTER and a CREATE INDEX on one table are one table
// to alter and one index to create, an index on a table nothing else touches
// does not make that table a table to alter, and an index drop is counted
// whether or not its dialect names the table.
func TestPlanCountsNamesIndexBuildsAndDropsInTheirOwnBuckets(t *testing.T) {
	var counts PlanCounts
	counts.AddTable("app", "create", "invoices")
	counts.AddTable("app", "alter", "orders")
	counts.AddTable("app", "alter", "users")
	counts.AddTable("app", "create_index", "events")
	counts.AddTable("app", "create_index", "orders")
	counts.AddTable("app", "drop_index", "")

	assert.Equal(t, 1, counts.Created)
	assert.Equal(t, 2, counts.Altered)
	assert.Zero(t, counts.Dropped)
	assert.Equal(t, 2, counts.IndexesCreated)
	assert.Equal(t, 1, counts.IndexesDropped)
	assert.Zero(t, counts.Other)
	assert.Equal(t, []string{"1 table to create", "2 tables to alter", "2 indexes to create", "1 index to drop"}, PlanSummaryParts(counts, 6, false))
	assert.Equal(t, []string{"**1** table to create", "**2** tables to alter", "**2** indexes to create", "**1** index to drop"}, PlanSummaryParts(counts, 6, true))
}

// A plan made only of index work names it rather than falling back to the raw
// statement total.
func TestPlanSummaryPartsNamesAnIndexOnlyPlan(t *testing.T) {
	var counts PlanCounts
	counts.AddTable("app", "create_index", "events")

	assert.Equal(t, []string{"1 index to create"}, PlanSummaryParts(counts, 1, false))
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
