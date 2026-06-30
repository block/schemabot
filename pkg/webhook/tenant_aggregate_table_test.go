package webhook

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A single-tenant repo renders without a Tenant column — identical in shape to
// a standard single-deployment check.
func TestBuildTenantAggregateTableSingleTenant(t *testing.T) {
	table := buildTenantAggregateTable([]TenantState{
		{Tenant: "tenant-a", SHA: "abc", Rollup: "pending", Databases: []TenantDatabaseState{
			{Database: "orders", Op: "ALTER TABLE", State: "running", Detail: "add column email"},
			{Database: "ledger", Op: "CREATE TABLE", State: "applied"},
		}},
	})

	assert.NotContains(t, table, "Tenant |", "single tenant omits the Tenant column")
	assert.Contains(t, table, "| Database | Change | Status |")
	assert.Contains(t, table, "`orders`")
	assert.Contains(t, table, "`add column email`") // Detail wins over Op
	assert.Contains(t, table, "running")
	assert.Contains(t, table, "`ledger`")
	assert.Contains(t, table, "`CREATE TABLE`") // falls back to Op when Detail is empty
	assert.Contains(t, table, "applied")
}

// Two or more tenants render the full (tenant, database) grid, and a no-changes
// tenant still appears as a single placeholder row.
func TestBuildTenantAggregateTableMultiTenant(t *testing.T) {
	table := buildTenantAggregateTable([]TenantState{
		{Tenant: "tenant-a", SHA: "abc", Rollup: "applied", Databases: []TenantDatabaseState{
			{Database: "coffeeshop", Op: "ADD COLUMN sku", State: "applied"},
		}},
		{Tenant: "tenant-b", SHA: "abc", Rollup: "pending", Databases: []TenantDatabaseState{
			{Database: "orders", Op: "ADD COLUMN email", State: "running"},
		}},
		{Tenant: "tenant-c", SHA: "abc", Rollup: "no_changes"},
	})

	assert.Contains(t, table, "| Tenant | Database | Change | Status |")
	assert.Contains(t, table, "| tenant-a | `coffeeshop` | `ADD COLUMN sku` | applied |")
	assert.Contains(t, table, "| tenant-b | `orders` | `ADD COLUMN email` | running |")
	assert.Contains(t, table, "| tenant-c | — | no changes | — |", "no-changes tenant gets a placeholder row")
}

// A tenant whose databases are empty (a "no changes" report) is still visible
// even on a single-tenant repo.
func TestBuildTenantAggregateTableNoChangesOnly(t *testing.T) {
	table := buildTenantAggregateTable([]TenantState{
		{Tenant: "tenant-a", SHA: "abc", Rollup: "no_changes"},
	})
	assert.Contains(t, table, "| — | no changes | — |")
}

// The table bounds its size to the Check Run text limit and truncates with a
// note rather than emitting an oversized body.
func TestBuildTenantAggregateTableTruncates(t *testing.T) {
	var dbs []TenantDatabaseState
	for range 5000 {
		dbs = append(dbs, TenantDatabaseState{Database: "db", Op: "ALTER TABLE add a very descriptive column name here", State: "running"})
	}
	table := buildTenantAggregateTable([]TenantState{{Tenant: "tenant-a", SHA: "abc", Rollup: "pending", Databases: dbs}})

	assert.Less(t, len(table), maxCheckRunTextLength, "output stays under the Check Run text limit")
	require.Contains(t, table, "more row(s)")
	assert.True(t, strings.HasSuffix(strings.TrimSpace(table), "more row(s)"), "truncation note is last")
}
