package webhook

import (
	"fmt"
	"strings"
)

// aggregateRow is one rendered row of the per-tenant aggregate table.
type aggregateRow struct {
	database string
	change   string
	status   string
}

// tenantAggregateRows returns the table rows for one tenant's state. A tenant
// that reported no databases (a "no changes" report) renders a single
// placeholder row so it is still visible in the breakdown.
func tenantAggregateRows(state TenantState) []aggregateRow {
	if len(state.Databases) == 0 {
		return []aggregateRow{{database: "—", change: "no changes", status: "—"}}
	}
	rows := make([]aggregateRow, 0, len(state.Databases))
	for _, db := range state.Databases {
		change := db.Detail
		if change == "" {
			change = db.Op
		}
		rows = append(rows, aggregateRow{
			database: db.Database,
			change:   change,
			status:   db.State,
		})
	}
	return rows
}

// buildTenantAggregateTable renders the leader-folded per-tenant state as the
// markdown table for the aggregate check's Details page. With a single tenant
// it omits the Tenant column, so a single-tenant repo looks identical to a
// standard single-deployment check; with two or more tenants it renders the
// full (tenant, database) grid. It bounds output to the Check Run text limit,
// truncating with a "... and N more" note rather than producing an oversized
// body.
func buildTenantAggregateTable(states []TenantState) string {
	multiTenant := len(states) > 1

	var sb strings.Builder
	if multiTenant {
		sb.WriteString("| Tenant | Database | Change | Status |\n")
		sb.WriteString("|--------|----------|--------|--------|\n")
	} else {
		sb.WriteString("| Database | Change | Status |\n")
		sb.WriteString("|----------|--------|--------|\n")
	}

	total := 0
	for _, state := range states {
		total += len(tenantAggregateRows(state))
	}

	rendered := 0
	for _, state := range states {
		for _, row := range tenantAggregateRows(state) {
			var line string
			if multiTenant {
				line = fmt.Sprintf("| %s | %s | %s | %s |\n", state.Tenant, formatCell(row.database), formatChange(row.change), row.status)
			} else {
				line = fmt.Sprintf("| %s | %s | %s |\n", formatCell(row.database), formatChange(row.change), row.status)
			}
			if sb.Len()+len(line) > maxCheckRunTextLength-1000 {
				fmt.Fprintf(&sb, "\n... and %d more row(s)\n", total-rendered)
				return sb.String()
			}
			sb.WriteString(line)
			rendered++
		}
	}
	return sb.String()
}

// formatCell backtick-quotes a database name, leaving the placeholder dash
// unquoted.
func formatCell(value string) string {
	if value == "" || value == "—" {
		return "—"
	}
	return "`" + value + "`"
}

// formatChange backtick-quotes a DDL change, leaving free-text placeholders like
// "no changes" (and empties) unquoted.
func formatChange(value string) string {
	switch value {
	case "", "no changes":
		if value == "" {
			return "—"
		}
		return value
	default:
		return "`" + value + "`"
	}
}
