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

	rowsByTenant := make([][]aggregateRow, len(states))
	total := 0
	for i, state := range states {
		rowsByTenant[i] = tenantAggregateRows(state)
		total += len(rowsByTenant[i])
	}

	rendered := 0
	for i, state := range states {
		for _, row := range rowsByTenant[i] {
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

// formatCell renders a database name as an inline code span, leaving the
// placeholder dash unquoted.
func formatCell(value string) string {
	if value == "" || value == "—" {
		return "—"
	}
	return codeSpan(value)
}

// formatChange renders a DDL change as an inline code span, leaving free-text
// placeholders like "no changes" (and empties) unquoted.
func formatChange(value string) string {
	switch value {
	case "", "no changes":
		if value == "" {
			return "—"
		}
		return value
	default:
		return codeSpan(value)
	}
}

// codeSpan wraps value in a Markdown inline code span, choosing a backtick fence
// longer than any run of backticks inside the value so quoted SQL identifiers
// (e.g. “ `email` “) don't break the span. Per CommonMark, a value that begins
// or ends with a backtick is padded with a single space inside the fence.
func codeSpan(value string) string {
	longest, run := 0, 0
	for _, r := range value {
		if r == '`' {
			run++
			if run > longest {
				longest = run
			}
			continue
		}
		run = 0
	}
	fence := strings.Repeat("`", longest+1)
	pad := ""
	if strings.HasPrefix(value, "`") || strings.HasSuffix(value, "`") {
		pad = " "
	}
	return fence + pad + value + pad + fence
}
