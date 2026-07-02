package templates

import (
	"fmt"
	"strings"
)

// SchemaChangeReconciliationData contains the apply-owned PR state that must be
// reconciled before SchemaBot can treat an empty PR diff as clean.
type SchemaChangeReconciliationData struct {
	RequestedBy string
	Timestamp   string
	// Tenant is the deployment's own tenant; when set, the pasteable stop and
	// rollback hints carry it so the command addresses this deployment.
	Tenant string
	Items  []SchemaChangeReconciliationItem
}

// SchemaChangeReconciliationItem describes one database/environment whose live
// schema may no longer match the current PR contents.
type SchemaChangeReconciliationItem struct {
	Database    string
	Environment string
	ApplyID     string
	State       string
	InProgress  bool
}

// RenderNoManagedSchemaChanges renders the clear no-op state for a PR that has
// no managed schema changes and no apply-owned SchemaBot state.
func RenderNoManagedSchemaChanges(data SchemaErrorData) string {
	var sb strings.Builder
	sb.WriteString("## ✅ No Managed Schema Changes\n\n")
	if data.Environment != "" {
		fmt.Fprintf(&sb, "**Environment**: `%s`\n\n", data.Environment)
	}
	writeRequestedLine(&sb, data.RequestedBy, data.Timestamp)
	sb.WriteString("\nThis PR does not contain schema changes managed by SchemaBot. SchemaBot did not find any apply-owned state that requires live database reconciliation.\n")
	return sb.String()
}

// RenderSchemaChangeReconciliationRequired explains that the current PR no
// longer contains a schema change whose apply has already started.
func RenderSchemaChangeReconciliationRequired(data SchemaChangeReconciliationData) string {
	var sb strings.Builder
	sb.WriteString("## ⚠️ Schema Change Reconciliation Required\n\n")
	writeReconciliationMetadata(&sb, data.Items)
	writeRequestedLine(&sb, data.RequestedBy, data.Timestamp)
	sb.WriteString("\n")

	if reconciliationHasInProgressApply(data.Items) {
		writeInProgressReconciliation(&sb, data.Tenant, data.Items)
	} else {
		writeCompletedReconciliation(&sb, data.Tenant, data.Items)
	}

	return sb.String()
}

func writeReconciliationMetadata(sb *strings.Builder, items []SchemaChangeReconciliationItem) {
	if len(items) == 1 {
		item := items[0]
		parts := []string{fmt.Sprintf("**Database**: `%s`", item.Database)}
		if item.Environment != "" {
			parts = append(parts, fmt.Sprintf("**Environment**: `%s`", item.Environment))
		}
		if item.ApplyID != "" {
			parts = append(parts, fmt.Sprintf("**Apply ID**: `%s`", item.ApplyID))
		}
		fmt.Fprintf(sb, "%s\n\n", strings.Join(parts, " | "))
		return
	}

	sb.WriteString("| Database | Environment | Apply ID | State |\n")
	sb.WriteString("|----------|-------------|----------|-------|\n")
	for _, item := range items {
		applyID := item.ApplyID
		if applyID == "" {
			applyID = "unknown"
		}
		state := item.State
		if state == "" {
			state = "unknown"
		}
		fmt.Fprintf(sb, "| `%s` | `%s` | `%s` | `%s` |\n", item.Database, item.Environment, applyID, state)
	}
	sb.WriteString("\n")
}

func writeRequestedLine(sb *strings.Builder, requestedBy, timestamp string) {
	if requestedBy == "" && timestamp == "" {
		return
	}
	if requestedBy == "" {
		fmt.Fprintf(sb, "*Requested at %s UTC*\n", timestamp)
		return
	}
	if timestamp == "" {
		fmt.Fprintf(sb, "*Requested by @%s*\n", requestedBy)
		return
	}
	fmt.Fprintf(sb, "*Requested by @%s at %s UTC*\n", requestedBy, timestamp)
}

func reconciliationHasInProgressApply(items []SchemaChangeReconciliationItem) bool {
	for _, item := range items {
		if item.InProgress {
			return true
		}
	}
	return false
}

func writeInProgressReconciliation(sb *strings.Builder, tenant string, items []SchemaChangeReconciliationItem) {
	sb.WriteString("SchemaBot is still applying a schema change from this PR, but the current PR no longer contains that change.\n\n")
	sb.WriteString("The live database operation was already started and may continue independently of the current PR diff.\n\n")
	sb.WriteString("### What to do next\n\n")
	sb.WriteString("1. First, resolve the in-flight apply:\n")
	sb.WriteString("   - Wait for SchemaBot to post the final apply result, or\n")
	sb.WriteString("   - If stopping is supported for this database, comment:\n")
	fmt.Fprintf(sb, "     ```\n     %s\n     ```\n", stopCommand(tenant, items))
	sb.WriteString("\n2. Then reconcile the final live schema:\n")
	sb.WriteString("   - If the live schema change should remain, add the schema change back to the PR, then comment:\n")
	fmt.Fprintf(sb, "     ```\n     %s\n     ```\n", planCommand(items))
	sb.WriteString("   - If the live schema change should not remain, roll it back:\n")
	fmt.Fprintf(sb, "     ```\n     %s\n     ```\n", rollbackCommand(tenant, items))
	sb.WriteString("     After rollback: push a no-op `schemabot.yaml` edit to trigger a fresh plan.\n")
}

func writeCompletedReconciliation(sb *strings.Builder, tenant string, items []SchemaChangeReconciliationItem) {
	sb.WriteString("SchemaBot already applied a schema change from this PR, but the current PR no longer contains that change.\n\n")
	sb.WriteString("The live database was already updated and may no longer match the current PR schema files.\n\n")
	sb.WriteString("### What to do next\n\n")
	sb.WriteString("Choose one:\n\n")
	sb.WriteString("1. Keep the live schema change:\n")
	sb.WriteString("   - add the schema change back to the PR\n")
	sb.WriteString("   - comment:\n")
	fmt.Fprintf(sb, "     ```\n     %s\n     ```\n", planCommand(items))
	sb.WriteString("\n2. Undo the live schema change:\n")
	sb.WriteString("   - comment:\n")
	fmt.Fprintf(sb, "     ```\n     %s\n     ```\n", rollbackCommand(tenant, items))
	sb.WriteString("   - after rollback: push a no-op `schemabot.yaml` edit to trigger a fresh plan\n")
}

func planCommand(items []SchemaChangeReconciliationItem) string {
	item, ok := singleCommandItem(items)
	if !ok {
		return "schemabot plan -e <environment> -d <database>"
	}
	cmd := fmt.Sprintf("schemabot plan -e %s", item.Environment)
	if item.Database != "" {
		cmd += fmt.Sprintf(" -d %s", item.Database)
	}
	return cmd
}

func stopCommand(tenant string, items []SchemaChangeReconciliationItem) string {
	item, ok := singleCommandItem(items)
	if !ok || item.ApplyID == "" {
		return appendTenantFlag("schemabot stop <apply-id> -e <environment>", tenant)
	}
	return appendTenantFlag(fmt.Sprintf("schemabot stop %s -e %s", item.ApplyID, item.Environment), tenant)
}

func rollbackCommand(tenant string, items []SchemaChangeReconciliationItem) string {
	item, ok := singleCommandItem(items)
	if !ok || item.ApplyID == "" {
		return appendTenantFlag("schemabot rollback <apply-id> -e <environment>", tenant)
	}
	return appendTenantFlag(fmt.Sprintf("schemabot rollback %s -e %s", item.ApplyID, item.Environment), tenant)
}

func singleCommandItem(items []SchemaChangeReconciliationItem) (SchemaChangeReconciliationItem, bool) {
	if len(items) != 1 {
		return SchemaChangeReconciliationItem{}, false
	}
	item := items[0]
	if item.Environment == "" {
		return SchemaChangeReconciliationItem{}, false
	}
	return item, true
}
