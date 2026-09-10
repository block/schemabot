package templates

import (
	"fmt"
	"strings"

	"github.com/block/schemabot/pkg/glyph"
)

// SchemaChangeReconciliationData contains the apply-owned PR state that must be
// reconciled before SchemaBot can treat an empty PR diff as clean.
type SchemaChangeReconciliationData struct {
	RequestedBy string
	Timestamp   string
	// Tenant is the deployment's own tenant; when set, the pasteable stop,
	// plan, and rollback hints carry it so the command addresses this deployment.
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

// RenderNoManagedSchemaChanges renders the clear no-op state for a PR whose
// diff touches no managed schema files and that has no apply-owned SchemaBot
// state.
func RenderNoManagedSchemaChanges(data SchemaErrorData) string {
	var sb strings.Builder
	sb.WriteString("## " + glyph.Info + " No Schema Files Changed\n\n")
	if data.Environment != "" {
		fmt.Fprintf(&sb, "**Environment**: `%s`\n\n", data.Environment)
	}
	writeRequestedLine(&sb, data.RequestedBy, data.Timestamp)
	sb.WriteString("\nSchemaBot found no changes to managed schema files in this PR and no apply-owned state that requires live database reconciliation.\n")
	return sb.String()
}

// NoManagedSchemaChangesChecksRefreshedData describes the outcome of a plan
// command that found no changes to managed schema files and refreshed the PR's
// SchemaBot check state instead of running a plan.
type NoManagedSchemaChangesChecksRefreshedData struct {
	RequestedBy string
	// Repository is the owner/name the head SHA links to; when empty the SHA
	// renders as plain text.
	Repository string
	HeadSHA    string
	// GatedOnTenants marks the aggregate-leader case: the refreshed check
	// gates on tenant deployments' own checks for the touched schema paths
	// instead of passing unconditionally.
	GatedOnTenants bool
}

// RenderNoManagedSchemaChangesChecksRefreshed reports that a plan command
// found no changes to managed schema files and recreated the PR's SchemaBot check
// state on the current head.
func RenderNoManagedSchemaChangesChecksRefreshed(data NoManagedSchemaChangesChecksRefreshedData) string {
	var sb strings.Builder
	sb.WriteString("## " + glyph.Info + " No Schema Files Changed\n\n")
	head := formatCommitRef(data.Repository, data.HeadSHA)
	if data.GatedOnTenants {
		fmt.Fprintf(&sb, "SchemaBot found no changes to schema files managed by this deployment in this PR, but the PR touches schema paths owned by tenant deployments. The SchemaBot check was refreshed on %s and will pass once every tenant deployment's own check succeeds.\n", head)
	} else {
		fmt.Fprintf(&sb, "SchemaBot found no changes to managed schema files in this PR. The SchemaBot checks were refreshed as passing on %s.\n", head)
		sb.WriteString("\n<details>\n<summary>Expected a plan?</summary>\n\n")
		sb.WriteString("A PR plan covers the databases whose schema directories the PR changes. This PR changes none, so no database was compared against its schema directory.\n\n")
		sb.WriteString("Two cases still need a plan even though the files are already correct:\n\n")
		sb.WriteString("- **A new database.** Its schema directory merged before the database was configured, so no PR ever applied the files and the live database is empty.\n")
		sb.WriteString("- **An existing database with drift.** The live schema was changed outside SchemaBot: DDL run directly against the database, a restore from an older snapshot, or schema files pulled from a different environment. The files did not change, so nothing triggers a plan.\n\n")
		sb.WriteString("In either case, give the PR a change in that database's schema directory: add or toggle a `# nonce` comment line in its `schemabot.yaml` and push. SchemaBot then plans the whole directory against the live schema, and the plan comment carries the apply command.\n\n</details>\n")
	}
	if data.RequestedBy != "" {
		fmt.Fprintf(&sb, "\n_Requested by @%s_\n", data.RequestedBy)
	}
	return sb.String()
}

// RenderSchemaChangeReconciliationRequired explains that the current PR no
// longer contains a schema change whose apply has already started.
func RenderSchemaChangeReconciliationRequired(data SchemaChangeReconciliationData) string {
	var sb strings.Builder
	sb.WriteString("## " + glyph.Attention + " Schema Change Reconciliation Required\n\n")
	writeReconciliationMetadata(&sb, data.Items)
	writeRequestedLine(&sb, data.RequestedBy, data.Timestamp)
	sb.WriteString("\n")

	if reconciliationHasInProgressApply(data.Items) {
		writeInProgressReconciliation(&sb, data.Tenant, data.Items)
	} else {
		writeCompletedReconciliation(&sb, data.Tenant, data.Items)
	}

	return offerSupportChannel(sb.String())
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
	fmt.Fprintf(sb, "     ```\n     %s\n     ```\n", planCommand(tenant, items))
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
	fmt.Fprintf(sb, "     ```\n     %s\n     ```\n", planCommand(tenant, items))
	sb.WriteString("\n2. Undo the live schema change:\n")
	sb.WriteString("   - comment:\n")
	fmt.Fprintf(sb, "     ```\n     %s\n     ```\n", rollbackCommand(tenant, items))
	sb.WriteString("   - after rollback: push a no-op `schemabot.yaml` edit to trigger a fresh plan\n")
}

func planCommand(tenant string, items []SchemaChangeReconciliationItem) string {
	item, ok := singleCommandItem(items)
	if !ok {
		return appendTenantFlag("schemabot plan -e <environment> -d <database>", tenant)
	}
	cmd := fmt.Sprintf("schemabot plan -e %s", item.Environment)
	if item.Database != "" {
		cmd += fmt.Sprintf(" -d %s", item.Database)
	}
	return appendTenantFlag(cmd, tenant)
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
