package templates

import (
	"fmt"
	"strings"

	"github.com/block/schemabot/pkg/glyph"
	"github.com/block/schemabot/pkg/ui"
)

// autoPlanSizeLimitListedDatabases is how many database names the comment
// lists before summarizing the rest as a count, so a PR across hundreds of
// databases still gets a comment that reads at a glance.
const autoPlanSizeLimitListedDatabases = 10

// AutoPlanSizeLimitData is what the comment posted for a PR too large to plan
// automatically says about it.
type AutoPlanSizeLimitData struct {
	// Databases names the databases the PR changes schema in, in the order
	// the comment lists them. They come from the PR's own schemabot.yaml
	// files, so each renders as a code span it cannot break out of.
	Databases []string
	// SchemaFiles is how many schema files the PR changes in those databases.
	SchemaFiles int
	// MaxDatabases and MaxSchemaFiles are the deployment's limits.
	MaxDatabases   int
	MaxSchemaFiles int
	// TooManyDatabases reports which limit the PR is over: the database limit
	// when true, otherwise the schema file limit.
	TooManyDatabases bool
	// Tenant is the deployment's tenant, appended to the command hint so it
	// reaches this deployment.
	Tenant string
}

// RenderAutoPlanSizeLimit renders the comment posted instead of a plan when a
// PR changes schema in more databases, or more schema files across several
// databases, than SchemaBot plans automatically. It names the limit the PR is
// over, what clears it, and the command that plans one database meanwhile.
func RenderAutoPlanSizeLimit(data AutoPlanSizeLimitData) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "## %s This PR is too large to plan automatically\n\n", glyph.Attention)
	sb.WriteString(AutoPlanSizeLimitReason(data) + "\n\n")
	sb.WriteString("**What to do next:** split this PR into smaller PRs. Until then its SchemaBot check stays blocked and `schemabot apply` is refused.\n\n")
	if len(data.Databases) > 0 {
		sb.WriteString("To see one database's plan now, comment:\n\n")
		command := appendTenantFlag("schemabot plan -d "+flattenIdentifier(data.Databases[0]), data.Tenant)
		fmt.Fprintf(&sb, "```\n%s\n```\n\n", command)
		sb.WriteString("Databases this PR changes: " + listedDatabases(data.Databases) + "\n")
	}
	writeSupportChannelOffer(&sb)
	return sb.String()
}

// AutoPlanSizeLimitReason states which limit a PR is over, in one sentence
// pair the comment and the Check Run summary share. It carries counts only, so
// it is safe in a Check Run summary, which renders no code spans.
func AutoPlanSizeLimitReason(data AutoPlanSizeLimitData) string {
	if data.TooManyDatabases {
		return fmt.Sprintf("It changes schema in %s databases, and SchemaBot plans at most %s databases in one PR.",
			ui.FormatNumber(int64(len(data.Databases))), ui.FormatNumber(int64(data.MaxDatabases)))
	}
	return fmt.Sprintf("It changes %s schema files across %s databases, and SchemaBot plans at most %s schema files in one PR that changes more than one database.",
		ui.FormatNumber(int64(data.SchemaFiles)), ui.FormatNumber(int64(len(data.Databases))), ui.FormatNumber(int64(data.MaxSchemaFiles)))
}

func listedDatabases(databases []string) string {
	shown := databases
	if len(shown) > autoPlanSizeLimitListedDatabases {
		shown = shown[:autoPlanSizeLimitListedDatabases]
	}
	list := strings.Join(inlineCodeList(shown), ", ")
	if rest := len(databases) - len(shown); rest > 0 {
		list += fmt.Sprintf(", and %s more", ui.FormatNumber(int64(rest)))
	}
	return list
}

// PreviewAutoPlanSizeLimitDatabases renders the comment for a PR that renames
// schema files across more databases than one automatic plan covers.
func PreviewAutoPlanSizeLimitDatabases() string {
	databases := []string{"billing", "catalog", "checkout", "fulfillment", "inventory", "invoices", "ledger", "loyalty", "notifications", "orders", "payments", "pricing"}
	for i := len(databases); i < 28; i++ {
		databases = append(databases, fmt.Sprintf("region_%02d", i))
	}
	return RenderAutoPlanSizeLimit(AutoPlanSizeLimitData{
		Databases:        databases,
		SchemaFiles:      400,
		MaxDatabases:     10,
		MaxSchemaFiles:   100,
		TooManyDatabases: true,
	})
}

// PreviewAutoPlanSizeLimitSchemaFiles renders the comment for a PR that
// changes more schema files across a few databases than one automatic plan
// covers.
func PreviewAutoPlanSizeLimitSchemaFiles() string {
	return RenderAutoPlanSizeLimit(AutoPlanSizeLimitData{
		Databases:      []string{"orders", "payments", "shipping"},
		SchemaFiles:    240,
		MaxDatabases:   10,
		MaxSchemaFiles: 100,
	})
}
