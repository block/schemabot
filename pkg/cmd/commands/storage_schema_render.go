package commands

import (
	"fmt"
	"strings"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/cmd/internal/templates"
	"github.com/block/schemabot/pkg/glyph"
	"github.com/block/schemabot/pkg/schema"
)

// A storage schema plan is rendered the way every other plan in this CLI is
// rendered, from the same templates: the header box, the environment, the
// per-table sections with their change symbols, the disclosure of changes that
// will not run, and the plan summary line.
//
// The database is unusual — SchemaBot's own bookkeeping storage rather than one
// an operator asked to change — but nothing an operator does with the output is
// unusual, so nothing about the output should be. Whoever can read `plan` can
// read `storage plan`, which matters because the second one is read during an
// incident and the first one is read every day.

// writeStorageSchemaHeader writes the header box and the environment heading.
//
// The schema the report was compared against is in the box because it changes
// what the rest of the output means: the same storage is converged against the
// release that is running and short of the release about to roll, and both
// plans are correct.
func writeStorageSchemaHeader(report *apitypes.StorageSchemaReport, isApply bool) {
	templates.WritePlanHeader(templates.PlanHeaderData{
		Database:    storageSchemaHeaderDatabase(report),
		SchemaName:  report.SchemaSource,
		SchemaLabel: "Schema",
		EngineLabel: storageSchemaEngineLabel(report.Dialect),
		IsApply:     isApply,
	})
	if report.Environment != "" {
		templates.WriteEnvironmentHeader(report.Environment)
	}
}

// storageSchemaHeaderDatabase names the database inside the box, where the
// title already carries the dialect and the line below carries the environment.
// It is storageSchemaDatabaseLabel without the two facts the box states
// elsewhere — an error message has nowhere else to put them, a box does.
func storageSchemaHeaderDatabase(report *apitypes.StorageSchemaReport) string {
	database := report.Database
	if database == "" {
		database = "the storage database"
	}
	if report.Host != "" {
		database += " on " + report.Host
	}
	if report.Deployment != "" {
		database += fmt.Sprintf(" (deployment %s)", report.Deployment)
	}
	return database
}

// storageSchemaEngineLabel titles the plan with the storage's own family. The
// two families SchemaBot stores its state on are not the two a schema change
// plan titles itself with, so the label is passed rather than derived from the
// engine of a user's database.
func storageSchemaEngineLabel(dialect string) string {
	switch schema.Dialect(dialect) {
	case schema.DialectPostgres:
		return "PostgreSQL"
	case schema.DialectMySQL:
		return "MySQL"
	default:
		return "Storage"
	}
}

// writeStorageSchemaBody writes everything under the header: the statements,
// the disposition of the ones that will not simply run, the summary, and any
// hint naming the next step.
func writeStorageSchemaBody(report *apitypes.StorageSchemaReport, isApply bool, hints []string) error {
	if report.Converged {
		// No hints under a converged plan: every one of them names a next step,
		// and there is no next step to take.
		templates.WriteNoChanges()
		return nil
	}

	dialect := schema.Dialect(report.Dialect)
	outstanding, err := storageSchemaChanges(report.Outstanding)
	if err != nil {
		return err
	}
	templates.WriteSQLChanges(outstanding, dialect)

	destructive, err := storageSchemaChanges(report.Destructive)
	if err != nil {
		return err
	}
	if len(destructive) > 0 {
		// The glyph follows the severity vocabulary: consent already in effect is
		// an escalation, a statement an apply has refused is a refusal, and the
		// same statement merely disclosed by a plan is attention.
		switch {
		case report.DestructiveAllowed:
			templates.WriteSQLChanges(destructive, dialect)
			templates.WriteChangeNotice(glyph.Escalation,
				"Destructive changes, running because destructive storage changes are allowed:",
				storageSchemaNotices(report.Destructive))
		case isApply:
			templates.WriteChangeNotice(glyph.Refused,
				"Destructive changes refused; the surplus state stays in place. Re-run with --allow-destructive to run them:",
				storageSchemaNotices(report.Destructive))
		default:
			templates.WriteChangeNotice(glyph.Attention,
				"Destructive changes, refused unless destructive storage changes are allowed:",
				storageSchemaNotices(report.Destructive))
		}
	}

	if len(report.Manual) > 0 {
		// A manual entry is not one change held back: while one is outstanding
		// the convergence refuses the whole drift set, so an apply reporting one
		// has run nothing at all.
		severity := glyph.Attention
		if isApply {
			severity = glyph.Refused
		}
		templates.WriteChangeNotice(severity,
			"Needs manual remediation; nothing converges until these are resolved by hand:",
			storageSchemaNotices(report.Manual))
	}

	templates.WritePlanSummary(storageSchemaRunnable(report, outstanding, destructive))
	writeStorageSchemaHints(hints)
	return nil
}

// storageSchemaRunnable is what the summary line counts: the changes this plan
// would actually run. Destructive statements count only where they are
// permitted, and nothing counts while a manual entry blocks the whole set —
// a summary promising three tables that the convergence will refuse to touch is
// worse than no summary.
func storageSchemaRunnable(report *apitypes.StorageSchemaReport, outstanding, destructive []templates.DDLChange) []templates.DDLChange {
	if len(report.Manual) > 0 {
		return nil
	}
	if !report.DestructiveAllowed {
		return outstanding
	}
	runnable := make([]templates.DDLChange, 0, len(outstanding)+len(destructive))
	runnable = append(runnable, outstanding...)
	return append(runnable, destructive...)
}

// outputStorageSchemaPlan prints a storage plan the way `plan` prints one.
func outputStorageSchemaPlan(report *apitypes.StorageSchemaReport, isApply bool, hints []string) error {
	writeStorageSchemaHeader(report, isApply)
	return writeStorageSchemaBody(report, isApply, hints)
}

// outputStorageSchemaConvergence prints what a convergence ran and what it left
// behind. Both are printed even on a clean run, because "nothing is left" is
// the fact an operator is looking for and an absent section does not state it.
//
// withPlan prints the plan that ran, for the unattended path where no preview
// was shown before it. The planned report is that plan, so printing it after
// the fact costs nothing and leaves every run self-describing.
func outputStorageSchemaConvergence(planned, remaining *apitypes.StorageSchemaReport, withPlan bool) error {
	if withPlan {
		writeStorageSchemaHeader(planned, true)
		if err := writeStorageSchemaBody(planned, true, nil); err != nil {
			return err
		}
	}

	applied := len(planned.AppliedStatements())
	if remaining.Converged {
		fmt.Printf("✓ Ran %d %s against %s. Nothing is outstanding.\n\n",
			applied, pluralStatements(applied), storageSchemaHeaderDatabase(planned))
		return nil
	}
	fmt.Printf("✓ Ran %d %s against %s.\n\n",
		applied, pluralStatements(applied), storageSchemaHeaderDatabase(planned))
	return writeStorageSchemaBody(remaining, true, []string{
		"These were not run. A destructive statement is refused unless --allow-destructive is passed; a manual entry has to be resolved by hand before anything else converges.",
	})
}

func writeStorageSchemaHints(hints []string) {
	for _, hint := range hints {
		fmt.Printf("%s\n\n", hint)
	}
}

// storageSchemaChanges renders report statements as the plan's own change
// records, so they print through the same formatter every other plan uses.
func storageSchemaChanges(statements []apitypes.StorageSchemaStatement) ([]templates.DDLChange, error) {
	if len(statements) == 0 {
		return nil, nil
	}
	changes := make([]templates.DDLChange, 0, len(statements))
	for _, statement := range statements {
		changeType, err := storageSchemaChangeType(statement.Operation)
		if err != nil {
			return nil, err
		}
		changes = append(changes, templates.DDLChange{
			ChangeType: changeType,
			TableName:  statement.Table,
			DDL:        statement.DDL,
		})
	}
	return changes, nil
}

// storageSchemaChangeType maps the report's operation vocabulary onto the
// plan's three change symbols. Adding a column and creating an index both alter
// a table that is already there, so both read as an alter — the plan's symbol
// says what happens to the table, and the statement below it says how.
//
// An operation with no mapping is an error rather than a default: the change
// symbol and the summary line both come from this, and labelling an unknown
// operation as an alter would misreport what is about to run.
func storageSchemaChangeType(operation string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(operation)) {
	case "create_table":
		return "CREATE", nil
	case "drop_table":
		return "DROP", nil
	case "alter_table", "add_column", "create_index":
		return "ALTER", nil
	default:
		return "", fmt.Errorf("unexpected storage schema operation %q: the storage schema converges by creating, altering and dropping tables only, so there is no way to say what this statement would do", operation)
	}
}

// storageSchemaNotices renders statements as the list entries a change notice
// numbers, carrying the reason the statement will not simply run.
func storageSchemaNotices(statements []apitypes.StorageSchemaStatement) []apitypes.UnsafeChange {
	if len(statements) == 0 {
		return nil
	}
	notices := make([]apitypes.UnsafeChange, 0, len(statements))
	for _, statement := range statements {
		reason := statement.Reason
		if reason == "" {
			// Every destructive and manual statement carries a reason; a report
			// that somehow has none still has to name what the statement does
			// rather than print a table with an empty line beside it.
			reason = statement.DDL
		}
		notices = append(notices, apitypes.UnsafeChange{
			Table:      statement.Table,
			Reason:     reason,
			DDL:        statement.DDL,
			ChangeType: statement.Operation,
		})
	}
	return notices
}

func pluralStatements(n int) string {
	if n == 1 {
		return "statement"
	}
	return "statements"
}

// storageSchemaDatabaseLabel names the database a report is about, as an
// operator would say it: the database, the server it is on, its dialect, and
// the deployment it belongs to when the report came from one.
//
// The server is in the label because "which database does this point at" is the
// question an operator has before they act on any of it, and the answer has to
// be legible without re-deriving it from a DSN, a config file, or a deployment
// name. A server that does not report a name of its own is left out rather than
// guessed at.
func storageSchemaDatabaseLabel(report *apitypes.StorageSchemaReport) string {
	label := report.Database
	if label == "" {
		label = "the storage database"
	}
	if report.Host != "" {
		label += " on " + report.Host
	}
	if report.Dialect != "" {
		label += fmt.Sprintf(" (%s)", report.Dialect)
	}
	if report.Deployment != "" {
		label += fmt.Sprintf(", deployment %s in %s", report.Deployment, report.Environment)
	}
	return label
}

// storageSchemaPlanHints names the next step for the plan that was just
// printed.
//
// Naming `storage apply` here would be wrong, whichever release was named: an
// apply converges the schema of the binary that runs it, which is not
// necessarily the schema this plan is about. The two ways to converge the named
// release's schema are the release itself, and this is where an operator is
// about to look for them.
func storageSchemaPlanHints(report *apitypes.StorageSchemaReport) []string {
	return []string{fmt.Sprintf("These are what %s needs in order to match %s. To converge them, run that release's binary against this database — its container image is that release — or let the release's first boot converge them.", storageSchemaHeaderDatabase(report), report.SchemaSource)}
}
