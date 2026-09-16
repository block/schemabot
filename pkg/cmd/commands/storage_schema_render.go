package commands

import (
	"fmt"
	"strings"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/cmd/internal/templates"
	"github.com/block/schemabot/pkg/glyph"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/ui"
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
func writeStorageSchemaBody(report *apitypes.StorageSchemaReport, isApply bool, rerun string, hints []string) error {
	if report.Converged {
		// No hints under a converged plan: every one of them names a next step,
		// and there is no next step to take.
		templates.WriteNoChanges()
		return nil
	}

	dialect := schema.Dialect(report.Dialect)
	// A manual entry gates the whole drift set: the convergence refuses every
	// statement in the report until an operator resolves it by hand. Printing
	// those statements as runnable SQL would put a block an operator can copy
	// under a heading that says what is about to happen, when nothing is — so
	// while one is present they are listed as what they are, gated, and the
	// manual entries below say what is holding them.
	gated := len(report.Manual) > 0

	outstanding, err := storageSchemaChanges(report.Outstanding)
	if err != nil {
		return err
	}
	if !gated {
		templates.WriteSQLChanges(outstanding, dialect)
	}

	destructive, err := storageSchemaChanges(report.Destructive)
	if err != nil {
		return err
	}
	// Set when the refusal below is one that stops the run, so it can be
	// written after the summary.
	var blocked func()
	if gated {
		templates.WriteChangeNotice(glyph.Attention,
			"Gated behind the manual remediation below; none of these run until it is resolved:",
			storageSchemaNotices(append(append([]apitypes.StorageSchemaStatement{}, report.Outstanding...), report.Destructive...)))
	}
	if len(destructive) > 0 && !gated {
		// The three dispositions a destructive change can be in are the three
		// the rest of the CLI already renders, so they are rendered by the same
		// templates: a plan disclosing one, an apply refusing one, and consent
		// already in effect. Each carries the severity glyph its own heading
		// earns, and each lists a statement the way `plan` and `apply` list an
		// unsafe change.
		switch {
		case report.DestructiveAllowed:
			templates.WriteSQLChanges(destructive, dialect)
			// The consent is not necessarily a flag: a deployment's storage
			// policy can permit these with nothing on the command line.
			templates.WriteUnsafeWarningAllowed(storageSchemaNotices(report.Destructive),
				"destructive storage changes allowed")
		case isApply:
			// Held until after the summary, which is where `apply` prints its
			// own refusal. It carries the command an operator copies, so it
			// belongs last on screen rather than above a summary of tables the
			// run is not going to touch.
			blocked = func() {
				templates.WriteUnsafeChangesBlocked(storageSchemaNotices(report.Destructive), rerun)
			}
		default:
			templates.WriteUnsafeChangesWarning(storageSchemaNotices(report.Destructive))
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
			"Needs manual remediation; nothing converges until these are resolved:",
			storageSchemaNotices(report.Manual))
	}

	templates.WritePlanSummary(storageSchemaRunnable(report, outstanding, destructive))
	// Hints before the refusal, so the command an operator copies is the last
	// thing on screen on every path that prints one. A hint explains what was
	// left behind; the refusal ends with the line that does something about it.
	writeStorageSchemaHints(hints)
	if blocked != nil {
		blocked()
	}
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
		return storageSchemaSummaryTables(outstanding)
	}
	runnable := make([]templates.DDLChange, 0, len(outstanding)+len(destructive))
	runnable = append(runnable, outstanding...)
	return storageSchemaSummaryTables(append(runnable, destructive...))
}

// storageSchemaSummaryTables collapses a change list to one entry per table and
// kind, because the summary counts in tables and a report does not.
//
// A report is a list of statements, and a dialect is free to need several for
// one table: a PostgreSQL plan emits an add_column per missing column and a
// create_index per missing index, so two columns missing from one table arrive
// as two alters. Counted directly they render as "2 tables to alter" against a
// plan that names one table, and the operator is left to work out which number
// is wrong. Only the summary collapses — every statement is still printed.
func storageSchemaSummaryTables(changes []templates.DDLChange) []templates.DDLChange {
	if len(changes) == 0 {
		return nil
	}
	type tableKind struct{ table, kind string }
	seen := make(map[tableKind]struct{}, len(changes))
	tables := make([]templates.DDLChange, 0, len(changes))
	for _, change := range changes {
		key := tableKind{table: change.TableName, kind: change.ChangeType}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		tables = append(tables, change)
	}
	return tables
}

// outputStorageSchemaPlan prints a storage plan the way `plan` prints one.
func outputStorageSchemaPlan(report *apitypes.StorageSchemaReport, isApply bool, rerun string, hints []string) error {
	writeStorageSchemaHeader(report, isApply)
	return writeStorageSchemaBody(report, isApply, rerun, hints)
}

// outputStorageSchemaConvergence prints what a convergence ran and what it left
// behind. Both are printed even on a clean run, because "nothing is left" is
// the fact an operator is looking for and an absent section does not state it.
//
// withPlan prints the plan that ran, for the unattended path where no preview
// was shown before it. The planned report is that plan, so printing it after
// the fact costs nothing and leaves every run self-describing.
func outputStorageSchemaConvergence(planned, remaining *apitypes.StorageSchemaReport, withPlan bool, rerun string) error {
	if withPlan {
		writeStorageSchemaHeader(planned, true)
		if err := writeStorageSchemaBody(planned, true, rerun, nil); err != nil {
			return err
		}
	}

	applied := len(planned.AppliedStatements())
	database := storageSchemaHeaderDatabase(planned)
	if remaining.Converged {
		fmt.Printf("✓ Ran %d %s against %s. Nothing is outstanding.\n\n",
			applied, ui.Pluralize("statement", applied), database)
		return nil
	}
	if applied == 0 {
		// Nothing ran and something is still outstanding, so the convergence
		// refused the whole set — a ✓ over a run that changed nothing reads as
		// a success an operator has to disprove from the sections below it.
		fmt.Printf("%s Ran no statements against %s; the storage schema is unchanged.\n\n", glyph.Refused, database)
	} else {
		fmt.Printf("✓ Ran %d %s against %s.\n\n", applied, ui.Pluralize("statement", applied), database)
	}
	if withPlan {
		// What remains is a subset of what was planned, and the plan above
		// listed all of it with the apply's own severity. Printing those
		// sections again reports one refusal twice — including on a run that
		// applied something and refused something, where the count of what ran
		// is the only new fact and the refusal below it is not.
		return nil
	}
	return writeStorageSchemaBody(remaining, true, rerun, []string{
		"These were not run. A destructive statement is refused unless --allow-unsafe is passed; a manual remediation blocks everything else until it is resolved.",
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
// numbers: one line per statement, carrying the reason it will not simply run.
//
// A statement with no reason of its own is named by what it would do, not by
// its DDL. The gated list is where that matters: a gated set is every statement
// in the report, and the outstanding ones among them are unremarkable — nothing
// is wrong with them beyond the gate — so they carry no reason at all. Printing
// their DDL instead would put a whole CREATE TABLE, newlines and indentation
// included, on one numbered line, and the list an operator is counting through
// would be lost inside it.
func storageSchemaNotices(statements []apitypes.StorageSchemaStatement) []apitypes.UnsafeChange {
	if len(statements) == 0 {
		return nil
	}
	notices := make([]apitypes.UnsafeChange, 0, len(statements))
	for _, statement := range statements {
		notice := apitypes.UnsafeChange{
			Table:      statement.Table,
			Reason:     statement.Reason,
			DDL:        statement.DDL,
			ChangeType: storageSchemaOperationLabel(statement.Operation),
		}
		if statement.Reason != "" {
			// One statement's reason is one finding. Declaring it keeps the
			// shared list from splitting a sentence at its semicolon and
			// numbering each half as something separate to fix.
			notice.Reasons = []string{statement.Reason}
		}
		notices = append(notices, notice)
	}
	return notices
}

// storageSchemaOperationLabel says what a statement does, in the words the
// report itself uses, for a list entry that has no reason to print instead.
func storageSchemaOperationLabel(operation string) string {
	label := strings.ReplaceAll(strings.ToLower(strings.TrimSpace(operation)), "_", " ")
	if label == "" {
		return "change"
	}
	return label
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
//
// The deployment clause names the environment only when there is one to name.
// A deployment with no environment is refused where a request is resolved, so
// the pairing should not arrive here — but a label is read during an incident,
// and "deployment west in " ends a sentence with a preposition and no object,
// which reads as a truncated label rather than as a missing field.
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
	switch {
	case report.Deployment != "" && report.Environment != "":
		label += fmt.Sprintf(", deployment %s in %s", report.Deployment, report.Environment)
	case report.Deployment != "":
		label += fmt.Sprintf(", deployment %s", report.Deployment)
	}
	return label
}
