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
// The question being asked is the same one either way — here is a live
// database, here is a desired schema, here is the DDL between them — and only
// the target differs. So anyone who can read `plan` can read `storage plan`,
// which matters because `plan` is read every day and `storage plan` is read
// during an incident.

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

	outstanding, err := storageSchemaChanges(report.Outstanding)
	if err != nil {
		return err
	}
	destructive, err := storageSchemaChanges(report.Destructive)
	if err != nil {
		return err
	}
	manual, err := storageSchemaChanges(report.Manual)
	if err != nil {
		return err
	}

	// Every statement in the report is printed as SQL, the way `plan` prints an
	// unsafe change it is about to refuse: the DDL says what the difference
	// between the two schemas is, and the sections under it say which of it
	// runs. A statement held back is the one an operator most needs in front of
	// them — a refused DROP to weigh, a manual remediation to run by hand.
	//
	// Each disposition renders on its own, rather than as one list, because the
	// dialect's formatter combines the alters of a single table into one
	// statement. A storage ALTER that was split so its safe half could run
	// would be recombined into a statement nothing is going to run, and the
	// split that is the whole point of the refusal would not be on screen.
	templates.WriteSQLChanges(outstanding, dialect)
	templates.WriteSQLChanges(destructive, dialect)
	templates.WriteSQLChanges(manual, dialect)

	// Set when the refusal below is one that stops the run, so it can be
	// written after the summary.
	var blocked func()
	if len(destructive) > 0 {
		// The dispositions a destructive change can be in are the ones the rest
		// of the CLI already renders, so they are rendered by the same
		// templates: a plan disclosing one, an apply refusing one, and consent
		// already in effect. Each carries the severity glyph its own heading
		// earns, and each lists a statement the way `plan` and `apply` list an
		// unsafe change.
		switch {
		case len(report.Manual) > 0:
			// A manual entry blocks the whole set, so these are neither about
			// to run nor the thing standing in the way: consent would permit
			// them and converge nothing. They are disclosed as what they are,
			// and the remediation below names the remedy that does unblock.
			templates.WriteUnsafeChangesWarning(storageSchemaNotices(report.Destructive))
		case report.DestructiveAllowed:
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

	all := make([]templates.DDLChange, 0, len(outstanding)+len(destructive)+len(manual))
	all = append(all, outstanding...)
	all = append(all, destructive...)
	templates.WritePlanSummary(storageSchemaSummaryTables(append(all, manual...)))
	// Hints before the refusal, so the command an operator copies is the last
	// thing on screen on every path that prints one. A hint explains what was
	// left behind; the refusal ends with the line that does something about it.
	writeStorageSchemaHints(hints)
	if blocked != nil {
		blocked()
	}
	return nil
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
	// Before the statements, because it changes how they should be read, and
	// before the body's converged short-circuit, because a convergence still
	// finishing up is worth saying over a report that found nothing
	// outstanding.
	//
	// This entry point renders a report before anything has run, which is what
	// makes the waiting variant true here: on the apply path the run being
	// described is the one that is about to queue behind the holder.
	writeStorageConvergenceInFlight(report, isApply)
	return writeStorageSchemaBody(report, isApply, rerun, hints)
}

// outputStorageSchemaConvergence prints what a convergence ran and what it left
// behind. Both are printed even on a clean run, because "nothing is left" is
// the fact an operator is looking for and an absent section does not state it.
//
// withPlan prints the plan that ran, for the unattended path where no preview
// was shown before it. The planned report is that plan, so printing it after
// the fact costs nothing and leaves every run self-describing.
//
// Neither report is rendered with the in-flight notice the plan path writes.
// The planned report carries the lock as it was read before this run started,
// and by the time anything here prints, whatever wait that implied is over —
// announcing it above the count of what ran would tell an operator they are
// about to queue for a lock they already queued for. The re-read below says
// the live version of the same fact instead.
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
	// The re-read happened after this run released the lock, so a lock held now
	// is somebody else's and what follows is a catalog read mid-convergence.
	// That is the plan's reading of the fact, not the apply's: this run is
	// finished and waits for nothing.
	writeStorageConvergenceInFlight(remaining, false)
	return writeStorageSchemaBody(remaining, true, rerun, []string{
		"These were not run. A destructive statement is refused unless --allow-unsafe is passed; a manual remediation blocks everything else until it is resolved.",
	})
}

// writeStorageConvergenceInFlight says that somebody else is converging this
// storage right now, when the report says so.
//
// Only the positive is printed. The report's false covers both "nobody is
// converging" and "the question could not be answered", and a line announcing
// an idle database would state the second as the first — on the one surface an
// operator reads to decide whether it is safe to start their own run.
//
// What it costs them to not know differs, so each caller says its own
// consequence. willWait picks between the two, and it asks about this run
// rather than about which command is running: a report rendered before a
// convergence starts belongs to a run that is about to sit on the lock for as
// long as the run ahead of it takes, while every other report — a plan, and
// the re-read taken after a convergence has already returned — only says that
// the catalog was read mid-convergence and will read differently in a minute.
//
// An apply renders reports of both kinds, which is why the distinction cannot
// be drawn from the command.
func writeStorageConvergenceInFlight(report *apitypes.StorageSchemaReport, willWait bool) {
	if !report.ConvergenceInFlight {
		return
	}
	consequence := "A statement a convergence is working on is absent from the live catalog until it finishes with it, so these statements are what is outstanding, not what is idle. Re-run this once it finishes to see what it left."
	if willWait {
		consequence = "This run waits on the storage bootstrap lock until that one finishes or its budget runs out, and may then find nothing left to do."
	}
	fmt.Printf("%s A storage convergence is already running against %s.%s %s\n\n",
		glyph.Attention, storageSchemaHeaderDatabase(report), storageConvergenceLockScope(report.Dialect), consequence)
}

// storageConvergenceLockScope qualifies which database the convergence that
// was detected is actually converging, on the dialect where the lock cannot
// say.
//
// MySQL scopes a named lock to the server rather than to the database the
// session is connected to, so two instances whose storage lives on one server
// read each other's convergence here. The consequence above holds either way —
// this run waits behind that one — but the database named beside it may not be
// the one being converged, and an operator who goes looking for the run needs
// that before they start looking in the wrong place.
//
// PostgreSQL advisory locks are per-database, so there the holder is
// converging this database and there is nothing to qualify.
func storageConvergenceLockScope(dialect string) string {
	if schema.Dialect(dialect) != schema.DialectMySQL {
		return ""
	}
	return " MySQL scopes the bootstrap lock to the server, so the run holding it may be converging another database on the same server."
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
// its DDL, so an entry stays one line. Printing the DDL instead would put a
// whole CREATE TABLE, newlines and indentation included, on one numbered line,
// and the list an operator is counting through would be lost inside it. The
// DDL is above in its own section, rendered as SQL.
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
