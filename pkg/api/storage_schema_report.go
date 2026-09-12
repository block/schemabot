package api

import (
	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/schema"
)

// A storage schema report is what one storage database needs in order to match
// a desired schema, said in a way an operator can act on without knowing which
// family the database belongs to.
//
// The shape is the whole point. Every dialect's differ reduces to the same
// three disjoint sets with the same three dispositions, so a report from a
// MySQL deployment and a report from a PostgreSQL one read alike, and a job
// that consumes one consumes the other. Which statements each differ produces
// is its own business; how they are classified, counted and rendered is here.

// StorageSchemaStatement is one outstanding storage-schema statement, with the
// classification the bootstrap would apply to it.
type StorageSchemaStatement struct {
	// Table is the storage table the statement acts on.
	Table string
	// Operation is the statement's kind, in the vocabulary the storage layer
	// uses for it (create_table, alter_table, add_column, create_index, ...).
	Operation string
	// DDL is the statement itself, runnable as printed.
	DDL string
	// Reason is why the statement is classified destructive, or why it needs
	// manual remediation. Empty for a statement that runs automatically.
	Reason string
}

// StorageSchemaReport is what the storage schema of one database needs in order
// to match a desired schema — the embedded schema of the binary that produced
// the report, unless SchemaSource names another.
//
// The three statement sets are disjoint and have different dispositions, so an
// operator reading the report never has to work out which statements would
// actually run:
//
//	Outstanding  runs on the next boot, and on apply
//	Destructive  refused unless destructive changes are explicitly allowed
//	Manual       blocks the whole convergence until an operator resolves it
type StorageSchemaReport struct {
	// Dialect is the storage database's family.
	Dialect schema.Dialect
	// Database is the live database the diff read, as the server reports it —
	// so a report cannot be misread as being about a different database.
	Database string
	// Host is the database server as it names itself, so a report names the
	// machine as well as the database on it. Empty when the server does not
	// report one.
	Host string
	// SchemaSource says where the desired side of the diff came from, in the
	// words StorageSchemaSource attributed it with. It is always set, because
	// one live database yields different answers against different releases and
	// a report that did not say which one it used could be read as either.
	SchemaSource string
	// Version is the SchemaBot version of the binary that produced the report.
	// It is reported, never consumed: the diff is computed from schema files,
	// and this says which binary read them — which is the same release that
	// embedded them unless SchemaSource says otherwise.
	Version string
	// Outstanding lists the statements that converge the schema and run
	// automatically, in the order the convergence would run them.
	Outstanding []StorageSchemaStatement
	// Destructive lists the statements the bootstrap classifies as destroying
	// data. They are refused unless destructive changes are explicitly
	// allowed; DestructiveAllowed says which of the two this database is in.
	Destructive []StorageSchemaStatement
	// DestructiveAllowed reports whether the destructive statements would
	// actually run. It reflects the effective policy for the request — the
	// storage config's allowance, or an explicit per-request opt-in.
	DestructiveAllowed bool
	// Manual lists changes that cannot run automatically, each naming the
	// situation and the remediation. Any entry aborts convergence before a
	// single statement executes, so an apply is refused while one is present.
	Manual []StorageSchemaStatement
}

// Converged reports whether the storage schema needs nothing at all. A report
// with only refused destructive statements is not converged: the surplus state
// stays in place deliberately (AV-9), and saying otherwise would tell an
// operator the database matches this binary's schema when it does not.
func (r *StorageSchemaReport) Converged() bool {
	return len(r.Outstanding) == 0 && len(r.Destructive) == 0 && len(r.Manual) == 0
}

// appliedCount is how many of a planned report's statements a convergence ran:
// the outstanding ones always, and the destructive ones when it was permitted
// to run them. Counting only the outstanding set would log that nothing ran
// after a convergence that dropped surplus state on purpose.
func (r *StorageSchemaReport) appliedCount() int {
	if len(r.Manual) > 0 {
		// A manual entry stops the whole drift set before any statement runs,
		// so nothing from this report was applied.
		return 0
	}
	if !r.DestructiveAllowed {
		return len(r.Outstanding)
	}
	return len(r.Outstanding) + len(r.Destructive)
}

// APIType converts a report to the HTTP response shape, which the CLI renders
// from. Deployment and Environment are left to the caller: only whoever routed
// the request knows which storage was asked for, and a report has to name the
// storage the operator meant rather than whichever one answered.
func (r *StorageSchemaReport) APIType() *apitypes.StorageSchemaReport {
	if r == nil {
		return nil
	}
	return &apitypes.StorageSchemaReport{
		Dialect:            string(r.Dialect),
		Database:           r.Database,
		Host:               r.Host,
		SchemaSource:       r.SchemaSource,
		Version:            r.Version,
		Converged:          r.Converged(),
		Outstanding:        storageSchemaStatementsAPIType(r.Outstanding),
		Destructive:        storageSchemaStatementsAPIType(r.Destructive),
		DestructiveAllowed: r.DestructiveAllowed,
		Manual:             storageSchemaStatementsAPIType(r.Manual),
	}
}

func storageSchemaStatementsAPIType(statements []StorageSchemaStatement) []apitypes.StorageSchemaStatement {
	if len(statements) == 0 {
		return nil
	}
	out := make([]apitypes.StorageSchemaStatement, 0, len(statements))
	for _, s := range statements {
		out = append(out, apitypes.StorageSchemaStatement{
			Table:     s.Table,
			Operation: s.Operation,
			DDL:       s.DDL,
			Reason:    s.Reason,
		})
	}
	return out
}

// A report names a statement's kind in one vocabulary whichever dialect the
// storage runs on, so an operator reading two deployments' reports — or a job
// parsing them — does not have to learn two. The PostgreSQL convergence
// already speaks it (postgresOpCreateTable and its siblings); the MySQL differ
// speaks in statement types, and storageSchemaOperation translates.
const (
	storageSchemaOpCreateTable = postgresOpCreateTable
	storageSchemaOpAlterTable  = "alter_table"
	storageSchemaOpDropTable   = "drop_table"
)
