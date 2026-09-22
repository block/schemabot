package apitypes

// StorageSchemaStatement is one outstanding statement in a storage schema
// report. The DDL is the statement itself, runnable as printed: an operator
// mid-incident copies these out and runs them by hand.
type StorageSchemaStatement struct {
	Table     string `json:"table"`
	Operation string `json:"operation"`
	DDL       string `json:"ddl"`
	// Reason is why the statement is destructive, or why it needs manual
	// remediation. Empty for a statement that runs automatically.
	Reason string `json:"reason,omitempty"`
}

// BootRemovalPolicy is what a boot of the answering deployment does to storage
// state its own schema does not declare.
type BootRemovalPolicy string

const (
	// BootRemovalUnknown is the answering side declining to say, and it is a
	// real answer rather than a default. A release that predates the field
	// leaves it empty, and so does a convergence addressed by DSN alone, which
	// has no deployment config to read. Treating it as BootRemovalPreserves is
	// how an operator gets told to pre-apply storage the next pod will drop, so
	// callers state the uncertainty instead of resolving it.
	BootRemovalUnknown BootRemovalPolicy = ""
	// BootRemovalPreserves is a boot leaving surplus storage state in place: it
	// either refuses the removals, or never computes one.
	BootRemovalPreserves BootRemovalPolicy = "preserves"
	// BootRemovalRemoves is a boot dropping the tables, columns and indexes its
	// own schema does not declare.
	BootRemovalRemoves BootRemovalPolicy = "removes"
)

// StorageSchemaReport is what one SchemaBot instance's storage database needs
// in order to match that instance's embedded schema.
//
// The three statement lists are disjoint and have different dispositions:
// Outstanding runs, Destructive is refused unless destructive changes are
// allowed, and any Manual entry blocks the whole convergence.
type StorageSchemaReport struct {
	// Deployment is the data plane whose storage this describes; empty means
	// the control plane's own storage — the server the request was made to.
	Deployment string `json:"deployment,omitempty"`
	// Environment is the deployment's environment; empty alongside an empty
	// Deployment.
	Environment string `json:"environment,omitempty"`
	// Dialect is the storage database's family: "mysql" or "postgres".
	Dialect string `json:"dialect"`
	// Database is the live database that was read, as its server reports it.
	// It is what makes a report unmistakably about one database.
	Database string `json:"database"`
	// Host is the database server as it names itself. Empty when the server
	// does not report one.
	Host string `json:"host,omitempty"`
	// SchemaSource is where the desired side of the diff came from: the
	// answering binary's embedded files, a directory, or a release. Always set,
	// because one live database yields different answers against different
	// releases.
	SchemaSource string `json:"schema_source,omitempty"`
	// Version is the SchemaBot version of the binary that answered. It is
	// attribution, not an input: the diff came from schema files, and
	// SchemaSource says which ones.
	Version string `json:"version,omitempty"`
	// Converged reports that the storage schema needs nothing at all. A report
	// carrying only refused destructive statements is not converged.
	Converged   bool                     `json:"converged"`
	Outstanding []StorageSchemaStatement `json:"outstanding,omitempty"`
	Destructive []StorageSchemaStatement `json:"destructive,omitempty"`
	// DestructiveAllowed reports whether the destructive statements run on this
	// call: the deployment's standing policy, widened by an opt-in this caller
	// sent. BootRemovalPolicy is the one to read for what some other process
	// does.
	DestructiveAllowed bool                     `json:"destructive_allowed"`
	Manual             []StorageSchemaStatement `json:"manual,omitempty"`
	// BootRemovalPolicy is what the next pod to start does to storage state its
	// own schema does not declare, which is what says whether state converged
	// ahead of a deploy survives until that deploy.
	//
	// It is the deployment's policy and its dialect, never this request's
	// opt-in. Empty is BootRemovalUnknown and is a real answer: see the
	// constants.
	BootRemovalPolicy BootRemovalPolicy `json:"boot_removal_policy,omitempty"`
	// ConvergenceInFlight reports that some instance held the storage bootstrap
	// lock when the diff was taken — a pod booting, or another operator's
	// apply. It is what separates "this DDL is outstanding" from "this DDL is
	// being run right now", which the statement lists cannot say on their own:
	// a statement a convergence is working on stays absent from the live
	// catalog until that convergence finishes with it.
	//
	// Only true is a finding. False is the absence of evidence, not a claim
	// that the database is idle.
	ConvergenceInFlight bool `json:"convergence_in_flight,omitempty"`
}

// AppliedStatements is what a convergence ran from this report of what it
// planned: the outstanding statements always, and the destructive ones when
// the convergence was permitted to run them. They stay in their own list
// either way, because the disposition is what an operator is reading the
// report for — but counting only the outstanding set would tell an operator
// who just dropped surplus state that nothing ran.
//
// A manual entry empties the result whatever else the report holds: the
// convergence refuses the whole drift set while one is outstanding, so every
// statement here is one that did not run.
func (r *StorageSchemaReport) AppliedStatements() []StorageSchemaStatement {
	if r == nil {
		return nil
	}
	if len(r.Manual) > 0 {
		return nil
	}
	if !r.DestructiveAllowed || len(r.Destructive) == 0 {
		return r.Outstanding
	}
	applied := make([]StorageSchemaStatement, 0, len(r.Outstanding)+len(r.Destructive))
	applied = append(applied, r.Outstanding...)
	return append(applied, r.Destructive...)
}
