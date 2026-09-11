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
	// Version is the SchemaBot version of the binary whose embedded schema
	// produced the diff. It is attribution, not an input: the diff came from
	// the files themselves.
	Version string `json:"version,omitempty"`
	// Converged reports that the storage schema needs nothing at all. A report
	// carrying only refused destructive statements is not converged.
	Converged          bool                     `json:"converged"`
	Outstanding        []StorageSchemaStatement `json:"outstanding,omitempty"`
	Destructive        []StorageSchemaStatement `json:"destructive,omitempty"`
	DestructiveAllowed bool                     `json:"destructive_allowed"`
	Manual             []StorageSchemaStatement `json:"manual,omitempty"`
}

// StorageSchemaDiffResponse is the HTTP response for
// GET /api/storage/schema/diff.
type StorageSchemaDiffResponse struct {
	Report *StorageSchemaReport `json:"report"`
}

// StorageSchemaApplyRequest is the HTTP request for
// POST /api/storage/schema/apply.
type StorageSchemaApplyRequest struct {
	// Deployment names the data plane whose storage to converge. Empty
	// converges the storage of the server the request is made to.
	Deployment string `json:"deployment,omitempty"`
	// Environment is required alongside Deployment, since a deployment serves
	// one gRPC endpoint per environment.
	Environment string `json:"environment,omitempty"`
	// AllowDestructive permits the destructive statements the convergence
	// would otherwise refuse. It widens the target's standing storage policy
	// and never narrows it.
	AllowDestructive bool `json:"allow_destructive,omitempty"`
	// Caller identifies the operator, so the converging instance's logs
	// attribute the convergence to a person. An authenticated identity
	// overrides it.
	Caller string `json:"caller,omitempty"`
}

// StorageSchemaApplyResponse is the HTTP response for
// POST /api/storage/schema/apply. It brackets the convergence: Planned is what
// was outstanding before it ran, Remaining is what is still outstanding after.
type StorageSchemaApplyResponse struct {
	Planned   *StorageSchemaReport `json:"planned"`
	Remaining *StorageSchemaReport `json:"remaining"`
}
