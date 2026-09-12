package apitypes

// StorageSchemaPlanRequest is the HTTP request for
// POST /api/storage/schema/plan.
//
// The request reads and changes nothing, and is a POST because the desired
// schema travels in the body: a caller asking what a database needs in order to
// match a later release sends that release's files, which the answering binary
// does not have. Every field is optional — the defaults ask the addressed
// server what its own storage needs to match its own embedded schema.
type StorageSchemaPlanRequest struct {
	// Deployment names the data plane whose storage to read. Empty reads the
	// storage of the server the request is made to.
	Deployment string `json:"deployment,omitempty"`
	// Environment is required alongside Deployment, since a deployment serves
	// one gRPC endpoint per environment.
	Environment string `json:"environment,omitempty"`
	// AllowDestructive reports the destructive statements as ones that would
	// run. It executes nothing either way: a diff never does.
	AllowDestructive bool `json:"allow_destructive,omitempty"`
	// SchemaFiles is the desired schema as file name → file contents. Empty
	// diffs against the answering binary's own embedded schema.
	SchemaFiles map[string]string `json:"schema_files,omitempty"`
	// SchemaSource says where SchemaFiles came from, in words, for the report
	// to attribute the answer to. Required with SchemaFiles and never inferred.
	SchemaSource string `json:"schema_source,omitempty"`
}

// StorageSchemaPlanResponse is the HTTP response for
// POST /api/storage/schema/plan.
type StorageSchemaPlanResponse struct {
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
