package tern

import (
	"context"
	"errors"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
)

// ErrInvalidStorageSchemaRequest marks a storage schema failure the caller
// caused and the caller can fix — a malformed schema file, a request field
// that does not describe a schema.
//
// It exists so the gRPC wrapper can answer InvalidArgument for those and
// Internal for everything else. The distinction is worth carrying because the
// two ask opposite things of whoever sees them: an InvalidArgument is a
// correction to make to the next request, while an Internal names a data plane
// to go and read the logs of. A serving adapter wraps the errors it raises on
// the caller's behalf with this; an error it does not wrap is its own.
var ErrInvalidStorageSchemaRequest = errors.New("invalid storage schema request")

// StorageSchemaService answers for the storage schema of one SchemaBot
// instance: which storage DDL is outstanding on the database that instance
// keeps its bookkeeping in, and converging it.
//
// One interface serves both ends of the wire, because both ends do the same
// thing to the same question:
//
//	serving side  an adapter the embedder supplies, bound to its own storage
//	              DSN and dialect, which reads that database and converges it
//	caller side   *GRPCClient, which forwards the call to the data plane whose
//	              endpoint it dials
//
// The distinction that matters is that it is never about a target database. A
// storage schema belongs to the instance that runs on it, so there is no
// database, environment, or route to name in the request: an instance answers
// for its own storage, and no field a caller can set points it elsewhere.
// That is what makes the answer trustworthy — the diff comes from the embedded
// schema files of the binary that read the live catalog, in the same call, so
// it cannot be a claim about a release pin somewhere else.
type StorageSchemaService interface {
	StorageSchemaPlan(ctx context.Context, req *ternv1.StorageSchemaPlanRequest) (*ternv1.StorageSchemaPlanResponse, error)
	StorageSchemaApply(ctx context.Context, req *ternv1.StorageSchemaApplyRequest) (*ternv1.StorageSchemaApplyResponse, error)
}

// StorageSchemaService is deliberately not part of Client. Client is the
// schema change surface — plan, apply, and control a change on a *target*
// database — and every implementation of it must be able to serve all of it.
// A storage schema is a property of a deployment's own installation, so a
// Client that happens to run in the caller's process has nothing to forward
// and nothing separate to report. Keeping the two apart lets a caller ask
// "can this route reach a data plane's storage?" as a type assertion instead
// of by testing a method for a not-implemented error.
var (
	_ StorageSchemaService = (*GRPCClient)(nil)
)
