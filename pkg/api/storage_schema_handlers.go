// storage_schema_handlers.go answers "which storage DDL is outstanding, right
// now" for SchemaBot's own storage — the control plane's, or a data plane's.
//
// The question turns up during a deploy that did not converge, and there the
// obvious way to answer it is wrong. Reading a consumer's go.mod pin, checking
// out that release tag, and diffing its embedded schema files tells you what
// that release *would* converge to; it does not tell you what the storage
// actually converged to, and the two answers differ exactly when a deploy has
// failed. Since that is the only time anyone asks, a version is never an input
// here: the live side is always read from the catalog, and the desired side is
// always files — the answering binary's own, or a release's, sent by the caller
// and named in the report.
//
// That is also why a data plane's storage is reached through the data plane
// rather than dialed from the control plane. Its storage database usually sits
// where neither an operator's workstation nor the control plane can open a
// connection, and the binary that owns it is the only one that can say what its
// own embedded schema declares. The gRPC link that already exists between the
// planes carries the question to it.
package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/block/schemabot/pkg/apitypes"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/tern"
)

// SetStorageSchemaService registers the answerer for this server's own storage
// schema. An embedder supplies one bound to the storage DSN and dialect it
// booted with — the same adapter its gRPC endpoint serves — so the answer a
// local request gets and the answer a peer control plane gets over gRPC come
// from one implementation rather than two that can drift.
//
// Without one the storage schema routes refuse rather than guessing at a DSN,
// because a guess here reads the wrong database and reports it as the right
// one.
func (s *Service) SetStorageSchemaService(service tern.StorageSchemaService) {
	s.storageSchemaMu.Lock()
	defer s.storageSchemaMu.Unlock()
	s.storageSchemaService = service
}

func (s *Service) localStorageSchemaService() tern.StorageSchemaService {
	s.storageSchemaMu.RLock()
	defer s.storageSchemaMu.RUnlock()
	return s.storageSchemaService
}

// storageSchemaTarget is the resolved answer to "whose storage is this request
// about". Resolution is explicit and total: every request either names one
// instance's storage or is refused. Nothing here falls back from an
// unreachable deployment to a reachable database — reporting the control
// plane's storage to an operator who asked about a data plane's would be a
// wrong answer dressed as a right one, and during an incident it is the kind
// of wrong answer that gets acted on.
type storageSchemaTarget struct {
	// deployment and environment are empty for the control plane's own
	// storage, and set for a data plane's.
	deployment  string
	environment string
	service     tern.StorageSchemaService
}

// resolveStorageSchemaTarget decides which instance answers for the request.
//
// An empty deployment means the server the request was made to: its storage is
// the one storage the request can reach without routing, and asking for it
// needs no deployment because there is nothing to choose between.
//
// A named deployment must have a configured gRPC endpoint for the environment.
// The endpoint is checked before the client is resolved, and deliberately not
// inferred from the client that comes back: TernClient prefers an in-process
// client whenever the name also matches a locally configured database, so
// resolving the client first and then asking what it turned out to be is how a
// request for a data plane's storage silently becomes a report about the
// control plane's.
func (s *Service) resolveStorageSchemaTarget(deployment, environment string) (*storageSchemaTarget, error) {
	deployment = strings.TrimSpace(deployment)
	environment = strings.TrimSpace(environment)

	if deployment == "" {
		if environment != "" {
			return nil, fmt.Errorf("environment %q was given without a deployment: an environment selects which of a deployment's endpoints to reach, so name the deployment too, or omit both to read this server's own storage", environment)
		}
		service := s.localStorageSchemaService()
		if service == nil {
			return nil, fmt.Errorf("this server does not expose its own storage schema: it was built without a storage schema service, so there is no storage it can name; name a deployment to read a data plane's storage instead")
		}
		return &storageSchemaTarget{service: service}, nil
	}
	if environment == "" {
		return nil, fmt.Errorf("deployment %q needs an environment: a deployment serves one endpoint per environment, so there is no single storage to read without one", deployment)
	}

	if _, err := s.config.TernDeployments.Endpoint(deployment, environment); err != nil {
		return nil, fmt.Errorf("no data plane configured for deployment %q in environment %q: the storage schema of a data plane is read through its gRPC endpoint, so add it under tern_deployments.%s.%s in the server config, or omit the deployment to read this server's own storage: %w",
			deployment, environment, deployment, environment, err)
	}
	client, err := s.TernClient(deployment, environment)
	if err != nil {
		return nil, fmt.Errorf("resolve data plane client for deployment %q environment %q: %w", deployment, environment, err)
	}
	service, ok := client.(tern.StorageSchemaService)
	if !ok {
		// A configured endpoint that resolves to an in-process client means
		// the routing config is ambiguous, not that the storage is local.
		// Saying so beats reporting this server's storage under the
		// deployment's name.
		return nil, fmt.Errorf("deployment %q environment %q has a configured data plane endpoint but resolves to an in-process client (%T), so its storage cannot be read remotely; check that no locally configured database shares the name %q",
			deployment, environment, client, deployment)
	}
	return &storageSchemaTarget{deployment: deployment, environment: environment, service: service}, nil
}

// handleStorageSchemaDiff is the HTTP handler for
// POST /api/storage/schema/diff.
//
// It reads and nothing else: it plans no change, stores nothing, and takes no
// lock, so it is safe to call repeatedly against production while an incident
// is in progress. It is a POST because the desired schema travels in the body —
// an operator asking what a database needs in order to match a later release
// sends that release's files, since the answering binary does not carry them.
// Being a POST also puts it at the write tier by the default rule, which is
// where it belongs: what it returns is the internal shape of SchemaBot's own
// bookkeeping database, and its sibling route converges that database.
func (s *Service) handleStorageSchemaDiff(w http.ResponseWriter, r *http.Request) {
	req, err := decodeStorageSchemaDiffRequest(r)
	if err != nil {
		s.writeBodyDecodeError(w, err)
		return
	}
	if !s.authorizeStorageSchemaOperation(w, r, storageSchemaDiffOperation) {
		return
	}
	if err := validateStorageSchemaDiffRequest(req); err != nil {
		s.logger.Warn("rejecting storage schema diff because its desired schema is incomplete",
			"deployment", req.Deployment, "environment", req.Environment, "error", err)
		s.writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	target, err := s.resolveStorageSchemaTarget(req.Deployment, req.Environment)
	if err != nil {
		s.logger.Warn("rejecting storage schema diff because its target could not be resolved",
			"deployment", req.Deployment, "environment", req.Environment, "error", err)
		s.writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	resp, err := target.service.StorageSchemaDiff(r.Context(), &ternv1.StorageSchemaDiffRequest{
		AllowDestructive: req.AllowDestructive,
		SchemaFiles:      req.SchemaFiles,
		SchemaSource:     req.SchemaSource,
	})
	if err != nil {
		s.logger.Error("storage schema diff failed",
			"deployment", target.deployment,
			"environment", target.environment,
			"schema_source", req.SchemaSource,
			"schema_file_count", len(req.SchemaFiles),
			"error", err)
		s.writeError(w, http.StatusInternalServerError, fmt.Sprintf("storage schema diff failed: %v", err))
		return
	}
	report := storageSchemaReportResponse(target, resp.GetReport())
	if report == nil {
		s.logger.Error("storage schema diff returned no report",
			"deployment", target.deployment, "environment", target.environment)
		s.writeError(w, http.StatusInternalServerError, "storage schema diff returned no report")
		return
	}
	s.writeJSON(w, http.StatusOK, apitypes.StorageSchemaDiffResponse{Report: report})
}

// handleStorageSchemaApply is the HTTP handler for
// POST /api/storage/schema/apply. It runs the target instance's startup
// bootstrap, under the advisory lock that bootstrap already takes, so two
// operators running it at once serialize the same way two booting pods do.
func (s *Service) handleStorageSchemaApply(w http.ResponseWriter, r *http.Request) {
	req, err := decodeStorageSchemaApplyRequest(r)
	if err != nil {
		s.writeBodyDecodeError(w, err)
		return
	}
	if !s.authorizeStorageSchemaOperation(w, r, storageSchemaApplyOperation) {
		return
	}
	target, err := s.resolveStorageSchemaTarget(req.Deployment, req.Environment)
	if err != nil {
		s.logger.Warn("rejecting storage schema apply because its target could not be resolved",
			"deployment", req.Deployment, "environment", req.Environment, "error", err)
		s.writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	operator := resolveCaller(r.Context(), req.Caller)
	s.logger.Info("converging storage schema on operator request",
		"deployment", target.deployment,
		"environment", target.environment,
		"allow_destructive", req.AllowDestructive,
		"caller", operator)

	resp, err := target.service.StorageSchemaApply(r.Context(), &ternv1.StorageSchemaApplyRequest{
		AllowDestructive: req.AllowDestructive,
		Caller:           operator,
	})
	if err != nil {
		s.logger.Error("storage schema apply failed",
			"deployment", target.deployment,
			"environment", target.environment,
			"allow_destructive", req.AllowDestructive,
			"caller", operator,
			"error", err)
		s.writeError(w, http.StatusInternalServerError, fmt.Sprintf("storage schema apply failed: %v", err))
		return
	}
	planned := storageSchemaReportResponse(target, resp.GetPlanned())
	remaining := storageSchemaReportResponse(target, resp.GetRemaining())
	if planned == nil || remaining == nil {
		// Both halves are required to say what happened: without the pair, an
		// operator cannot tell a convergence that finished from one that left
		// statements behind, which is the only thing this answer is for.
		s.logger.Error("storage schema apply returned an incomplete result",
			"deployment", target.deployment,
			"environment", target.environment,
			"has_planned", planned != nil,
			"has_remaining", remaining != nil)
		s.writeError(w, http.StatusInternalServerError, "storage schema apply returned an incomplete result; check the target's logs for whether it converged")
		return
	}
	s.writeJSON(w, http.StatusOK, apitypes.StorageSchemaApplyResponse{Planned: planned, Remaining: remaining})
}

// storageSchemaReportResponse converts one wire report to its HTTP form,
// stamping the target it describes. Stamping here rather than at the source is
// deliberate: only the control plane knows which route it asked, and an
// operator reading a report needs it to name the storage they meant, not just
// the storage that answered.
func storageSchemaReportResponse(target *storageSchemaTarget, report *ternv1.StorageSchemaReport) *apitypes.StorageSchemaReport {
	response := StorageSchemaReportFromProto(report).APIType()
	if response == nil {
		return nil
	}
	response.Deployment = target.deployment
	response.Environment = target.environment
	return response
}

// The storage schema operations' names in the authorization decision metric
// and denial logs.
const (
	storageSchemaDiffOperation  = "storage_schema_diff"
	storageSchemaApplyOperation = "storage_schema_apply"
)

// authorizeStorageSchemaOperation gates both storage schema routes on admin
// membership and reports whether the request may proceed.
//
// Admin-only, with no scoped lane, because there is nothing to scope to. A
// database operator grant authorizes an operator for their own database's
// schema changes; SchemaBot's storage database is not any team's database, it
// is the instance's own bookkeeping — the same class of operation as changing
// deployment settings or redriving webhooks, which are admin-only for the same
// reason.
//
// This is the second of two gates and it is not the one that usually bites.
// The first is the tier the auth middleware admits the route at, and both
// routes are classified write there by auth.TierForRequest's default rule,
// since both are non-GET. That classification is what makes the admin
// requirement real on a deployment whose whole authorization model is read
// groups and write groups: the handler-level scoped-write decision is a
// pass-through until some database configures operator_groups, so a route left
// on the read tier would be readable by every reader no matter what this
// function said.
func (s *Service) authorizeStorageSchemaOperation(w http.ResponseWriter, r *http.Request, operation string) bool {
	return s.authorizeDirectAdminWrite(w, r, operation)
}

// decodeStorageSchemaApplyRequest decodes the apply body, tolerating an empty
// one.
func decodeStorageSchemaApplyRequest(r *http.Request) (apitypes.StorageSchemaApplyRequest, error) {
	return decodeOptionalStorageSchemaBody[apitypes.StorageSchemaApplyRequest](r)
}

// decodeStorageSchemaDiffRequest decodes the diff body, tolerating an empty
// one.
func decodeStorageSchemaDiffRequest(r *http.Request) (apitypes.StorageSchemaDiffRequest, error) {
	return decodeOptionalStorageSchemaBody[apitypes.StorageSchemaDiffRequest](r)
}

// decodeOptionalStorageSchemaBody decodes a storage schema request body,
// tolerating an empty one. Every field of both requests is optional — the
// defaults name this server's own storage, its own embedded schema, and refuse
// destructive statements — so a caller sending no body at all gets the safe
// defaults rather than a decode error. Unknown fields are still rejected, so a
// misspelled "deployment" cannot quietly become a report about, or a
// convergence of, the wrong storage.
func decodeOptionalStorageSchemaBody[T any](r *http.Request) (T, error) {
	var req T
	if r.Body == nil {
		return req, nil
	}
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		var zero T
		if errors.Is(err, io.EOF) {
			return zero, nil
		}
		return zero, err
	}
	return req, nil
}

// validateStorageSchemaDiffRequest refuses a desired schema that is only half
// supplied. The two fields travel together or not at all: files without a
// source would produce a report that cannot say what it was diffed against,
// and a source without files would label this server's own embedded schema
// with somebody else's name — which is the one way a report of this kind can
// be actively misleading rather than merely wrong.
func validateStorageSchemaDiffRequest(req apitypes.StorageSchemaDiffRequest) error {
	source := strings.TrimSpace(req.SchemaSource)
	switch {
	case len(req.SchemaFiles) > 0 && source == "":
		return fmt.Errorf("schema_files was sent without schema_source: a report has to say which schema it was diffed against, so name the source (a release, a directory) alongside the files")
	case len(req.SchemaFiles) == 0 && source != "":
		return fmt.Errorf("schema_source %q was sent without schema_files: with no files the diff would run against this server's own embedded schema and report it under that name; send the files, or drop schema_source to ask about the embedded schema", source)
	default:
		return nil
	}
}
