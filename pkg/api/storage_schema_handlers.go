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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

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
			return nil, storageSchemaTargetFault(http.StatusNotImplemented,
				"this server does not expose its own storage schema: it was built without a storage schema service, so there is no storage it can name; name a deployment to read a data plane's storage instead")
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
		// The cause names this deployment's own infrastructure — the configured
		// endpoint, a TLS material path, a dial failure — and that does not
		// travel back over a route an operator's workstation calls. The caller
		// gets the fact and where to look; the cause stays here.
		s.logger.Error("could not resolve the data plane client for a storage schema request",
			"deployment", deployment, "environment", environment, "error", err)
		return nil, storageSchemaTargetFault(http.StatusServiceUnavailable,
			"the data plane for deployment %q in environment %q could not be reached: its endpoint is configured but the client could not be built, so check this server's logs for the cause", deployment, environment)
	}
	service, ok := client.(tern.StorageSchemaService)
	if !ok {
		// A configured endpoint that resolves to an in-process client means
		// the routing config is ambiguous, not that the storage is local.
		// Saying so beats reporting this server's storage under the
		// deployment's name.
		return nil, storageSchemaTargetFault(http.StatusInternalServerError,
			"deployment %q environment %q has a configured data plane endpoint but resolves to an in-process client (%T), so its storage cannot be read remotely; check that no locally configured database shares the name %q",
			deployment, environment, client, deployment)
	}
	return &storageSchemaTarget{deployment: deployment, environment: environment, service: service}, nil
}

// handleStorageSchemaPlan is the HTTP handler for
// POST /api/storage/schema/plan.
//
// It reads and nothing else: it plans no change, stores nothing, and takes no
// lock, so it is safe to call repeatedly against production while an incident
// is in progress. It is a POST because the desired schema travels in the body —
// an operator asking what a database needs in order to match a later release
// sends that release's files, since the answering binary does not carry them.
// Being a POST also puts it at the write tier by the default rule, which is
// where it belongs: what it returns is the internal shape of SchemaBot's own
// bookkeeping database, and its sibling route converges that database.
func (s *Service) handleStorageSchemaPlan(w http.ResponseWriter, r *http.Request) {
	req, err := decodeStorageSchemaPlanRequest(r)
	if err != nil {
		s.writeBodyDecodeError(w, err)
		return
	}
	if !s.authorizeStorageSchemaOperation(w, r, storageSchemaPlanOperation) {
		return
	}
	if err := validateStorageSchemaSource(req.SchemaFiles, req.SchemaSource); err != nil {
		s.logger.Warn("rejecting storage schema plan because its desired schema is incomplete",
			"deployment", req.Deployment, "environment", req.Environment, "error", err)
		s.writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	target, err := s.resolveStorageSchemaTarget(req.Deployment, req.Environment)
	if err != nil {
		s.refuseStorageSchemaTarget(w, storageSchemaPlanOperation, req.Deployment, req.Environment, err)
		return
	}

	ctx, cancel := s.extendOperatorWriteDeadline(w, r, storageSchemaPlanWriteBudget)
	defer cancel()

	resp, err := target.service.StorageSchemaPlan(ctx, &ternv1.StorageSchemaPlanRequest{
		AllowDestructive: req.AllowDestructive,
		SchemaFiles:      req.SchemaFiles,
		SchemaSource:     req.SchemaSource,
	})
	if err != nil {
		s.logger.Error("storage schema plan failed",
			"deployment", target.deployment,
			"environment", target.environment,
			"schema_source", req.SchemaSource,
			"schema_file_count", len(req.SchemaFiles),
			"error", err)
		s.writeStorageSchemaFailure(w, err, "storage schema plan failed")
		return
	}
	report := storageSchemaReportResponse(target, resp.GetReport())
	if report == nil {
		s.logger.Error("storage schema plan returned no report",
			"deployment", target.deployment, "environment", target.environment)
		s.writeError(w, http.StatusInternalServerError, "storage schema plan returned no report")
		return
	}
	if s.refuseUnhonoredSchema(w, target, storageSchemaPlanOperation, req.SchemaSource, report) {
		return
	}
	s.writeJSON(w, http.StatusOK, apitypes.StorageSchemaPlanResponse{Report: report})
}

// handleStorageSchemaApply is the HTTP handler for
// POST /api/storage/schema/apply. It runs the target instance's startup
// bootstrap, under the advisory lock that bootstrap already takes, so two
// operators running it at once serialize the same way two booting pods do.
//
// The schema it converges to is the target's own embedded files, or the ones
// the request carries — a release an operator is rolling, which the target
// cannot converge from files it does not have. Either way the bootstrap decides
// what runs, so a supplied schema changes which statements are computed and
// nothing about which of them are permitted (AV-9).
func (s *Service) handleStorageSchemaApply(w http.ResponseWriter, r *http.Request) {
	req, err := decodeStorageSchemaApplyRequest(r)
	if err != nil {
		s.writeBodyDecodeError(w, err)
		return
	}
	if !s.authorizeStorageSchemaOperation(w, r, storageSchemaApplyOperation) {
		return
	}
	if err := validateStorageSchemaSource(req.SchemaFiles, req.SchemaSource); err != nil {
		s.logger.Warn("rejecting storage schema apply because the schema to converge to is incomplete",
			"deployment", req.Deployment, "environment", req.Environment, "error", err)
		s.writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	// A request naming no budget runs under the boot's. SchemaBot's own client
	// always names one, so this is a caller that could not — and the boot
	// budget is the one such a caller can wait out, where the operator default
	// would hold the bootstrap lock for an hour after it had given up.
	budget, err := apitypes.ResolveStorageApplyTimeout(req.TimeoutSeconds, EnsureSchemaTimeout)
	if err != nil {
		s.logger.Warn("rejecting storage schema convergence because its timeout is out of range",
			"deployment", req.Deployment, "environment", req.Environment,
			"timeout_seconds", req.TimeoutSeconds, "error", err)
		s.writeError(w, http.StatusBadRequest, fmt.Sprintf("timeout_seconds: %s", err))
		return
	}
	target, err := s.resolveStorageSchemaTarget(req.Deployment, req.Environment)
	if err != nil {
		s.refuseStorageSchemaTarget(w, storageSchemaApplyOperation, req.Deployment, req.Environment, err)
		return
	}

	// A named schema costs a diff first, so the deadline has to cover both.
	writeBudget := storageSchemaApplyWriteBudget(budget)
	if namesSchema(req.SchemaSource) {
		writeBudget += StorageSchemaPlanTimeout
	}
	ctx, cancel := s.extendOperatorWriteDeadline(w, r, writeBudget)
	defer cancel()

	if s.refuseUnverifiedNamedSchema(ctx, w, target, req) {
		return
	}

	operator := resolveCaller(r.Context(), req.Caller)
	s.logger.Info("converging storage schema on operator request",
		"deployment", target.deployment,
		"environment", target.environment,
		"allow_destructive", req.AllowDestructive,
		"convergence_timeout", budget,
		"schema_source", req.SchemaSource,
		"schema_file_count", len(req.SchemaFiles),
		"caller", operator)

	resp, err := target.service.StorageSchemaApply(ctx, &ternv1.StorageSchemaApplyRequest{
		AllowDestructive: req.AllowDestructive,
		Caller:           operator,
		TimeoutSeconds:   int64(budget / time.Second),
		SchemaFiles:      req.SchemaFiles,
		SchemaSource:     req.SchemaSource,
	})
	if err != nil {
		s.logger.Error("storage schema apply failed",
			"deployment", target.deployment,
			"environment", target.environment,
			"allow_destructive", req.AllowDestructive,
			"schema_source", req.SchemaSource,
			"schema_file_count", len(req.SchemaFiles),
			"caller", operator,
			"error", err)
		s.writeStorageSchemaFailure(w, err, "storage schema apply failed")
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
	if s.refuseUnhonoredConvergence(w, target, req.SchemaSource, planned, remaining) {
		return
	}
	s.writeJSON(w, http.StatusOK, apitypes.StorageSchemaApplyResponse{Planned: planned, Remaining: remaining})
}

// storageSchemaTargetError carries the status a target-resolution failure
// should be answered with, for the failures that are not the caller's fault.
//
// The distinction is whether the caller can do anything about it. A deployment
// and environment that name no configured data plane is theirs to fix and stays
// a 400. A server built without its own storage schema service, a configured
// data plane whose client will not build, and a routing config that resolves a
// remote deployment to an in-process client are all this server's to fix — and
// answering those 400 tells a pre-deploy gate that its request was malformed,
// so it stops rather than retrying or escalating. The remote paths already map
// the same two conditions to 501 and 503 (see writeStorageSchemaFailure), so
// these are the local spellings of the same answers.
type storageSchemaTargetError struct {
	status int
	err    error
}

func (e *storageSchemaTargetError) Error() string { return e.err.Error() }

func (e *storageSchemaTargetError) Unwrap() error { return e.err }

func storageSchemaTargetFault(status int, format string, args ...any) error {
	return &storageSchemaTargetError{status: status, err: fmt.Errorf(format, args...)}
}

// refuseStorageSchemaTarget answers a request whose target could not be
// resolved, at the status the cause earns and the log level it earns.
//
// A caller's mistake is a warning: the response says what to fix and nobody
// needs to read this server's logs. A server fault is an error, because the
// response deliberately does not carry the cause and the operator's next stop
// is these logs.
func (s *Service) refuseStorageSchemaTarget(w http.ResponseWriter, operation, deployment, environment string, err error) {
	status := http.StatusBadRequest
	var fault *storageSchemaTargetError
	if errors.As(err, &fault) {
		status = fault.status
	}
	attrs := []any{
		"operation", operation,
		"deployment", deployment,
		"environment", environment,
		"status", status,
		"error", err,
	}
	if status >= http.StatusInternalServerError {
		s.logger.Error("refusing a storage schema request: its target could not be resolved", attrs...)
	} else {
		s.logger.Warn("refusing a storage schema request: its target was not named in a way this server can resolve", attrs...)
	}
	s.writeError(w, status, err.Error())
}

// writeStorageSchemaFailure answers a failed storage schema call with the
// status that says whose problem it is.
//
// A caller who sent an unreadable schema file gets 400 and the reason, because
// they wrote the input and can correct it. Everything else is 500 and says only
// where to look, because the cause belongs to the instance that answered — a
// DSN, a host, a driver error — and none of that goes back over a route an
// operator's workstation calls. The cause is logged at the call site either
// way, with the deployment and environment that identify whose logs to read.
//
// Both spellings of the caller's fault are recognized, because the same handler
// serves an adapter in this process and one reached over gRPC: in-process the
// sentinel arrives intact, and over the wire the data plane's gRPC server has
// already turned it into an InvalidArgument status.
//
// Two failures of the data plane itself are worth more than a 500, because each
// names a remedy the operator can act on without reading anyone's logs: a data
// plane that does not serve these RPCs needs upgrading, and one that cannot be
// reached needs looking at. Collapsing either into "see the logs" sends an
// operator to read logs that say the same thing the status code already did.
func (s *Service) writeStorageSchemaFailure(w http.ResponseWriter, err error, summary string) {
	if errors.Is(err, tern.ErrInvalidStorageSchemaRequest) || status.Code(err) == codes.InvalidArgument {
		s.writeError(w, http.StatusBadRequest, fmt.Sprintf("%s: %v", summary, err))
		return
	}
	switch status.Code(err) {
	case codes.Unimplemented:
		s.writeError(w, http.StatusNotImplemented,
			summary+": the answering deployment does not serve storage schema requests; it is running a release that predates them, so upgrade that deployment and retry")
	case codes.Unavailable:
		s.writeErrorCode(w, http.StatusServiceUnavailable, apitypes.ErrCodeEngineUnavailable,
			summary+": the answering deployment could not be reached; check that it is healthy and retry")
	default:
		s.writeError(w, http.StatusInternalServerError, summary+"; see the answering deployment's logs")
	}
}

// namesSchema reports whether a request asked about a schema of its own rather
// than about the answering target's embedded one.
func namesSchema(source string) bool { return strings.TrimSpace(source) != "" }

// refuseUnverifiedNamedSchema proves, before a single statement runs, that the
// target honors the schema this convergence names.
//
// A diff is the only way to ask: there is no capability handshake, and the
// fields carrying a named schema are ones an instance that predates them drops
// silently. Since a diff answers the same question a convergence does and runs
// nothing, the answer to "would this target work from the schema I sent" can be
// had for a catalog read instead of for executed DDL.
//
// Asking after the convergence instead is not equivalent, because a target that
// ignored the schema did not ignore the rest of the request. It still honors
// allow_destructive, so it diffs its own older embedded schema against a
// database holding a newer release's state, finds that state surplus, and drops
// it — removals no boot of that deployment would have run, since a boot never
// sees an operator's opt-in. The convergence cannot be taken back once it has
// run, so the question is asked while the answer can still prevent it (AV-9).
func (s *Service) refuseUnverifiedNamedSchema(ctx context.Context, w http.ResponseWriter, target *storageSchemaTarget, req apitypes.StorageSchemaApplyRequest) bool {
	if !namesSchema(req.SchemaSource) {
		return false
	}
	// Bounded on its own so a slow diff cannot spend the convergence's budget:
	// what this buys is worth a catalog read, not the run it is protecting.
	ctx, cancel := context.WithTimeout(ctx, StorageSchemaPlanTimeout)
	defer cancel()

	resp, err := target.service.StorageSchemaPlan(ctx, &ternv1.StorageSchemaPlanRequest{
		AllowDestructive: req.AllowDestructive,
		SchemaFiles:      req.SchemaFiles,
		SchemaSource:     req.SchemaSource,
	})
	if err != nil {
		s.logger.Error("refusing a storage convergence whose target could not be asked which schema it would use",
			"operation", storageSchemaApplyOperation,
			"deployment", target.deployment,
			"environment", target.environment,
			"asked_schema_source", req.SchemaSource,
			"schema_file_count", len(req.SchemaFiles),
			"error", err)
		s.writeStorageSchemaFailure(w, err, "could not confirm the target converges the named schema, so nothing was converged")
		return true
	}
	report := storageSchemaReportResponse(target, resp.GetReport())
	if report == nil {
		s.logger.Error("refusing a storage convergence whose target answered no report when asked which schema it would use",
			"operation", storageSchemaApplyOperation,
			"deployment", target.deployment,
			"environment", target.environment,
			"asked_schema_source", req.SchemaSource)
		s.writeError(w, http.StatusBadGateway,
			"the target returned no report when asked which schema it would converge, so nothing was converged; see the answering deployment's logs")
		return true
	}
	return s.refuseUnhonoredSchema(w, target, storageSchemaApplyOperation, req.SchemaSource, report)
}

// refuseUnhonoredConvergence is refuseUnhonoredSchema for an answer that
// arrives once the DDL has run.
//
// The convergence is preceded by a diff that proves the target honors the named
// schema, so reaching here means the pod that converged is not the pod that
// answered the diff. A deployment is many pods and a roll makes them different
// releases, so the check is made again on the answer that matters rather than
// trusted from the one before it.
//
// It is a separate refusal because the remedy is: statements have executed
// against a schema nobody asked for, and saying only "upgrade that target"
// would leave an operator to discover that from the database. What ran is
// named here, and the reports are logged whole, since the response carries an
// error rather than them.
func (s *Service) refuseUnhonoredConvergence(w http.ResponseWriter, target *storageSchemaTarget, asked string, planned, remaining *apitypes.StorageSchemaReport) bool {
	asked = strings.TrimSpace(asked)
	if asked == "" || planned == nil || planned.SchemaSource == asked {
		return false
	}
	s.logger.Error("a storage convergence ran against a schema the caller did not ask for",
		"operation", storageSchemaApplyOperation,
		"deployment", target.deployment,
		"environment", target.environment,
		"database", planned.Database,
		"dialect", planned.Dialect,
		"asked_schema_source", asked,
		"converged_schema_source", planned.SchemaSource,
		"converged_version", planned.Version,
		"statements_run", storageSchemaStatementsRun(planned),
		"planned", planned,
		"remaining", remaining)
	s.writeError(w, http.StatusBadGateway, fmt.Sprintf(
		"the target converged %q, not the schema that was asked for: it is running a release that does not accept a named schema and worked from its own embedded files instead, and %d statement(s) have already run against %s. Reconcile that database against the release the target is running before retrying, then upgrade the target or address a release that carries this schema",
		planned.SchemaSource, storageSchemaStatementsRun(planned), storageSchemaDatabaseLabel(planned)))
	return true
}

// storageSchemaStatementsRun is how many statements a convergence executed,
// which is the outstanding set plus the destructive one wherever the target
// permitted it. A refused destructive statement did not run, so counting it
// would overstate what an operator has to reconcile.
func storageSchemaStatementsRun(planned *apitypes.StorageSchemaReport) int {
	run := len(planned.Outstanding)
	if planned.DestructiveAllowed {
		run += len(planned.Destructive)
	}
	return run
}

// storageSchemaDatabaseLabel names the database a report is about, for an error
// an operator has to act on. The host is included when the target reported one,
// since a database name alone does not say which instance to go to.
func storageSchemaDatabaseLabel(report *apitypes.StorageSchemaReport) string {
	if report.Host == "" {
		return report.Database
	}
	return report.Database + " on " + report.Host
}

// refuseUnhonoredSchema stops an answer whose report does not name the schema
// the caller asked about, where nothing has run yet.
//
// A named schema travels as fields on the request, and an instance that
// predates them ignores what it does not recognize and works from its own
// embedded files instead — the defined behavior of the wire format, and
// indistinguishable from success in the response. The report's own schema
// source is the proof, because the answering side sets it from the schema it
// actually diffed: honored, it echoes what was sent; ignored, it names the
// answering binary's own files.
//
// Refusing is the only safe reading. A report attributed to the release an
// operator asked about but computed from another is the input to their decision
// about whether to pre-apply, and it says the named release's storage is ready
// when nothing has compared the two. The gap surfaces when that release's pods
// boot and find their storage short — the failure this command exists to
// prevent, now with an operator who has been told it cannot happen.
func (s *Service) refuseUnhonoredSchema(w http.ResponseWriter, target *storageSchemaTarget, operation, asked string, report *apitypes.StorageSchemaReport) bool {
	asked = strings.TrimSpace(asked)
	if asked == "" || report == nil || report.SchemaSource == asked {
		return false
	}
	s.logger.Error("refusing a storage schema answer computed from a schema the caller did not ask for",
		"operation", operation,
		"deployment", target.deployment,
		"environment", target.environment,
		"database", report.Database,
		"dialect", report.Dialect,
		"asked_schema_source", asked,
		"answered_schema_source", report.SchemaSource,
		"answered_version", report.Version)
	s.writeError(w, http.StatusBadGateway, fmt.Sprintf(
		"the target answered about %q, not the schema that was asked for; it is running a release that does not accept a named schema and worked from its own embedded files instead. Upgrade that target, or address a release that carries this schema",
		report.SchemaSource))
	return true
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
	storageSchemaPlanOperation  = "storage_schema_plan"
	storageSchemaApplyOperation = "storage_schema_apply"
)

// The write deadlines the storage schema routes lift the server-wide one to.
//
// Each is the budget the work is already bounded by, plus room to write the
// answer. The server-wide write timeout is exactly the diff's own budget and a
// fraction of the convergence's, so without a lift neither route can return
// what it computed: the work runs on server-side while the connection closes
// under it, and the operator cannot tell whether their storage was converged.
//
// A convergence is three bounded steps — the diff before it, the bootstrap
// itself, and the diff after it — so its budget is their sum rather than the
// bootstrap's alone.
const (
	storageSchemaResponseMargin  = 30 * time.Second
	storageSchemaPlanWriteBudget = StorageSchemaPlanTimeout + storageSchemaResponseMargin
)

// storageSchemaApplyWriteBudget is the write deadline a convergence of the
// given budget needs. It is a function rather than a constant because the
// convergence's own budget is now the caller's to name: a deadline pinned to
// any single value would close the connection under every convergence that
// asked for longer, which is the exact failure the lift exists to prevent — the
// DDL runs on server-side while the operator sees a truncated response and
// cannot tell whether their storage was converged.
func storageSchemaApplyWriteBudget(convergence time.Duration) time.Duration {
	return convergence + 2*StorageSchemaPlanTimeout + storageSchemaResponseMargin
}

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

// decodeStorageSchemaPlanRequest decodes the diff body, tolerating an empty
// one.
func decodeStorageSchemaPlanRequest(r *http.Request) (apitypes.StorageSchemaPlanRequest, error) {
	return decodeOptionalStorageSchemaBody[apitypes.StorageSchemaPlanRequest](r)
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

// validateStorageSchemaSource refuses a schema that is only half supplied. The
// two fields travel together or not at all: files without a source would
// produce a report that cannot say which schema it describes, and a source
// without files would label this server's own embedded schema with somebody
// else's name — which is the one way an answer of this kind can be actively
// misleading rather than merely wrong.
//
// Both routes validate through this, because on the convergence route the
// misattribution is worse than a wrong report. A convergence that ran the
// embedded schema under a release's name would tell an operator their storage
// is ready for a release it was never compared against.
func validateStorageSchemaSource(files map[string]string, schemaSource string) error {
	source := strings.TrimSpace(schemaSource)
	switch {
	case len(files) > 0 && source == "":
		return fmt.Errorf("schema_files was sent without schema_source: an answer has to say which schema it used, so name the source (a release, a directory) alongside the files")
	case len(files) == 0 && source != "":
		return fmt.Errorf("schema_source %q was sent without schema_files: with no files this server's own embedded schema would be used and reported under that name; send the files, or drop schema_source to use the embedded schema", source)
	default:
		return nil
	}
}
