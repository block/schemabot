package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"slices"
	"sort"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otelcodes "go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/lint"
	"github.com/block/schemabot/pkg/metrics"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/routing"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/tern"
)

const applyOperationKeyMaxLen = 255

// finalizerOperationKeySegment ends every group_finalizer key. The rollout
// projection and the claim query read the finalizer's scope back as the key in
// front of it (state.FinalizerFinalizesWork), so it is that package's constant.
const finalizerOperationKeySegment = state.GroupFinalizerKeySegment

// PlanRequest is the HTTP request body for POST /api/plan.
type PlanRequest struct {
	Database    string                         `json:"database"`
	Environment string                         `json:"environment"`
	Type        string                         `json:"type"` // database type; must match the server's configured type for the database
	SchemaFiles map[string]*ternv1.SchemaFiles `json:"schema_files"`
	Repository  string                         `json:"repository,omitempty"`
	PullRequest *int32                         `json:"pull_request,omitempty"`
	// HeadSHA is the PR HEAD SHA at the time the schema files were discovered.
	// Persisted on the plan record and used at apply-confirm time to detect the
	// cross-delivery race where HEAD advances between plan and confirm.
	// Optional — absent for non-webhook callers (e.g. CLI plan invocations without a PR).
	HeadSHA    *string `json:"head_sha,omitempty"`
	SchemaPath string  `json:"-"`
	// IgnoredNamespaces lists the namespaces the caller removed from
	// SchemaFiles per the config's ignore_namespaces, resolved for the
	// environment. Forwarded to the data plane so it can refuse engine shapes
	// that cannot honor the exclusion.
	IgnoredNamespaces []string `json:"ignored_namespaces,omitempty"`
	// IgnoreTables lists the live tables the config's ignore_tables withholds
	// from the planner, so a table no schema file declares is not proposed for
	// DROP TABLE. Unlike ignored namespaces the exclusion cannot be expressed
	// by leaving files out of the request — the tables are on the target, not
	// in the repository — so the data plane applies it and reports what it
	// actually withheld through exempt_tables on the response.
	IgnoreTables []string `json:"ignore_tables,omitempty"`

	// SourceTrusted is set by the GitHub webhook path after SchemaBot has
	// discovered the PR source itself. It is deliberately not JSON-decodable:
	// direct API clients cannot attest repo/path ownership.
	SourceTrusted bool `json:"-"`

	// GroupedExecution reports whether an apply of this plan will hand the
	// engine every ALTER at once or one table at a time. Engines predicting what
	// an apply will do to unfinished work already on the target need the
	// grouping the apply will actually run under; see engine.PlanRequest.
	GroupedExecution bool `json:"grouped_execution,omitempty"`

	// Target narrows the plan to one rollout member of the environment, named
	// by its target or by deployment/target. Empty plans the rollout primary,
	// the way every plan did before members could be selected. A narrowed plan
	// speaks for its one member only, so it never records stored check state.
	Target string `json:"target,omitempty"`
}

type unsupportedPullSchemaError struct {
	DatabaseType string
}

func (e *unsupportedPullSchemaError) Error() string {
	return fmt.Sprintf("pull schema is not supported for %s databases on this deployment", e.DatabaseType)
}

// unsupportedLintDialectError reports a pull request that asked for linting on
// a database whose dialect has no schema-shape audit. Failing the request is
// deliberate: silently returning zero violations would read as a clean audit.
type unsupportedLintDialectError struct {
	DatabaseType string
}

func (e *unsupportedLintDialectError) Error() string {
	return fmt.Sprintf("schema linting on pull is not supported for %s databases", e.DatabaseType)
}

// unlintablePulledTableError reports a pulled table entry whose DDL the schema
// linters cannot audit — it failed classification, or it is not a CREATE TABLE
// statement. Failing the request is deliberate: skipping the entry would
// silently turn the audit into a partial result.
type unlintablePulledTableError struct {
	Database  string
	Namespace string
	Table     string
	Detail    string
}

func (e *unlintablePulledTableError) Error() string {
	return fmt.Sprintf("lint pulled schema for database %q namespace %q: table %q %s", e.Database, e.Namespace, e.Table, e.Detail)
}

// databaseTypeMismatchError reports a request whose declared database type
// disagrees with the server's configured type for that database. The declared
// type is a caller assertion checked against server config — the one source
// of truth for the type vocabulary — so a mismatch is a caller defect (400),
// not a server failure.
type databaseTypeMismatchError struct {
	Database    string
	RequestType string
	ConfigType  string
}

func (e *databaseTypeMismatchError) Error() string {
	return fmt.Sprintf("database %q type %q does not match server config type %q", e.Database, e.RequestType, e.ConfigType)
}

// requestedRouteNotConfigured reports whether err means the request named a
// database or environment this server has no configuration for. Both are the
// same class of defect — the caller asked for a route that does not exist —
// so the handlers map them to 400 rather than a server failure.
func requestedRouteNotConfigured(err error) bool {
	var dbNotConfigured *DatabaseNotConfiguredError
	var envNotConfigured *EnvironmentNotConfiguredError
	return errors.As(err, &dbNotConfigured) || errors.As(err, &envNotConfigured)
}

// RemoteDeploymentUnavailableError carries routing metadata for remote
// schema change service availability failures so callers can render actionable
// operator-facing errors without parsing strings.
type RemoteDeploymentUnavailableError struct {
	Deployment string
	Target     string
	Err        error
}

func (e *RemoteDeploymentUnavailableError) Error() string {
	if e.Target == "" {
		return fmt.Sprintf("remote deployment %q unavailable: %v", e.Deployment, e.Err)
	}
	return fmt.Sprintf("remote deployment %q target %q unavailable: %v", e.Deployment, e.Target, e.Err)
}

func (e *RemoteDeploymentUnavailableError) Unwrap() error {
	return e.Err
}

// handlePullSchema handles POST /api/pull requests.
func (s *Service) handlePullSchema(w http.ResponseWriter, r *http.Request) {
	var req apitypes.PullSchemaRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		s.writeBodyDecodeError(w, err)
		return
	}
	req.Database = storage.CanonicalKey(req.Database)
	req.Environment = storage.CanonicalKey(req.Environment)
	req.Type = storage.CanonicalKey(req.Type)
	req.App = storage.CanonicalKey(strings.TrimSpace(req.App))

	if req.App != "" && req.Database != "" {
		s.writeError(w, http.StatusBadRequest, "database and app both select the database; set exactly one")
		return
	}
	if req.Database == "" && req.App == "" {
		s.writeError(w, http.StatusBadRequest, "database or app is required")
		return
	}
	if req.Environment == "" {
		s.writeError(w, http.StatusBadRequest, "environment is required")
		return
	}
	if _, err := pullCatalogDetail(req.CatalogDetail); err != nil {
		s.writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	// Spend the caller's budget once the request is well-formed and before
	// either selector resolves, so a malformed request costs nothing while
	// every request that puts the server to work is counted. A request naming
	// a database this server does not route — or an app no database declares —
	// still spends budget: route resolution is server-side work, and a client
	// looping on unresolvable names is exactly the runaway the budget exists
	// to bound.
	if !s.checkPullCallerBudget(w, r, req.Database, req.App, req.Environment) {
		return
	}

	// App is an alternate selector, not an additional filter: resolving it to
	// a database here means everything downstream — the target budget, routing,
	// authorization of the route — sees the same request shape either way. The
	// resolution itself was paid for by the caller budget above, so cycling
	// through unresolvable apps is bounded exactly like cycling through
	// unknown database names.
	if req.App != "" {
		database, err := s.config.withDatabaseSnapshot().DatabaseForApp(req.App)
		if err != nil {
			s.logger.Warn("pull schema rejected for unresolvable app", "app", req.App, "environment", req.Environment, "error", err)
			s.writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		req.Database = database
	}

	// The target budget is keyed on the resolved database, so a by-app pull
	// and a by-name pull of the same database drain one shared bucket rather
	// than each selector minting its own.
	if !s.checkPullTargetBudget(w, r, req.Database, req.Environment) {
		return
	}

	resp, err := s.ExecutePullSchema(r.Context(), req)
	if err != nil {
		if typeMismatchErr, ok := errors.AsType[*databaseTypeMismatchError](err); ok {
			s.logger.Warn("pull schema rejected for mismatched database type", "database", req.Database, "environment", req.Environment, "request_type", typeMismatchErr.RequestType, "config_type", typeMismatchErr.ConfigType)
			s.writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		if requestedRouteNotConfigured(err) {
			s.logger.Warn("pull schema rejected for unconfigured database route", "database", req.Database, "environment", req.Environment, "error", err)
			s.writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		if unsupportedErr, ok := errors.AsType[*unsupportedPullSchemaError](err); ok {
			s.logger.Warn("pull schema rejected for unsupported database type", "database", req.Database, "environment", req.Environment, "type", unsupportedErr.DatabaseType)
			s.writeError(w, http.StatusNotImplemented, err.Error())
			return
		}
		if lintDialectErr, ok := errors.AsType[*unsupportedLintDialectError](err); ok {
			s.logger.Warn("pull schema lint rejected for unsupported dialect", "database", req.Database, "environment", req.Environment, "type", lintDialectErr.DatabaseType)
			s.writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		if unlintableErr, ok := errors.AsType[*unlintablePulledTableError](err); ok {
			s.logger.Warn("pull schema lint rejected unlintable pulled table", "database", req.Database, "environment", req.Environment, "namespace", unlintableErr.Namespace, "table", unlintableErr.Table, "detail", unlintableErr.Detail)
			s.writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		if unavailableErr, ok := errors.AsType[*RemoteDeploymentUnavailableError](err); ok {
			s.logger.Error("pull schema failed because remote deployment is unavailable",
				"database", req.Database,
				"environment", req.Environment,
				"deployment", unavailableErr.Deployment,
				"target", unavailableErr.Target,
				"error", err)
			s.writeErrorCode(w, http.StatusServiceUnavailable, apitypes.ErrCodeEngineUnavailable, "pull schema failed: "+err.Error())
			return
		}
		s.logger.Error("pull schema failed", "database", req.Database, "environment", req.Environment, "error", err)
		s.writeError(w, http.StatusInternalServerError, "pull schema failed: "+err.Error())
		return
	}

	s.writeJSON(w, http.StatusOK, resp)
}

// ExecutePullSchema resolves a configured database route and fetches its live schema.
func (s *Service) ExecutePullSchema(ctx context.Context, req apitypes.PullSchemaRequest) (*apitypes.PullSchemaResponse, error) {
	ctx, span := otel.Tracer("schemabot").Start(ctx, "ExecutePullSchema",
		trace.WithAttributes(
			attribute.String("database", req.Database),
			attribute.String("environment", req.Environment),
			attribute.String("type", req.Type),
		),
	)
	defer span.End()

	// Pull the live schema from the primary deployment (first in rollout order:
	// explicit deployment_order when set, otherwise alphabetical). For a
	// multi-deployment environment the primary is the canonical source for the
	// schema diff; the apply itself fans out across every deployment. This
	// matches the route ExecutePlan resolves and the deployment
	// createStoredApply records.
	resolvedTarget, err := s.config.ResolvePrimaryDatabaseTarget(req.Database, req.Environment)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, "resolve target")
		return nil, fmt.Errorf("resolve target for %s/%s: %w", req.Database, req.Environment, err)
	}
	if req.Type != "" && req.Type != resolvedTarget.DatabaseType {
		typeErr := &databaseTypeMismatchError{Database: req.Database, RequestType: req.Type, ConfigType: resolvedTarget.DatabaseType}
		span.RecordError(typeErr)
		span.SetStatus(otelcodes.Error, "type mismatch")
		return nil, typeErr
	}
	dialect := schema.DialectForDatabaseType(resolvedTarget.DatabaseType)
	if req.Lint && !pullLintSupported(dialect) {
		lintErr := &unsupportedLintDialectError{DatabaseType: resolvedTarget.DatabaseType}
		span.RecordError(lintErr)
		span.SetStatus(otelcodes.Error, "lint dialect unsupported")
		return nil, lintErr
	}
	namespaces, err := pullNamespaces(dialect, req.Namespaces)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, "invalid namespaces")
		return nil, err
	}
	catalogDetail, err := pullCatalogDetail(req.CatalogDetail)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, "invalid catalog detail")
		return nil, err
	}

	merged, err := s.pullTargetSchema(ctx, req, resolvedTarget, namespaces, catalogDetail)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, "pull schema failed")
		return nil, err
	}

	// An environment whose targets each hold their own schema has no single live
	// schema, so the primary's is reported alongside how the others differ from
	// it. Comparing here, rather than leaving it to the caller, keeps a pull from
	// presenting one target's schema as the environment's.
	members, err := s.pullMemberDivergence(ctx, req, resolvedTarget, merged, namespaces, catalogDetail)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, "compare rollout members")
		return nil, err
	}

	span.SetAttributes(attribute.Int("table_count", int(merged.TableCount)))
	s.logger.Info("ExecutePullSchema: pull schema response",
		"database", merged.Database,
		"type", merged.Type,
		"environment", merged.Environment,
		"table_count", merged.TableCount,
		"namespace_count", len(merged.Namespaces),
		"member_target_count", len(members),
	)

	httpResp := pullSchemaResponseFromProto(merged)
	// Echo the database's app identifier whether the request selected it by
	// name or by app, so callers joining on the app never need a second
	// lookup. The proto carries no app: it is config metadata, not tern's.
	if dbConfig, ok := s.config.DatabaseConfigs()[req.Database]; ok {
		httpResp.App = dbConfig.App
	}
	httpResp.Targets = members
	if req.Lint {
		if err := lintPulledNamespaces(httpResp, dialect); err != nil {
			span.RecordError(err)
			span.SetStatus(otelcodes.Error, "lint pulled schema")
			return nil, err
		}
	}
	return httpResp, nil
}

// pullLintSupported reports whether a dialect has a schema-shape audit for
// pulled tables: Spirit's linters for the MySQL family, the PostgreSQL audit
// in pkg/lint for PostgreSQL.
func pullLintSupported(dialect schema.Dialect) bool {
	return dialect == schema.DialectMySQL || dialect == schema.DialectPostgres
}

// lintPulledNamespaces runs the schema-shape linters (primary key types,
// charsets, and other properties of the schema itself) over every pulled table
// and attaches the violations to each namespace, using the same linter config
// a plan uses. A pull carries no proposed change, so the diff-based linters a
// plan also runs (unsafe drops, invisible-index-before-drop) never apply here:
// a clean pull audit says the existing schema is well-shaped, not that any
// particular change to it is safe. Results are sorted for a stable response
// body regardless of table iteration order.
func lintPulledNamespaces(resp *apitypes.PullSchemaResponse, dialect schema.Dialect) error {
	linter := lint.New()
	for name, ns := range resp.Namespaces {
		results, err := lintPulledTables(linter, dialect, resp.Database, name, ns.Tables)
		if err != nil {
			return err
		}
		violations := make([]*apitypes.LintViolationResponse, 0, len(results))
		for _, r := range results {
			violations = append(violations, &apitypes.LintViolationResponse{
				Message:  r.Message,
				Table:    r.Table,
				Column:   r.Column,
				Linter:   r.Linter,
				Severity: r.Severity,
			})
		}
		sort.Slice(violations, func(i, j int) bool {
			if violations[i].Table != violations[j].Table {
				return violations[i].Table < violations[j].Table
			}
			if violations[i].Column != violations[j].Column {
				return violations[i].Column < violations[j].Column
			}
			if violations[i].Linter != violations[j].Linter {
				return violations[i].Linter < violations[j].Linter
			}
			return violations[i].Message < violations[j].Message
		})
		ns.Lint = violations
	}
	return nil
}

// lintPulledTables audits one namespace's tables with the dialect's linter.
// Every pulled table entry must be a table definition the audit covers in
// full; an entry of another kind would go unlinted and turn the audit into a
// partial result, so it fails the request as an unlintablePulledTableError.
func lintPulledTables(linter *lint.Linter, dialect schema.Dialect, database, namespace string, tables map[string]string) ([]lint.Result, error) {
	switch dialect {
	case schema.DialectPostgres:
		results, err := linter.LintPostgresSchema(tables)
		if unlintable, ok := errors.AsType[*lint.UnlintableTableError](err); ok {
			return nil, &unlintablePulledTableError{
				Database:  database,
				Namespace: namespace,
				Table:     unlintable.Table,
				Detail:    unlintable.Detail,
			}
		}
		if err != nil {
			return nil, fmt.Errorf("lint pulled schema for database %q namespace %q: %w", database, namespace, err)
		}
		return results, nil
	case schema.DialectMySQL:
		// Spirit's linters silently pass over statement kinds other than
		// CREATE TABLE, so the kind is checked up front.
		for tableName, tableDDL := range tables {
			stmtType, _, err := ddl.ClassifyStatement(tableDDL)
			if err != nil {
				return nil, &unlintablePulledTableError{
					Database:  database,
					Namespace: namespace,
					Table:     tableName,
					Detail:    fmt.Sprintf("cannot be classified: %v", err),
				}
			}
			if stmtType != ddl.StatementCreateTable {
				return nil, &unlintablePulledTableError{
					Database:  database,
					Namespace: namespace,
					Table:     tableName,
					Detail:    fmt.Sprintf("is a %s statement, expected CREATE TABLE", stmtType),
				}
			}
		}
		results, err := linter.LintSchema(tables)
		if err != nil {
			return nil, fmt.Errorf("lint pulled schema for database %q namespace %q: %w", database, namespace, err)
		}
		return results, nil
	default:
		return nil, fmt.Errorf("lint pulled schema for database %q namespace %q: no schema audit for dialect %q", database, namespace, dialect)
	}
}

func pullNamespaces(dialect schema.Dialect, namespaces []string) ([]string, error) {
	if len(namespaces) == 0 {
		return []string{""}, nil
	}
	result := make([]string, 0, len(namespaces))
	seenOutput := make(map[string]struct{}, len(namespaces))
	for _, namespace := range namespaces {
		if strings.TrimSpace(namespace) != namespace || namespace == "" {
			return nil, fmt.Errorf("pull namespace %q must be non-empty and contain no leading or trailing whitespace", namespace)
		}
		if strings.Contains(namespace, "..") || strings.ContainsAny(namespace, `/\`) {
			return nil, fmt.Errorf("pull namespace %q must be a single path component", namespace)
		}
		if schema.HasNamespaceEnvironmentPlaceholder(namespace) {
			return nil, fmt.Errorf("pull namespace %q must be a concrete live namespace; resolve {env} or $ENV before calling pull", namespace)
		}
		if schema.IsReservedPullNamespaceForDialect(dialect, namespace) {
			return nil, fmt.Errorf("pull namespace %q is reserved and cannot be pulled", namespace)
		}
		if _, ok := seenOutput[namespace]; ok {
			return nil, fmt.Errorf("duplicate pull namespace %q", namespace)
		}
		seenOutput[namespace] = struct{}{}
		result = append(result, namespace)
	}
	return result, nil
}

func pullCatalogDetail(detail string) (ternv1.PullCatalogDetail, error) {
	switch detail {
	case "", "basic":
		return ternv1.PullCatalogDetail_PULL_CATALOG_DETAIL_BASIC, nil
	case "detailed":
		return ternv1.PullCatalogDetail_PULL_CATALOG_DETAIL_DETAILED, nil
	default:
		return ternv1.PullCatalogDetail_PULL_CATALOG_DETAIL_BASIC, fmt.Errorf("catalog_detail %q must be basic or detailed", detail)
	}
}

func mergePullSchemaResponse(merged, resp *ternv1.PullSchemaResponse, requestedNamespace string) error {
	if resp == nil {
		return fmt.Errorf("pull schema response is empty")
	}
	if requestedNamespace != "" {
		if len(resp.Namespaces) != 1 {
			return fmt.Errorf("pull namespace %q returned %d namespaces; expected 1", requestedNamespace, len(resp.Namespaces))
		}
		for responseNamespace, pulled := range resp.Namespaces {
			if responseNamespace != requestedNamespace {
				return fmt.Errorf("pull namespace %q returned namespace %q", requestedNamespace, responseNamespace)
			}
			if _, ok := merged.Namespaces[responseNamespace]; ok {
				return fmt.Errorf("pull schema response contains duplicate namespace %q", responseNamespace)
			}
			merged.Namespaces[responseNamespace] = pulled
		}
		merged.TableCount += resp.TableCount
		return nil
	}
	for responseNamespace, pulled := range resp.Namespaces {
		if _, ok := merged.Namespaces[responseNamespace]; ok {
			return fmt.Errorf("pull schema response contains duplicate namespace %q", responseNamespace)
		}
		merged.Namespaces[responseNamespace] = pulled
	}
	merged.TableCount += resp.TableCount
	return nil
}

// ApplyRequest is the HTTP request body for POST /api/apply.
type ApplyRequest struct {
	PlanID         string            `json:"plan_id"`
	Environment    string            `json:"environment"`
	Options        map[string]string `json:"options,omitempty"`
	Caller         string            `json:"caller,omitempty"`          // Identity of the caller (e.g., "cli:user@host")
	InstallationID int64             `json:"installation_id,omitempty"` // GitHub App installation ID (for PR comment tracking)
	// Target narrows the apply to one rollout member, named by its target or
	// by deployment/target. Empty applies the whole rollout.
	Target string `json:"target,omitempty"`
	// ExpectedLockOwner and ExpectedPendingPlanID are internal webhook guards;
	// direct API callers cannot assert a lock intent through JSON.
	ExpectedLockOwner     string `json:"-"`
	ExpectedPendingPlanID string `json:"-"`
	// ConfirmedMemberWork is an internal webhook guard, set only by a pull
	// request apply-confirm that checked the confirmation against every other
	// target's statements and execution modes, on a comment disclosing each
	// target's direct changes under that target. Without it, apply creation
	// refuses another target's direct-execution change: no other caller was
	// shown it. Direct API callers cannot assert it through JSON.
	ConfirmedMemberWork bool `json:"-"`
}

// handlePlan handles POST /api/plan requests.
func (s *Service) handlePlan(w http.ResponseWriter, r *http.Request) {
	var req PlanRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		s.writeBodyDecodeError(w, err)
		return
	}
	req.Database = storage.CanonicalKey(req.Database)
	req.Environment = storage.CanonicalKey(req.Environment)
	req.Type = storage.CanonicalKey(req.Type)
	req.Repository = storage.CanonicalKey(req.Repository)

	if req.Database == "" {
		s.writeError(w, http.StatusBadRequest, "database is required")
		return
	}
	if req.Environment == "" {
		s.writeError(w, http.StatusBadRequest, "environment is required")
		return
	}
	if req.Type == "" {
		s.writeError(w, http.StatusBadRequest, "type is required")
		return
	}
	if warning, err := validateSchemaFiles(req.SchemaFiles); err != nil {
		s.writeError(w, http.StatusBadRequest, err.Error())
		return
	} else if warning != "" {
		s.logger.Warn("plan request has empty schema files", "warning", warning, "database", req.Database)
	}

	// Planning stages a change against a specific database and reads its live
	// schema, so it takes the same per-database authorization as apply.
	if !s.authorizeDirectWrite(w, r, "plan", req.Database, req.Environment) {
		return
	}

	resp, err := s.ExecutePlan(r.Context(), req)
	if err != nil {
		if typeMismatchErr, ok := errors.AsType[*databaseTypeMismatchError](err); ok {
			s.logger.Warn("plan rejected for mismatched database type", "database", req.Database, "environment", req.Environment, "request_type", typeMismatchErr.RequestType, "config_type", typeMismatchErr.ConfigType)
			s.writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		if requestedRouteNotConfigured(err) {
			s.logger.Warn("plan rejected for unconfigured database route", "database", req.Database, "environment", req.Environment, "error", err)
			s.writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		if NamespacePlacementRefused(err) {
			s.logger.Warn("plan rejected for namespace placement the targets entries and schema files disagree on", "database", req.Database, "environment", req.Environment, "error", err)
			s.writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		if _, ok := errors.AsType[*RolloutMemberSelectionError](err); ok {
			s.logger.Warn("plan rejected for a target that names no single rollout member", "database", req.Database, "environment", req.Environment, "selector", req.Target, "error", err)
			s.writeErrorCode(w, http.StatusBadRequest, apitypes.ErrCodeInvalidRequest, err.Error())
			return
		}
		if _, ok := errors.AsType[*SourcePolicyError](err); ok {
			s.writeErrorCode(w, http.StatusForbidden, apitypes.ErrCodeSourcePolicyDenied, "plan failed: "+err.Error())
			return
		}
		s.logger.Error("plan failed", "database", req.Database, "error", err)
		s.writeError(w, http.StatusInternalServerError, "plan failed: "+err.Error())
		return
	}

	s.writeJSON(w, http.StatusOK, resp)
}

// ExecutePlan executes a plan request via the Tern client, stores the result,
// and returns the plan response. This is the shared implementation used by both
// the HTTP handler and the webhook handler.
func (s *Service) ExecutePlan(ctx context.Context, req PlanRequest) (*apitypes.PlanResponse, error) {
	_, resp, err := s.ExecutePlanProto(ctx, req)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// ExecutePlanProto runs a plan and returns both the reviewed primary plan proto
// and its API projection. The proto is the reviewed baseline the review-time
// drift rollup compares deployments against, so it is exposed alongside the API
// response rather than reconstructed from storage.
func (s *Service) ExecutePlanProto(ctx context.Context, req PlanRequest) (*ternv1.PlanResponse, *apitypes.PlanResponse, error) {
	ctx, span := otel.Tracer("schemabot").Start(ctx, "ExecutePlan",
		trace.WithAttributes(
			attribute.String("database", req.Database),
			attribute.String("environment", req.Environment),
			attribute.String("type", req.Type),
		),
	)
	defer span.End()

	if warning, err := validateSchemaFiles(req.SchemaFiles); err != nil {
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, "invalid schema files")
		return nil, nil, err
	} else if warning != "" {
		s.logger.Warn("plan request has empty schema files", "warning", warning, "database", req.Database)
	}

	planStart := time.Now()
	deployment := ""

	resolvedTarget, err := s.planMember(req)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, "resolve target")
		metrics.RecordPlan(ctx, req.Repository, req.Database, deployment, req.Environment, "error")
		metrics.RecordPlanDuration(ctx, time.Since(planStart), req.Repository, req.Database, deployment, req.Environment, "error")
		return nil, nil, fmt.Errorf("resolve target for %s/%s: %w", req.Database, req.Environment, err)
	}
	deployment = resolvedTarget.Deployment
	if req.Type != resolvedTarget.DatabaseType {
		typeErr := &databaseTypeMismatchError{Database: req.Database, RequestType: req.Type, ConfigType: resolvedTarget.DatabaseType}
		span.RecordError(typeErr)
		span.SetStatus(otelcodes.Error, "type mismatch")
		metrics.RecordPlan(ctx, req.Repository, req.Database, deployment, req.Environment, "error")
		metrics.RecordPlanDuration(ctx, time.Since(planStart), req.Repository, req.Database, deployment, req.Environment, "error")
		return nil, nil, typeErr
	}
	// Every declared namespace must be held by some rollout member, on every
	// plan of the environment and not only a pull request review, so a lone
	// target selecting a subset cannot report a clean plan that leaves the rest
	// planned nowhere.
	targets, err := s.config.ResolveDatabaseTargets(req.Database, req.Environment)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, "resolve targets")
		metrics.RecordPlan(ctx, req.Repository, req.Database, deployment, req.Environment, "error")
		metrics.RecordPlanDuration(ctx, time.Since(planStart), req.Repository, req.Database, deployment, req.Environment, "error")
		return nil, nil, fmt.Errorf("resolve targets for %s/%s: %w", req.Database, req.Environment, err)
	}
	if err := requireNamespaceCoverage(req, targets); err != nil {
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, "namespace coverage")
		metrics.RecordPlan(ctx, req.Repository, req.Database, deployment, req.Environment, "error")
		metrics.RecordPlanDuration(ctx, time.Since(planStart), req.Repository, req.Database, deployment, req.Environment, "error")
		return nil, nil, err
	}
	// The primary plans, and its plan row records, only the namespaces its
	// targets entry selects. req is this call's copy, so narrowing it here
	// leaves the caller's request, which the other members are planned from,
	// untouched.
	primarySchemaFiles, err := memberSchemaFiles(req, resolvedTarget)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, "select namespaces")
		metrics.RecordPlan(ctx, req.Repository, req.Database, deployment, req.Environment, "error")
		metrics.RecordPlanDuration(ctx, time.Since(planStart), req.Repository, req.Database, deployment, req.Environment, "error")
		return nil, nil, err
	}
	if len(resolvedTarget.Namespaces) > 0 {
		s.logger.Info("plan covers only the namespaces the primary target's entry selects",
			"database", req.Database,
			"environment", req.Environment,
			"deployment", deployment,
			"target", resolvedTarget.Target,
			"repository", req.Repository,
			"namespaces", resolvedTarget.Namespaces,
			"declared_namespace_count", len(req.SchemaFiles))
	}
	unselected := unselectedNamespaces(req.SchemaFiles, resolvedTarget)
	req.SchemaFiles = primarySchemaFiles

	prInt := 0
	if req.PullRequest != nil {
		prInt = int(*req.PullRequest)
	}
	trustedSchemaPath := ""
	if req.SourceTrusted {
		trustedSchemaPath = req.SchemaPath
	}
	// Source policy checks only apply to SchemaBot-discovered PR sources. Direct
	// operator/API plans remain available through the existing endpoint access
	// model until the dedicated auth layer is added.
	if !req.SourceTrusted {
		s.logger.Debug("skipping source policy for direct plan request",
			"database", req.Database,
			"environment", req.Environment,
			"repository", req.Repository,
			"pull_request", prInt)
	} else {
		if err := s.config.AuthorizePlanSource(PlanSourcePolicyRequest{
			Database:    req.Database,
			Repository:  req.Repository,
			PullRequest: prInt,
			SchemaPath:  trustedSchemaPath,
		}); err != nil {
			reason := sourcePolicyReason(err)
			span.RecordError(err)
			span.SetStatus(otelcodes.Error, "source policy")
			metrics.RecordPlan(ctx, req.Repository, req.Database, deployment, req.Environment, "error")
			metrics.RecordPlanDuration(ctx, time.Since(planStart), req.Repository, req.Database, deployment, req.Environment, "error")
			metrics.RecordSourcePolicyBlock(ctx, "plan", req.Database, req.Environment, reason)
			s.logger.Warn("plan blocked by source policy",
				"database", req.Database,
				"environment", req.Environment,
				"repository", req.Repository,
				"pull_request", prInt,
				"schema_path", req.SchemaPath,
				"reason", reason,
				"error", err)
			return nil, nil, fmt.Errorf("source policy: %w", err)
		}
	}

	client, err := s.TernClient(deployment, req.Environment)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, "tern client")
		metrics.RecordPlan(ctx, req.Repository, req.Database, deployment, req.Environment, "error")
		metrics.RecordPlanDuration(ctx, time.Since(planStart), req.Repository, req.Database, deployment, req.Environment, "error")
		return nil, nil, fmt.Errorf("database %q (%s): %w", req.Database, req.Environment, err)
	}

	directExecution, err := s.config.DirectExecutionPolicyFor(req.Database, req.Environment, resolvedTarget.DatabaseType)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, "direct execution policy")
		metrics.RecordPlan(ctx, req.Repository, req.Database, deployment, req.Environment, "error")
		metrics.RecordPlanDuration(ctx, time.Since(planStart), req.Repository, req.Database, deployment, req.Environment, "error")
		return nil, nil, fmt.Errorf("resolve direct_execution policy for database %q environment %q: %w", req.Database, req.Environment, err)
	}
	ternReq := &ternv1.PlanRequest{
		Database:          req.Database,
		Type:              resolvedTarget.DatabaseType,
		SchemaFiles:       req.SchemaFiles,
		Repository:        req.Repository,
		Environment:       req.Environment,
		Target:            resolvedTarget.Target,
		SchemaPath:        trustedSchemaPath,
		IgnoredNamespaces: req.IgnoredNamespaces,
		// The namespaces the primary's entry leaves to other targets, so an
		// engine that diffs the whole target as one unit refuses rather than
		// planning their live tables as drops.
		UnselectedNamespaces: unselected,
		IgnoreTables:         req.IgnoreTables,
		// Always stated, never left absent: absence tells the data plane the
		// caller predates the grouping choice, and this caller has made one.
		GroupedExecution: new(req.GroupedExecution),
		// The verdict on a statement the engine refuses belongs to this
		// server's configuration, wherever the statement runs. A data plane
		// resolving an opaque target has no registration for the database to
		// read a policy from, so an unstated one leaves it blocked there.
		DirectExecution: tern.DirectExecutionPolicyProto(directExecution),
	}
	if req.PullRequest != nil {
		ternReq.PullRequest = *req.PullRequest
	}
	if req.HeadSHA != nil {
		ternReq.HeadSha = *req.HeadSHA
	}

	s.logger.Info("ExecutePlan: calling client.Plan",
		"database", req.Database,
		"type", resolvedTarget.DatabaseType,
		"deployment", deployment,
		"target", resolvedTarget.Target,
		"is_remote", client.IsRemote(),
		"schema_file_count", len(req.SchemaFiles),
	)
	if len(req.IgnoredNamespaces) > 0 {
		// The desired state is deliberately partial: the caller withheld these
		// namespaces per the config's ignore_namespaces. Recorded so an operator
		// tracing "why does this plan not touch namespace X" finds the answer in
		// server logs.
		s.logger.Info("plan request excludes ignored namespaces",
			"database", req.Database,
			"environment", req.Environment,
			"deployment", deployment,
			"repository", req.Repository,
			"ignored_namespaces", req.IgnoredNamespaces,
		)
	}
	if len(req.IgnoreTables) > 0 {
		// The live-schema view is deliberately partial: the config withholds
		// these tables from the planner. Recorded so an operator tracing "why
		// does this plan not touch table X" finds the answer in server logs.
		s.logger.Info("plan request withholds live tables named by ignore_tables",
			"database", req.Database,
			"environment", req.Environment,
			"deployment", deployment,
			"repository", req.Repository,
			"ignore_tables", req.IgnoreTables,
		)
	}

	resp, err := client.Plan(ctx, ternReq)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, "plan failed")
		metrics.RecordPlan(ctx, req.Repository, req.Database, deployment, req.Environment, "error")
		metrics.RecordPlanDuration(ctx, time.Since(planStart), req.Repository, req.Database, deployment, req.Environment, "error")
		s.logger.Error("ExecutePlan: client.Plan failed",
			"database", req.Database,
			"type", resolvedTarget.DatabaseType,
			"deployment", deployment,
			"target", resolvedTarget.Target,
			"environment", req.Environment,
			"repository", req.Repository,
			"pull_request", prInt,
			"endpoint", client.Endpoint(),
			"is_remote", client.IsRemote(),
			"error", err,
		)
		if client.IsRemote() && grpcstatus.Code(err) == grpccodes.Unavailable {
			return nil, nil, &RemoteDeploymentUnavailableError{
				Deployment: deployment,
				Target:     resolvedTarget.Target,
				Err:        err,
			}
		}
		return nil, nil, err
	}
	span.SetAttributes(attribute.String("plan_id", resp.PlanId), attribute.Int("change_count", len(resp.Changes)))
	metrics.RecordPlan(ctx, req.Repository, req.Database, deployment, req.Environment, "success")
	metrics.RecordPlanDuration(ctx, time.Since(planStart), req.Repository, req.Database, deployment, req.Environment, "success")

	s.logger.Info("ExecutePlan: plan response",
		"plan_id", resp.PlanId,
		"change_count", len(resp.Changes),
	)
	for _, ch := range resp.Changes {
		for _, tc := range ch.TableChanges {
			s.logger.Info("ExecutePlan: table change",
				"table", tc.TableName,
				"change_type", tc.ChangeType.String(),
				"ddl_len", len(tc.Ddl),
			)
		}
	}

	if err := s.refuseDropsOfWithheldTables(req, resp, deployment); err != nil {
		return nil, nil, err
	}

	s.normalizeExecutionVerdicts(resp, req.Database, deployment)

	route := storedPlanRoute{
		DatabaseType:    resolvedTarget.DatabaseType,
		Deployment:      deployment,
		Target:          resolvedTarget.Target,
		DirectExecution: resolvedDirectExecution(directExecution),
	}
	if err := s.storePlanResponse(ctx, req, resp, route); err != nil {
		return nil, nil, err
	}

	planResp := planResponseFromProto(resp)
	// Record the primary rollout member this plan was created against so the
	// review-time drift rollup can verify the baseline still maps to the primary
	// at rollup time. Both halves are needed: one deployment can address several
	// targets, so the deployment alone does not identify the member.
	planResp.Deployment = deployment
	planResp.Target = resolvedTarget.Target
	planResp.SelectedNamespaces = slices.Clone(resolvedTarget.Namespaces)
	if req.Target != "" {
		planResp.NarrowedTo = resolvedTarget.MemberID()
	}
	return resp, planResp, nil
}

// planMember resolves the rollout member a plan is made against: the member
// the request's target selector names, or the rollout primary when it names
// none.
func (s *Service) planMember(req PlanRequest) (routing.ExecutionTarget, error) {
	if req.Target == "" {
		return s.config.ResolvePrimaryDatabaseTarget(req.Database, req.Environment)
	}
	member, err := s.resolveRolloutMember(req.Database, req.Environment, req.Target)
	if err != nil {
		return routing.ExecutionTarget{}, err
	}
	s.logger.Info("plan narrowed to one rollout member; the plan speaks for that member only",
		"database", req.Database,
		"environment", req.Environment,
		"repository", req.Repository,
		"selector", req.Target,
		"deployment", member.Deployment,
		"target", member.Target)
	return member, nil
}

// normalizeExecutionVerdicts normalizes a whole plan response, for the paths
// that hold one. A nil response is a no-op so callers that may not have reached
// a planner do not have to guard the call themselves.
func (s *Service) normalizeExecutionVerdicts(resp *ternv1.PlanResponse, database, deployment string) {
	if resp == nil {
		return
	}
	s.normalizePlanExecutionVerdicts(resp.Changes, resp.Shards, database, deployment)
}

// normalizePlanExecutionVerdicts validates every table change's execution-mode
// verdict against its closed vocabulary — empty (executable), "blocked", or
// "direct" — before the plan is stored or returned. The verdict crosses the
// wire as a free-form string and everything except "blocked" executes as the
// engine's default path downstream, so a value this build does not recognize
// (a skewed remote planner, a newer plan contract) must fail closed rather
// than run.
//
// It takes the plan's parts rather than a response because not every path that
// stores a plan holds one: a member planned against its own live schema arrives
// from the non-persisting diff RPC, and its changes reach storage through the
// same shared writer. Normalizing there is what keeps the vocabulary enforced
// once for every plan row, whichever RPC produced it.
func (s *Service) normalizePlanExecutionVerdicts(changes []*ternv1.SchemaChange, shards []*ternv1.ShardPlan, database, deployment string) {
	for _, change := range changes {
		if change == nil {
			continue
		}
		for _, tc := range change.TableChanges {
			s.normalizeExecutionVerdict(tc, database, deployment)
		}
	}
	for _, shard := range shards {
		if shard == nil {
			continue
		}
		for _, tc := range shard.Changes {
			s.normalizeExecutionVerdict(tc, database, deployment)
		}
	}
}

func (s *Service) normalizeExecutionVerdict(tc *ternv1.TableChange, database, deployment string) {
	if tc == nil || recognizedExecutionMode(tc.ExecutionMode) {
		return
	}
	s.logger.Warn("plan response carried an unrecognized execution-mode verdict; the change will be blocked",
		"database", database,
		"deployment", deployment,
		"table", tc.TableName,
		"execution_mode", tc.ExecutionMode,
	)
	// The quoted verdict is planner output, so the reason is neutralized like
	// any other cause an engine composes from text it did not write.
	tc.ModeReason = engine.SanitizeBlockedCause(fmt.Sprintf("planner returned execution-mode verdict %q, which this SchemaBot build does not recognize; the change is blocked because SchemaBot cannot determine how the statement would run", tc.ExecutionMode))
	tc.ExecutionMode = engine.ExecutionModeBlocked
}

// recognizedExecutionMode reports whether mode is in the closed execution-mode
// vocabulary: empty (the engine executes the statement on its default path),
// blocked, or direct.
func recognizedExecutionMode(mode string) bool {
	return mode == "" ||
		strings.EqualFold(mode, engine.ExecutionModeBlocked) ||
		strings.EqualFold(mode, engine.ExecutionModeDirect)
}

// storedPlanRoute is what a stored plan row is stamped with beyond the request:
// the member the plan was produced for, and — for a member planned against its
// own live schema — the reviewed plan it was produced alongside.
type storedPlanRoute struct {
	DatabaseType string
	Deployment   string
	Target       string

	// PrimaryPlanIdentifier names the reviewed plan of this member's review
	// round. Empty for the reviewed plan itself and for every plan of an
	// environment whose members all run it.
	PrimaryPlanIdentifier string

	// DirectExecution is the policy this route's execution verdicts were
	// judged under, stamped on the row so the apply that runs them runs under
	// the same one. Nil records nothing, which reads back at admission as a
	// plan whose policy the configuration still decides — so a producer that
	// resolved an answer states it, disabled included, and only a producer
	// with no answer to give leaves this unset.
	DirectExecution *storage.DirectExecutionPolicy
}

// refuseDropsOfWithheldTables refuses a plan that proposes dropping a table the
// request asked to withhold. Every path that turns a PlanRequest into a stored
// plan runs it, the rollback re-plan included: a rollback is planned against
// the same target by the same data plane, so a build that discards the
// exclusion answers it the same way, and the refusal is only worth anything
// where the plan would otherwise be stored and surfaced for review.
//
// A configured entry that withheld nothing is ordinarily a typo, a case
// mismatch, or a stale entry for a table that no longer exists — the table it
// names is fully reconciled, so it is surfaced as a warning rather than letting
// the config imply an exclusion that is not happening. But an entry the plan
// did not withhold and whose exact name it proposes dropping is not a typo: the
// target holds that table, so the exclusion reached a data plane that did not
// apply it — one that predates the field and discarded it. Every other
// unmatched shape names a table that is not there to drop, so this one is
// unambiguous, and letting it through would turn a reviewed exclusion into the
// drop it was written to prevent.
func (s *Service) refuseDropsOfWithheldTables(req PlanRequest, resp *ternv1.PlanResponse, deployment string) error {
	unmatched := schema.UnmatchedIgnoreTables(req.IgnoreTables, withheldTablesFromProto(resp.ExemptTables))
	if len(unmatched) == 0 {
		return nil
	}
	var prInt int
	if req.PullRequest != nil {
		prInt = int(*req.PullRequest)
	}
	s.logger.Warn("ignore_tables entries matched no live table and withheld nothing",
		"database", req.Database,
		"environment", req.Environment,
		"deployment", deployment,
		"repository", req.Repository,
		"pull_request", prInt,
		"unmatched_entries", unmatched,
	)
	dropped := plannedDropsAmong(resp.Changes, unmatched)
	if len(dropped) == 0 {
		return nil
	}
	s.logger.Error("plan proposes dropping tables that ignore_tables withholds",
		"database", req.Database,
		"environment", req.Environment,
		"deployment", deployment,
		"repository", req.Repository,
		"pull_request", prInt,
		"tables", dropped,
	)
	if len(dropped) == 1 {
		return fmt.Errorf(
			"the plan from deployment %q proposes dropping %q, which ignore_tables withholds. That deployment's planner never saw the exclusion, so the plan is refused rather than reviewed as a drop. Upgrade that deployment to a build that supports ignore_tables",
			deployment, dropped[0])
	}
	return fmt.Errorf(
		"the plan from deployment %q proposes dropping %s, which ignore_tables withholds. That deployment's planner never saw the exclusion, so the plan is refused rather than reviewed as drops. Upgrade that deployment to a build that supports ignore_tables",
		deployment, quotedTableList(dropped))
}

// quotedTableList renders table names for an operator-facing message, quoted
// so a name with a space or a trailing character reads as one name.
func quotedTableList(names []string) string {
	quoted := make([]string, 0, len(names))
	for _, name := range names {
		quoted = append(quoted, fmt.Sprintf("%q", name))
	}
	return strings.Join(quoted, ", ")
}

// resolvedDirectExecution renders a policy the configuration resolved into the
// form the plan row records. A resolver that returned nothing means no grant
// was in force, and the plan records that as a disabled policy rather than as
// nothing: a row holding nothing is read as one written before the column
// existed and sends admission back to the configuration, which is exactly the
// drift this record exists to remove.
//
// This is for producers that ran the resolver. A producer carrying a policy
// forward from somewhere else passes it through unchanged, so a source that
// itself recorded nothing stays recorded as nothing.
func resolvedDirectExecution(policy *storage.DirectExecutionPolicy) *storage.DirectExecutionPolicy {
	if policy == nil {
		return &storage.DirectExecutionPolicy{Enabled: false}
	}
	return policy
}

func (s *Service) storePlanResponse(ctx context.Context, req PlanRequest, resp *ternv1.PlanResponse, route storedPlanRoute) error {
	return s.storePlan(ctx, req, resp.PlanId, resp.Changes, resp.Shards, route)
}

// storePlan writes one plan row for a single rollout member: the changes and
// shards that member would run, stamped with the member's own route and the
// request's PR context. It is the one place a plan row is built, so the primary
// member's reviewed plan and a non-primary member's independently produced plan
// are stored identically and are indistinguishable to everything downstream.
//
// planIdentifier is the plan's external identifier — minted by the planner for
// the primary, minted here for a member whose plan came from the non-persisting
// diff RPC.
//
// An identifier that is already stored is not an error: a re-plan of unchanged
// content re-stores the same plan, and the row already there is that plan. It is
// held to this member's route, though (keepStoredPlanOnRoute).
func (s *Service) storePlan(ctx context.Context, req PlanRequest, planIdentifier string, changes []*ternv1.SchemaChange, shards []*ternv1.ShardPlan, route storedPlanRoute) error {
	if planIdentifier == "" {
		return fmt.Errorf("store plan for database %s deployment %q target %q: plan has no identifier", req.Database, route.Deployment, route.Target)
	}
	prInt := 0
	if req.PullRequest != nil {
		prInt = int(*req.PullRequest)
	}
	trustedSchemaPath := ""
	if req.SourceTrusted {
		trustedSchemaPath = req.SchemaPath
	}
	headSHA := ""
	if req.HeadSHA != nil {
		headSHA = *req.HeadSHA
	}
	s.normalizePlanExecutionVerdicts(changes, shards, req.Database, route.Deployment)
	namespaces, err := protoChangesToNamespaces(changes, req.SchemaFiles)
	if err != nil {
		return fmt.Errorf("convert plan namespaces: %w", err)
	}
	storedShards, err := protoShardPlansToStorage(shards)
	if err != nil {
		return fmt.Errorf("convert plan shards: %w", err)
	}
	storedPlan := &storage.Plan{
		PlanIdentifier:        planIdentifier,
		Database:              req.Database,
		DatabaseType:          route.DatabaseType,
		Deployment:            route.Deployment,
		Target:                route.Target,
		Repository:            req.Repository,
		PullRequest:           prInt,
		SchemaPath:            trustedSchemaPath,
		Environment:           req.Environment,
		SchemaFiles:           protoToSchemaFiles(req.SchemaFiles),
		Namespaces:            namespaces,
		Shards:                storedShards,
		HeadSHA:               headSHA,
		PrimaryPlanIdentifier: route.PrimaryPlanIdentifier,
		DirectExecution:       route.DirectExecution,
		CreatedAt:             time.Now(),
	}
	storedPlan.RecordIgnoreTables(req.IgnoreTables)
	// An identifier the planner supplied can already name a stored row when a
	// plan is delivered twice, or when the planner stored the row itself, and
	// re-storing it keeps that row rather than failing. A member's identifier is
	// minted here per call, so it never collides and this only ever forgives the
	// supplied kind.
	_, err = s.storage.Plans().Create(ctx, storedPlan)
	switch {
	case err == nil:
		return nil
	case errors.Is(err, storage.ErrPlanIDExists):
		return s.keepStoredPlanOnRoute(ctx, storedPlan)
	default:
		return fmt.Errorf("store plan %s: %w", planIdentifier, err)
	}
}

// keepStoredPlanOnRoute holds the row already stored under a plan's identifier
// to the rollout member the service planned it for.
//
// A planner sharing this storage, such as a local client or a target router
// serving this server's own requests, stores the row for a plan with changes
// before the service does, stamped with the route it knows: the database it was
// configured with as the deployment, and the target it resolved. The service
// keeps that row, so the row has to name the member: an apply finds the
// reviewed target among the rollout's members by the plan's deployment and
// target, and a row stamped with anything else reads as a member with no stored
// plan, refused after the operator has confirmed. A row for another database or
// environment is not this plan at all, and fails the plan rather than being
// taken for it.
func (s *Service) keepStoredPlanOnRoute(ctx context.Context, plan *storage.Plan) error {
	plans := s.storage.Plans()
	existing, err := plans.Get(ctx, plan.PlanIdentifier)
	if err != nil {
		return fmt.Errorf("load plan %s already stored under its identifier: %w", plan.PlanIdentifier, err)
	}
	if existing == nil {
		return fmt.Errorf("plan %s was reported as already stored, but no row carries its identifier", plan.PlanIdentifier)
	}
	if existing.Database != plan.Database || existing.Environment != plan.Environment {
		return fmt.Errorf("plan %s for database %q environment %q collides with a stored plan for database %q environment %q",
			plan.PlanIdentifier, plan.Database, plan.Environment, existing.Database, existing.Environment)
	}
	if existing.Deployment == plan.Deployment && existing.Target == plan.Target {
		return nil
	}
	s.logger.Info("plan row stored by the planner names a different route; restamping it with the rollout member it was planned for",
		"plan_id", plan.PlanIdentifier, "database", plan.Database, "environment", plan.Environment,
		"stored_deployment", existing.Deployment, "stored_target", existing.Target,
		"deployment", plan.Deployment, "target", plan.Target)
	if err := plans.UpdateRoute(ctx, plan.PlanIdentifier, plan.Deployment, plan.Target); err != nil {
		return fmt.Errorf("restamp plan %s with its rollout member: %w", plan.PlanIdentifier, err)
	}
	return nil
}

// handleApply handles POST /api/apply requests.
func (s *Service) handleApply(w http.ResponseWriter, r *http.Request) {
	var req ApplyRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		s.writeBodyDecodeError(w, err)
		return
	}
	req.Environment = storage.CanonicalKey(req.Environment)

	if req.PlanID == "" {
		s.writeError(w, http.StatusBadRequest, "plan_id is required")
		return
	}
	if req.Environment == "" {
		s.writeError(w, http.StatusBadRequest, "environment is required")
		return
	}

	if !s.authorizeDirectWriteForStoredPlan(w, r, "apply", req.PlanID, req.Environment) {
		return
	}

	resp, applyID, err := s.ExecuteApply(r.Context(), req)
	if err != nil {
		if errors.Is(err, errPlanLookupFailed) {
			s.logger.Error("apply failed to load the stored plan", "plan_id", req.PlanID, "environment", req.Environment, "error", err)
			s.writeErrorCode(w, http.StatusInternalServerError, apitypes.ErrCodeStorageError, storedPlanLookupFailedMessage("apply", req.PlanID))
			return
		}
		if errors.Is(err, storage.ErrPlanNotFound) {
			s.logger.Warn("apply rejected because the stored plan does not exist", "plan_id", req.PlanID, "environment", req.Environment)
			s.writeErrorCode(w, http.StatusNotFound, apitypes.ErrCodeNotFound, storedPlanNotFoundMessage("apply", req.PlanID))
			return
		}
		if _, ok := errors.AsType[*planEnvironmentMismatchError](err); ok {
			s.logger.Warn("apply rejected because the stored plan was created for another environment", "plan_id", req.PlanID, "environment", req.Environment, "error", err)
			s.writeErrorCode(w, http.StatusBadRequest, apitypes.ErrCodeInvalidRequest, "apply rejected: "+err.Error())
			return
		}
		if _, ok := errors.AsType[*planRoutingMetadataError](err); ok {
			s.logger.Warn("apply rejected because the stored plan lacks routing metadata", "plan_id", req.PlanID,
				"environment", req.Environment, "error", err)
			s.writeErrorCode(w, http.StatusBadRequest, apitypes.ErrCodeInvalidRequest, "apply rejected: "+err.Error())
			return
		}
		if errors.Is(err, storage.ErrActiveApplyExists) {
			s.logger.Warn("apply blocked by active apply", "plan_id", req.PlanID, "environment", req.Environment, "error", err)
			s.writeErrorCode(w, http.StatusConflict, apitypes.ErrCodeActiveApplyExists, "apply blocked by active apply: "+err.Error())
			return
		}
		if _, ok := errors.AsType[*SourcePolicyError](err); ok {
			s.writeErrorCode(w, http.StatusForbidden, apitypes.ErrCodeSourcePolicyDenied, "apply failed: "+err.Error())
			return
		}
		if _, ok := errors.AsType[*UnsupportedFeatureError](err); ok {
			s.logger.Warn("apply rejected because the database type does not support a requested feature", "plan_id", req.PlanID, "environment", req.Environment, "error", err)
			s.writeErrorCode(w, http.StatusBadRequest, apitypes.ErrCodeInvalidRequest, "apply rejected: "+err.Error())
			return
		}
		if _, ok := errors.AsType[*RolloutMemberSelectionError](err); ok {
			s.logger.Warn("apply rejected for a target that names no single rollout member", "plan_id", req.PlanID, "environment", req.Environment, "selector", req.Target, "error", err)
			s.writeErrorCode(w, http.StatusBadRequest, apitypes.ErrCodeInvalidRequest, "apply rejected: "+err.Error())
			return
		}
		s.logger.Error("apply failed", "plan_id", req.PlanID, "error", err)
		s.writeError(w, http.StatusInternalServerError, "apply failed: "+err.Error())
		return
	}

	_ = applyID // HTTP handler doesn't need the stored apply ID

	s.writeJSON(w, http.StatusOK, resp)
}

func applyMetricStatusForError(err error) string {
	if errors.Is(err, storage.ErrActiveApplyExists) {
		return "conflict"
	}
	return "error"
}

// UnsupportedFeatureError identifies an apply option that the target database
// cannot execute. Callers may present this operator-actionable rejection
// without exposing an internal failure.
type UnsupportedFeatureError struct {
	Database     string
	DatabaseType string
	Feature      schema.Feature
}

func (e *UnsupportedFeatureError) Error() string {
	return fmt.Sprintf("database %q: %s is not supported for database_type: %s", e.Database, e.Feature, e.DatabaseType)
}

// errPlanLookupFailed marks an apply that could not read its stored plan. The
// storage read failed, so the request is neither accepted nor known to be
// wrong: it is a server failure, kept apart from a plan that does not exist.
var errPlanLookupFailed = errors.New("plan lookup failed")

// storedPlanLookupFailedMessage is the response for an operation whose stored
// plan could not be read. The storage error stays in the server log; the
// caller learns only that the read failed and that retrying is safe.
func storedPlanLookupFailedMessage(operation, planID string) string {
	return fmt.Sprintf("%s failed: failed to get plan %s; see server logs, then retry", operation, planID)
}

// storedPlanNotFoundMessage is the response for an operation that names a
// plan SchemaBot has no record of.
func storedPlanNotFoundMessage(operation, planID string) string {
	return fmt.Sprintf("%s rejected: plan not found: %s; check the plan_id or create a new plan", operation, planID)
}

// planEnvironmentMismatchError identifies an apply that names a different
// environment than the one its stored plan was created for. The plan was
// reviewed for its own environment only, so the request is refused as a
// caller error rather than applied somewhere it was not reviewed for.
type planEnvironmentMismatchError struct {
	PlanID               string
	PlanEnvironment      string
	RequestedEnvironment string
}

func (e *planEnvironmentMismatchError) Error() string {
	return fmt.Sprintf("plan %s was created for environment %q, not %q; apply it to %q or create a plan for %q",
		e.PlanID, e.PlanEnvironment, e.RequestedEnvironment, e.PlanEnvironment, e.RequestedEnvironment)
}

// planRoutingMetadataError identifies a stored plan that lacks one of the
// server-side routing fields (deployment, target) the operator needs to
// dispatch it. The plan cannot be repaired from the apply request, so the
// caller is told to create a new plan rather than retry this one.
type planRoutingMetadataError struct {
	PlanID string
	Field  string
}

func (e *planRoutingMetadataError) Error() string {
	return fmt.Sprintf("plan %s is missing server-side routing metadata field %q; create a new plan and retry apply",
		e.PlanID, e.Field)
}

// ExecuteApply queues an apply request in storage and returns once the work is
// durable. Operator drivers own dispatching queued work through the Tern
// client so request cancellation cannot orphan in-memory execution.
//
// Flow:
//  1. Load the plan from SchemaBot storage (source of truth for database, DDL changes).
//  2. Resolve the Tern client to validate the deployment/environment.
//  3. Create a pending Apply record and pending Task records from the plan.
//  4. Attach any pending observer to the stored apply before dispatch can start.
//  5. Wake an operator driver so fresh applies usually start immediately.
//  6. Return the SchemaBot apply_identifier to the HTTP caller.
//
// Returns the API response, the stored apply ID (0 if not stored), and any error.
func (s *Service) ExecuteApply(ctx context.Context, req ApplyRequest) (*apitypes.ApplyResponse, int64, error) {
	ctx, span := otel.Tracer("schemabot").Start(ctx, "ExecuteApply",
		trace.WithAttributes(
			attribute.String("plan_id", req.PlanID),
			attribute.String("environment", req.Environment),
		),
	)
	defer span.End()

	plan, err := s.loadPlanForApply(ctx, span, req)
	if err != nil {
		return nil, 0, err
	}
	if err := s.authorizeStoredPlanSource(ctx, span, plan, req); err != nil {
		return nil, 0, err
	}
	return s.queueValidatedApply(ctx, span, plan, req)
}

// EnqueueAuthorizedApply queues an apply for a stored plan without evaluating
// source policy. The caller asserts that source authorization for this apply
// already happened — for example, a control plane that evaluated source policy
// against its own database config before dispatching to this deployment, or a
// host process that gates its own callers.
//
// This entry point exists because source policy can only be evaluated where
// the database config lives. A deployment that executes applies dispatched by
// a separate control plane has no database config, so re-evaluating the
// policy there can only fail closed.
//
// It is intentionally not reachable from SchemaBot's HTTP API. All execution
// invariants still apply: the plan must exist, match the requested
// environment, and carry stored routing metadata, and storage still enforces
// one active apply per target.
func (s *Service) EnqueueAuthorizedApply(ctx context.Context, req ApplyRequest) (*apitypes.ApplyResponse, int64, error) {
	ctx, span := otel.Tracer("schemabot").Start(ctx, "EnqueueAuthorizedApply",
		trace.WithAttributes(
			attribute.String("plan_id", req.PlanID),
			attribute.String("environment", req.Environment),
		),
	)
	defer span.End()

	plan, err := s.loadPlanForApply(ctx, span, req)
	if err != nil {
		return nil, 0, err
	}
	s.logger.Info("queueing apply without source policy evaluation; caller asserted source authorization",
		"plan_id", req.PlanID,
		"database", plan.Database,
		"deployment", plan.Deployment,
		"environment", req.Environment,
		"repository", plan.Repository,
		"pull_request", plan.PullRequest,
		"schema_path", plan.SchemaPath,
		"caller", req.Caller)
	return s.queueValidatedApply(ctx, span, plan, req)
}

// loadPlanForApply loads the stored plan for an apply request and enforces the
// execution invariants every queue path requires: the plan exists, was created
// for the requested environment, and carries the server-side routing metadata
// (deployment, target) the operator needs to dispatch it.
//
// The apply counter is keyed by the plan's repository, database, and
// deployment, so a failed or empty lookup deliberately records nothing: there
// is no plan to attribute the failure to, and a sentinel-labelled point would
// only dilute the per-database series. Those failures stay on the span and in
// the handler's log line. Every invariant checked after the plan loads records
// an error against the plan's own labels.
func (s *Service) loadPlanForApply(ctx context.Context, span trace.Span, req ApplyRequest) (*storage.Plan, error) {
	// Load plan first; it is the source of truth for database, type, and routing.
	plan, err := s.storage.Plans().Get(ctx, req.PlanID)
	if err != nil {
		span.RecordError(err)
		if errors.Is(err, storage.ErrPlanNotFound) {
			// A store that reports a missing plan as the sentinel rather than
			// a nil plan is still a caller error, not a storage failure.
			span.SetStatus(otelcodes.Error, "plan not found")
			return nil, fmt.Errorf("%w: %s", err, req.PlanID)
		}
		span.SetStatus(otelcodes.Error, "plan lookup failed")
		return nil, fmt.Errorf("%w for %s: %w", errPlanLookupFailed, req.PlanID, err)
	}
	if plan == nil {
		planErr := fmt.Errorf("%w: %s", storage.ErrPlanNotFound, req.PlanID)
		span.RecordError(planErr)
		span.SetStatus(otelcodes.Error, "plan not found")
		return nil, planErr
	}
	span.SetAttributes(attribute.String("database", plan.Database))
	if plan.Environment != req.Environment {
		applyErr := &planEnvironmentMismatchError{
			PlanID:               req.PlanID,
			PlanEnvironment:      plan.Environment,
			RequestedEnvironment: req.Environment,
		}
		span.RecordError(applyErr)
		span.SetStatus(otelcodes.Error, "environment mismatch")
		metrics.RecordApply(ctx, plan.Repository, plan.Database, plan.Deployment, req.Environment, "error")
		return nil, applyErr
	}
	if plan.Deployment == "" {
		applyErr := &planRoutingMetadataError{PlanID: req.PlanID, Field: "deployment"}
		span.RecordError(applyErr)
		span.SetStatus(otelcodes.Error, "missing stored deployment")
		metrics.RecordApply(ctx, plan.Repository, plan.Database, plan.Deployment, req.Environment, "error")
		return nil, applyErr
	}
	if plan.Target == "" {
		applyErr := &planRoutingMetadataError{PlanID: req.PlanID, Field: "target"}
		span.RecordError(applyErr)
		span.SetStatus(otelcodes.Error, "missing stored target")
		metrics.RecordApply(ctx, plan.Repository, plan.Database, plan.Deployment, req.Environment, "error")
		return nil, applyErr
	}
	return plan, nil
}

// authorizeStoredPlanSource evaluates source policy for a stored plan before
// it is queued. Source policy is evaluated for plans created from SchemaBot's
// trusted GitHub PR discovery path. Direct operator/API plans do not have a
// server-discovered schema path today; those remain governed by endpoint
// access until the dedicated auth layer is added.
func (s *Service) authorizeStoredPlanSource(ctx context.Context, span trace.Span, plan *storage.Plan, req ApplyRequest) error {
	if plan.SchemaPath == "" {
		s.logger.Debug("skipping source policy for apply because stored plan has no trusted schema path",
			"plan_id", req.PlanID,
			"database", plan.Database,
			"deployment", plan.Deployment,
			"environment", req.Environment,
			"repository", plan.Repository,
			"pull_request", plan.PullRequest)
		return nil
	}
	if err := s.config.AuthorizePlanSource(PlanSourcePolicyRequest{
		Database:    plan.Database,
		Repository:  plan.Repository,
		PullRequest: plan.PullRequest,
		SchemaPath:  plan.SchemaPath,
	}); err != nil {
		reason := sourcePolicyReason(err)
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, "source policy")
		metrics.RecordApply(ctx, plan.Repository, plan.Database, plan.Deployment, req.Environment, "error")
		metrics.RecordSourcePolicyBlock(ctx, "apply", plan.Database, req.Environment, reason)
		s.logger.Warn("apply blocked by source policy",
			"plan_id", req.PlanID,
			"database", plan.Database,
			"deployment", plan.Deployment,
			"environment", req.Environment,
			"repository", plan.Repository,
			"pull_request", plan.PullRequest,
			"schema_path", plan.SchemaPath,
			"reason", reason,
			"error", err)
		return fmt.Errorf("source policy for plan %s: %w", req.PlanID, err)
	}
	return nil
}

// queueValidatedApply stores the pending apply and tasks for a validated plan
// and wakes an operator driver. Callers must have run loadPlanForApply first;
// gated entry points also run authorizeStoredPlanSource before queueing.
func (s *Service) queueValidatedApply(ctx context.Context, span trace.Span, plan *storage.Plan, req ApplyRequest) (*apitypes.ApplyResponse, int64, error) {
	deployment := plan.Deployment
	deferredCutover := storage.ApplyOptionsFromMap(req.Options).DeferCutover
	if deferredCutover && !schema.SupportsFeature(plan.DatabaseType, schema.FeatureDeferredCutover) {
		err := &UnsupportedFeatureError{Database: plan.Database, DatabaseType: plan.DatabaseType, Feature: schema.FeatureDeferredCutover}
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, "unsupported apply feature")
		metrics.RecordApply(ctx, plan.Repository, plan.Database, plan.Deployment, req.Environment, "error")
		s.logger.Warn("apply rejected because the database type does not support a requested feature",
			"plan_id", req.PlanID,
			"database", plan.Database,
			"database_type", plan.DatabaseType,
			"deployment", plan.Deployment,
			"environment", req.Environment,
			"repository", plan.Repository,
			"pull_request", plan.PullRequest,
			"error", err)
		return nil, 0, err
	}

	client, err := s.TernClient(deployment, req.Environment)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, "tern client")
		metrics.RecordApply(ctx, plan.Repository, plan.Database, plan.Deployment, req.Environment, "error")
		return nil, 0, fmt.Errorf("database %q (%s): %w", plan.Database, req.Environment, err)
	}

	options := maps.Clone(req.Options)
	if options == nil {
		options = make(map[string]string)
	}
	options["target"] = plan.Target

	enqueueStart := time.Now()
	recordApplyResult := func(status string) {
		metrics.RecordApply(ctx, plan.Repository, plan.Database, plan.Deployment, req.Environment, status)
		metrics.RecordApplyDuration(ctx, time.Since(enqueueStart), plan.Repository, plan.Database, plan.Deployment, req.Environment, status)
	}
	recordApplyError := func(status string, err error) {
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, status)
		recordApplyResult(applyMetricStatusForError(err))
	}

	attachObserver := func(storedApplyID int64) {
		observer := s.consumePendingObserver(plan.Database, deployment, req.Environment)
		if observer == nil {
			return
		}
		type applyIDSetter interface{ SetApplyID(int64) }
		if setter, ok := observer.(applyIDSetter); ok {
			setter.SetApplyID(storedApplyID)
		}
		client.SetObserver(storedApplyID, observer)
	}

	applyIdentifier, storedApplyID, err := s.enqueueApply(ctx, plan, req, options, attachObserver)
	if err != nil {
		recordApplyError("enqueue apply", err)
		return nil, 0, err
	}
	if storedApplyID <= 0 {
		applyErr := fmt.Errorf("accepted apply missing stored apply id")
		recordApplyError("apply missing stored id", applyErr)
		return nil, 0, applyErr
	}

	span.SetAttributes(attribute.String("apply_id", applyIdentifier), attribute.Bool("accepted", true))
	recordApplyResult("success")
	metrics.AdjustActiveApplies(ctx, 1, plan.Database, plan.Deployment, req.Environment)
	s.wakeOperator(applyIdentifier, plan.Database, req.Environment)

	return &apitypes.ApplyResponse{
		Accepted: true,
		ApplyID:  applyIdentifier,
	}, storedApplyID, nil
}

func (s *Service) enqueueApply(
	ctx context.Context,
	plan *storage.Plan,
	req ApplyRequest,
	options map[string]string,
	onApplyCreated func(int64),
) (string, int64, error) {
	applyIdentifier := engine.NewApplyID()
	apply, storedApplyID, err := s.createStoredApply(ctx, plan, req, options, applyIdentifier)
	if err != nil {
		return "", 0, err
	}
	if onApplyCreated != nil {
		onApplyCreated(storedApplyID)
	}
	return apply.ApplyIdentifier, storedApplyID, nil
}

// applyDirectExecution resolves the policy an apply created from this plan
// runs under. The plan's own record is authoritative: it is what the plan's
// execution verdicts were computed against, and a configuration change
// between review and apply must not move the statement to a different bound
// than the one the operator saw. A plan stored before the column existed
// recorded nothing, and falls back to configuration the way admission
// resolved it then.
func (s *Service) applyDirectExecution(plan *storage.Plan, environment string) (*storage.DirectExecutionPolicy, error) {
	if plan.DirectExecution != nil {
		return plan.DirectExecution, nil
	}
	policy, err := s.config.DirectExecutionPolicyFor(plan.Database, environment, plan.DatabaseType)
	if err != nil {
		return nil, fmt.Errorf("resolve direct_execution policy for database %q environment %q: %w", plan.Database, environment, err)
	}
	return policy, nil
}

func (s *Service) createStoredApply(
	ctx context.Context,
	plan *storage.Plan,
	req ApplyRequest,
	options map[string]string,
	applyIdentifier string,
) (*storage.Apply, int64, error) {
	now := time.Now()
	applyOpts := storage.ApplyOptionsFromMap(options)
	// The apply runs under the policy its plan's execution verdicts were
	// judged under, never under one the caller named in its options.
	// Recording it here is what lets the drive that eventually runs the
	// statement — a later one, possibly on another pod or after this server
	// has been reconfigured — route it under the policy the operator
	// reviewed.
	directExecution, err := s.applyDirectExecution(plan, req.Environment)
	if err != nil {
		return nil, 0, err
	}
	applyOpts.DirectExecution = directExecution
	// Blocked changes reject before unsafe changes because no opt-in can make a
	// statement the engine refuses executable.
	if err := plan.BlockedApplyError(); err != nil {
		return nil, 0, err
	}
	if err := rejectUnsafeStoredPlanWithoutOptIn(plan, applyOpts); err != nil {
		return nil, 0, err
	}

	targets, err := s.applyTargets(plan, req)
	if err != nil {
		return nil, 0, err
	}
	// A narrowed apply records the member it ran on, so a later rollback can
	// tell it changed that member alone and not the whole rollout.
	if req.Target != "" {
		applyOpts.NarrowedTo = targets[0].MemberID()
	}

	var lockID int64
	lock, err := s.storage.Locks().Get(ctx, plan.Database, plan.DatabaseType)
	if err != nil {
		return nil, 0, fmt.Errorf("lookup lock for %s/%s: %w", plan.Database, plan.DatabaseType, err)
	}
	if lock != nil {
		lockID = lock.ID
	}

	// Attribute the apply to the authenticated caller when the request carried a
	// real identity (API auth enabled); see resolveCaller.
	caller := resolveCaller(ctx, req.Caller)

	apply := &storage.Apply{
		ApplyIdentifier:       applyIdentifier,
		LockID:                lockID,
		ExpectedLockOwner:     req.ExpectedLockOwner,
		ExpectedPendingPlanID: req.ExpectedPendingPlanID,
		PlanID:                plan.ID,
		Database:              plan.Database,
		DatabaseType:          plan.DatabaseType,
		Repository:            plan.Repository,
		PullRequest:           plan.PullRequest,
		Environment:           req.Environment,
		Deployment:            targets[0].Deployment,
		Caller:                caller,
		InstallationID:        req.InstallationID,
		Engine:                storage.EngineForType(plan.DatabaseType),
		State:                 state.Apply.Pending,
		Options:               storage.MarshalApplyOptions(applyOpts),
		CreatedAt:             now,
		UpdatedAt:             now,
	}

	taskChanges := applyTaskChanges(plan)
	cutoverPolicy := s.config.CutoverPolicyFor(plan.Database, req.Environment)
	onFailure := s.config.OnFailure(plan.Database, req.Environment)
	members, err := s.resolveApplyMembers(ctx, plan, req.Environment, targets)
	if err != nil {
		return nil, 0, err
	}
	// Admission is per member, because the plan a member runs is the plan it was
	// paired with. A member planned against its own live schema carries DDL the
	// apply's plan does not, so the checks above clear the apply's plan and say
	// nothing about that member's — and the tasks built below come from the
	// member's. Members that run the apply's plan re-clear the same checks here,
	// which is a no-op rather than a second verdict.
	//
	// A member's direct-execution verdict is the verdict of the target that
	// runs the statement (RV-4), so its task carries it, whether or not the
	// reviewed plan has work. Only a pull request apply-confirm has disclosed
	// it: that comment names each target's direct changes under that target,
	// and the confirm re-checks each target's statements and execution modes
	// against the confirmed round before it creates the apply. Any other caller
	// was shown the reviewed plan alone, so a member's direct change is refused
	// for it.
	names := applyMemberDisplayNames(members)
	for i, member := range members {
		if err := rejectUnapplyableMemberPlan(member, names[i], plan); err != nil {
			return nil, 0, err
		}
		if !req.ConfirmedMemberWork {
			if err := rejectUnconfirmedMemberDirectExecution(member, plan); err != nil {
				return nil, 0, err
			}
		}
	}
	// An apply whose own plan has no work exists only to run the other members'
	// own plans, since its reviewed target is already at the desired schema.
	if !plan.HasWork() {
		for _, member := range members {
			if err := rejectMemberWorkAnEmptyReviewedPlanCannotCarry(member); err != nil {
				return nil, 0, err
			}
		}
	}
	groups, shardedFanout, err := buildApplyOperationGroups(plan, taskChanges, members, req.Environment, applyOpts, cutoverPolicy, onFailure, now)
	if err != nil {
		return nil, 0, err
	}
	if shardedFanout {
		s.logger.Info("createStoredApply: queueing sharded apply operation groups",
			"plan_id", plan.PlanIdentifier,
			"database", plan.Database,
			"database_type", plan.DatabaseType,
			"environment", req.Environment,
			"deployment_count", len(targets),
			"operation_group_count", len(groups))
	}

	storedApplyID, err := s.storage.Applies().CreateWithGroupedOperations(ctx, apply, groups)
	if err != nil {
		return nil, 0, fmt.Errorf("store apply and tasks: %w", err)
	}
	apply.ID = storedApplyID

	if logStore := s.storage.ApplyLogs(); logStore != nil {
		if err := logStore.Append(ctx, &storage.ApplyLog{
			ApplyID:   storedApplyID,
			Level:     storage.LogLevelInfo,
			EventType: storage.LogEventInfo,
			Source:    storage.LogSourceSchemaBot,
			Message:   fmt.Sprintf("Apply queued: %s", applyIdentifier),
			NewState:  state.Apply.Pending,
			CreatedAt: now,
		}); err != nil {
			s.logger.Warn("failed to log queued apply", "apply_id", applyIdentifier, "error", err)
		}
	}

	return apply, storedApplyID, nil
}

// MemberPlanRefusal is why apply creation refused one rollout member's own plan.
type MemberPlanRefusal int

const (
	// MemberPlanBlocked is a change the member's engine refuses to execute.
	MemberPlanBlocked MemberPlanRefusal = iota
	// MemberPlanUndisclosedUnsafe is an unsafe change the reviewed plan does
	// not carry, so the disclosure the operator confirmed never named it and no
	// opt-in covers it.
	MemberPlanUndisclosedUnsafe
)

// MemberPlanRefusedError is apply creation refusing one rollout member's own
// plan. It names the member the way an operator addresses it and the table or
// namespace the refused change is on, so a caller can tell the operator which
// target stopped the apply from fields SchemaBot controls, without presenting
// the underlying error.
type MemberPlanRefusedError struct {
	// MemberID is the member's full identifier, for logs.
	MemberID string
	// Target is the member the way the plan comment names it.
	Target  string
	Refusal MemberPlanRefusal
	// Table is the refused change's table, empty for a VSchema change.
	Table string
	// Namespace is the refused VSchema change's namespace, empty for a table
	// change.
	Namespace string
	Err       error
}

func (e *MemberPlanRefusedError) Error() string {
	return fmt.Sprintf("rollout member %s: %v", e.MemberID, e.Err)
}

func (e *MemberPlanRefusedError) Unwrap() error {
	return e.Err
}

// applyMemberDisplayNames names each member the way the plan comment does, so a
// refusal names the target the operator already read it under.
func applyMemberDisplayNames(members []applyMember) []string {
	targets := make([]routing.ExecutionTarget, len(members))
	for i, m := range members {
		targets[i] = m.Target
	}
	return routing.DisplayNames(targets)
}

// applyTargets resolves the rollout members an apply creates operations for.
//
// A narrowed apply runs on the one member its target selector names. Narrowing
// needs the configured member set to select from, so a config that cannot
// resolve the environment fails the apply rather than falling back to the
// plan's stored route, which could be a different member than the one named.
//
// A rollout-wide apply runs from the rollout primary's plan: the plan the
// other members are verified against, or that they mirror. A plan made for any
// other member (a narrowed plan, or one stranded by a deployment_order change)
// describes a schema no other member was planned against, so running it
// rollout-wide is refused.
func (s *Service) applyTargets(plan *storage.Plan, req ApplyRequest) ([]routing.ExecutionTarget, error) {
	if req.Target != "" {
		member, err := s.resolveRolloutMember(plan.Database, req.Environment, req.Target)
		if err != nil {
			return nil, err
		}
		s.logger.Info("apply narrowed to one rollout member; the rest of the rollout is not touched",
			"plan_id", plan.PlanIdentifier,
			"database", plan.Database,
			"environment", req.Environment,
			"repository", plan.Repository,
			"pull_request", plan.PullRequest,
			"selector", req.Target,
			"deployment", member.Deployment,
			"target", member.Target)
		return []routing.ExecutionTarget{member}, nil
	}

	// The plan already carries the resolved primary (deployment, target) from
	// plan time, and is authoritative for single-deployment applies and the
	// config-light trusted control-plane enqueue path. Multi-deployment fan-out
	// additionally needs the full ordered target set, which only the server
	// config knows; use it only when it defines more than one deployment so
	// single-deployment creation stays unchanged and does not depend on
	// database config being present.
	targets := []routing.ExecutionTarget{{
		DatabaseType: plan.DatabaseType,
		Deployment:   plan.Deployment,
		Target:       plan.Target,
	}}
	resolved, err := s.config.ResolveDatabaseTargets(plan.Database, req.Environment)
	if err != nil {
		s.logger.Debug("createStoredApply: using plan's stored target; config did not resolve database targets",
			"database", plan.Database, "environment", req.Environment, "error", err)
		return targets, nil
	}
	if len(resolved) <= 1 {
		return targets, nil
	}
	if !planIsForMember(plan, resolved[0]) {
		return nil, fmt.Errorf("apply for %s/%s runs the whole rollout, but plan %s was made for rollout member %s, not the rollout primary %s; apply it with target %s, or plan the whole environment",
			plan.Database, req.Environment, plan.PlanIdentifier,
			routing.ExecutionTarget{Deployment: plan.Deployment, Target: plan.Target}.MemberID(),
			resolved[0].MemberID(),
			routing.ExecutionTarget{Deployment: plan.Deployment, Target: plan.Target}.MemberID())
	}
	return resolved, nil
}

// planIsForMember reports whether a plan was made against the given member.
func planIsForMember(plan *storage.Plan, member routing.ExecutionTarget) bool {
	return plan.Deployment == member.Deployment && plan.Target == member.Target
}

// rejectUnapplyableMemberPlan runs a rollout member's own plan through the same
// admission checks the apply's plan cleared, naming the member so an operator
// reading the refusal knows which target's plan carries the change rather than
// looking for it in the plan they reviewed.
//
// Blocked changes reject before unsafe ones for the same reason they do there:
// no opt-in can make a statement the engine refuses executable.
//
// The unsafe opt-in itself needs no second check here. The apply's plan cleared
// it before the members were resolved, and a member's unsafe change passes only
// when the apply's plan carries the same change, so it clears the opt-in the
// apply's plan cleared.
func rejectUnapplyableMemberPlan(member applyMember, target string, applyPlan *storage.Plan) error {
	if err := member.Plan.BlockedApplyError(); err != nil {
		return &MemberPlanRefusedError{
			MemberID: member.MemberID(), Target: target, Refusal: MemberPlanBlocked,
			Table: member.Plan.BlockedChanges()[0].Table, Err: err,
		}
	}
	return rejectMemberUndisclosedUnsafe(member, target, applyPlan)
}

// rejectUnconfirmedMemberDirectExecution refuses a member planned on its own
// whose plan runs direct-execution DDL, for an apply whose caller was not
// shown that member's plan. A member running the apply's plan runs exactly the
// statements, and the verdicts, the caller was shown.
//
// This holds even when the reviewed plan runs the identical statement directly,
// unlike an unsafe change the reviewed plan also carries
// (rejectMemberUndisclosedUnsafe). An unsafe change's consequence is the
// statement's own, so disclosing it for one target discloses it for every
// target running it. A direct statement's consequence is its table's: it blocks
// that table's writes for as long as the statement runs, and the reviewed plan
// names the reviewed target's table with the size the planner measured there.
// Another target's copy of the table was measured on its own and can be any
// size under the bound. Only the pull request comment shows that target's
// verdict, with its own measured reason, under that target.
func rejectUnconfirmedMemberDirectExecution(member applyMember, applyPlan *storage.Plan) error {
	if member.Plan == applyPlan {
		return nil
	}
	if table := firstDirectExecutionTable(member.Plan); table != "" {
		return fmt.Errorf("rollout member %s: plan %s runs table %q as direct-execution DDL, which runs only from a pull request apply-confirm on the comment that discloses it under that target; this apply's caller was shown only the reviewed plan %s",
			member.MemberID(), member.Plan.PlanIdentifier, table, applyPlan.PlanIdentifier)
	}
	return nil
}

// firstDirectExecutionTable returns the table of the first change the plan
// routes to direct execution, across namespace-level and per-shard changes, or
// "" when it routes none.
func firstDirectExecutionTable(plan *storage.Plan) string {
	for _, change := range plan.FlatDDLChanges() {
		if change.DirectExecution() {
			return change.Table
		}
	}
	for _, shard := range plan.Shards {
		for _, change := range shard.Changes {
			if change.DirectExecution() {
				return change.Table
			}
		}
	}
	return ""
}

// rejectMemberWorkAnEmptyReviewedPlanCannotCarry refuses a member's work that
// an apply created from an empty reviewed plan cannot run as it was planned. The
// PR apply refuses the same work before it asks for confirmation; this is the
// check every caller creating such an apply passes through.
func rejectMemberWorkAnEmptyReviewedPlanCannotCarry(member applyMember) error {
	if reason := MemberWorkAConvergedReviewedPlanCannotRun(member.Plan); reason != "" {
		return fmt.Errorf("rollout member %s: plan %s %s, which an apply whose reviewed target is already at the desired schema cannot run",
			member.MemberID(), member.Plan.PlanIdentifier, reason)
	}
	return nil
}

// MemberWorkAConvergedReviewedPlanCannotRun describes the first thing in a
// member's plan that an apply created from an empty reviewed plan cannot run as
// it was planned, or returns "" when there is none. The description names only
// tables and namespaces, so it is fit for a PR comment.
//
// The apply's shape, whether a per-shard fan-out, a finalizer, or one work
// operation per member, is chosen from the apply's own plan, so an empty one
// leaves every member on one work operation per member. Per-shard changes and
// finalizer work have no place in that shape, and a member carrying only them
// would be settled as having nothing to do while its target never got the
// change. Blocked changes never run. Unsafe changes are refused whatever the
// command's flags, by the same rule that holds when the reviewed plan has work:
// a member's unsafe change runs only when the reviewed plan carries the same
// change, since the operator consents against the reviewed plan's unsafe
// disclosure, and an empty reviewed plan carries none
// (rejectMemberUndisclosedUnsafe). A direct-execution change is not refused:
// the plan comment discloses it under the member that runs it, and
// apply-confirm re-checks that each member's statements and execution modes
// are the ones confirmed.
func MemberWorkAConvergedReviewedPlanCannotRun(plan *storage.Plan) string {
	if plan.BlockedApplyError() != nil {
		return "carries changes its target's engine refuses"
	}
	if shards := changingShardsByNamespace(plan.Shards); len(shards) > 0 {
		return "carries per-shard changes"
	}
	if namespaces := plan.FinalizerNamespaces(); len(namespaces) > 0 {
		return fmt.Sprintf("finalizes namespaces %v", namespaces)
	}
	if unsafe := plan.UnsafeDDLChanges(); len(unsafe) > 0 {
		return fmt.Sprintf("carries an unsafe change for table %q", unsafe[0].Table)
	}
	if unsafe := plan.UnsafeVSchemaChanges(); len(unsafe) > 0 {
		return fmt.Sprintf("carries an unsafe VSchema change in namespace %q", unsafe[0].Namespace)
	}
	return ""
}

// rejectMemberUndisclosedUnsafe refuses a member planned on its own whose plan
// carries an unsafe change the reviewed plan does not, whatever the command's
// flags. The operator's unsafe opt-in is given against the disclosure on the
// comment it confirms, which lists the reviewed plan's unsafe changes and no
// other target's: the other targets' plans render as statements alone. So a
// member's unsafe change runs under the opt-in only when the reviewed plan
// carries the same change, which the disclosure named (RV-3). A member running
// the apply's plan runs exactly the changes that disclosure names.
//
// The refusal is a MemberPlanRefusedError naming the target, the way the plan
// comment names it, and the change's table or namespace.
func rejectMemberUndisclosedUnsafe(member applyMember, target string, applyPlan *storage.Plan) error {
	if member.Plan == applyPlan {
		return nil
	}
	change, ok := firstUndisclosedMemberUnsafeChange(applyPlan, member.Plan)
	if !ok {
		return nil
	}
	return &MemberPlanRefusedError{
		MemberID: member.MemberID(), Target: target, Refusal: MemberPlanUndisclosedUnsafe,
		Table: change.Table, Namespace: change.Namespace,
		Err: fmt.Errorf("plan %s %s, so the disclosure on reviewed plan %s never named it and no opt-in covers it",
			member.Plan.PlanIdentifier, change.description(), applyPlan.PlanIdentifier),
	}
}

// undisclosedUnsafeChange is an unsafe change in a member's own plan that the
// reviewed plan does not carry: a table change, or, when Table is empty, a
// namespace's VSchema change.
type undisclosedUnsafeChange struct {
	Table string
	// Namespace is the VSchema change's namespace, empty for a table change.
	Namespace string
	// StatementDiffers is set for a table change on a table the reviewed plan
	// also changes unsafely, with a statement that is not the one it discloses.
	StatementDiffers bool
}

func (c undisclosedUnsafeChange) description() string {
	switch {
	case c.Table != "" && c.StatementDiffers:
		return fmt.Sprintf("carries an unsafe change for table %q whose statement differs from the one the reviewed plan discloses for that table", c.Table)
	case c.Table != "":
		return fmt.Sprintf("carries an unsafe change for table %q that the reviewed plan does not carry", c.Table)
	default:
		return fmt.Sprintf("carries an unsafe VSchema change in namespace %q that the reviewed plan does not carry", c.Namespace)
	}
}

// UndisclosedMemberUnsafeChange describes the first unsafe change in a member's
// own plan that the reviewed plan does not carry, or returns "" when every
// unsafe change the member carries is one the reviewed plan's disclosure names.
// An empty reviewed plan discloses nothing, so every unsafe member change is
// undisclosed. The description names only tables and namespaces, so it is fit
// for a PR comment.
//
// A table change is the same when it touches the same namespace's table with
// the same operation and a statement sameUnsafeStatement reads as the same; a
// VSchema change, when it is the same namespace's change for the same reason.
// The whole statement is compared, not each of its clauses: a member whose
// ALTER drops the column the reviewed ALTER drops, without the column the
// reviewed ALTER also adds, runs a statement the disclosure never showed, and
// the description says the statements differ so the operator knows which.
func UndisclosedMemberUnsafeChange(reviewed, member *storage.Plan) string {
	change, ok := firstUndisclosedMemberUnsafeChange(reviewed, member)
	if !ok {
		return ""
	}
	return change.description()
}

// firstUndisclosedMemberUnsafeChange returns the first unsafe change in the
// member's own plan that the reviewed plan does not carry, and whether there
// is one. UndisclosedMemberUnsafeChange states the rule.
func firstUndisclosedMemberUnsafeChange(reviewed, member *storage.Plan) (undisclosedUnsafeChange, bool) {
	disclosed := reviewed.UnsafeDDLChanges()
	for _, change := range member.UnsafeDDLChanges() {
		if slices.ContainsFunc(disclosed, func(named storage.TableChange) bool {
			return sameUnsafeTableChange(member.DatabaseType, named, change)
		}) {
			continue
		}
		differs := slices.ContainsFunc(disclosed, func(named storage.TableChange) bool {
			return named.Namespace == change.Namespace && named.Table == change.Table
		})
		return undisclosedUnsafeChange{Table: change.Table, StatementDiffers: differs}, true
	}
	disclosedVSchema := reviewed.UnsafeVSchemaChanges()
	for _, change := range member.UnsafeVSchemaChanges() {
		if !slices.Contains(disclosedVSchema, change) {
			return undisclosedUnsafeChange{Namespace: change.Namespace}, true
		}
	}
	return undisclosedUnsafeChange{}, false
}

func sameUnsafeTableChange(databaseType string, a, b storage.TableChange) bool {
	return a.Namespace == b.Namespace && a.Table == b.Table && a.Operation == b.Operation && sameUnsafeStatement(databaseType, a.DDL, b.DDL)
}

// sameUnsafeStatement reports whether two statements for one namespace's table
// are the same change the way the comment groups targets: canonicalized by the
// dialect's parser with the schema qualifier of the relation they change
// removed (ddl.StatementParser.CanonicalizeUnqualified, the form the review-time
// rollup keys members on). Two targets that map one namespace to differently
// named physical schemas render one change with two qualifiers, and the comment
// shows them as one group, so the disclosure of one names the other. A statement
// the parser cannot read canonicalizes to itself, and a dialect without a parser
// compares byte for byte, so an unreadable statement only ever reads as
// undisclosed.
func sameUnsafeStatement(databaseType, a, b string) bool {
	if a == b {
		return true
	}
	parser, err := ddl.ParserForDialect(schema.DialectForDatabaseType(databaseType))
	if err != nil {
		return false
	}
	return parser.CanonicalizeUnqualified(a) == parser.CanonicalizeUnqualified(b)
}

func rejectUnsafeStoredPlanWithoutOptIn(plan *storage.Plan, applyOpts storage.ApplyOptions) error {
	if applyOpts.AllowUnsafe {
		return nil
	}
	if unsafeChanges := plan.UnsafeDDLChanges(); len(unsafeChanges) > 0 {
		change := unsafeChanges[0]
		return fmt.Errorf("stored plan %s contains unsafe change for table %q: %s; retry with allow_unsafe=true", plan.PlanIdentifier, change.Table, change.UnsafeOptInReason())
	}
	if vschemaChanges := plan.UnsafeVSchemaChanges(); len(vschemaChanges) > 0 {
		change := vschemaChanges[0]
		return fmt.Errorf("stored plan %s contains an unsafe VSchema change in namespace %q: %s; retry with allow_unsafe=true", plan.PlanIdentifier, change.Namespace, change.Reason)
	}
	return nil
}

// applyTaskChanges returns the per-table DDL changes that become apply tasks.
// VSchema application is no longer modeled as a synthetic task: PlanetScale
// surfaces its VSchema status/diff from engine resume metadata, and a sharded
// apply runs VSchema as a task-less group_finalizer derived from the plan.
func applyTaskChanges(plan *storage.Plan) []storage.TableChange {
	return plan.FlatDDLChanges()
}

func buildApplyOperationGroups(
	plan *storage.Plan,
	taskChanges []storage.TableChange,
	members []applyMember,
	environment string,
	applyOpts storage.ApplyOptions,
	cutoverPolicy string,
	onFailure string,
	now time.Time,
) ([]*storage.ApplyOperationWithTasks, bool, error) {
	// Fan a plan out per shard whenever it carries per-shard changes. Only an
	// instance-local sharded engine (Strata) produces those, so an
	// externally-authoritative engine (e.g. PlanetScale) — whose plans never
	// carry per-shard changes — is never fanned out, regardless of transport.
	keys := newMemberOperationKeys(members)
	shape := operationShapeOf(plan, taskChanges)
	for _, member := range members {
		if err := rejectMemberWorkOutsideShape(member, plan, shape); err != nil {
			return nil, false, err
		}
	}
	if shape == operationShapeSharded {
		groups, err := buildShardedApplyOperationGroups(plan, members, keys, environment, applyOpts, cutoverPolicy, onFailure, now)
		if err != nil {
			return nil, false, err
		}
		return groups, true, nil
	}

	// A finalizer-only plan carries no per-table work: its only change is one
	// or more namespaces' finalizers — a VSchema document to apply, or a
	// finalize the engine asked for — which are never modeled as task rows.
	// Shape it as one deployment-scoped task-less group_finalizer per target —
	// the kind whose drive finalizes every such namespace from the plan in a
	// single engine apply — so no work operation is ever created without tasks
	// to drive. One operation, not one per namespace: a branch-based engine
	// stands up one branch covering the whole deployment and validates every
	// keyspace in it, so splitting the namespaces across operations would have
	// each drive validating keyspaces whose VSchema it never applied.
	if shape == operationShapeFinalizer {
		groups := make([]*storage.ApplyOperationWithTasks, 0, len(members))
		for _, member := range members {
			operationKey, err := keys.qualify(member, finalizerOperationKeySegment)
			if err != nil {
				return nil, false, err
			}
			operation := newPendingApplyOperation(member, plan, operationKey, cutoverPolicy, onFailure, now)
			operation.OperationKind = storage.ApplyOperationKindGroupFinalizer
			if memberAlreadyConverged(member, plan) {
				// A member planned on its own that already holds the change has
				// no namespace to finalize, so a finalizer driven from its plan
				// could never run. It is recorded as the settled work it is, as
				// the other shapes record it; the reviewed plan has finalizer
				// work, so the apply keeps a drivable operation.
				operation.State = state.ApplyOperation.Completed
				operation.CompletedAt = &now
			}
			groups = append(groups, &storage.ApplyOperationWithTasks{Operation: operation})
		}
		return groups, false, nil
	}

	// Past this point the apply runs as one work operation per target with no
	// finalizer. That shape carries a VSchema change inside the work itself,
	// but it has nowhere to run a finalize the engine asked for, so refuse the
	// plan rather than complete an apply that skipped it.
	for _, member := range members {
		if namespaces := member.Plan.EngineFinalizedNamespaces(); len(namespaces) > 0 {
			return nil, false, fmt.Errorf("plan %s asks to finalize namespaces %v after their DDL, but its changes carry no per-shard plan to schedule a group finalizer behind; re-plan, and report this if it repeats",
				member.Plan.PlanIdentifier, namespaces)
		}
	}

	groups := make([]*storage.ApplyOperationWithTasks, 0, len(members))
	for _, member := range members {
		// A member planned on its own runs its own changes, not the apply plan's:
		// its target holds a schema the apply plan never described.
		memberChanges := applyTaskChanges(member.Plan)
		tasks := buildApplyTasks(member.Plan, memberChanges, environment, applyOpts, "", now)
		operationKey, err := keys.qualify(member, "")
		if err != nil {
			return nil, false, err
		}
		groups = append(groups, &storage.ApplyOperationWithTasks{
			Operation: newPendingApplyOperation(member, plan, operationKey, cutoverPolicy, onFailure, now),
			Tasks:     tasks,
		})
	}
	settleConvergedMemberOperations(groups, now)
	return groups, false, nil
}

// settleConvergedMemberOperations records the members that had nothing left to
// run as completed at creation, once the rollout is known to have work
// somewhere. A member planned on its own can already hold the reviewed change,
// so its plan has nothing left to run, and it still belongs to the rollout:
// dropping it would make the apply silently address fewer targets than the
// operator asked for, while a pending work operation with no tasks can never be
// driven and would halt the rollout. So it is recorded as the already-settled
// work it is.
//
// "Already settled" only means anything against an apply that is going
// somewhere. When no member has work, the apply has nothing to drive at all,
// and settling its operations would turn that into an apply admitted with every
// operation terminal before a driver ever saw it — holding the database lock
// with nothing left to resolve it. An apply must keep at least one drivable
// operation, so an empty one is left pending for storage to refuse.
func settleConvergedMemberOperations(groups []*storage.ApplyOperationWithTasks, now time.Time) {
	drivable := false
	for _, group := range groups {
		if len(group.Tasks) > 0 {
			drivable = true
			break
		}
	}
	if !drivable {
		return
	}
	for _, group := range groups {
		if len(group.Tasks) > 0 {
			continue
		}
		// Completed, but never started: no driver claimed it and nothing ran on
		// its target. Leaving StartedAt unset is what distinguishes it from an
		// apply that ran and finished instantly, and keeps it out of the
		// earliest-start calculation the apply's progress summary makes.
		group.Operation.State = state.ApplyOperation.Completed
		group.Operation.CompletedAt = &now
	}
}

// memberAlreadyConverged reports whether a member runs a plan of its own that
// has nothing left to do. A member running the apply's plan is never settled
// here: its work is the apply's.
func memberAlreadyConverged(member applyMember, applyPlan *storage.Plan) bool {
	return member.Plan != applyPlan && !member.Plan.HasWork()
}

// operationShape is how an apply's work is laid out as operations. One shape is
// chosen per apply, from the apply's own plan, and every member is built into
// it.
type operationShape int

const (
	// operationShapePerMember is one work operation per member carrying that
	// member's table changes, with any VSchema change riding inside the work.
	operationShapePerMember operationShape = iota
	// operationShapeSharded is one work operation per changing shard and table,
	// plus a finalizer per namespace that needs one.
	operationShapeSharded
	// operationShapeFinalizer is one task-less finalizer per member.
	operationShapeFinalizer
)

func (s operationShape) String() string {
	switch s {
	case operationShapeSharded:
		return "per-shard operations"
	case operationShapeFinalizer:
		return "a namespace finalizer only"
	default:
		return "one table-by-table work operation per target"
	}
}

// operationShapeOf is the shape a plan's work is laid out in when it is the
// apply's own plan.
func operationShapeOf(plan *storage.Plan, taskChanges []storage.TableChange) operationShape {
	switch {
	case canBuildShardedOperationGroups(plan, taskChanges):
		return operationShapeSharded
	case len(taskChanges) == 0 && len(plan.FinalizerNamespaces()) > 0:
		return operationShapeFinalizer
	default:
		return operationShapePerMember
	}
}

// rejectMemberWorkOutsideShape refuses a member whose own plan has work the
// apply's shape has no place for.
//
// A member planned against its own live schema can need a different shape than
// the reviewed plan: a target whose only change is its VSchema, one with
// per-shard changes under a reviewed plan without them, or the reverse. Built
// into the apply's shape anyway, that work gets no operation at all, or an
// operation with no tasks that is settled as done, and the member reads as
// converged while its target never got the change. So the member must need the
// same shape, and outside the per-shard shape every namespace with changing
// shards must carry the table statements its work is built from. The reviewed
// plan is held to the second rule as well, since it chose the shape without
// being checked against it. A member with no work fits every shape.
func rejectMemberWorkOutsideShape(member applyMember, applyPlan *storage.Plan, shape operationShape) error {
	if reason := memberWorkOutsideShape(member.Plan, applyPlan, shape); reason != "" {
		return fmt.Errorf("rollout member %s: plan %s %s, so the apply was not created rather than mark the target done without its change",
			member.MemberID(), member.Plan.PlanIdentifier, reason)
	}
	return nil
}

// memberWorkOutsideShape describes the work in memberPlan that an apply of
// shape, chosen from applyPlan, has no place for, or returns "" when it fits.
func memberWorkOutsideShape(memberPlan, applyPlan *storage.Plan, shape operationShape) string {
	if !memberPlan.HasWork() {
		return ""
	}
	memberChanges := applyTaskChanges(memberPlan)
	if memberPlan != applyPlan {
		if needs := operationShapeOf(memberPlan, memberChanges); needs != shape {
			return fmt.Sprintf("needs %s, but this apply runs %s, chosen from the reviewed plan", needs, shape)
		}
	}
	if shape == operationShapeSharded {
		return ""
	}
	if namespaces := shardWorkWithoutTableChanges(memberPlan, memberChanges); len(namespaces) > 0 {
		return fmt.Sprintf("has per-shard changes in namespaces %v with no table statements to run them from, and this apply runs %s", namespaces, shape)
	}
	return ""
}

// MemberWorkTheReviewedPlanCannotRun describes the first thing in a member's
// own plan that apply creation refuses when the apply is created from reviewed,
// or returns "" when there is none: a blocked change, an unsafe change the
// reviewed plan's disclosure does not name, or work the apply's shape has no
// place for. It asks what createStoredApply asks of each member, so a caller
// can refuse before it pins a confirmation that apply creation would refuse. A
// member's direct-execution change is not among them: the comment discloses it
// under the target that runs it. The description names only tables,
// namespaces, and the apply's shape, so it is fit for a PR comment.
func MemberWorkTheReviewedPlanCannotRun(reviewed, member *storage.Plan) string {
	if member.BlockedApplyError() != nil {
		return "carries changes its target's engine refuses"
	}
	if reason := UndisclosedMemberUnsafeChange(reviewed, member); reason != "" {
		return reason
	}
	return memberWorkOutsideShape(member, reviewed, operationShapeOf(reviewed, applyTaskChanges(reviewed)))
}

// shardWorkWithoutTableChanges returns, in sorted order, the namespaces whose
// shards have changes of their own while the plan carries no table change for
// the namespace. Outside the per-shard shape a namespace's work is built from
// its table changes alone, so those shards' changes would have no operation.
func shardWorkWithoutTableChanges(plan *storage.Plan, taskChanges []storage.TableChange) []string {
	withTableChanges := make(map[string]bool, len(taskChanges))
	for _, change := range taskChanges {
		withTableChanges[change.Namespace] = true
	}
	var namespaces []string
	for namespace := range changingShardsByNamespace(plan.Shards) {
		if !withTableChanges[namespace] {
			namespaces = append(namespaces, namespace)
		}
	}
	sort.Strings(namespaces)
	return namespaces
}

// buildNamespaceFinalizerOperations builds one task-less group_finalizer per
// namespace in the plan that needs one — its VSchema changes, or the engine
// asked to finalize it — for one target. The finalizer runs once the
// namespace's shard work (if any) completes; it is driven from the plan
// (reconstructed by namespace at drive time), not from a synthetic task. A
// namespace with no shard work still gets a finalizer so its VSchema change or
// requested finalize is never dropped.
func buildNamespaceFinalizerOperations(
	applyPlan *storage.Plan,
	member applyMember,
	keys memberOperationKeys,
	cutoverPolicy string,
	onFailure string,
	now time.Time,
) ([]*storage.ApplyOperationWithTasks, error) {
	namespaces := member.Plan.FinalizerNamespaces()
	groups := make([]*storage.ApplyOperationWithTasks, 0, len(namespaces))
	for _, namespace := range namespaces {
		if err := validateOperationKeyPart("namespace", namespace); err != nil {
			return nil, err
		}
		operationKey, err := keys.qualify(member, finalizerOperationKey(namespace))
		if err != nil {
			return nil, err
		}
		if len(operationKey) > applyOperationKeyMaxLen {
			return nil, fmt.Errorf("operation key for namespace %q finalizer exceeds %d characters", namespace, applyOperationKeyMaxLen)
		}
		operation := newPendingApplyOperation(member, applyPlan, operationKey, cutoverPolicy, onFailure, now)
		operation.OperationKind = storage.ApplyOperationKindGroupFinalizer
		groups = append(groups, &storage.ApplyOperationWithTasks{
			Operation: operation,
		})
	}
	return groups, nil
}

func canBuildShardedOperationGroups(plan *storage.Plan, taskChanges []storage.TableChange) bool {
	if plan == nil || len(plan.Shards) == 0 || len(taskChanges) == 0 {
		return false
	}
	shardsByNamespace := changingShardsByNamespace(plan.Shards)
	if len(shardsByNamespace) == 0 {
		return false
	}
	hasWorkChange := false
	for _, ddlChange := range taskChanges {
		if ddlChange.DDL == "" || ddlChange.Table == "" {
			return false
		}
		if len(shardsByNamespace[ddlChange.Namespace]) == 0 {
			return false
		}
		hasWorkChange = true
	}
	return hasWorkChange
}

func buildShardedApplyOperationGroups(
	applyPlan *storage.Plan,
	members []applyMember,
	keys memberOperationKeys,
	environment string,
	applyOpts storage.ApplyOptions,
	cutoverPolicy string,
	onFailure string,
	now time.Time,
) ([]*storage.ApplyOperationWithTasks, error) {
	groups := make([]*storage.ApplyOperationWithTasks, 0, len(members)*(len(applyPlan.Shards)+1))
	// One group per (member, operation key). A member is identified by its
	// deployment and target together, so two targets of one deployment get their
	// own groups instead of one member's shard work being folded into the other's.
	groupsByMemberAndKey := make(map[string]*storage.ApplyOperationWithTasks)
	for _, member := range members {
		if memberAlreadyConverged(member, applyPlan) {
			// A member planned on its own that already holds the change has no
			// changing shard and no namespace to finalize, so it would get no
			// operation at all and the apply would address fewer targets than
			// the rollout has. It is recorded as the settled work it is, as the
			// other shapes record it; the reviewed plan has per-shard work, so the
			// apply keeps a drivable operation.
			operationKey, err := keys.qualify(member, "")
			if err != nil {
				return nil, err
			}
			operation := newPendingApplyOperation(member, applyPlan, operationKey, cutoverPolicy, onFailure, now)
			operation.State = state.ApplyOperation.Completed
			operation.CompletedAt = &now
			groups = append(groups, &storage.ApplyOperationWithTasks{Operation: operation})
			continue
		}
		// A member planned on its own carries its own shards and changes; a member
		// of a mirrored environment carries the apply's plan, so this is the same
		// shard set for every member there.
		shardsByNamespace := changingShardsByNamespace(member.Plan.Shards)
		namespaces := make([]string, 0, len(shardsByNamespace))
		for namespace := range shardsByNamespace {
			namespaces = append(namespaces, namespace)
		}
		sort.Strings(namespaces)

		for _, namespace := range namespaces {
			for _, shard := range shardsByNamespace[namespace] {
				// Each shard is driven from its own changes; it is in
				// shardsByNamespace only because those changes are non-empty.
				for _, ddlChange := range shard.Changes {
					// Fail closed on a malformed per-shard change rather than building
					// an operation key or engine request from empty/mismatched fields.
					if strings.TrimSpace(ddlChange.Table) == "" || strings.TrimSpace(ddlChange.DDL) == "" || strings.TrimSpace(ddlChange.Operation) == "" {
						return nil, fmt.Errorf("namespace %q shard %q has a change with an empty table, DDL, or operation", namespace, shard.Shard)
					}
					if ddlChange.Namespace != "" && ddlChange.Namespace != namespace {
						return nil, fmt.Errorf("namespace %q shard %q change for table %q has mismatched namespace %q", namespace, shard.Shard, ddlChange.Table, ddlChange.Namespace)
					}
					if err := validateShardOperationKeyParts(namespace, shard.Shard, ddlChange.Table); err != nil {
						return nil, err
					}
					operationKey, err := keys.qualify(member, storage.ShardOperationKey(namespace, shard.Shard, ddlChange.Table))
					if err != nil {
						return nil, err
					}
					if len(operationKey) > applyOperationKeyMaxLen {
						return nil, fmt.Errorf("operation key for namespace %q shard %q table %q exceeds %d characters", namespace, shard.Shard, ddlChange.Table, applyOperationKeyMaxLen)
					}
					groupKey := member.MemberID() + "\x00" + operationKey
					group := groupsByMemberAndKey[groupKey]
					if group == nil {
						group = &storage.ApplyOperationWithTasks{
							Operation: newPendingApplyOperation(member, applyPlan, operationKey, cutoverPolicy, onFailure, now),
						}
						groupsByMemberAndKey[groupKey] = group
						groups = append(groups, group)
					}
					group.Tasks = append(group.Tasks, buildApplyTask(member.Plan, ddlChange, environment, applyOpts, shard.Shard, now))
				}
			}
		}
		finalizers, err := buildNamespaceFinalizerOperations(applyPlan, member, keys, cutoverPolicy, onFailure, now)
		if err != nil {
			return nil, err
		}
		groups = append(groups, finalizers...)
	}
	return groups, nil
}

// memberOperationKeys decides how one apply's operation keys are qualified.
//
// An operation is unique on (apply, deployment, operation key), so a deployment
// addressing several targets needs the target in the key or its members collide
// on one row. A deployment addressing one target does not: its key is already
// unique, and naming the target would change the shape of every key every
// existing reader parses, for no gain. So the target leads the key exactly where
// the deployment stops identifying the member — the same rule that decides
// whether an operator sees a member named "eu" or "eu/shop-002".
//
// Keeping both shapes live is what makes widening the rule later a change to
// this one predicate: readers already recover the scoped key by matching the
// operation's own target rather than by counting components.
type memberOperationKeys struct {
	multiTargetDeployments map[string]bool
}

func newMemberOperationKeys(members []applyMember) memberOperationKeys {
	targets := make([]routing.ExecutionTarget, len(members))
	for i, member := range members {
		targets[i] = member.Target
	}
	return memberOperationKeys{multiTargetDeployments: routing.MultiTargetDeployments(targets)}
}

// qualify returns the operation key for one member's scoped work. scopedKey is
// the key within the member — a shard key, a finalizer key, or empty for work
// covering the whole target.
func (k memberOperationKeys) qualify(member applyMember, scopedKey string) (string, error) {
	if !k.multiTargetDeployments[member.Target.Deployment] {
		return scopedKey, nil
	}
	// The config that admits a target refuses the delimiter in its name, but a
	// stored plan can carry a target from a config that no longer applies. A key
	// that cannot be split back into the target it came from is not recoverable
	// once written, so it is refused here too.
	if err := validateOperationKeyPart("target", member.Target.Target); err != nil {
		return "", err
	}
	return storage.TargetOperationKey(member.Target.Target, scopedKey), nil
}

func validateShardOperationKeyParts(namespace, shard, table string) error {
	for _, part := range []struct {
		label string
		value string
	}{
		{label: "namespace", value: namespace},
		{label: "shard", value: shard},
		{label: "table", value: table},
	} {
		if err := validateOperationKeyPart(part.label, part.value); err != nil {
			return err
		}
	}
	return nil
}

func validateOperationKeyPart(label, value string) error {
	if strings.Contains(value, storage.OperationKeyDelimiter) {
		return fmt.Errorf("operation key %s component %q contains reserved delimiter %q", label, value, storage.OperationKeyDelimiter)
	}
	return nil
}

func changingShardsByNamespace(shards []storage.ShardPlan) map[string][]storage.ShardPlan {
	shardsByNamespace := make(map[string][]storage.ShardPlan)
	for _, shard := range shards {
		// A shard is changing iff it carries its own changes.
		if len(shard.Changes) == 0 {
			continue
		}
		shardsByNamespace[shard.Namespace] = append(shardsByNamespace[shard.Namespace], shard)
	}
	for namespace := range shardsByNamespace {
		sort.Slice(shardsByNamespace[namespace], func(i, j int) bool {
			return shardsByNamespace[namespace][i].Shard < shardsByNamespace[namespace][j].Shard
		})
	}
	return shardsByNamespace
}

func finalizerOperationKey(namespace string) string {
	return namespace + storage.OperationKeyDelimiter + finalizerOperationKeySegment
}

// newPendingApplyOperation builds one member's pending operation.
//
// applyPlan is the plan the apply itself was created from. PlanID is stamped
// only when the member runs a different plan, so an operation names a plan of
// its own exactly when it was planned on its own — every other operation runs
// its apply's plan, which is what an unset PlanID already means.
func newPendingApplyOperation(member applyMember, applyPlan *storage.Plan, operationKey, cutoverPolicy, onFailure string, now time.Time) *storage.ApplyOperation {
	op := &storage.ApplyOperation{
		Deployment:    member.Target.Deployment,
		OperationKey:  operationKey,
		OperationKind: storage.ApplyOperationKindWork,
		Target:        member.Target.Target,
		State:         state.ApplyOperation.Pending,
		CutoverPolicy: cutoverPolicy,
		OnFailure:     onFailure,
		CreatedAt:     now,
		UpdatedAt:     now,
	}
	if member.Plan != nil && applyPlan != nil && member.Plan.ID != applyPlan.ID {
		op.PlanID = member.Plan.ID
	}
	return op
}

func buildApplyTasks(
	plan *storage.Plan,
	taskChanges []storage.TableChange,
	environment string,
	applyOpts storage.ApplyOptions,
	shard string,
	now time.Time,
) []*storage.Task {
	tasks := make([]*storage.Task, 0, len(taskChanges))
	for _, ddlChange := range taskChanges {
		tasks = append(tasks, buildApplyTask(plan, ddlChange, environment, applyOpts, shard, now))
	}
	return tasks
}

func buildApplyTask(
	plan *storage.Plan,
	ddlChange storage.TableChange,
	environment string,
	applyOpts storage.ApplyOptions,
	shard string,
	now time.Time,
) *storage.Task {
	return &storage.Task{
		TaskIdentifier: engine.NewTaskID(),
		PlanID:         plan.ID,
		Database:       plan.Database,
		DatabaseType:   plan.DatabaseType,
		Engine:         storage.EngineForType(plan.DatabaseType),
		Repository:     plan.Repository,
		PullRequest:    plan.PullRequest,
		Environment:    environment,
		State:          state.Task.Pending,
		Options:        storage.MarshalApplyOptions(applyOpts),
		Namespace:      ddlChange.Namespace,
		TableName:      ddlChange.Table,
		Shard:          shard,
		DDL:            ddlChange.DDL,
		DDLAction:      ddlChange.Operation,
		ExecutionMode:  ddlChange.ExecutionMode,
		ModeReason:     ddlChange.ModeReason,
		EstimatedBytes: ddlChange.TaskEstimatedBytes(shard),
		CreatedAt:      now,
		UpdatedAt:      now,
	}
}

// ExecuteRollbackPlanForApply generates a rollback plan for a specific apply.
func (s *Service) ExecuteRollbackPlanForApply(ctx context.Context, apply *storage.Apply) (*apitypes.PlanResponse, error) {
	// Every caller resolves the apply through the rollback source guardrails
	// first, so a nil apply is a programmer invariant violation: an internal
	// failure that retrying the same request cannot repair.
	if apply == nil {
		return nil, terminalControlf("apply is required")
	}
	if !state.IsState(apply.State, state.Apply.Completed) {
		return nil, controlConflictf("apply %s is in state %q; only completed applies can be rolled back", apply.ApplyIdentifier, apply.State)
	}
	// A rollback plan is applied to the whole rollout, and a narrowed apply
	// changed one member. Reverting it everywhere would run the reversal on
	// members that never received the change, so it is refused until rollback
	// can be narrowed the same way.
	if member := apply.GetOptions().NarrowedTo; member != "" {
		s.logger.Warn("rollback refused for an apply narrowed to one rollout member",
			append(apply.LogAttrs(), "narrowed_to", member)...)
		return nil, controlConflictf("apply %s ran on rollout member %s only; rollback of a narrowed apply is not supported, so restore that target by planning and applying the previous schema with target %s",
			apply.ApplyIdentifier, member, member)
	}

	plan, err := s.storage.Plans().GetByID(ctx, apply.PlanID)
	if err != nil {
		return nil, fmt.Errorf("get rollback source plan: %w", err)
	}
	if plan == nil {
		return nil, terminalControlf("plan not found for apply %s", apply.ApplyIdentifier)
	}
	if !rollbackSourcePlanMatchesApply(plan, apply) {
		return nil, terminalControlf("source plan %s belongs to %s/%s/%s, not apply %s for %s/%s/%s",
			plan.PlanIdentifier, plan.Database, plan.DatabaseType, plan.Environment,
			apply.ApplyIdentifier, apply.Database, apply.DatabaseType, apply.Environment)
	}
	schemaFiles, err := rollbackSchemaFiles(plan)
	if err != nil {
		return nil, terminalControlf("rollback source plan is invalid: %w", err)
	}

	deployment, err := storedDeploymentForApply(apply)
	if err != nil {
		return nil, terminalControlf("rollback source apply is invalid: %w", err)
	}
	if plan.Target == "" {
		return nil, terminalControlf("plan %s is missing server-side routing metadata field %q; create a new plan and retry rollback", plan.PlanIdentifier, "target")
	}
	// Client resolution is a config lookup: a deployment that cannot resolve a
	// Tern client resolves the same way on every attempt, so the failure is
	// terminal until an operator fixes the routing configuration.
	client, err := s.TernClient(deployment, apply.Environment)
	if err != nil {
		return nil, terminalControlf("database %q (%s): %w", apply.Database, apply.Environment, err)
	}

	prNumber := int32(apply.PullRequest)
	req := PlanRequest{
		Database:    apply.Database,
		Environment: apply.Environment,
		Type:        apply.DatabaseType,
		SchemaFiles: schemaFiles,
		Repository:  apply.Repository,
		PullRequest: &prNumber,
		// The rollback restores the schema the source plan captured, and that
		// capture never included the tables the plan's ignore_tables withheld.
		// Withholding them again is what keeps the rollback from proposing to
		// drop tables the repository asked SchemaBot to leave alone, and
		// carrying them on the request is what puts them on the rollback's own
		// stored plan, so a re-plan of *that* plan withholds them too.
		IgnoreTables: plan.IgnoreTables(),
	}
	resp, err := client.Plan(ctx, &ternv1.PlanRequest{
		Database:     req.Database,
		Type:         req.Type,
		SchemaFiles:  req.SchemaFiles,
		Repository:   req.Repository,
		PullRequest:  prNumber,
		Environment:  req.Environment,
		Target:       plan.Target,
		IgnoreTables: req.IgnoreTables,
		// A rollback's own statements are planned under the policy the apply
		// it reverses was admitted under, read off that apply rather than
		// resolved again from configuration. The two can differ: a rollback
		// runs after the forward apply, and configuration changes in between.
		// Re-resolving would let a grant withdrawn since dispatch refuse the
		// statement that undoes a change it allowed, leaving the schema on
		// the target the operator is trying to walk back.
		DirectExecution: tern.DirectExecutionPolicyProto(apply.GetOptions().DirectExecution),
	})
	if err != nil {
		// Mirror ExecutePlanProto's transport classification: only remote
		// unavailability is worth retrying, because the same rollback plan
		// request is safe to re-send. Every other failure is the engine's
		// deterministic answer for this stored plan, so it is terminal for
		// durable command processing.
		if client.IsRemote() && grpcstatus.Code(err) == grpccodes.Unavailable {
			return nil, &RemoteDeploymentUnavailableError{
				Deployment: deployment,
				Target:     plan.Target,
				Err:        err,
			}
		}
		return nil, terminalControlf("rollback plan for database %q (%s): %w%s",
			apply.Database, apply.Environment, err, recordedIgnoreTablesNote(req.IgnoreTables))
	}
	if err := s.refuseDropsOfWithheldTables(req, resp, deployment); err != nil {
		return nil, terminalControlf("rollback plan for database %q (%s): %w", apply.Database, apply.Environment, err)
	}
	s.normalizeExecutionVerdicts(resp, apply.Database, deployment)
	route := storedPlanRoute{
		DatabaseType: apply.DatabaseType,
		Deployment:   deployment,
		Target:       plan.Target,
		// The rollback's plan records the same policy its request stated, so
		// the apply admitted from it runs the reversal under the policy the
		// forward change was admitted with rather than under one resolved
		// again from a configuration that has moved on since. A source apply
		// that recorded nothing carries nothing forward: its own dispatch
		// deferred to the configuration, and the reversal does the same.
		DirectExecution: apply.GetOptions().DirectExecution,
	}
	if err := s.storePlanResponse(ctx, req, resp, route); err != nil {
		return nil, err
	}

	return planResponseFromProto(resp), nil
}

func rollbackSourcePlanMatchesApply(plan *storage.Plan, apply *storage.Apply) bool {
	return plan.Database == apply.Database &&
		plan.DatabaseType == apply.DatabaseType &&
		plan.Environment == apply.Environment
}

// recordedIgnoreTablesNote qualifies a failed rollback re-plan with where its
// exclusions came from. The rollback restores a snapshot the entries were never
// checked against, so an engine can refuse a contradiction the reviewed plan
// never had — and its remedy, removing the entry, reads as a config edit. The
// entries are the source plan's frozen record, so that edit changes nothing and
// the operator's way out is a pull request that restores the schema. Empty when
// the plan recorded none, which is every rollback of a plan that withheld
// nothing.
func recordedIgnoreTablesNote(entries []string) string {
	if len(entries) == 0 {
		return ""
	}
	return ". This rollback uses the plan's recorded ignore_tables, not schemabot.yaml"
}

func rollbackSchemaFiles(plan *storage.Plan) (map[string]*ternv1.SchemaFiles, error) {
	schemaFiles := make(map[string]*ternv1.SchemaFiles)
	for ns, nsData := range plan.Namespaces {
		if nsData == nil || !nsData.OriginalFilesCaptured {
			return nil, fmt.Errorf("no original schema files available for rollback namespace %q", ns)
		}
		files := make(map[string]string, len(nsData.OriginalFiles))
		maps.Copy(files, nsData.OriginalFiles)
		schemaFiles[ns] = &ternv1.SchemaFiles{Files: files}
	}
	if len(schemaFiles) == 0 {
		return nil, fmt.Errorf("no namespaces available for rollback")
	}
	return schemaFiles, nil
}

// validateSchemaFiles checks that schema_files has at least one namespace and
// that every namespace carries a non-null value. An empty Files map within a
// namespace is valid (signals "drop all tables"), so we only reject when
// schema_files itself is missing or a namespace value is null.
//
// A null namespace value (e.g. JSON `{"default": null}`) is rejected as a hard
// error: it cannot be converted to schema files and is almost always a
// malformed request.
//
// Returns a warning message if any namespace has an empty (but non-null) files
// map (could indicate a JSON field name bug like "sql_files" instead of
// "files"). Callers should log this but not reject the request.
func validateSchemaFiles(schemaFiles map[string]*ternv1.SchemaFiles) (warning string, err error) {
	if len(schemaFiles) == 0 {
		return "", fmt.Errorf("schema_files is required: must contain at least one namespace (JSON field for files is \"files\", not \"sql_files\")")
	}
	for ns, sf := range schemaFiles {
		if sf == nil {
			return "", fmt.Errorf("schema_files[%q] is null: each namespace must be an object with a \"files\" map", ns)
		}
		if len(sf.GetFiles()) == 0 {
			warning = fmt.Sprintf("schema_files[%q] has no files — if this is unintentional, check that the JSON field is \"files\" (not \"sql_files\")", ns)
		}
	}
	return warning, nil
}
