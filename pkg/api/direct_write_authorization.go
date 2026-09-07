package api

import (
	"fmt"
	"net/http"
	"slices"
	"sort"
	"strings"

	"github.com/block/schemabot/pkg/auth"
	"github.com/block/schemabot/pkg/metrics"
)

// Direct-write authorization decides whether a CLI/direct-API caller may run a
// mutating operation, once the handler has resolved the target database. It is
// the second half of a decision the forward-auth middleware starts: the
// middleware admits any write-capable caller (deployment write groups plus
// every database's operator groups) before the request body is parsed, and the
// handler enforces the per-database and per-environment scope here. Both
// halves consume the same forward-auth identity recorded in the request
// context; this layer never re-reads headers.
//
// Reasons are stable for metrics and mirror the PR-door
// ActorAuthorizationResult vocabulary.
const (
	DirectWriteReasonAdminAllow            = "admin_allow"
	DirectWriteReasonScopedAllow           = "scoped_allow"
	DirectWriteReasonScopedLaneDisabled    = "scoped_lane_disabled"
	DirectWriteReasonTargetUnresolved      = "target_unresolved"
	DirectWriteReasonMissingIdentity       = "missing_identity"
	DirectWriteReasonNotAdmin              = "not_admin"
	DirectWriteReasonNotDatabaseOperator   = "not_database_operator"
	DirectWriteReasonEnvironmentNotAllowed = "environment_not_allowed"
	DirectWriteReasonMissingDatabaseConfig = "missing_database_config"
)

// DirectWriteAuthorizationResult describes the decision for a direct API/CLI
// caller running a mutating operation. Reason is stable for metrics;
// MatchedPrincipal names the group that granted access on allow paths.
type DirectWriteAuthorizationResult struct {
	Allowed          bool
	Reason           string
	MatchedPrincipal string
}

// scopedWriteEnabled reports whether any database opts into scoped direct
// write. When false, the middleware write gate (write groups only) is the
// whole decision and the handler-level check is a pass-through, preserving
// the semantics of deployments that never configure operator groups.
func (c *ServerConfig) scopedWriteEnabled() bool {
	for _, dbConfig := range c.Databases {
		if len(trimmedNonEmpty(dbConfig.OperatorGroups)) > 0 {
			return true
		}
	}
	return false
}

// AuthorizeDirectWrite decides whether the caller may run a mutating direct
// API/CLI operation against the given database and environment. Admin (write
// group) membership grants every database; operator-group membership grants
// only the caller's configured databases, in the instance-wide operator
// environments (auth.forward_auth.operator_environments). Uncertainty never
// widens access: an unconfigured database or an out-of-scope environment
// denies scoped callers.
func (c *ServerConfig) AuthorizeDirectWrite(user *auth.User, database, environment string) DirectWriteAuthorizationResult {
	result, decided := c.directWriteCommonDecision(user)
	if decided {
		return result
	}

	dbConfig, ok := c.Databases[database]
	if !ok {
		return DirectWriteAuthorizationResult{Allowed: false, Reason: DirectWriteReasonMissingDatabaseConfig}
	}
	operatorGroups := trimmedNonEmpty(dbConfig.OperatorGroups)
	matched, isOperator := auth.MatchedGroup(user.Groups, operatorGroups)
	if !isOperator {
		return DirectWriteAuthorizationResult{Allowed: false, Reason: DirectWriteReasonNotDatabaseOperator}
	}
	if !slices.Contains(trimmedNonEmpty(c.Auth.ForwardAuth.OperatorEnvironments), environment) {
		return DirectWriteAuthorizationResult{Allowed: false, Reason: DirectWriteReasonEnvironmentNotAllowed}
	}
	return DirectWriteAuthorizationResult{Allowed: true, Reason: DirectWriteReasonScopedAllow, MatchedPrincipal: matched}
}

// AuthorizeDirectDatabaseWrite is AuthorizeDirectWrite without the environment
// gate, for operations that target a database but have no environment
// dimension. Database locks are the case in point: a named lock is an
// operator control on the database itself (holding applies off), keyed by
// database and type only, so the owning team's grant applies as a whole.
func (c *ServerConfig) AuthorizeDirectDatabaseWrite(user *auth.User, database string) DirectWriteAuthorizationResult {
	result, decided := c.directWriteCommonDecision(user)
	if decided {
		return result
	}

	dbConfig, ok := c.Databases[database]
	if !ok {
		return DirectWriteAuthorizationResult{Allowed: false, Reason: DirectWriteReasonMissingDatabaseConfig}
	}
	matched, isOperator := auth.MatchedGroup(user.Groups, trimmedNonEmpty(dbConfig.OperatorGroups))
	if !isOperator {
		return DirectWriteAuthorizationResult{Allowed: false, Reason: DirectWriteReasonNotDatabaseOperator}
	}
	return DirectWriteAuthorizationResult{Allowed: true, Reason: DirectWriteReasonScopedAllow, MatchedPrincipal: matched}
}

// AuthorizeDirectAdminWrite decides whether the caller may run a mutating
// operation that has no single target database (deployment settings, stored
// check state, webhook redrive). Such operations stay admin-only: without a
// database to scope to, a scoped grant cannot apply.
func (c *ServerConfig) AuthorizeDirectAdminWrite(user *auth.User) DirectWriteAuthorizationResult {
	result, decided := c.directWriteCommonDecision(user)
	if decided {
		return result
	}
	return DirectWriteAuthorizationResult{Allowed: false, Reason: DirectWriteReasonNotAdmin}
}

// directWriteCommonDecision resolves the outcomes shared by every direct-write
// check: the scoped lane being unconfigured (pass-through — the middleware
// write gate was the whole decision), a missing identity (fail closed), and
// admin membership (global allow). decided=false means the caller is a
// write-capable non-admin and the per-endpoint scope check must run.
func (c *ServerConfig) directWriteCommonDecision(user *auth.User) (DirectWriteAuthorizationResult, bool) {
	if !c.scopedWriteEnabled() {
		return DirectWriteAuthorizationResult{Allowed: true, Reason: DirectWriteReasonScopedLaneDisabled}, true
	}
	if user == nil {
		return DirectWriteAuthorizationResult{Allowed: false, Reason: DirectWriteReasonMissingIdentity}, true
	}
	writeGroups := c.Auth.ForwardAuth.WriteGroups
	if matched, ok := auth.MatchedGroup(user.Groups, writeGroups); ok {
		return DirectWriteAuthorizationResult{Allowed: true, Reason: DirectWriteReasonAdminAllow, MatchedPrincipal: matched}, true
	}
	return DirectWriteAuthorizationResult{}, false
}

// validateOperatorScoping rejects a half-configured or unusable scoped
// direct-write configuration at startup. Per-database grants
// (operator_groups) and the instance-wide environment policy
// (auth.forward_auth.operator_environments) must be set together: either
// half alone is inert — it fails closed — but the operator who wrote it
// believes it works, so it is a configuration error, not a warning. The lane
// also requires the forward_auth authenticator: under any other auth type
// the middleware never admits a non-admin caller, so a grant could never
// take effect.
func (c *ServerConfig) validateOperatorScoping() error {
	environments := trimmedNonEmpty(c.Auth.ForwardAuth.OperatorEnvironments)

	var grantedDatabases []string
	for name, dbConfig := range c.Databases {
		groups := trimmedNonEmpty(dbConfig.OperatorGroups)
		if len(groups) == 0 {
			continue
		}
		if err := validateUniqueNames("databases."+name+".operator_groups", groups); err != nil {
			return err
		}
		grantedDatabases = append(grantedDatabases, name)
	}
	sort.Strings(grantedDatabases)

	if len(grantedDatabases) == 0 && len(environments) == 0 {
		return nil
	}
	if len(grantedDatabases) == 0 {
		return fmt.Errorf("auth.forward_auth.operator_environments requires at least one databases.*.operator_groups grant")
	}
	if len(environments) == 0 {
		return fmt.Errorf("databases.%s.operator_groups requires at least one auth.forward_auth.operator_environments entry", grantedDatabases[0])
	}
	if c.Auth.Type != "forward_auth" {
		return fmt.Errorf("databases.%s.operator_groups requires auth type forward_auth (got %q): no other authenticator forwards the caller groups the grant matches against", grantedDatabases[0], c.Auth.Type)
	}
	if err := validateUniqueNames("auth.forward_auth.operator_environments", environments); err != nil {
		return err
	}
	for _, env := range environments {
		if !c.environmentConfiguredOnAnyDatabase(env) {
			return fmt.Errorf("auth.forward_auth.operator_environments entry %q is not a configured environment on any database", env)
		}
	}
	for _, name := range grantedDatabases {
		if !c.databaseHasAnyEnvironment(name, environments) {
			return fmt.Errorf("databases.%s.operator_groups grant can never authorize an operation: none of the database's environments appear in auth.forward_auth.operator_environments (%s)",
				name, strings.Join(environments, ", "))
		}
	}
	return nil
}

// databaseHasAnyEnvironment reports whether the database defines at least one
// of the given environments. A scoped grant on a database whose environments
// are all outside the operator-environment policy can never authorize an
// operation, so it is a configuration error, not a silent no-op.
func (c *ServerConfig) databaseHasAnyEnvironment(database string, environments []string) bool {
	dbConfig, ok := c.Databases[database]
	if !ok {
		return false
	}
	for _, env := range environments {
		if _, ok := dbConfig.Environments[env]; ok {
			return true
		}
	}
	return false
}

// environmentConfiguredOnAnyDatabase reports whether any configured database
// defines the environment. Used as a typo guard on instance-wide environment
// lists: an environment nobody configures can never authorize anything.
func (c *ServerConfig) environmentConfiguredOnAnyDatabase(env string) bool {
	for _, dbConfig := range c.Databases {
		if _, ok := dbConfig.Environments[env]; ok {
			return true
		}
	}
	return false
}

// OperatorGroupUnion returns the sorted, deduplicated union of every
// database's OperatorGroups. The forward-auth middleware widens write-tier
// admission to this union; the per-database half of the decision runs in the
// handler once the target database is known.
func (c *ServerConfig) OperatorGroupUnion() []string {
	seen := make(map[string]struct{})
	var union []string
	for _, dbConfig := range c.Databases {
		for _, g := range trimmedNonEmpty(dbConfig.OperatorGroups) {
			if _, ok := seen[g]; ok {
				continue
			}
			seen[g] = struct{}{}
			union = append(union, g)
		}
	}
	sort.Strings(union)
	return union
}

// metricDatabaseAttribute bounds the database metric attribute to configured
// database names. The value can arrive straight from a request body (plan,
// locks), so an unrecognized name is collapsed to a sentinel — the counter
// stays low-cardinality no matter what callers send, and the sentinel itself
// is the signal (requests naming unconfigured databases). The real name is in
// the denial log.
func (c *ServerConfig) metricDatabaseAttribute(database string) string {
	if database == "" {
		return ""
	}
	if _, ok := c.Databases[database]; ok {
		return database
	}
	return "unconfigured"
}

// metricEnvironmentAttribute bounds the environment metric attribute to
// environments configured on at least one database, for the same reason as
// metricDatabaseAttribute.
func (c *ServerConfig) metricEnvironmentAttribute(environment string) string {
	if environment == "" {
		return ""
	}
	if c.environmentConfiguredOnAnyDatabase(environment) {
		return environment
	}
	return "unconfigured"
}

// trimmedNonEmpty returns the values with surrounding whitespace removed and
// empty entries dropped.
func trimmedNonEmpty(values []string) []string {
	trimmed := make([]string, 0, len(values))
	for _, v := range values {
		if v = strings.TrimSpace(v); v != "" {
			trimmed = append(trimmed, v)
		}
	}
	return trimmed
}

// authorizeDirectWrite enforces the per-database, per-environment half of the
// direct-write decision for an HTTP handler and reports whether the request
// may proceed. On denial it records the decision, logs the triage
// identifiers, and writes the 403 naming the principals that would grant
// access, so a blocked caller knows who to ask instead of guessing.
func (s *Service) authorizeDirectWrite(w http.ResponseWriter, r *http.Request, operation, database, environment string) bool {
	result := s.config.AuthorizeDirectWrite(auth.UserFromContext(r.Context()), database, environment)
	return s.finishDirectWriteDecision(w, r, operation, database, environment, result)
}

// authorizeDirectWriteForStoredPlan resolves the target database from the
// stored plan — the source of truth for what an apply will mutate — and then
// enforces the per-database half of the direct-write decision. The plan must
// exist and resolve at decision time: a missing plan rejects the request with
// the same error the apply path reports for it, and a plan-load storage failure
// rejects it with the operation's 500 error. Neither is an authorization
// denial; both land on the decision metric as skipped/target_unresolved, and
// neither lets the request proceed — an unresolvable target must never
// authorize. Failing closed here (rather than deferring the missing plan to the
// handler's own plan load) keeps the authorization bound to a plan that existed
// when the decision was made.
func (s *Service) authorizeDirectWriteForStoredPlan(w http.ResponseWriter, r *http.Request, operation, planID, environment string) bool {
	if !s.config.scopedWriteEnabled() {
		metrics.RecordDirectWriteAuthorization(r.Context(), operation, "",
			s.config.metricEnvironmentAttribute(environment), "skipped", DirectWriteReasonScopedLaneDisabled)
		return true
	}
	plan, err := s.storage.Plans().Get(r.Context(), planID)
	if err != nil {
		s.logger.Error("failed to load plan for direct write authorization",
			"operation", operation, "plan_id", planID, "environment", environment, "error", err)
		s.recordUnresolvedDirectWriteTarget(r, operation, environment)
		s.writeError(w, http.StatusInternalServerError, fmt.Sprintf("%s failed: get plan %s: %v", operation, planID, err))
		return false
	}
	if plan == nil {
		s.logger.Warn("rejecting direct write because the stored plan does not exist",
			"operation", operation, "plan_id", planID, "environment", environment)
		s.recordUnresolvedDirectWriteTarget(r, operation, environment)
		s.writeError(w, http.StatusInternalServerError, fmt.Sprintf("%s failed: plan not found: %s", operation, planID))
		return false
	}
	return s.authorizeDirectWrite(w, r, operation, plan.Database, environment)
}

// recordUnresolvedDirectWriteTarget counts a direct-write decision that never
// reached an authorization outcome because the target database could not be
// resolved. The request is rejected by the operation's own error path, but the
// decision still lands on the metric as skipped so a run of storage failures on
// the authorization path is visible as a rate rather than only in the logs.
func (s *Service) recordUnresolvedDirectWriteTarget(r *http.Request, operation, environment string) {
	metrics.RecordDirectWriteAuthorization(r.Context(), operation, "",
		s.config.metricEnvironmentAttribute(environment), "skipped", DirectWriteReasonTargetUnresolved)
}

// authorizeDirectDatabaseWrite is authorizeDirectWrite for environment-less
// operations (database locks).
func (s *Service) authorizeDirectDatabaseWrite(w http.ResponseWriter, r *http.Request, operation, database string) bool {
	result := s.config.AuthorizeDirectDatabaseWrite(auth.UserFromContext(r.Context()), database)
	return s.finishDirectWriteDecision(w, r, operation, database, "", result)
}

// authorizeDirectAdminWrite enforces admin-only access for mutating endpoints
// with no single target database and reports whether the request may proceed.
func (s *Service) authorizeDirectAdminWrite(w http.ResponseWriter, r *http.Request, operation string) bool {
	result := s.config.AuthorizeDirectAdminWrite(auth.UserFromContext(r.Context()))
	return s.finishDirectWriteDecision(w, r, operation, "", "", result)
}

// authorizeDirectForceLockRelease enforces admin-only access for force lock
// release. Force bypasses the lock ownership check — an administrative
// override, not an operator control on the caller's own database — so a
// database operator grant does not cover it. The database still rides along
// for the decision metric and denial log.
func (s *Service) authorizeDirectForceLockRelease(w http.ResponseWriter, r *http.Request, database string) bool {
	result := s.config.AuthorizeDirectAdminWrite(auth.UserFromContext(r.Context()))
	return s.finishDirectWriteDecision(w, r, "lock_force_release", database, "", result)
}

func (s *Service) finishDirectWriteDecision(w http.ResponseWriter, r *http.Request, operation, database, environment string, result DirectWriteAuthorizationResult) bool {
	metrics.RecordDirectWriteAuthorization(r.Context(), operation,
		s.config.metricDatabaseAttribute(database), s.config.metricEnvironmentAttribute(environment),
		directWriteMetricStatus(result), result.Reason)
	if result.Allowed {
		return true
	}

	subject := ""
	if user := auth.UserFromContext(r.Context()); user != nil {
		subject = user.Subject
	}
	s.logger.Warn("direct write denied by per-database authorization",
		"operation", operation,
		"database", database,
		"environment", environment,
		"subject", subject,
		"reason", result.Reason)
	s.writeError(w, http.StatusForbidden, s.directWriteDenialMessage(operation, database, environment, result))
	return false
}

// directWriteMetricStatus collapses a decision into the allowed/denied/skipped
// status attribute, mirroring the PR-door actor-authorization metric.
func directWriteMetricStatus(result DirectWriteAuthorizationResult) string {
	if result.Reason == DirectWriteReasonScopedLaneDisabled {
		return "skipped"
	}
	if result.Allowed {
		return "allowed"
	}
	return "denied"
}

// directWriteDenialMessage names the principals that would grant the denied
// operation. Group names are visible only to callers already authenticated
// behind the trusted proxy.
func (s *Service) directWriteDenialMessage(operation, database, environment string, result DirectWriteAuthorizationResult) string {
	writeGroups := s.config.Auth.ForwardAuth.WriteGroups
	switch result.Reason {
	case DirectWriteReasonEnvironmentNotAllowed:
		return fmt.Sprintf("%s on database %q is not allowed in environment %q for your operator grant (allowed: %s); deployment write groups (%s) may target any environment",
			operation, database, environment,
			strings.Join(trimmedNonEmpty(s.config.Auth.ForwardAuth.OperatorEnvironments), ", "),
			strings.Join(writeGroups, ", "))
	case DirectWriteReasonNotAdmin:
		return fmt.Sprintf("%s requires membership in a deployment write group (%s); database operator grants do not cover it",
			operation, strings.Join(writeGroups, ", "))
	case DirectWriteReasonMissingIdentity:
		return fmt.Sprintf("%s requires an authenticated caller identity", operation)
	default:
		grantingGroups := writeGroups
		if dbConfig, ok := s.config.Databases[database]; ok {
			grantingGroups = append(slices.Clone(writeGroups), trimmedNonEmpty(dbConfig.OperatorGroups)...)
		}
		return fmt.Sprintf("%s on database %q requires membership in one of: %s",
			operation, database, strings.Join(grantingGroups, ", "))
	}
}
