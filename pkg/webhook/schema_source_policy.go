package webhook

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/metrics"
)

// silentDiscoveryFailureOnUnscopedFanOut reports whether a failed schema
// discovery for an unscoped (no -t) command should be a logged silent skip
// rather than a PR comment. Three independent outcomes qualify — the
// discovered schema belongs to another deployment, a participant's partial
// view cannot resolve the named database, or another leader serving the
// command's environment answers for an unregistered database — and they
// belong together here because they say the same thing to the user: a
// different deployment owns this command's answer, so a reply from this one
// would land as duplicate noise beside the real one. Every other failure still
// surfaces, and a -t-scoped command (tenant != "") always reports, since it
// named a specific deployment. environment is the command's -e value, empty
// for a command that named none.
func (h *Handler) silentDiscoveryFailureOnUnscopedFanOut(repo, environment, tenant string, err error) bool {
	if tenant != "" {
		return false
	}
	if h.silentUnownedSchemaOnAggregateFanOut(repo, environment, err) {
		h.logLeftToEnvironmentResponder(repo, environment, err)
		return true
	}
	return h.silentUnresolvedDatabaseOnParticipantFanOut(repo, err)
}

// silentUnownedSchemaOnAggregateFanOut reports whether a "schema not owned by
// this deployment" error should be silently ignored instead of reported on the
// PR. On an aggregate repo (leader or participant), an unscoped command is a
// fan-out broadcast every installed deployment receives; a deployment that owns
// none of the changed schema is expected to do nothing while the deployment
// that does own it handles the command. Posting "config not authorized" or
// "database not configured" from every non-owning deployment would be exactly
// the noise fan-out removes.
//
// The silence rests on some other deployment answering. A leader can tell
// when none it knows of will: a config outside its own allowed_dirs whose
// directory none of its expected participants manages is one it reports
// against its own allowed_dirs, and an unregistered database under no such
// directory is one it reports as not registered here. Several leaders can
// split one repository's environments, and each would reach the second
// conclusion from its own registry, so only the leader serving the command's
// environment makes it (answersForUnregisteredDatabase).
func (h *Handler) silentUnownedSchemaOnAggregateFanOut(repo, environment string, err error) bool {
	config, ok := h.serverConfig()
	if !ok {
		return false
	}
	if config.AggregateRoleForRepo(repo) == "" {
		return false
	}
	var notRegistered *databaseNotRegisteredError
	if errors.As(err, &notRegistered) {
		answers, _ := answersForUnregisteredDatabase(config, environment)
		return !answers
	}
	if !isSchemaUnownedByDeploymentError(err) {
		return false
	}
	var outsideAllowedDirs *schemaConfigOutsideAllowedDirsError
	if errors.As(err, &outsideAllowedDirs) {
		return h.schemaUnderExpectedParticipant(config, repo, outsideAllowedDirs.SchemaPath)
	}
	return true
}

// deploymentIdentity names this deployment in a claim about its own registry.
// A deployment serving one environment is named by it. One serving several
// but not every environment has no single name, so it is identified by the
// environments it serves, in promotion order. One serving every environment
// returns neither and speaks as "this" deployment.
func (h *Handler) deploymentIdentity() (label string, environments []string) {
	config, ok := h.serverConfig()
	if !ok {
		return "", nil
	}
	switch len(config.AllowedEnvironments) {
	case 0:
		return "", nil
	case 1:
		return config.AllowedEnvironments[0], nil
	default:
		return "", config.OrderedEnvironments(config.AllowedEnvironments)
	}
}

// answersForUnregisteredDatabase reports whether this leader is the one to
// answer a command with Database Not Registered, and names the environment
// whose deployment answers. A command with -e is answered by the leader
// serving that environment; environment routing already keeps it from every
// other deployment. An unscoped command reaches every leader on the
// repository, and each finds the database missing from its own registry, so
// the leader serving the first environment in the promotion order answers and
// the others stay silent. A repository with one leader serving every
// environment always answers.
func answersForUnregisteredDatabase(config *api.ServerConfig, environment string) (bool, string) {
	if environment != "" {
		return config.IsEnvironmentAllowed(environment), environment
	}
	return config.ServesFirstPromotionEnvironment()
}

// logLeftToEnvironmentResponder logs why a leader stays silent on a database
// it found unregistered: the reply belongs to the leader serving another
// environment. Other silent fan-out outcomes are logged by their callers.
func (h *Handler) logLeftToEnvironmentResponder(repo, environment string, err error) {
	var notRegistered *databaseNotRegisteredError
	if !errors.As(err, &notRegistered) {
		return
	}
	config, ok := h.serverConfig()
	if !ok {
		return
	}
	_, responder := answersForUnregisteredDatabase(config, environment)
	h.logger.Info("database not registered on this leader; the leader serving the responder environment answers the command",
		"repo", repo, "environment", environment, "responder_environment", responder,
		"allowed_environments", config.AllowedEnvironments,
		"databases", notRegistered.Databases(), "schema_paths", notRegistered.SchemaPaths(),
		"policy", "unscoped commands are answered by the deployment serving the first environment in environment_order")
}

// databaseNotRegisteredOnLeader reports whether this deployment, as the
// aggregate leader, finds a schema config it cannot attribute to any
// deployment it knows of: the config's database has no entry in this
// deployment's registry, and its directory is under none of the participant
// paths in this deployment's expected-tenant set. That is all the claim
// covers. A sibling leader serving other environments may register the
// database, and this deployment cannot see its registry or allowed_dirs. A
// leader that resolves databases dynamically (TargetResolver) has no registry
// to consult, so it never makes the claim; its misplaced configs surface
// through the allowed_dirs class instead.
func (h *Handler) databaseNotRegisteredOnLeader(config *api.ServerConfig, repo, database, schemaPath string) bool {
	if config.TargetResolver.Enabled() {
		return false
	}
	return !h.schemaUnderExpectedParticipant(config, repo, schemaPath) && config.Database(database) == nil
}

// schemaUnderExpectedParticipant reports whether a schema config this
// deployment does not manage can belong to a participant on repo. A
// participant sees only its own slice of the fleet, so for it the answer is
// always yes. A leader's expected-tenant set names its participants' path
// prefixes, so on a leader the answer is yes only when one of them covers the
// config's schema directory.
func (h *Handler) schemaUnderExpectedParticipant(config *api.ServerConfig, repo, schemaPath string) bool {
	if !config.IsAggregateLeaderForRepo(repo) {
		return true
	}
	return config.ExpectedTenantManagesSchemaPath(repo, schemaPath)
}

// silentUnresolvedDatabaseOnParticipantFanOut reports whether a database
// discovery miss should be silently deferred on this deployment. A
// participant's discovery covers only its own slice of the fleet, so an
// authoritative "database not found" from that local view cannot establish
// that the fleet has no matching schema — the aggregate leader, whose view
// spans the fleet, answers instead while the participant stays silent. Only
// the authoritative miss defers: an uncertain outcome (for example a truncated
// repository tree the configured schema directory hints cannot recover) still
// surfaces fail-closed, because the participant might own the database and
// simply be unable to prove it.
func (h *Handler) silentUnresolvedDatabaseOnParticipantFanOut(repo string, err error) bool {
	config, ok := h.serverConfig()
	if !ok {
		return false
	}
	if config.AggregateRoleForRepo(repo) != api.AggregateRoleParticipant {
		return false
	}
	var notFound *ghclient.DatabaseNotFoundError
	return errors.As(err, &notFound)
}

// isSchemaUnownedByDeploymentError reports whether err means the command
// resolved to schema another deployment owns: either the schema config lives
// outside this server's allowed_dirs, or the discovered database has no entry
// in this server's databases registry at all. Under the aggregate contract both
// mean the same thing — this deployment is not the owner — so on unscoped
// fan-out both are silently skipped rather than reported. Anything else is a
// real failure and must still surface.
func isSchemaUnownedByDeploymentError(err error) bool {
	var notOwned *schemaConfigOutsideAllowedDirsError
	if errors.As(err, &notOwned) {
		return true
	}
	var notConfigured *api.DatabaseNotConfiguredError
	return errors.As(err, &notConfigured)
}

// silentOnUnscopedFanOut reports whether a "nothing to do on this deployment"
// outcome for an unscoped (no -t) command should be a logged silent skip rather
// than a PR comment. On an aggregate repo (leader or participant) an unscoped
// command fans out to every deployment, so one that finds no pending work — for
// example apply-confirm after this deployment's own databases already
// auto-applied and released their locks — must stay quiet; only the deployment
// that actually has work to confirm responds. A -t-scoped command (tenant != "")
// named a specific deployment, so its "nothing to do" answer is useful and still
// surfaces.
func (h *Handler) silentOnUnscopedFanOut(repo, tenant string) bool {
	if tenant != "" {
		return false
	}
	config, ok := h.serverConfig()
	if !ok {
		return false
	}
	return config.AggregateRoleForRepo(repo) != ""
}

// silentUsageErrorOnUnscopedFanOut reports whether a usage-error reply (a bad
// or missing flag or argument) to an unscoped (no -t) command should be a
// logged silent skip on this deployment. A usage error is decidable by every
// deployment from the comment text alone, so on an aggregate repo a fan-out
// would post one copy per participant. Unlike a "nothing to do on this
// deployment" outcome (silentOnUnscopedFanOut), no owner is going to answer —
// the command is malformed everywhere — so participants stay silent and the
// leader posts the error exactly once. A -t-scoped command named this
// deployment, so its answer always surfaces.
func (h *Handler) silentUsageErrorOnUnscopedFanOut(repo, tenant string) bool {
	if tenant != "" {
		return false
	}
	config, ok := h.serverConfig()
	if !ok {
		return false
	}
	return config.AggregateRoleForRepo(repo) == api.AggregateRoleParticipant
}

// silentUnknownEnvOnAggregateFanOut reports whether an unknown-environment
// rejection should be a logged silent skip on this deployment. An aggregate
// participant's config holds only its own slice of the fleet's environments,
// so an -e value it does not recognize may be a perfectly valid environment
// served by a sibling deployment — rejecting it from that partial worldview
// posts a spurious "Invalid Environment" next to the sibling's real work.
// Participants therefore defer silently even when the command names their own
// tenant, since the same tenant name can be served in other environments by
// sibling deployments. On unscoped commands the leader, whose environment
// order spans the fleet, still rejects a genuinely unknown value exactly once.
func (h *Handler) silentUnknownEnvOnAggregateFanOut(repo string) bool {
	config, ok := h.serverConfig()
	if !ok {
		return false
	}
	return config.AggregateRoleForRepo(repo) == api.AggregateRoleParticipant
}

// environmentNotConfiguredError reports that the requested environment has no
// entry for the database on this server — a targeting rejection the same
// command will always reproduce, not a transient failure.
type environmentNotConfiguredError struct {
	Database    string
	Environment string
}

func (e *environmentNotConfiguredError) Error() string {
	return fmt.Sprintf("database %q environment %q is not configured on this server", e.Database, e.Environment)
}

type schemaConfigOutsideAllowedDirsError struct {
	Database     string
	DatabaseType string
	SchemaPath   string
}

func (e *schemaConfigOutsideAllowedDirsError) Error() string {
	return fmt.Sprintf("schema config for database %q at %q is outside server allowed_dirs", e.Database, e.SchemaPath)
}

// databaseNotRegisteredError reports discovered schema configs the aggregate
// leader cannot attribute to any deployment it knows of: each config's
// database has no entry in this deployment's registry, and its schema
// directory is under none of the participant paths in this deployment's
// expected-tenant set (databaseNotRegisteredOnLeader). It is not an
// ownership signal: no participant this leader knows of will answer, so
// whether this leader answers or stays silent is decided by the command's
// environment instead (answersForUnregisteredDatabase). It names every such
// config the command discovered, so one reply covers them all.
type databaseNotRegisteredError struct {
	Configs []unregisteredSchemaConfig
}

// unregisteredSchemaConfig identifies one schema config whose database this
// leader has not registered.
type unregisteredSchemaConfig struct {
	Database     string
	DatabaseType string
	SchemaPath   string
}

func (e *databaseNotRegisteredError) Error() string {
	described := make([]string, 0, len(e.Configs))
	for _, cfg := range e.Configs {
		described = append(described, fmt.Sprintf("database %q at %q", cfg.Database, cfg.SchemaPath))
	}
	return fmt.Sprintf("schema config for %s: database not registered on this deployment, and schema directory under no expected participant's paths", strings.Join(described, ", "))
}

// Databases lists the database each unregistered config declares, in
// discovery order.
func (e *databaseNotRegisteredError) Databases() []string {
	databases := make([]string, 0, len(e.Configs))
	for _, cfg := range e.Configs {
		databases = append(databases, cfg.Database)
	}
	return databases
}

// SchemaPaths lists each unregistered config's schema directory, in discovery
// order.
func (e *databaseNotRegisteredError) SchemaPaths() []string {
	paths := make([]string, 0, len(e.Configs))
	for _, cfg := range e.Configs {
		paths = append(paths, cfg.SchemaPath)
	}
	return paths
}

// unownedSchemaConfigError describes why a discovered schema config is not
// this deployment's to process, matching the error class to the ownership
// contract that dropped it. On the aggregate leader, a config whose directory
// none of its expected participants manages and whose database it has not
// registered is reported as not registered here, so that one leader can
// answer the command (answersForUnregisteredDatabase). Otherwise, when
// the repo has a directory allowlist, the config was outside it. In open mode
// (no allowlist for the repo) the only drop reason is the database registry,
// so the database is reported as not configured — an allowed_dirs remediation
// would be misleading on a repo that has no allowlist to amend. The latter two
// classes count as unowned for unscoped fan-out silencing
// (isSchemaUnownedByDeploymentError); the distinction between them only
// changes what a -t/-d-scoped command reports.
func (h *Handler) unownedSchemaConfigError(repo, database, databaseType, schemaPath string) error {
	config, ok := h.serverConfig()
	if ok && h.databaseNotRegisteredOnLeader(config, repo, database, schemaPath) {
		return &databaseNotRegisteredError{Configs: []unregisteredSchemaConfig{{
			Database:     database,
			DatabaseType: databaseType,
			SchemaPath:   schemaPath,
		}}}
	}
	if ok && !config.RepoHasSchemaDirAllowlist(repo) {
		return &api.DatabaseNotConfiguredError{Database: database}
	}
	return &schemaConfigOutsideAllowedDirsError{
		Database:     database,
		DatabaseType: databaseType,
		SchemaPath:   schemaPath,
	}
}

// unownedDiscoveredConfigError is unownedSchemaConfigError for a discovered
// config that may have failed to parse: a nil config carries no database
// identity to report, so it surfaces as ErrNoConfig instead.
func (h *Handler) unownedDiscoveredConfigError(repo string, config *ghclient.SchemabotConfig, schemaPath string) error {
	if config == nil {
		return ghclient.ErrNoConfig
	}
	return h.unownedSchemaConfigError(repo, config.Database, string(config.GetType()), schemaPath)
}

// unregisteredDatabaseError reports a -d database this deployment's registry
// has no entry for, before any repository discovery runs. The registry is the
// authoritative answer here, and discovery cannot improve on it: on a
// repository whose tree GitHub truncates, a database-scoped search probes only
// the directories the registry configures for the database, so for an
// unregistered database it would search nothing and report the config as
// missing from the repository even when the file exists. Deciding from the
// registry gives every repository size the same answer, and the error class
// is the one fan-out silencing already reads as "another deployment owns
// this" (isSchemaUnownedByDeploymentError).
//
// Two deployments keep discovering instead. An aggregate leader's registry
// covers only its own slice of the fleet, and it owes the fleet-authoritative
// Database Not Found when no deployment can answer, which only a repository
// search can establish. A deployment that resolves databases dynamically
// (TargetResolver) does not describe what it manages in the registry at all,
// the same exception the source policy makes. Returns nil when the command
// named no database.
func (h *Handler) unregisteredDatabaseError(repo, databaseName string) error {
	if databaseName == "" {
		return nil
	}
	config, ok := h.serverConfig()
	if !ok {
		return nil
	}
	if config.IsAggregateLeaderForRepo(repo) || config.TargetResolver.Enabled() {
		return nil
	}
	if config.Database(databaseName) != nil {
		return nil
	}
	h.logger.Info("database named by -d is not configured on this deployment; answering from the registry without repository discovery",
		"repo", repo, "database", databaseName)
	return &api.DatabaseNotConfiguredError{Database: databaseName}
}

// answerUnregisteredDatabase is the registry-first gate a command path runs
// before any GitHub read, including the preflight for a PR with no managed schema changes:
// a -d database this deployment does not serve gets its answer from the
// registry alone (unregisteredDatabaseError), so the response never depends on
// what the PR happens to touch or on a repository read succeeding. The answer
// follows the fan-out rules every other discovery outcome follows — silent on
// an unscoped aggregate fan-out, a comment otherwise. Returns whether the
// command is answered; the caller stops when it is.
func (h *Handler) answerUnregisteredDatabase(repo string, pr int, installationID int64, environment, databaseName, tenant, requestedBy, commandName string, suppressRetryComments bool) bool {
	err := h.unregisteredDatabaseError(repo, databaseName)
	if err == nil {
		return false
	}
	if h.silentDiscoveryFailureOnUnscopedFanOut(repo, environment, tenant, err) {
		h.logger.Debug("unscoped fan-out command names a database this deployment does not serve; staying silent",
			"repo", repo, "pr", pr, "environment", environment, "database", databaseName, "action", commandName)
		return true
	}
	h.handleSchemaRequestError(repo, pr, installationID, environment, databaseName, requestedBy, commandName, err, suppressRetryComments)
	return true
}

func (h *Handler) createManagedSchemaRequestFromPR(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, environment, databaseName, source string) (*ghclient.SchemaRequestResult, error) {
	if err := h.unregisteredDatabaseError(repo, databaseName); err != nil {
		return nil, err
	}
	var schemaResult *ghclient.SchemaRequestResult
	var err error
	if databaseName == "" {
		schemaResult, err = h.createUnscopedSchemaRequestFromPR(ctx, client, repo, pr, environment, source)
	} else {
		schemaResult, err = client.CreateSchemaRequestFromPR(ctx, repo, pr, environment, databaseName, h.validateRequestedDatabaseEnvironment)
	}
	if err != nil {
		return nil, err
	}
	// Re-check the resolved schema root: environment resolution can retarget it
	// (for example through an environment symlink), so ownership must hold for
	// the path the schema files were actually fetched from, not just the
	// discovered config directory.
	if !h.shouldProcessSchemaConfig(ctx, repo, pr, schemaResult.HeadSHA, schemaResult.Database, schemaResult.Type, schemaResult.SchemaPath, source) {
		return nil, h.unownedSchemaConfigError(repo, schemaResult.Database, schemaResult.Type, schemaResult.SchemaPath)
	}
	return schemaResult, nil
}

// createUnscopedSchemaRequestFromPR resolves which database an unscoped
// command (no -d) targets and fetches that database's schema files.
func (h *Handler) createUnscopedSchemaRequestFromPR(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, environment, source string) (*ghclient.SchemaRequestResult, error) {
	config, configDir, err := h.resolveUnscopedManagedConfig(ctx, client, repo, pr, environment, source)
	if err != nil {
		return nil, err
	}
	return client.CreateSchemaRequestForConfig(ctx, repo, pr, environment, config, configDir, h.validateRequestedDatabaseEnvironment)
}

// resolveUnscopedManagedConfig resolves which database an unscoped command
// (no -d) targets. Discovery mirrors auto-plan — changed config files plus the
// nearest config above each changed schema file — and the discovered configs
// are filtered to the ones this deployment manages before deciding
// multiplicity, so on fan-out repos each deployment decides from its own slice:
//
//   - no configs discovered at all: fall back to repo-wide single-config
//     discovery (a changeless PR can still carry commands for its database)
//   - none of the discovered configs managed here: an unowned-schema error,
//     which unscoped fan-out handling downgrades to a silent skip
//   - exactly one managed config: the command targets it, even when the PR
//     also touches configs other deployments own
//   - several managed configs: ErrMultipleConfigs — one apply drives one
//     database, so the user must scope the command with -d
//
// environment is the command's -e value, empty when it named none; it decides
// which of several leaders answers for an unregistered database.
func (h *Handler) resolveUnscopedManagedConfig(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, environment, source string) (*ghclient.SchemabotConfig, string, error) {
	configs, err := client.FindAllConfigsForPR(ctx, repo, pr)
	if err != nil {
		return nil, "", err
	}
	if len(configs) == 0 {
		config, configDir, _, err := client.FindConfigInRepo(ctx, repo, pr)
		if err != nil {
			return nil, "", err
		}
		if !h.configPathManagedByRepo(ctx, repo, pr, "", config, configDir, source) {
			return nil, "", h.unownedDiscoveredConfigError(repo, config, configDir)
		}
		return config, configDir, nil
	}
	// filterManagedDiscoveredConfigs filters in place, so give it a copy —
	// callers may still need the full discovery result.
	managed := h.filterManagedDiscoveredConfigs(ctx, repo, pr, "", source, slices.Clone(configs))
	if len(managed) == 0 {
		h.logger.Info("unscoped command discovered only schema configs this deployment does not manage",
			"repo", repo, "pr", pr, "source", source,
			"discovered_configs", len(configs))
		return nil, "", h.unownedDiscoveredConfigsError(repo, environment, configs)
	}
	// The directory allowlist is only half the ownership contract: on repos
	// partitioned by database registry rather than allowed_dirs, a config for
	// another deployment's database passes the directory filter here. Narrow to
	// the configs whose database this deployment has registered before deciding
	// multiplicity, so a PR spanning several deployments' databases is not
	// falsely ambiguous for the one that owns a single database in it.
	registered := h.registeredDiscoveredConfigs(managed)
	if len(registered) == 0 {
		if len(managed) == 1 {
			return managed[0].Config, managed[0].SchemaDir, nil
		}
		h.logger.Info("unscoped command discovered only schema configs for databases not registered on this deployment",
			"repo", repo, "pr", pr, "source", source,
			"discovered_configs", len(configs), "managed_configs", len(managed))
		return nil, "", &api.DatabaseNotConfiguredError{Database: managed[0].Config.Database}
	}
	return ghclient.SingleDiscoveredConfig(registered)
}

// unownedDiscoveredConfigsError picks the error to report when discovery found
// only configs this deployment does not manage. On an unscoped fan-out most of
// them are silently left to their owners, so a config this leader would answer
// for must not hide behind one a participant owns: the first config whose
// error this deployment would answer rather than defer decides the report, and
// when every one defers the first config stands for the set. When the deciding
// config's database is not registered here, the report names every such
// config, so the author learns about all of them from one reply instead of one
// per retry.
func (h *Handler) unownedDiscoveredConfigsError(repo, environment string, configs []ghclient.DiscoveredConfig) error {
	var answered error
	var notRegistered *databaseNotRegisteredError
	for _, cfg := range configs {
		err := h.unownedDiscoveredConfigError(repo, cfg.Config, cfg.SchemaDir)
		if h.silentUnownedSchemaOnAggregateFanOut(repo, environment, err) {
			continue
		}
		var one *databaseNotRegisteredError
		if errors.As(err, &one) {
			if notRegistered == nil {
				notRegistered = &databaseNotRegisteredError{}
			}
			notRegistered.Configs = append(notRegistered.Configs, one.Configs...)
		}
		if answered == nil {
			answered = err
		}
	}
	if answered == nil {
		return h.unownedDiscoveredConfigError(repo, configs[0].Config, configs[0].SchemaDir)
	}
	var first *databaseNotRegisteredError
	if errors.As(answered, &first) {
		return notRegistered
	}
	return answered
}

// registeredDiscoveredConfigs narrows discovered configs to the ones whose
// database has an entry in this deployment's databases registry.
func (h *Handler) registeredDiscoveredConfigs(configs []ghclient.DiscoveredConfig) []ghclient.DiscoveredConfig {
	config, ok := h.serverConfig()
	if !ok {
		return configs
	}
	var registered []ghclient.DiscoveredConfig
	for _, cfg := range configs {
		if config.Database(cfg.Config.Database) != nil {
			registered = append(registered, cfg)
		}
	}
	return registered
}

func (h *Handler) validateRequestedDatabaseEnvironment(database, environment string) error {
	if environment == "" {
		return nil
	}
	environments, err := h.configuredDatabaseEnvironments(database)
	if err != nil {
		return fmt.Errorf("resolve configured environments for database %q: %w", database, err)
	}
	if slices.Contains(environments, environment) {
		return nil
	}
	return &environmentNotConfiguredError{Database: database, Environment: environment}
}

func (h *Handler) configPathManagedByRepo(ctx context.Context, repo string, pr int, headSHA string, config *ghclient.SchemabotConfig, schemaPath, source string) bool {
	if config == nil {
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:  "schema_config_discovery",
			Repository: repo,
			Status:     "skipped",
		})
		h.logger.Warn("schema config is missing parsed config and will be ignored",
			"repo", repo, "pr", pr, "head_sha", headSHA,
			"schema_path", schemaPath, "source", source)
		return false
	}
	return h.shouldProcessSchemaConfig(ctx, repo, pr, headSHA, config.Database, string(config.GetType()), schemaPath, source)
}

func (h *Handler) shouldProcessSchemaConfig(ctx context.Context, repo string, pr int, headSHA, database, databaseType, schemaPath, source string) bool {
	config, ok := h.serverConfig()
	if !ok {
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:    "schema_config_source_policy",
			Repository:   repo,
			Database:     database,
			DatabaseType: databaseType,
			Status:       "error",
		})
		h.logger.Warn("schema config source policy cannot be evaluated because server config is unavailable",
			"repo", repo, "pr", pr, "head_sha", headSHA,
			"database", database, "database_type", databaseType,
			"schema_path", schemaPath, "source", source)
		return true
	}

	if !config.RepoHasSchemaDirAllowlist(repo) {
		// Open mode: with no directory allowlist for the repo, every discovered
		// config is this deployment's to process. On an aggregate-role repo,
		// however, ownership is partitioned across deployments by database
		// registry — a config for a database this deployment has not registered
		// belongs to a sibling deployment. Keeping it would plan a database this
		// deployment cannot resolve and convert routine fan-out into a failing
		// aggregate, so it is dropped: the leader gates on the owner's Check Run
		// via the expected-participants fold, and a participant stays silent.
		// Deployments that resolve databases dynamically instead of through the
		// registry keep open mode as-is — the registry says nothing about what
		// they manage.
		if config.AggregateRoleForRepo(repo) == "" || config.TargetResolver.Enabled() {
			return true
		}
		if config.Database(database) != nil {
			return true
		}
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:    "schema_config_source_policy",
			Repository:   repo,
			Database:     database,
			DatabaseType: databaseType,
			Status:       "skipped",
		})
		h.logger.Info("schema config on aggregate repo is for a database not registered on this deployment; its owning deployment handles it",
			"repo", repo, "pr", pr, "head_sha", headSHA,
			"database", database, "database_type", databaseType,
			"schema_path", schemaPath, "source", source)
		return false
	}

	if !config.SchemaPathAllowedForRepo(repo, schemaPath) {
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:  "schema_config_source_policy",
			Repository: repo,
			Status:     "skipped",
		})
		// On an aggregate-role repo a config outside this deployment's
		// allowed_dirs is routine fan-out — another deployment owns it. On any
		// other repo nothing will ever act on the config, so warn.
		logDropped := h.logger.Warn
		if config.AggregateRoleForRepo(repo) != "" {
			logDropped = h.logger.Info
		}
		logDropped("schema config is outside repo allowed_dirs and will be ignored",
			"repo", repo, "pr", pr, "head_sha", headSHA,
			"database", database, "database_type", databaseType,
			"schema_path", schemaPath, "source", source)
		return false
	}

	if config.Database(database) == nil {
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:    "schema_config_source_policy",
			Repository:   repo,
			Database:     database,
			DatabaseType: databaseType,
			Status:       "error",
		})
		h.logger.Warn("schema config is inside repo allowed_dirs but database is not configured",
			"repo", repo, "pr", pr, "head_sha", headSHA,
			"database", database, "database_type", databaseType,
			"schema_path", schemaPath, "source", source)
	}

	return true
}
