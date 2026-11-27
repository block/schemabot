package webhook

import (
	"context"
	"errors"
	"fmt"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
)

// verifySharedSchemaIgnoreLists refuses a pull request's plan or apply of a
// database whose schema files and ignore_tables disagree with another database
// it shares a schema with on the same target (api.CheckSharedSchemaIgnoreLists).
// A pull request that changes only one of the two would otherwise pass: its
// own plan withholds nothing it should not, and the missing entry surfaces only
// as a DROP TABLE in the other database's next plan, long after the change
// merged.
//
// The other database's files are read at the same pull request head as the
// planned database's, so a pull request that adds a table to one database and
// the entry to the other in the same change passes. They are read only from
// a source the server authorizes for that database, the same policy its own
// plans pass. A peer whose schemabot.yaml a complete search did not find in
// this repository is managed from elsewhere and cannot be compared here; it is
// logged and skipped. Any other failure to read it is returned, so the caller
// fails the environment closed rather than passing a pair it could not compare.
//
// It reads GitHub only for databases that share a target with this one
// (AV-15). peerConfigs carries where each peer's schemabot.yaml was found
// across the environments of one delivery; nil looks each one up again.
//
// It returns the inputs of each database it compared with, so an apply can
// refuse a branch whose base has since changed them (see
// assertBaseSchemaStillCurrent): the comparison holds only for the versions it
// read.
func (h *Handler) verifySharedSchemaIgnoreLists(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, environment, source string, schemaResult *ghclient.SchemaRequestResult, peerConfigs *sharedSchemaPeerConfigs) ([]sharedSchemaPeerInputs, error) {
	config := h.service.Config()
	peers, err := config.SharedSchemaPeers(repo, schemaResult.Database, environment)
	if err != nil {
		return nil, fmt.Errorf("find the databases that share a target with %q in %s: %w", schemaResult.Database, environment, err)
	}
	if len(peers) == 0 {
		return nil, nil
	}

	peerSides := make([]api.SharedSchemaDatabase, 0, len(peers))
	peerInputs := make([]sharedSchemaPeerInputs, 0, len(peers))
	for _, peer := range peers {
		peerResult, err := h.sharedSchemaPeerRequest(ctx, client, repo, pr, environment, peer, source, schemaResult.HeadSHA, peerConfigs)
		if peerConfigOutsideRepository(err) {
			h.logger.Info("database shares a target with the planned database but its schemabot.yaml is not in this repository; its ignore_tables are not compared with this plan",
				"repo", repo, "pr", pr, "head_sha", schemaResult.HeadSHA, "database", schemaResult.Database, "database_type", schemaResult.Type,
				"environment", environment, "peer_database", peer, "error", err)
			continue
		}
		if err != nil {
			h.logger.Error("could not read the schema files of a database that shares a target with the planned database; the environment fails closed",
				"repo", repo, "pr", pr, "head_sha", schemaResult.HeadSHA, "database", schemaResult.Database, "database_type", schemaResult.Type,
				"environment", environment, "peer_database", peer, "error", err)
			return nil, &sharedSchemaPeerUnreadableError{Peer: peer, Database: schemaResult.Database, Environment: environment, Cause: err}
		}
		peerSides = append(peerSides, sharedSchemaSideOf(peerResult))
		peerInputs = append(peerInputs, sharedSchemaPeerInputsOf(peerResult))
	}

	err = config.CheckSharedSchemaIgnoreLists(ctx, environment, sharedSchemaSideOf(schemaResult), peerSides, h.service)
	var mismatch *api.SharedSchemaIgnoreListsError
	if errors.As(err, &mismatch) {
		h.logger.Warn("schema files and ignore_tables disagree with a database sharing the same schema; the environment is refused until they agree",
			"repo", repo, "pr", pr, "head_sha", schemaResult.HeadSHA, "database", schemaResult.Database, "database_type", schemaResult.Type,
			"environment", environment, "peer_databases", peers, "error", err)
		return nil, err
	}
	if err != nil {
		return nil, fmt.Errorf("compare ignore_tables of %q with the databases sharing its targets in %s: %w", schemaResult.Database, environment, err)
	}
	h.logger.Debug("ignore_tables agree with every database sharing a schema",
		"repo", repo, "pr", pr, "database", schemaResult.Database, "environment", environment, "peer_databases", peers)
	return peerInputs, nil
}

// sharedSchemaPeerInputs are the paths whose content decided one peer's side
// of a shared schema comparison: its schemabot.yaml, which holds its
// ignore_tables, and the paths that decide its schema in the environment.
type sharedSchemaPeerInputs struct {
	Database string
	Paths    []string
}

func sharedSchemaPeerInputsOf(result *ghclient.SchemaRequestResult) sharedSchemaPeerInputs {
	paths := environmentSchemaPaths(result)
	if result.ConfigPath != "" {
		paths = append(paths, result.ConfigPath)
	}
	return sharedSchemaPeerInputs{Database: result.Database, Paths: paths}
}

// sharedSchemaPeerUnreadableError is a failure to read a database that shares
// a schema with the planned one. It deliberately does not unwrap to its cause:
// the cause describes the other database (its source refused, its config
// missing), and a caller classifying the planned database's own failures must
// not mistake it for one of those. The environment fails as a plan failure.
type sharedSchemaPeerUnreadableError struct {
	Peer        string
	Database    string
	Environment string
	Cause       error
}

func (e *sharedSchemaPeerUnreadableError) Error() string {
	return fmt.Sprintf("read the schema files of %q, which shares a target with %q in %s, to compare their ignore_tables: %v", e.Peer, e.Database, e.Environment, e.Cause)
}

// sharedSchemaPeerRequest reads a peer database's schema files for one
// environment at headSHA, from a source the server authorizes for it.
func (h *Handler) sharedSchemaPeerRequest(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, environment, peer, source, headSHA string, peerConfigs *sharedSchemaPeerConfigs) (*ghclient.SchemaRequestResult, error) {
	found, err := peerConfigs.find(peer, func() (sharedSchemaPeerConfig, error) {
		return h.findSharedSchemaPeerConfig(ctx, client, repo, pr, headSHA, peer)
	})
	if err != nil {
		return nil, err
	}
	result, err := client.CreateSchemaRequestForConfig(ctx, repo, pr, environment, found.config, found.configDir, h.validateRequestedDatabaseEnvironment)
	if err != nil {
		return nil, err
	}
	if result.HeadSHA != headSHA {
		return nil, fmt.Errorf("the pull request head moved from %s to %s while its files were read; re-run the plan", headSHA, result.HeadSHA)
	}
	if !h.shouldProcessSchemaConfig(ctx, repo, pr, result.HeadSHA, result.Database, result.Type, result.SchemaPath, source) {
		return nil, h.unownedSchemaConfigError(repo, result.Database, result.Type, result.SchemaPath)
	}
	if err := h.service.Config().AuthorizePlanSource(api.PlanSourcePolicyRequest{
		Database:    result.Database,
		Repository:  repo,
		PullRequest: pr,
		SchemaPath:  result.SchemaPath,
	}); err != nil {
		return nil, fmt.Errorf("authorize schema path %s for database %q: %w", result.SchemaPath, result.Database, err)
	}
	return result, nil
}

// findSharedSchemaPeerConfig finds a peer's schemabot.yaml at headSHA. It
// scans first the directories the server allows the peer's config in, so the
// usual cost stays with the peer's own inputs however many other configs the
// repository holds (AV-15). It searches the repository the way a command
// naming the peer with -d would when those directories cannot bound where the
// config lives, and when they hold no config for the peer: a scoped miss shows
// only that the config is not where the server allows it, not that it is
// absent from this repository. A config found elsewhere is then refused by the
// peer's source policy, so the environment fails closed. Only a peer this
// repository is not allowed to plan at all is settled by the scoped miss.
func (h *Handler) findSharedSchemaPeerConfig(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, headSHA, peer string) (sharedSchemaPeerConfig, error) {
	config, configDir, scoped, err := client.FindConfigByDatabaseNameInItsDirs(ctx, repo, headSHA, peer)
	if !scoped {
		h.logger.Info("database sharing a schema has no directories bounding where its config lives; searching the repository for it",
			"repo", repo, "pr", pr, "head_sha", headSHA, "peer_database", peer)
		return h.findSharedSchemaPeerConfigInRepo(ctx, client, repo, pr, peer)
	}
	var notFound *ghclient.DatabaseNotFoundError
	if !errors.As(err, &notFound) || notFound.RepositoryNotAccepted() {
		return sharedSchemaPeerConfig{config: config, configDir: configDir}, err
	}
	h.logger.Info("database sharing a schema has no schemabot.yaml in the directories this repository allows it in; searching the repository for it",
		"repo", repo, "pr", pr, "head_sha", headSHA, "peer_database", peer, "searched_dirs", notFound.SearchedDirs)
	return h.findSharedSchemaPeerConfigInRepo(ctx, client, repo, pr, peer)
}

func (h *Handler) findSharedSchemaPeerConfigInRepo(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, peer string) (sharedSchemaPeerConfig, error) {
	config, configDir, err := client.FindConfigByDatabaseName(ctx, repo, pr, peer)
	return sharedSchemaPeerConfig{config: config, configDir: configDir}, err
}

// sharedSchemaPeerConfigs remembers, for one delivery, where each peer's
// schemabot.yaml was found, or that a complete search showed it is not in this
// repository, so planning several environments looks each peer up once. Any
// other failed lookup is not remembered: the next environment tries it again,
// and fails closed again if it fails again.
type sharedSchemaPeerConfigs struct {
	found   map[string]sharedSchemaPeerConfig
	outside map[string]error
}

type sharedSchemaPeerConfig struct {
	config    *ghclient.SchemabotConfig
	configDir string
}

func newSharedSchemaPeerConfigs() *sharedSchemaPeerConfigs {
	return &sharedSchemaPeerConfigs{found: map[string]sharedSchemaPeerConfig{}, outside: map[string]error{}}
}

func (c *sharedSchemaPeerConfigs) find(peer string, lookup func() (sharedSchemaPeerConfig, error)) (sharedSchemaPeerConfig, error) {
	if c == nil {
		return lookup()
	}
	if found, ok := c.found[peer]; ok {
		return found, nil
	}
	if err, ok := c.outside[peer]; ok {
		return sharedSchemaPeerConfig{}, err
	}
	found, err := lookup()
	if peerConfigOutsideRepository(err) {
		c.outside[peer] = err
	}
	if err != nil {
		return sharedSchemaPeerConfig{}, err
	}
	c.found[peer] = found
	return found, nil
}

func sharedSchemaSideOf(result *ghclient.SchemaRequestResult) api.SharedSchemaDatabase {
	return api.SharedSchemaDatabase{
		Database:     result.Database,
		ConfigPath:   result.ConfigPath,
		SchemaFiles:  result.SchemaFiles,
		IgnoreTables: result.IgnoreTables,
	}
}

// peerConfigOutsideRepository reports whether a peer's schema request failed
// only because its schemabot.yaml is not in this repository. A search that
// skipped configs it could not read is not conclusive: the peer's config may
// be one of them, so that miss fails closed like any other read failure.
func peerConfigOutsideRepository(err error) bool {
	if errors.Is(err, ghclient.ErrNoConfig) {
		return true
	}
	var notFound *ghclient.DatabaseNotFoundError
	return errors.As(err, &notFound) && notFound.Conclusive()
}

// enforceSharedSchemaIgnoreLists refuses an apply or apply-confirm of a
// database whose ignore_tables disagree with a database it shares a schema
// with. The plan of the same commit has already failed the environment's check
// for the same reason; refusing the apply keeps a completed apply from storing
// a passing row over it. A mismatch is answered on the PR and returns blocked;
// a failure to read the other database returns the error, so the command fails
// closed and can be retried. On success it returns the inputs of the databases
// it compared with, for the apply's base freshness checks.
func (h *Handler) enforceSharedSchemaIgnoreLists(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, installationID int64, environment, requestedBy, commandName string, schemaResult *ghclient.SchemaRequestResult, suppressRetryComments bool) ([]sharedSchemaPeerInputs, bool, error) {
	peerInputs, err := h.verifySharedSchemaIgnoreLists(ctx, client, repo, pr, environment, commandName, schemaResult, nil)
	if err == nil {
		return peerInputs, false, nil
	}
	var mismatch *api.SharedSchemaIgnoreListsError
	if errors.As(err, &mismatch) {
		h.postCommandError(repo, pr, installationID, commandName, environment, requestedBy, userFacingError(err))
		return nil, true, nil
	}
	h.logger.Error("shared schema ignore_tables check could not complete; the command is refused",
		"repo", repo, "pr", pr, "head_sha", schemaResult.HeadSHA, "database", schemaResult.Database, "database_type", schemaResult.Type,
		"environment", environment, "command", commandName, "error", err)
	if !suppressRetryComments {
		h.postCommandError(repo, pr, installationID, commandName, environment, requestedBy, userFacingError(err))
	}
	return nil, false, err
}
