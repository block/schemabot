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
func (h *Handler) verifySharedSchemaIgnoreLists(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, environment, source string, schemaResult *ghclient.SchemaRequestResult, peerConfigs *sharedSchemaPeerConfigs) error {
	config := h.service.Config()
	peers, err := config.SharedSchemaPeers(repo, schemaResult.Database, environment)
	if err != nil {
		return fmt.Errorf("find the databases that share a target with %q in %s: %w", schemaResult.Database, environment, err)
	}
	if len(peers) == 0 {
		return nil
	}

	peerSides := make([]api.SharedSchemaDatabase, 0, len(peers))
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
			return &sharedSchemaPeerUnreadableError{Peer: peer, Database: schemaResult.Database, Environment: environment, Cause: err}
		}
		peerSides = append(peerSides, sharedSchemaSideOf(peerResult))
	}

	err = config.CheckSharedSchemaIgnoreLists(environment, sharedSchemaSideOf(schemaResult), peerSides)
	var mismatch *api.SharedSchemaIgnoreListsError
	if errors.As(err, &mismatch) {
		h.logger.Warn("schema files and ignore_tables disagree with a database sharing the same schema; the environment is refused until they agree",
			"repo", repo, "pr", pr, "head_sha", schemaResult.HeadSHA, "database", schemaResult.Database, "database_type", schemaResult.Type,
			"environment", environment, "peer_databases", peers, "error", err)
		return err
	}
	if err != nil {
		return fmt.Errorf("compare ignore_tables of %q with the databases sharing its targets in %s: %w", schemaResult.Database, environment, err)
	}
	h.logger.Debug("ignore_tables agree with every database sharing a schema",
		"repo", repo, "pr", pr, "database", schemaResult.Database, "environment", environment, "peer_databases", peers)
	return nil
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
// scans only the directories the server allows the peer's config in, so the
// cost stays with the peer's own inputs however many other configs the
// repository holds (AV-15). Only a peer allowed anywhere in the repository is
// searched for the way a command naming it with -d would be.
func (h *Handler) findSharedSchemaPeerConfig(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, headSHA, peer string) (sharedSchemaPeerConfig, error) {
	config, configDir, scoped, err := client.FindConfigByDatabaseNameInItsDirs(ctx, repo, headSHA, peer)
	if scoped {
		return sharedSchemaPeerConfig{config: config, configDir: configDir}, err
	}
	h.logger.Info("database sharing a schema has no directories bounding where its config lives; searching the repository for it",
		"repo", repo, "pr", pr, "head_sha", headSHA, "peer_database", peer)
	config, configDir, err = client.FindConfigByDatabaseName(ctx, repo, pr, peer)
	return sharedSchemaPeerConfig{config: config, configDir: configDir}, err
}

// sharedSchemaPeerConfigs remembers, for one delivery, where each peer's
// schemabot.yaml was found, so planning several environments looks each peer
// up once. A failed lookup is not remembered: the next environment tries it
// again, and fails closed again if it fails again.
type sharedSchemaPeerConfigs struct {
	found map[string]sharedSchemaPeerConfig
}

type sharedSchemaPeerConfig struct {
	config    *ghclient.SchemabotConfig
	configDir string
}

func newSharedSchemaPeerConfigs() *sharedSchemaPeerConfigs {
	return &sharedSchemaPeerConfigs{found: map[string]sharedSchemaPeerConfig{}}
}

func (c *sharedSchemaPeerConfigs) find(peer string, lookup func() (sharedSchemaPeerConfig, error)) (sharedSchemaPeerConfig, error) {
	if c == nil {
		return lookup()
	}
	if found, ok := c.found[peer]; ok {
		return found, nil
	}
	found, err := lookup()
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
// closed and can be retried.
func (h *Handler) enforceSharedSchemaIgnoreLists(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, installationID int64, environment, requestedBy, commandName string, schemaResult *ghclient.SchemaRequestResult, suppressRetryComments bool) (bool, error) {
	err := h.verifySharedSchemaIgnoreLists(ctx, client, repo, pr, environment, commandName, schemaResult, nil)
	if err == nil {
		return false, nil
	}
	var mismatch *api.SharedSchemaIgnoreListsError
	if errors.As(err, &mismatch) {
		h.postCommandError(repo, pr, installationID, commandName, environment, requestedBy, userFacingError(err))
		return true, nil
	}
	if !suppressRetryComments {
		h.postCommandError(repo, pr, installationID, commandName, environment, requestedBy, userFacingError(err))
	}
	return false, err
}
