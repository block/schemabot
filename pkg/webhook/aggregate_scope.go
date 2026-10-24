package webhook

import (
	"context"
	"fmt"

	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/storage"
)

// aggregatePRScope is evidence from successful discovery at one PR head, rather
// than an inference from stored rows. Missing results for its local targets
// remain blocking even before a planner has stored its first row.
type aggregatePRScope struct {
	files               []ghclient.PRFile
	configs             []ghclient.DiscoveredConfig
	local               []*storage.Check
	participantSelected bool
	environments        []string
}

func (h *Handler) discoverAggregatePRScope(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, headSHA string) (*aggregatePRScope, error) {
	prInfo, err := client.FetchPullRequest(ctx, repo, pr)
	if err != nil {
		return nil, fmt.Errorf("read PR before aggregate scope %s#%d: %w", repo, pr, err)
	}
	files, err := client.FetchPRFiles(ctx, repo, pr)
	if err != nil {
		return nil, fmt.Errorf("list files for aggregate scope %s#%d: %w", repo, pr, err)
	}
	files, _, err = client.PRFilesProposedAgainstDefaultBranch(ctx, repo, headSHA, files)
	if err != nil {
		return nil, fmt.Errorf("scope aggregate files to proposed changes %s#%d: %w", repo, pr, err)
	}
	configs, err := client.FindConfigsForPRFiles(ctx, repo, headSHA, files)
	if err != nil {
		return nil, fmt.Errorf("discover aggregate configs %s#%d: %w", repo, pr, err)
	}
	if unmanaged := h.unmanagedServerManagedSchemaChanges(repo, files, configs); len(unmanaged) > 0 {
		return nil, fmt.Errorf("aggregate scope %s#%d has managed schema paths without configs: %v", repo, pr, unmanaged)
	}
	environments, err := h.service.Config().CheckEnvironmentsForRepo(ctx, client, repo, headSHA)
	if err != nil {
		return nil, err
	}
	scope := &aggregatePRScope{files: files, configs: configs, environments: environments}
	config := h.service.Config()
	// Participants use their configured managed paths for the same conservative
	// selection as leaders. A touched README within those paths still needs a
	// no-work report; a PR outside them stays silent.
	scope.participantSelected = h.participantPathsTouched(repo, files)
	for _, tenant := range config.ExpectedParticipantChecksForPR(repo, prFilePaths(files)) {
		// A leader cannot infer per-database overrides in another deployment.
		// Only the selected participant's coverage uses the global order;
		// local databases are validated against their own effective orders.
		var covered []string
		for _, env := range environments {
			if tenant.PublishesEnvironment(env) {
				covered = append(covered, env)
			}
		}
		if err := config.ValidateCheckEnvironmentOrder("", covered); err != nil {
			return nil, fmt.Errorf("validate environment order for participant %q: %w", tenant.Tenant, err)
		}
	}

	for _, cfg := range configs {
		if cfg.Config == nil {
			return nil, fmt.Errorf("aggregate scope %s#%d has no parsed config at %s", repo, pr, cfg.SchemaDir)
		}
		if !h.configPathManagedByRepo(ctx, repo, pr, headSHA, cfg.Config, cfg.SchemaDir, "aggregate_scope") {
			// A leader may defer only to a declared participant or an explicit
			// directory policy. An unknown database in its own managed paths is
			// unresolved ownership, never proof that an environment has no work.
			if config.IsAggregateLeaderForRepo(repo) && !config.ExpectedTenantManagesSchemaPath(repo, cfg.SchemaDir) && !config.RepoHasSchemaDirAllowlist(repo) {
				return nil, fmt.Errorf("aggregate scope %s#%d cannot resolve ownership for database %q at %s", repo, pr, cfg.Config.Database, cfg.SchemaDir)
			}
			continue
		}
		environments, err := h.configuredDatabaseEnvironments(cfg.Config.Database)
		if err != nil {
			return nil, fmt.Errorf("resolve aggregate environments %s#%d: %w", repo, pr, err)
		}
		if len(environments) == 0 {
			return nil, fmt.Errorf("aggregate scope %s#%d database %q has no configured environments", repo, pr, cfg.Config.Database)
		}
		if err := config.ValidateCheckEnvironmentOrder(cfg.Config.Database, environments); err != nil {
			return nil, err
		}
		scope.participantSelected = true
		for _, environment := range environments {
			if config.IsEnvironmentAllowed(environment) {
				scope.local = append(scope.local, &storage.Check{
					Environment: environment, DatabaseType: string(cfg.Config.GetType()), DatabaseName: cfg.Config.Database,
				})
			}
		}
	}
	moved, err := h.autoPlanInputsMoved(ctx, client, repo, pr, headSHA, prInfo.BaseRef)
	if err != nil {
		return nil, fmt.Errorf("re-verify aggregate scope %s#%d: %w", repo, pr, err)
	}
	if moved {
		return nil, fmt.Errorf("PR %s#%d moved during aggregate discovery", repo, pr)
	}
	return scope, nil
}

func (s *aggregatePRScope) withPendingChecks(checks []*storage.Check, repo string, pr int, headSHA string) []*storage.Check {
	for _, target := range s.local {
		found := false
		for _, check := range checks {
			if check.Environment == target.Environment && check.DatabaseType == target.DatabaseType && check.DatabaseName == target.DatabaseName {
				found = true
				break
			}
		}
		if !found {
			pending := *target
			pending.Repository = repo
			pending.PullRequest = pr
			pending.HeadSHA = headSHA
			pending.Status = checkStatusInProgress
			checks = append(checks, &pending)
		}
	}
	return checks
}
