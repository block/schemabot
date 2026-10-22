package api

import (
	"context"
	"fmt"
	"slices"

	"github.com/block/schemabot/pkg/checkstate"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/storage"
)

// CheckEnvironmentDiscovery supplies verified repository coverage when database
// registrations do not bind their ownership to a particular repository.
type CheckEnvironmentDiscovery interface {
	FindAllConfigs(context.Context, string, string) (*ghclient.FindAllConfigsResult, error)
}

// CheckEnvironmentsForRepo resolves publication from registrations and repository
// ownership, independently of stored results. A nil discovery client returns a
// conservative candidate set, useful before a particular head has been read.
func (c *ServerConfig) CheckEnvironmentsForRepo(ctx context.Context, client CheckEnvironmentDiscovery, repo, headSHA string) ([]string, error) {
	if len(c.AllowedEnvironments) == 0 {
		return slices.Clone(c.AllowedEnvironments), nil
	}
	coverage := map[string]bool{}
	ambiguous := false
	for _, db := range c.DatabaseConfigs() {
		if !databaseAllowsRepo(db, repo) {
			continue
		}
		if len(db.AllowedRepos) == 0 || slices.Contains(db.AllowedRepos, "*") {
			ambiguous = true
			if client != nil {
				continue
			}
		}
		for env := range db.Environments {
			coverage[env] = true
		}
	}
	if ambiguous && client != nil {
		discovered, err := client.FindAllConfigs(ctx, repo, headSHA)
		if err != nil {
			return nil, fmt.Errorf("discover check coverage for %s@%s: %w", repo, headSHA, err)
		}
		if len(discovered.InvalidConfigs) > 0 {
			return nil, fmt.Errorf("check coverage for %s@%s contains invalid configs", repo, headSHA)
		}
		for _, found := range discovered.ValidConfigs {
			if found.Config == nil {
				return nil, fmt.Errorf("check coverage for %s has an unparsed config", repo)
			}
			db := c.Database(found.Config.Database)
			if db == nil {
				if c.ExpectedTenantManagesSchemaPath(repo, found.SchemaDir) {
					continue
				}
				if c.RepoHasSchemaDirAllowlist(repo) && !c.SchemaPathAllowedForRepo(repo, found.SchemaDir) {
					continue
				}
				if c.AggregateRoleForRepo(repo) == AggregateRoleParticipant {
					continue
				}
				return nil, fmt.Errorf("cannot verify check coverage for database %q in %s", found.Config.Database, repo)
			}
			if !databaseAllowsRepo(*db, repo) {
				continue
			}
			if len(db.AllowedDirs) > 0 && !schemaPathAllowed(db.AllowedDirs, found.SchemaDir) {
				continue
			}
			for env := range db.Environments {
				coverage[env] = true
			}
		}
	}
	if rc, ok := c.Repos[storage.CanonicalKey(repo)]; ok && rc.Aggregate.isLeader() {
		for _, tenant := range rc.Aggregate.ExpectedTenants {
			for _, env := range c.AllowedEnvironments {
				if tenant.PublishesEnvironment(env) {
					coverage[env] = true
				}
			}
		}
	}
	// With no registry this deployment has no evidence to narrow its established
	// required names. Missing configuration must never silently remove a gate.
	if len(c.DatabaseConfigs()) == 0 && len(coverage) == 0 {
		return slices.Clone(c.AllowedEnvironments), nil
	}
	var environments []string
	for _, env := range c.AllowedEnvironments {
		if coverage[env] {
			environments = append(environments, env)
		}
	}
	return environments, nil
}

// LocalCheckPrerequisites derives earlier, registered environments owned by
// this publisher. Retained results also preserve removed registrations.
func (c *ServerConfig) LocalCheckPrerequisites(database, environment string, retained []string) []string {
	order := c.PromotionOrderForDatabase(database)
	index := max(slices.Index(order, environment), 0)
	var result []string
	db := c.Database(database)
	for _, prior := range order[:index] {
		if !c.IsEnvironmentAllowed(prior) {
			continue
		}
		registered := false
		if db != nil {
			_, registered = db.Environments[prior]
		}
		if registered || slices.Contains(retained, prior) {
			result = append(result, prior)
		}
	}
	// Removed registrations can also remove the order that explained retained
	// work. Keep that work as a prerequisite until it can be reconciled safely.
	for _, prior := range retained {
		if prior == environment || !c.IsEnvironmentAllowed(prior) || slices.Contains(result, prior) {
			continue
		}
		if db == nil || !slices.Contains(order, prior) {
			result = append(result, prior)
		}
	}
	return result
}

// resolvedWebhookCheckNames uses the same coverage evidence as publication.
// Explicit operator overrides still name exactly the check requested.
func resolvedWebhookCheckNames(ctx context.Context, cfg *ServerConfig, client any, repo, headSHA, environment, override string, retained []*storage.Check) ([]webhookExpectedCheckName, error) {
	if override != "" || len(cfg.AllowedEnvironments) == 0 {
		return webhookExpectedCheckNames(cfg, repo, environment, override), nil
	}
	discovery, _ := client.(CheckEnvironmentDiscovery)
	environments, err := cfg.CheckEnvironmentsForRepo(ctx, discovery, repo, headSHA)
	if err != nil {
		return nil, err
	}
	for _, check := range retained {
		if check.Status == checkstate.StatusCompleted && check.Conclusion == checkstate.ConclusionSuccess && check.BlockingReason == "" {
			continue
		}
		if cfg.IsEnvironmentAllowed(check.Environment) && !slices.Contains(environments, check.Environment) {
			environments = append(environments, check.Environment)
		}
	}
	var result []webhookExpectedCheckName
	for _, env := range environments {
		if environment != "" && env != environment {
			continue
		}
		result = append(result, webhookExpectedCheckName{Name: fmt.Sprintf("%s (%s)", cfg.GitHubCheckNameBaseForRepo(repo), env), Environment: env})
	}
	return result, nil
}

// CheckRowsForEnvironment includes local predecessors that explain a later
// environment's gate. Missing evidence is represented as pending, just as in
// the publisher, rather than disappearing from environment-filtered inspection.
func (c *ServerConfig) CheckRowsForEnvironment(checks []*storage.Check, environment, repo string, pr int, headSHA string) []*storage.Check {
	if environment == "" {
		return checks
	}
	var result []*storage.Check
	for _, check := range checks {
		if check.Environment == environment {
			result = append(result, check)
		}
	}
	// Retained earlier work also gates a later no-work result, even when its
	// planner never stored a later row before the PR removed the schema files.
	for _, check := range checks {
		if checkstate.IsAggregate(check) || check.Environment == environment {
			continue
		}
		if check.HeadSHA == headSHA && check.Status == checkstate.StatusCompleted && check.Conclusion == checkstate.ConclusionSuccess && check.BlockingReason == "" {
			continue
		}
		if !slices.Contains(c.LocalCheckPrerequisites(check.DatabaseName, environment, []string{check.Environment}), check.Environment) {
			continue
		}
		present := slices.ContainsFunc(result, func(own *storage.Check) bool {
			return own.DatabaseName == check.DatabaseName && own.DatabaseType == check.DatabaseType
		})
		if !present {
			result = append(result, &storage.Check{Repository: repo, PullRequest: pr, HeadSHA: headSHA, Environment: environment, DatabaseType: check.DatabaseType, DatabaseName: check.DatabaseName, Status: checkstate.StatusInProgress})
		}
	}
	own := slices.Clone(result)
	seen := map[string]bool{}
	for _, check := range own {
		if checkstate.IsAggregate(check) {
			continue
		}
		var retained []string
		for _, row := range checks {
			if row.DatabaseName == check.DatabaseName && row.DatabaseType == check.DatabaseType {
				retained = append(retained, row.Environment)
			}
		}
		for _, prior := range c.LocalCheckPrerequisites(check.DatabaseName, environment, retained) {
			key := prior + "\x00" + check.DatabaseType + "\x00" + check.DatabaseName
			if seen[key] {
				continue
			}
			seen[key] = true
			evidence := &storage.Check{Repository: repo, PullRequest: pr, HeadSHA: headSHA, Environment: prior, DatabaseType: check.DatabaseType, DatabaseName: check.DatabaseName, Status: checkstate.StatusInProgress}
			for _, row := range checks {
				if row.Environment == prior && row.DatabaseName == check.DatabaseName && row.DatabaseType == check.DatabaseType {
					evidence = row
					break
				}
			}
			if evidence.HeadSHA == headSHA && evidence.Status == checkstate.StatusCompleted && evidence.Conclusion == checkstate.ConclusionSuccess && evidence.BlockingReason == "" {
				continue
			}
			result = append(result, evidence)
		}
	}
	return result
}
