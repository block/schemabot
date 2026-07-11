package api

import (
	"fmt"
	"strings"
)

// Aggregate-check roles for a repository. In a multi-tenant aggregate check,
// several SchemaBot deployments coordinate one shared Check Run per environment
// through GitHub: exactly one deployment is the leader, the rest are
// participants.
const (
	// AggregateRoleLeader marks a deployment as the sole creator and writer of a
	// repository's aggregate Check Run. The leader folds every participating
	// tenant's reported state into the single check.
	AggregateRoleLeader = "leader"

	// AggregateRoleParticipant marks a deployment that reports only its own state
	// for a repository and never writes the aggregate check.
	AggregateRoleParticipant = "participant"
)

// AggregateConfig configures this deployment's role in a multi-tenant aggregate
// check for one repository. It is set under repos.<repo>.aggregate. When unset,
// the repository uses the standard single-deployment check behavior.
type AggregateConfig struct {
	// Role is this deployment's role for the repo: "leader" or "participant".
	Role string `yaml:"role"`

	// ExpectedTenants is the set of tenants expected to participate on this repo,
	// each with the repo-relative directory prefixes it manages (same matching as
	// databases.<db>.allowed_dirs — directory prefixes, not globs). It is required
	// for, and only meaningful to, a leader: it is the fail-closed safety contract
	// for the aggregate fold. A leader's database config cannot know other tenants'
	// databases, so this is the only place the leader learns which tenants to
	// expect — at tenant granularity, with the path prefixes used to decide which
	// tenants a given PR touches. Must be empty for a participant.
	ExpectedTenants []ExpectedTenant `yaml:"expected_tenants,omitempty"`
}

// ExpectedTenant names a tenant expected to participate on a repository and the
// repo-relative directory prefixes it manages there. The leader uses these
// prefixes against a PR's changed files to decide which tenants must report
// before the aggregate check can pass.
type ExpectedTenant struct {
	Tenant string   `yaml:"tenant"`
	Paths  []string `yaml:"paths"`

	// CheckName is the participant's aggregate check-name base (e.g.
	// "SchemaBot BB Block"). The leader env-scopes it with
	// aggregateCheckNameForEnv to find the participant's per-environment Check
	// Run and fold it into the aggregate. It is required on a leader's expected
	// tenants: the leader cannot gate on a participant whose Check Run name it
	// cannot resolve, so a missing name is a fail-closed configuration error.
	CheckName string `yaml:"check_name"`
}

func (a *AggregateConfig) isLeader() bool {
	return a != nil && a.Role == AggregateRoleLeader
}

// AggregateRoleForRepo returns this deployment's configured aggregate role for
// repo ("leader" or "participant"), or "" if the repo has no aggregate config.
func (c *ServerConfig) AggregateRoleForRepo(repo string) string {
	if c == nil {
		return ""
	}
	repoConfig, ok := c.Repos[repo]
	if !ok || repoConfig.Aggregate == nil {
		return ""
	}
	return repoConfig.Aggregate.Role
}

// IsAggregateLeaderForRepo reports whether this deployment is the aggregate-check
// leader (the sole check writer) for repo.
func (c *ServerConfig) IsAggregateLeaderForRepo(repo string) bool {
	return c.AggregateRoleForRepo(repo) == AggregateRoleLeader
}

// ExpectedTenantsForPR returns the tenants expected to report on repo for a PR
// touching changedFiles: the subset of the repo's expected-tenant set whose
// managed path prefixes cover at least one changed file. This is the fail-closed
// contract for the aggregate fold — every returned tenant must report
// terminal-success before the check can pass. Returns nil when this deployment
// is not the leader for repo, since only the leader holds the expected set.
func (c *ServerConfig) ExpectedTenantsForPR(repo string, changedFiles []string) []string {
	expected := c.ExpectedParticipantChecksForPR(repo, changedFiles)
	if expected == nil {
		return nil
	}
	names := make([]string, 0, len(expected))
	for _, tenant := range expected {
		names = append(names, tenant.Tenant)
	}
	return names
}

// ExpectedParticipantChecksForPR returns the full expected-tenant entries
// (tenant, paths, and participant check-name base) that the leader must gate on
// for a PR touching changedFiles: the subset of the repo's expected-tenant set
// whose managed path prefixes cover at least one changed file. The leader
// env-scopes each returned CheckName to find the participant's per-environment
// Check Run and fold it into the aggregate. Returns nil when this deployment is
// not the leader for repo, since only the leader holds the expected set.
func (c *ServerConfig) ExpectedParticipantChecksForPR(repo string, changedFiles []string) []ExpectedTenant {
	if c == nil {
		return nil
	}
	repoConfig, ok := c.Repos[repo]
	if !ok || !repoConfig.Aggregate.isLeader() {
		return nil
	}
	var expected []ExpectedTenant
	for _, tenant := range repoConfig.Aggregate.ExpectedTenants {
		if anyPathCovered(tenant.Paths, changedFiles) {
			expected = append(expected, tenant)
		}
	}
	return expected
}

// anyPathCovered reports whether any changed file falls under one of dirs, using
// the same directory-prefix matching as databases.<db>.allowed_dirs.
func anyPathCovered(dirs, changedFiles []string) bool {
	for _, file := range changedFiles {
		if schemaPathAllowed(dirs, file) {
			return true
		}
	}
	return false
}

// validateAggregateConfig validates a repository's aggregate config. A leader
// must carry a non-empty, well-formed expected-tenant set; a participant must
// not (only the leader holds it). Fails closed on any ambiguity so a
// misconfiguration cannot silently weaken the gate.
func validateAggregateConfig(repo string, agg *AggregateConfig) error {
	if agg == nil {
		return nil
	}

	switch agg.Role {
	case AggregateRoleLeader, AggregateRoleParticipant:
	case "":
		return fmt.Errorf("repos.%s.aggregate.role is required (must be %q or %q)", repo, AggregateRoleLeader, AggregateRoleParticipant)
	default:
		return fmt.Errorf("repos.%s.aggregate.role %q is invalid (must be %q or %q)", repo, agg.Role, AggregateRoleLeader, AggregateRoleParticipant)
	}

	if agg.Role == AggregateRoleParticipant {
		if len(agg.ExpectedTenants) > 0 {
			return fmt.Errorf("repos.%s.aggregate.expected_tenants must be empty for role %q (only the leader holds the expected-tenant set)", repo, AggregateRoleParticipant)
		}
		return nil
	}

	if len(agg.ExpectedTenants) == 0 {
		return fmt.Errorf("repos.%s.aggregate.expected_tenants is required for role %q", repo, AggregateRoleLeader)
	}
	seen := make(map[string]struct{}, len(agg.ExpectedTenants))
	for _, tenant := range agg.ExpectedTenants {
		name := strings.TrimSpace(tenant.Tenant)
		if name == "" {
			return fmt.Errorf("repos.%s.aggregate.expected_tenants contains an entry with an empty tenant", repo)
		}
		if _, ok := seen[name]; ok {
			return fmt.Errorf("repos.%s.aggregate.expected_tenants contains duplicate tenant %q", repo, name)
		}
		seen[name] = struct{}{}
		if strings.TrimSpace(tenant.CheckName) == "" {
			return fmt.Errorf("repos.%s.aggregate.expected_tenants[%q].check_name is required for role %q", repo, name, AggregateRoleLeader)
		}
		if len(tenant.Paths) == 0 {
			return fmt.Errorf("repos.%s.aggregate.expected_tenants[%q].paths is empty", repo, name)
		}
		for _, p := range tenant.Paths {
			if _, err := normalizeSchemaPath(p); err != nil {
				return fmt.Errorf("repos.%s.aggregate.expected_tenants[%q].paths contains invalid value %q: %w", repo, name, p, err)
			}
		}
	}
	return nil
}
