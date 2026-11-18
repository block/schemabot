package api

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"
	"unicode"

	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/storage"
)

// PRSchemaCapPolicy sets the defaults for planning admission.
type PRSchemaCapPolicy struct {
	Limit          int    `yaml:"limit,omitempty"`
	Label          string `yaml:"label,omitempty"`
	Observe        bool   `yaml:"observe,omitempty"`
	PublishComment *bool  `yaml:"publish_comment,omitempty"`
}

// PRSchemaCapOverride contains the repo's policy overrides and its local
// publication and registration settings. A nil policy field inherits.
type PRSchemaCapOverride struct {
	Limit          *int                               `yaml:"limit,omitempty"`
	Label          *string                            `yaml:"label,omitempty"`
	Observe        *bool                              `yaml:"observe,omitempty"`
	PublishComment *bool                              `yaml:"publish_comment,omitempty"`
	Registrations  map[string]PRSchemaCapRegistration `yaml:"registrations,omitempty"`
}

// PRSchemaCapConfig is the effective admission config for a repository.
// A zero limit leaves planning uncapped.
type PRSchemaCapConfig struct {
	PRSchemaCapPolicy
	Registrations map[string]PRSchemaCapRegistration
}

// PRSchemaCapForRepo resolves the policy used by both webhook and API requests.
func (c *ServerConfig) PRSchemaCapForRepo(repo string) PRSchemaCapConfig {
	repo = storage.CanonicalKey(repo)
	if c == nil || repo == "" || !c.IsRepoAllowed(repo) {
		return PRSchemaCapConfig{}
	}
	return c.resolvePRSchemaCap(c.Repos[repo])
}

func (c *ServerConfig) resolvePRSchemaCap(rc RepoConfig) PRSchemaCapConfig {
	override := rc.PRSchemaCap
	policy := c.PRSchemaCap
	if override.Limit != nil {
		policy.Limit = *override.Limit
	}
	if override.Label != nil {
		policy.Label = *override.Label
	}
	if override.Observe != nil {
		policy.Observe = *override.Observe
	} else if rc.ListenOnly {
		policy.Observe = true
	}
	if override.PublishComment != nil {
		policy.PublishComment = override.PublishComment
	}
	return PRSchemaCapConfig{PRSchemaCapPolicy: policy, Registrations: override.Registrations}
}

// PRSchemaCapRegistration describes where a logical database is declared and
// which environment layouts it uses, including on other control planes.
type PRSchemaCapRegistration struct {
	Type         string   `yaml:"type"`
	Paths        []string `yaml:"paths,omitempty"`
	Environments []string `yaml:"environments"`
}

func (r PRSchemaCapRegistration) AllowsPath(dir string) bool {
	return len(r.Paths) == 0 || schemaPathAllowed(r.Paths, dir)
}

// PRSchemaCapRegistrations returns the repo-wide inventory used by admission.
// When configured explicitly each participating control plane uses that same
// inventory. Standalone deployments can use their existing database registry.
func (c *ServerConfig) PRSchemaCapRegistrations(repo string) (map[string]PRSchemaCapRegistration, error) {
	repo = storage.CanonicalKey(repo)
	if cap := c.Repos[repo].PRSchemaCap; len(cap.Registrations) > 0 {
		if err := c.validatePRSchemaCapCoverage(repo, cap.Registrations); err != nil {
			return nil, err
		}
		return cap.Registrations, nil
	}
	registrations := make(map[string]PRSchemaCapRegistration)
	for name, db := range c.DatabaseConfigs() {
		if len(db.AllowedRepos) > 0 && !repoAllowed(db.AllowedRepos, repo) {
			continue
		}
		registrations[name] = PRSchemaCapRegistration{
			Type: db.Type, Paths: db.AllowedDirs, Environments: slices.Sorted(maps.Keys(db.Environments)),
		}
	}
	return registrations, nil
}

func validatePRSchemaCapPolicy(prefix string, cap PRSchemaCapPolicy) error {
	if cap.Limit < 0 {
		return fmt.Errorf("%s.limit must be nonnegative", prefix)
	}
	if cap.Limit > 0 && (cap.Label == "" || len([]rune(cap.Label)) > 50 || cap.Label != strings.TrimSpace(cap.Label) ||
		strings.IndexFunc(cap.Label, unicode.IsControl) >= 0) {
		return fmt.Errorf("%s.label must be a GitHub label name of 1–50 characters", prefix)
	}
	return nil
}

func (c *ServerConfig) validateDefaultPRSchemaCap() error {
	if err := validatePRSchemaCapPolicy("pr_schema_cap", c.PRSchemaCap); err != nil {
		return err
	}
	if c.PRSchemaCap.Limit > 0 && c.TargetResolver.Enabled() && len(c.Repos) == 0 {
		return fmt.Errorf("pr_schema_cap requires explicit repos with registration inventories when dynamic target routing is enabled")
	}
	return nil
}

func (c *ServerConfig) validatePRSchemaCap(repo string, rc RepoConfig) error {
	cap := c.resolvePRSchemaCap(rc)
	prefix := fmt.Sprintf("repos.%s.pr_schema_cap", repo)
	if err := validatePRSchemaCapPolicy(prefix, cap.PRSchemaCapPolicy); err != nil {
		return err
	}
	if cap.Limit == 0 {
		return nil
	}
	if (rc.Aggregate != nil || c.TargetResolver.Enabled()) && len(cap.Registrations) == 0 {
		return fmt.Errorf("%s.registrations must contain the repo-wide inventory for aggregate or dynamically routed repositories", prefix)
	}
	if rc.Aggregate != nil && rc.Aggregate.Role == AggregateRoleParticipant && rc.PRSchemaCap.PublishComment != nil && *rc.PRSchemaCap.PublishComment {
		return fmt.Errorf("%s.publish_comment must be false on aggregate participants", prefix)
	}
	if rc.ListenOnly && !cap.Observe {
		return fmt.Errorf("%s.observe must be true for a listen-only repository", prefix)
	}
	for name, registration := range cap.Registrations {
		if name == "" || storage.CanonicalKey(name) != name || len(registration.Environments) == 0 {
			return fmt.Errorf("%s.registrations.%s requires a canonical database name and at least one environment", prefix, name)
		}
		switch registration.Type {
		case storage.DatabaseTypeMySQL, storage.DatabaseTypePostgres, storage.DatabaseTypeVitess, storage.DatabaseTypeStrata:
		default:
			return fmt.Errorf("%s.registrations.%s has invalid type %q", prefix, name, registration.Type)
		}
		if err := validateDatabaseSourcePolicy(name, DatabaseConfig{AllowedDirs: registration.Paths}); err != nil {
			return fmt.Errorf("%s.registrations: %w", prefix, err)
		}
		if err := validateUniqueNames(prefix+".registrations."+name+".environments", registration.Environments); err != nil {
			return err
		}
		for _, env := range registration.Environments {
			if err := schema.ValidateEnvironmentPathSegment(env); err != nil {
				return fmt.Errorf("%s.registrations.%s: %w", prefix, name, err)
			}
		}
	}
	if len(cap.Registrations) == 0 {
		return nil
	}
	return c.validatePRSchemaCapCoverage(repo, cap.Registrations)
}

func (c *ServerConfig) validatePRSchemaCapCoverage(repo string, registrations map[string]PRSchemaCapRegistration) error {
	prefix := fmt.Sprintf("repos.%s.pr_schema_cap", repo)
	for name, local := range c.DatabaseConfigs() {
		if !databaseAllowsRepo(local, repo) {
			continue
		}
		global, ok := registrations[name]
		if !ok || global.Type != local.Type {
			return fmt.Errorf("%s.registrations must cover the local %s database %q", prefix, local.Type, name)
		}
		for env := range local.Environments {
			if !slices.Contains(global.Environments, env) {
				return fmt.Errorf("%s.registrations.%s omits local environment %q", prefix, name, env)
			}
		}
		paths := local.AllowedDirs
		if len(paths) == 0 {
			paths = []string{"*"}
		}
		for _, dir := range paths {
			if !global.AllowsPath(dir) {
				return fmt.Errorf("%s.registrations.%s does not cover local schema path %q", prefix, name, dir)
			}
		}
	}
	return nil
}

// PRSchemaAdmissionError indicates that a PR-associated API request could not
// enter planning or application.
type PRSchemaAdmissionError struct {
	Cause error
}

func (e *PRSchemaAdmissionError) Error() string { return e.Cause.Error() }
func (e *PRSchemaAdmissionError) Unwrap() error { return e.Cause }

func (s *Service) admitPRSchemaRequest(ctx context.Context, repo string, pr int, head string) error {
	repo = storage.CanonicalKey(repo)
	policy := s.config.PRSchemaCapForRepo(repo)
	if pr <= 0 || policy.Limit == 0 || policy.Observe {
		return nil
	}
	if s.OnPRSchemaAdmission == nil {
		return &PRSchemaAdmissionError{Cause: fmt.Errorf("PR schema admission is unavailable for %s#%d", repo, pr)}
	}
	if err := s.OnPRSchemaAdmission(ctx, repo, pr, head); err != nil {
		return &PRSchemaAdmissionError{Cause: err}
	}
	return nil
}
