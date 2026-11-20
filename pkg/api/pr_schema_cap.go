package api

import (
	"context"
	"fmt"
	"strings"

	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/tern"
)

// PRSchemaCapConfig controls admission for work associated with a repository's
// pull requests. A configured cap defaults to five managed databases.
type PRSchemaCapConfig struct {
	Limit        int    `yaml:"limit,omitempty"`
	Observe      bool   `yaml:"observe,omitempty"`
	ControlPlane string `yaml:"control_plane,omitempty"`
	Label        string `yaml:"label,omitempty"`
	ManageLabel  *bool  `yaml:"manage_label,omitempty"`
}

func (c *ServerConfig) PRSchemaCapForRepo(repo string) PRSchemaCapConfig {
	if c == nil {
		return PRSchemaCapConfig{}
	}
	policy := c.Repos[storage.CanonicalKey(repo)].PRSchemaCap
	if policy == nil {
		return PRSchemaCapConfig{}
	}
	result := *policy
	if result.Limit == 0 {
		result.Limit = 5
	}
	if result.ControlPlane == "" {
		result.ControlPlane = c.Tenant
	}
	if result.ControlPlane == "" {
		result.ControlPlane = c.GitHubCheckNameBaseForRepo(repo)
	}
	return result
}

func (c *ServerConfig) validatePRSchemaCap(repo string, rc RepoConfig) error {
	cap := rc.PRSchemaCap
	if cap == nil {
		return nil
	}
	field := fmt.Sprintf("repos.%s.pr_schema_cap", repo)
	if cap.Limit < 0 {
		return fmt.Errorf("%s.limit must be positive", field)
	}
	if cap.Label == "" {
		return fmt.Errorf("%s.label is required", field)
	}
	if cap.Label != strings.TrimSpace(cap.Label) || len(cap.Label) > 50 {
		return fmt.Errorf("%s.label must be a GitHub label of up to 50 characters", field)
	}
	if rc.ListenOnly && !cap.Observe {
		return fmt.Errorf("%s requires observation mode for listen-only repositories", field)
	}
	return nil
}

// PRSchemaAdmissionError reports a refusal before PR-associated work starts.
type PRSchemaAdmissionError struct {
	Cause error
}

func (e *PRSchemaAdmissionError) Error() string { return e.Cause.Error() }
func (e *PRSchemaAdmissionError) Unwrap() error { return e.Cause }

func (s *Service) admitPRSchemaWork(ctx context.Context, repo string, pr int, head string) error {
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

func (s *Service) withPRSchemaPlanAdmission(ctx context.Context, client tern.Client, repo string, pr int, head string) (context.Context, error) {
	policy := s.config.PRSchemaCapForRepo(repo)
	if pr <= 0 || policy.Limit == 0 || policy.Observe {
		return ctx, nil
	}
	admit := func(ctx context.Context) error { return s.admitPRSchemaWork(ctx, repo, pr, head) }
	if _, remote := client.(*tern.GRPCClient); remote {
		return tern.WithPlanAdmission(ctx, admit), nil
	}
	return ctx, admit(ctx)
}
