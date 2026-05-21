package api

import (
	"errors"
	"fmt"
	"path"
	"strings"
)

const (
	SourcePolicyReasonUnknown               = "unknown"
	SourcePolicyReasonMissingServerConfig   = "missing_server_config"
	SourcePolicyReasonMissingDatabaseConfig = "missing_database_config"
	SourcePolicyReasonMissingRepository     = "missing_repository"
	SourcePolicyReasonMissingPullRequest    = "missing_pull_request"
	SourcePolicyReasonMissingSchemaPath     = "missing_schema_path"
	SourcePolicyReasonUnauthorizedRepo      = "unauthorized_repo"
	SourcePolicyReasonUnauthorizedSchemaDir = "unauthorized_schema_dir"
)

// SourcePolicyError is returned when server-side source policy blocks a plan or
// apply. Reason is stable for metrics and API clients; Message is the
// operator-facing text shown by PR comments and direct API errors.
type SourcePolicyError struct {
	Reason  string
	Message string
}

func (e *SourcePolicyError) Error() string {
	return e.Message
}

func sourcePolicyReason(err error) string {
	var policyErr *SourcePolicyError
	if errors.As(err, &policyErr) {
		return policyErr.Reason
	}
	return SourcePolicyReasonUnknown
}

// PlanSourcePolicyRequest is the source metadata used for database ownership
// checks.
type PlanSourcePolicyRequest struct {
	Database    string
	Repository  string
	PullRequest int
	SchemaPath  string
}

// AuthorizePlanSource verifies that a trusted GitHub PR source is allowed to
// manage the requested database. Direct operator/API paths skip this helper at
// the caller because they do not have SchemaBot-discovered source metadata.
func (c *ServerConfig) AuthorizePlanSource(req PlanSourcePolicyRequest) error {
	if c == nil {
		return &SourcePolicyError{
			Reason:  SourcePolicyReasonMissingServerConfig,
			Message: fmt.Sprintf("database %q source policy could not be evaluated because server config is missing", req.Database),
		}
	}

	dbConfig := c.Database(req.Database)
	if dbConfig == nil {
		return &SourcePolicyError{
			Reason:  SourcePolicyReasonMissingDatabaseConfig,
			Message: fmt.Sprintf("database %q source policy could not be evaluated because database config is missing", req.Database),
		}
	}
	if !dbConfig.hasSourcePolicy() {
		return nil
	}

	if strings.TrimSpace(req.Repository) == "" {
		return &SourcePolicyError{
			Reason:  SourcePolicyReasonMissingRepository,
			Message: fmt.Sprintf("database %q has source policy configured, but the plan source repository is missing", req.Database),
		}
	}
	if req.PullRequest <= 0 {
		return &SourcePolicyError{
			Reason:  SourcePolicyReasonMissingPullRequest,
			Message: fmt.Sprintf("database %q has source policy configured, but the plan source pull request is missing", req.Database),
		}
	}
	if strings.TrimSpace(req.SchemaPath) == "" {
		return &SourcePolicyError{
			Reason:  SourcePolicyReasonMissingSchemaPath,
			Message: fmt.Sprintf("database %q has source policy configured, but the plan source schema path is missing", req.Database),
		}
	}

	if len(dbConfig.AllowedRepos) > 0 {
		if !repoAllowed(dbConfig.AllowedRepos, req.Repository) {
			return &SourcePolicyError{
				Reason: SourcePolicyReasonUnauthorizedRepo,
				Message: fmt.Sprintf(
					"repo %q is not authorized for database %q; update databases.%s.allowed_repos in server config if this is intentional",
					req.Repository, req.Database, req.Database,
				),
			}
		}
	}

	if len(dbConfig.AllowedDirs) > 0 {
		if !schemaPathAllowed(dbConfig.AllowedDirs, req.SchemaPath) {
			return &SourcePolicyError{
				Reason: SourcePolicyReasonUnauthorizedSchemaDir,
				Message: fmt.Sprintf(
					"schema path %q is not authorized for database %q; update databases.%s.allowed_dirs in server config if this is intentional",
					req.SchemaPath, req.Database, req.Database,
				),
			}
		}
	}

	return nil
}

func (d DatabaseConfig) hasSourcePolicy() bool {
	return len(d.AllowedRepos) > 0 || len(d.AllowedDirs) > 0
}

func validateDatabaseSourcePolicy(database string, dbConfig DatabaseConfig) error {
	if err := validateAllowedRepos("databases."+database+".allowed_repos", dbConfig.AllowedRepos); err != nil {
		return err
	}

	normalizedDirs := make(map[string]struct{}, len(dbConfig.AllowedDirs))
	for _, dir := range dbConfig.AllowedDirs {
		normalized, err := normalizeSchemaPath(dir)
		if err != nil {
			return fmt.Errorf("databases.%s.allowed_dirs contains invalid value %q: %w", database, dir, err)
		}
		if _, ok := normalizedDirs[normalized]; ok {
			return fmt.Errorf("databases.%s.allowed_dirs contains duplicate value %q", database, normalized)
		}
		normalizedDirs[normalized] = struct{}{}
	}

	return nil
}

func validateAllowedRepos(field string, repos []string) error {
	seen := make(map[string]struct{}, len(repos))
	for _, repo := range repos {
		trimmed := strings.TrimSpace(repo)
		if trimmed == "" {
			return fmt.Errorf("%s contains an empty value", field)
		}
		if trimmed != repo {
			return fmt.Errorf("%s contains value %q with leading or trailing whitespace", field, repo)
		}
		if _, ok := seen[trimmed]; ok {
			return fmt.Errorf("%s contains duplicate value %q", field, trimmed)
		}
		seen[trimmed] = struct{}{}
	}
	return nil
}

func repoAllowed(allowedRepos []string, repo string) bool {
	repo = strings.TrimSpace(repo)
	for _, allowed := range allowedRepos {
		switch strings.TrimSpace(allowed) {
		case "*":
			return true
		case repo:
			return true
		}
	}
	return false
}

func schemaPathAllowed(allowedDirs []string, schemaPath string) bool {
	cleanSchemaPath, err := normalizeSchemaPath(schemaPath)
	if err != nil {
		return false
	}

	for _, allowedDir := range allowedDirs {
		cleanAllowedDir, err := normalizeSchemaPath(allowedDir)
		if err != nil {
			return false
		}
		if cleanAllowedDir == "*" {
			return true
		}
		if cleanSchemaPath == cleanAllowedDir {
			return true
		}
		if cleanAllowedDir != "." && strings.HasPrefix(cleanSchemaPath, cleanAllowedDir+"/") {
			return true
		}
	}
	return false
}

func normalizeSchemaPath(value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", fmt.Errorf("path is empty")
	}
	if value == "*" {
		return "*", nil
	}
	if strings.HasPrefix(value, "/") {
		return "", fmt.Errorf("path must be repo-relative")
	}

	cleaned := path.Clean(value)
	if cleaned == ".." || strings.HasPrefix(cleaned, "../") {
		return "", fmt.Errorf("path must not escape the repository")
	}
	return cleaned, nil
}
