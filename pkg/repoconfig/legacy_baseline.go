// Package repoconfig contains configuration shared by the CLI and GitHub
// integration for files owned by a consumer repository.
package repoconfig

import (
	"fmt"
	"path"
	"regexp"
	"strings"
)

const LegacyBaselineVersion = 1

var fullCommitSHA = regexp.MustCompile(`^[0-9a-fA-F]{40}$`)

// LegacyBaseline records the base-branch commit through which a declarative
// schema accounts for changes under its former schema paths. Callers validate
// it whenever present; history checks stop for each path once it is absent from
// the current base branch, so the metadata can remain after the handoff.
type LegacyBaseline struct {
	Version     int      `yaml:"version" json:"version"`
	BaseCommit  string   `yaml:"base_commit" json:"base_commit"`
	LegacyPaths []string `yaml:"legacy_paths" json:"legacy_paths"`
}

// Validate rejects metadata that cannot safely scope a history check. A path
// names one repository-relative file or directory exactly; globs and the
// repository root are deliberately unsupported because either can make the
// protected surface broader or less obvious than the reviewed metadata.
func (b *LegacyBaseline) Validate() error {
	if b == nil {
		return fmt.Errorf("legacy_baseline is required")
	}
	if b.Version != LegacyBaselineVersion {
		return fmt.Errorf("legacy_baseline.version must be %d", LegacyBaselineVersion)
	}
	if !fullCommitSHA.MatchString(b.BaseCommit) {
		return fmt.Errorf("legacy_baseline.base_commit must be a full 40-character commit SHA")
	}
	if len(b.LegacyPaths) == 0 {
		return fmt.Errorf("legacy_baseline.legacy_paths must contain at least one path")
	}
	seen := make(map[string]struct{}, len(b.LegacyPaths))
	for _, legacyPath := range b.LegacyPaths {
		if strings.TrimSpace(legacyPath) != legacyPath || legacyPath == "" {
			return fmt.Errorf("legacy_baseline path %q must be non-empty and contain no leading or trailing whitespace", legacyPath)
		}
		if strings.Contains(legacyPath, `\`) || strings.ContainsAny(legacyPath, "*?[") {
			return fmt.Errorf("legacy_baseline path %q must be an exact repository-relative path", legacyPath)
		}
		cleaned := path.Clean(legacyPath)
		if path.IsAbs(legacyPath) || cleaned != legacyPath || cleaned == "." || cleaned == ".." || strings.HasPrefix(cleaned, "../") {
			return fmt.Errorf("legacy_baseline path %q must be a normalized repository-relative path below the repository root", legacyPath)
		}
		if _, ok := seen[legacyPath]; ok {
			return fmt.Errorf("legacy_baseline path %q is duplicated", legacyPath)
		}
		seen[legacyPath] = struct{}{}
	}
	return nil
}
