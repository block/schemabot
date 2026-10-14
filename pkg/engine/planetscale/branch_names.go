package planetscale

import (
	"fmt"
	"strings"
	"unicode/utf8"
)

// branchNamePrefix starts every branch name SchemaBot generates.
const branchNamePrefix = "schemabot-"

// generatedBranchIDLen is how many trailing characters of the plan identifier
// a generated branch name carries.
const generatedBranchIDLen = 8

// IsGeneratedBranchName reports whether branch has the shape SchemaBot gives the
// branches it creates for database (generateBranchName): a name an operator
// supplied with the branch option, or any other branch, does not. A caller
// deciding whether a branch is SchemaBot's to reclaim treats only these as
// candidates.
func IsGeneratedBranchName(database, branch string) bool {
	prefix := branchNamePrefix + sanitizedBranchDatabase(database) + "-"
	rest, ok := strings.CutPrefix(branch, prefix)
	if !ok {
		return false
	}
	n := utf8.RuneCountInString(rest)
	return n > 0 && n <= generatedBranchIDLen
}

// sanitizedBranchDatabase is the database part of a generated branch name.
func sanitizedBranchDatabase(database string) string {
	sanitized := strings.ReplaceAll(database, "_", "-")
	if len(sanitized) > 20 {
		sanitized = sanitized[:20]
	}
	return sanitized
}

// BranchNameFromResumeMetadata returns the branch named by a PlanetScale
// apply's stored resume metadata, or "" when it names none yet. Metadata that
// cannot be decoded is an error, so a caller deciding whether a branch is in
// use never reads an unreadable record as "no branch".
func BranchNameFromResumeMetadata(metadata string) (string, error) {
	if metadata == "" {
		return "", nil
	}
	meta, err := decodePSMetadata(metadata)
	if err != nil {
		return "", fmt.Errorf("read branch from PlanetScale resume metadata: %w", err)
	}
	return meta.BranchName, nil
}
