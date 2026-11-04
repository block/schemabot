package planetscale

import (
	"fmt"
	"strings"
)

// branchNamePrefix starts every branch name SchemaBot generates.
const branchNamePrefix = "schemabot-"

// generatedBranchIDLen is how many trailing characters of the plan identifier
// a generated branch name carries.
const generatedBranchIDLen = 8

// GenerateBranchName is the name of the branch SchemaBot creates for a plan on
// a PlanetScale database: the prefix, the database name with underscores
// replaced and truncated, and the tail of the plan identifier. It is a pure
// function of its inputs, so an apply re-run for the same plan names the same
// branch, and a reader that knows the database and the plan can name the
// branch without the apply having recorded it.
func GenerateBranchName(database, planID string) string {
	shortID := planID
	if len(shortID) > generatedBranchIDLen {
		shortID = shortID[len(shortID)-generatedBranchIDLen:]
	}
	return branchNamePrefix + sanitizedBranchDatabase(database) + "-" + shortID
}

// IsGeneratedBranchName reports whether branch has the shape SchemaBot gives the
// branches it creates for database (GenerateBranchName): the prefix for the
// database followed by exactly the tail a minted plan identifier leaves, which
// is generatedBranchIDLen lowercase hex digits. A name an operator supplied
// with the branch option, or wrote by hand with a readable suffix, does not
// have it. A caller deciding whether a branch is SchemaBot's to reclaim treats
// only these as candidates, and still checks the applies in storage, because
// a hand-written suffix of the same alphabet and length is indistinguishable
// by shape alone.
func IsGeneratedBranchName(database, branch string) bool {
	prefix := branchNamePrefix + sanitizedBranchDatabase(database) + "-"
	rest, ok := strings.CutPrefix(branch, prefix)
	if !ok {
		return false
	}
	return isGeneratedBranchID(rest)
}

func isGeneratedBranchID(id string) bool {
	if len(id) != generatedBranchIDLen {
		return false
	}
	for _, r := range id {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') {
			return false
		}
	}
	return true
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
