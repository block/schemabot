package schema

import (
	"fmt"
	"path"
	"sort"
	"strings"
)

// GroupFilesByNamespace groups schema files by namespace using their relative paths.
// Two layouts are supported:
//
//  1. Flat layout — SQL files directly in the schema directory. The namespace
//     key is defaultNamespace (the schema directory name = the MySQL database name).
//
//     schema/aurora_coffeeshop_exemplar/    → defaultNamespace = "aurora_coffeeshop_exemplar"
//     ├── schemabot.yaml
//     ├── baristas.sql                      → namespace: "aurora_coffeeshop_exemplar"
//     └── customers.sql                     → namespace: "aurora_coffeeshop_exemplar"
//
//  2. Subdirectory layout — SQL files in named subdirectories. Each subdirectory
//     name becomes the namespace key. defaultNamespace is not used.
//
//     schema/
//     ├── schemabot.yaml
//     ├── payments/
//     │   └── transactions.sql              → namespace: "payments"
//     └── payments_audit/
//     └── audit_log.sql                 → namespace: "payments_audit"
//
// Mixed layouts (both flat files and subdirectories) are rejected as ambiguous.
//
// Both the CLI (ReadSchemaFiles) and webhook (groupFilesByNamespace) call this
// function with the schema directory's base name as defaultNamespace:
//   - CLI:     filepath.Base(dir)       e.g. "aurora_coffeeshop_exemplar"
//   - Webhook: path.Base(schemaPath)    e.g. "aurora_coffeeshop_exemplar"
//
// The input map keys are relative paths (e.g., "users.sql" or "payments/users.sql")
// and values are file contents. Only .sql files and vschema.json are included;
// other files (like schemabot.yaml) are skipped.
//
// The environment parameter enables $ENV substitution in namespace names.
// If environment is non-empty, any literal "$ENV" in namespace keys (from
// directory names or defaultNamespace) is replaced with the environment value.
// This allows a single directory like "bikeshare_$ENV/" to resolve to
// "bikeshare_staging" or "bikeshare_production" depending on the target.
// If environment is empty, "$ENV" is left as-is.
//
// ignoreNamespaces lists namespaces to exclude from the result — schema
// directories that exist in the repository but must not be reconciled against
// the live database (for example a keyspace only used by local test
// infrastructure). Entries receive the same $ENV substitution as directory
// names and are matched against the post-substitution namespace keys. Layout
// validation still sees ignored directories: a mixed flat/subdirectory layout
// is rejected even when the subdirectories are all ignored.
//
// The second return value lists the namespace keys that were actually removed
// by ignoreNamespaces, sorted. An entry that matches no namespace removes
// nothing (matching is exact and case-sensitive); callers that report
// exclusions must report the removed keys, and should warn when a configured
// entry is absent from them so a typo or stale entry is visible instead of
// silently reconciling the namespace it was meant to exclude.
func GroupFilesByNamespace(files map[string]string, defaultNamespace string, environment string, ignoreNamespaces []string) (SchemaFiles, []string, error) {
	result := make(SchemaFiles)
	var hasFlatFile, hasNamespacedFile bool

	for relativePath, content := range files {
		filename := path.Base(relativePath)

		// Skip non-schema files
		if !strings.HasSuffix(filename, ".sql") && filename != "vschema.json" {
			continue
		}

		namespace := path.Dir(relativePath)
		if namespace == "." || namespace == "" {
			namespace = defaultNamespace
			hasFlatFile = true
		} else {
			hasNamespacedFile = true
		}

		// Replace $ENV in namespace keys when environment is known.
		if environment != "" {
			namespace = strings.ReplaceAll(namespace, "$ENV", environment)
		}

		if result[namespace] == nil {
			result[namespace] = &Namespace{Files: make(map[string]string)}
		}
		result[namespace].Files[filename] = content
	}

	// Reject mixed flat + namespaced files
	if hasFlatFile && hasNamespacedFile {
		return nil, nil, fmt.Errorf("schema directory has both flat files and namespace subdirectories — use one layout or the other")
	}

	var removed []string
	for _, ignored := range ResolveIgnoreNamespaces(ignoreNamespaces, environment) {
		if _, ok := result[ignored]; !ok {
			continue
		}
		delete(result, ignored)
		removed = append(removed, ignored)
	}
	sort.Strings(removed)

	return result, removed, nil
}

// ResolveIgnoreNamespaces applies the same $ENV substitution to
// ignore_namespaces entries that GroupFilesByNamespace applies to namespace
// directory names, returning the configured entries as they will be matched
// against namespace keys for the environment. Resolution does not check
// existence — an entry may match no namespace; GroupFilesByNamespace reports
// which entries actually removed one.
func ResolveIgnoreNamespaces(namespaces []string, environment string) []string {
	if len(namespaces) == 0 {
		return nil
	}
	resolved := make([]string, len(namespaces))
	for i, ns := range namespaces {
		if environment != "" {
			ns = strings.ReplaceAll(ns, "$ENV", environment)
		}
		resolved[i] = ns
	}
	return resolved
}

// UnmatchedIgnoreEntries returns the resolved ignore_namespaces entries that
// removed no namespace during grouping — a typo, a case mismatch, or a stale
// entry for a directory that no longer exists. The namespaces such entries
// name are fully reconciled, so callers should warn with the returned values
// rather than letting the config imply an exclusion that is not happening.
func UnmatchedIgnoreEntries(configured []string, environment string, removed []string) []string {
	removedSet := make(map[string]bool, len(removed))
	for _, ns := range removed {
		removedSet[ns] = true
	}
	var unmatched []string
	for _, ns := range ResolveIgnoreNamespaces(configured, environment) {
		if !removedSet[ns] {
			unmatched = append(unmatched, ns)
		}
	}
	return unmatched
}

// ValidateIgnoreNamespaces rejects ignore_namespaces entries that cannot match
// a namespace subdirectory of the schema root: blank entries, entries padded
// with whitespace (namespace keys are never padded, so such an entry would
// silently exclude nothing), and entries with path separators.
func ValidateIgnoreNamespaces(namespaces []string) error {
	for _, ns := range namespaces {
		if strings.TrimSpace(ns) == "" {
			return fmt.Errorf("ignore_namespaces entries must not be blank")
		}
		if ns != strings.TrimSpace(ns) {
			return fmt.Errorf("ignore_namespaces entry %q must not have leading or trailing whitespace", ns)
		}
		if strings.ContainsAny(ns, `/\`) {
			return fmt.Errorf("ignore_namespaces entry %q must be a namespace name, not a path", ns)
		}
	}
	return nil
}
