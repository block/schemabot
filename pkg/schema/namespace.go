package schema

import (
	"fmt"
	"path"
	"regexp"
	"regexp/syntax"
	"slices"
	"sort"
	"strings"
)

// EmptyNamespaceDeclaration keeps an explicitly empty namespace in version
// control. It is metadata for grouping, not a SQL statement sent to an engine.
const EmptyNamespaceDeclaration = "-- This namespace is empty. Add CREATE TABLE declarations here.\n"

// GroupFilesByNamespace groups schema files by namespace using their relative paths.
// An explicit empty-namespace marker retains the namespace with an empty Files map;
// the marker itself is metadata and is not sent to an engine.
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
// The environment parameter enables {env} and legacy $ENV substitution in
// namespace names. If environment is non-empty, either token in namespace keys
// (from directory names or defaultNamespace) is replaced with the environment.
// This allows a single directory like "bikeshare_{env}/" to resolve to
// "bikeshare_staging" or "bikeshare_production" depending on the target.
// If environment is empty, both tokens are left as-is.
//
// ignoreNamespaces lists namespaces to exclude from the result — schema
// directories that exist in the repository but must not be reconciled against
// the live database (for example a keyspace only used by local test
// infrastructure). Plain entries receive the same substitution as directory
// names and pattern entries are compiled (see CompileIgnoreNamespaces); both
// are matched against the post-substitution namespace keys. Layout
// validation still sees ignored directories: a mixed flat/subdirectory layout
// is rejected even when the subdirectories are all ignored.
//
// The second return value lists the namespace keys that were actually removed
// by ignoreNamespaces, sorted. An entry that matches no namespace removes
// nothing (matching is case-sensitive); callers that report
// exclusions must report the removed keys, and should warn when a configured
// entry is absent from them so a typo or stale entry is visible instead of
// silently reconciling the namespace it was meant to exclude.
func GroupFilesByNamespace(files map[string]string, defaultNamespace string, environment string, ignoreNamespaces []string) (SchemaFiles, []string, error) {
	ignore, err := CompileIgnoreNamespaces(ignoreNamespaces, environment)
	if err != nil {
		return nil, nil, err
	}
	result := make(SchemaFiles)
	resolvedSources := make(map[string]string)
	var hasFlatFile, hasNamespacedFile bool

	for relativePath, content := range files {
		filename := path.Base(relativePath)

		// Skip non-schema files
		if !strings.HasSuffix(filename, ".sql") && filename != "vschema.json" {
			continue
		}

		sourceNamespace := path.Dir(relativePath)
		if sourceNamespace == "." || sourceNamespace == "" {
			sourceNamespace = defaultNamespace
		}
		namespace, namespaced := NamespaceForRelativePath(relativePath, defaultNamespace, environment)
		if previous, ok := resolvedSources[namespace]; ok && previous != sourceNamespace {
			return nil, nil, fmt.Errorf("schema directories %q and %q both resolve to namespace %q", previous, sourceNamespace, namespace)
		}
		resolvedSources[namespace] = sourceNamespace
		if !namespaced {
			hasFlatFile = true
		} else {
			hasNamespacedFile = true
		}

		if result[namespace] == nil {
			result[namespace] = &Namespace{Files: make(map[string]string)}
		}
		if strings.TrimSpace(content) == strings.TrimSpace(EmptyNamespaceDeclaration) {
			continue
		}
		result[namespace].Files[filename] = content
	}

	// Reject mixed flat + namespaced files
	if hasFlatFile && hasNamespacedFile {
		return nil, nil, fmt.Errorf("schema directory has both flat files and namespace subdirectories — use one layout or the other")
	}

	var removed []string
	for namespace := range result {
		if ignore.Ignores(namespace) {
			removed = append(removed, namespace)
		}
	}
	for _, namespace := range removed {
		delete(result, namespace)
	}
	sort.Strings(removed)

	return result, removed, nil
}

// NamespaceForRelativePath derives the namespace key used when grouping a
// schema file. The bool reports whether the file is in a namespace directory.
func NamespaceForRelativePath(relativePath, defaultNamespace, environment string) (string, bool) {
	namespace := path.Dir(relativePath)
	namespaced := namespace != "." && namespace != ""
	if !namespaced {
		namespace = defaultNamespace
	}
	if environment != "" {
		namespace = ResolveNamespaceEnvironment(namespace, environment)
	}
	return namespace, namespaced
}

// HasNamespaceEnvironmentPlaceholder reports whether a name contains either
// supported namespace-directory placeholder. Schema-pull and onboarding
// namespace arguments require concrete names; repository paths may use tokens.
func HasNamespaceEnvironmentPlaceholder(namespace string) bool {
	return strings.Contains(namespace, "{env}") || strings.Contains(namespace, "$ENV")
}

// ResolveNamespaceEnvironment substitutes both the preferred {env} token and
// the legacy $ENV token in a namespace name. With no environment, it leaves
// the name unchanged.
func ResolveNamespaceEnvironment(namespace, environment string) string {
	if environment == "" {
		return namespace
	}
	replacer := strings.NewReplacer("{env}", environment, "$ENV", environment)
	return replacer.Replace(namespace)
}

// ignoreNamespacePatternDelimiter wraps a pattern entry, the same delimiter
// ignore_tables uses. A namespace name cannot carry it, since a plain entry
// containing a path separator is refused, so an entry it wraps is
// unambiguously a pattern and every other entry keeps meaning the one
// namespace it names.
const ignoreNamespacePatternDelimiter = "/"

// maxIgnoreNamespacePatternBytes caps a pattern entry's length, slashes
// included, and maxIgnoreNamespacePatternsTotalBytes caps a config's distinct
// pattern entries taken together, so the compilation work every plan does
// before it groups schema files stays bounded. A namespace name is a database
// or keyspace name, short enough that a pattern describing a family of them
// never needs to come close to either cap.
const (
	maxIgnoreNamespacePatternBytes       = 256
	maxIgnoreNamespacePatternsTotalBytes = 1024
)

// IgnoreNamespaces is a schemabot.yaml ignore_namespaces list resolved for one
// environment: plain entries with {env} and $ENV substituted the way namespace
// directory names are, and pattern entries compiled. It is the one rule every
// reader of the config applies, so the CLI, the GitHub flow, and the
// unmatched-entry warnings agree on which namespaces a list withholds.
type IgnoreNamespaces struct {
	entries []ignoreNamespaceEntry
}

type ignoreNamespaceEntry struct {
	// written is the entry as an operator finds it in schemabot.yaml, with
	// {env} and $ENV substituted for a plain entry. Unmatched reports it.
	written string
	// pattern is set for a pattern entry; a plain entry matches written
	// exactly.
	pattern *regexp.Regexp
}

// CompileIgnoreNamespaces resolves an ignore_namespaces list for environment.
// A plain entry receives the same substitution as directory names and matches
// one namespace exactly and case-sensitively. An entry wrapped in slashes is a
// pattern (see CompileIgnoreNamespacePattern). With no environment, plain
// entries keep their tokens and so match the unsubstituted directory names.
func CompileIgnoreNamespaces(entries []string, environment string) (*IgnoreNamespaces, error) {
	if err := checkIgnoreNamespacePatternBudget(entries); err != nil {
		return nil, err
	}
	ignore := &IgnoreNamespaces{entries: make([]ignoreNamespaceEntry, 0, len(entries))}
	for _, entry := range entries {
		if !IsIgnoreNamespacePattern(entry) {
			ignore.entries = append(ignore.entries, ignoreNamespaceEntry{written: ResolveNamespaceEnvironment(entry, environment)})
			continue
		}
		re, err := CompileIgnoreNamespacePattern(entry)
		if err != nil {
			return nil, err
		}
		ignore.entries = append(ignore.entries, ignoreNamespaceEntry{written: entry, pattern: re})
	}
	return ignore, nil
}

// Ignores reports whether any entry withholds namespace.
func (l *IgnoreNamespaces) Ignores(namespace string) bool {
	return slices.ContainsFunc(l.entries, func(e ignoreNamespaceEntry) bool { return e.matches(namespace) })
}

// Unmatched returns the entries that withheld none of removed, the namespaces
// grouping actually withheld: a typo, a case mismatch, a stale entry for a
// directory that no longer exists, or a pattern written for another
// environment. The namespaces a mistaken entry was meant to name are fully
// reconciled, so callers should warn with the returned values rather than let
// the config imply an exclusion that is not happening.
func (l *IgnoreNamespaces) Unmatched(removed []string) []string {
	var unmatched []string
	for _, e := range l.entries {
		if !slices.ContainsFunc(removed, e.matches) {
			unmatched = append(unmatched, e.written)
		}
	}
	return unmatched
}

func (e ignoreNamespaceEntry) matches(namespace string) bool {
	if e.pattern != nil {
		return e.pattern.MatchString(namespace)
	}
	return e.written == namespace
}

// UnmatchedIgnoreEntries returns the configured ignore_namespaces entries,
// resolved for environment, that withheld none of removed. See
// IgnoreNamespaces.Unmatched.
func UnmatchedIgnoreEntries(configured []string, environment string, removed []string) ([]string, error) {
	ignore, err := CompileIgnoreNamespaces(configured, environment)
	if err != nil {
		return nil, err
	}
	return ignore.Unmatched(removed), nil
}

// IsIgnoreNamespacePattern reports whether an ignore_namespaces entry is a
// pattern: a regular expression wrapped in slashes. A lone slash is not one;
// it is refused as a path.
func IsIgnoreNamespacePattern(entry string) bool {
	return len(entry) >= 2 &&
		strings.HasPrefix(entry, ignoreNamespacePatternDelimiter) &&
		strings.HasSuffix(entry, ignoreNamespacePatternDelimiter)
}

// CompileIgnoreNamespacePattern compiles a pattern entry into a regular
// expression that matches a namespace name only in full: the expression is
// anchored at both ends whether or not it is written with anchors, so a
// pattern meant for one family of namespaces cannot reach one that merely
// contains a matching run of characters. Matching is case-sensitive, the same
// as a plain entry. The syntax is Go's RE2, which has no backtracking, and an
// entry longer than maxIgnoreNamespacePatternBytes is refused, so no pattern
// can stall a plan.
//
// A pattern is matched against namespace names after {env} and $ENV are
// substituted, so it cannot use either token: an entry written for one
// environment spells that environment out. Refusing the tokens keeps a
// pattern's meaning the same in every environment, so an entry that passes
// validation never fails a plan for its expression.
//
// The expression is compiled on its own before it is anchored, which is what
// makes the anchoring hold: an expression that only parses once wrapped, such
// as `a)|(b`, would otherwise close the anchoring group early and match any
// name starting with a or ending with b.
func CompileIgnoreNamespacePattern(entry string) (*regexp.Regexp, error) {
	quoted := QuoteIgnoreTablesEntry(entry)
	if !IsIgnoreNamespacePattern(entry) {
		return nil, fmt.Errorf("ignore_namespaces entry %s is not a pattern: a pattern is a regular expression wrapped in slashes", quoted)
	}
	if len(entry) > maxIgnoreNamespacePatternBytes {
		return nil, fmt.Errorf("ignore_namespaces entry %s is longer than %d bytes: split it into several pattern entries", quoted, maxIgnoreNamespacePatternBytes)
	}
	expr := entry[len(ignoreNamespacePatternDelimiter) : len(entry)-len(ignoreNamespacePatternDelimiter)]
	if expr == "" {
		return nil, fmt.Errorf("ignore_namespaces entry %s is an empty pattern: write a regular expression between the slashes", quoted)
	}
	if HasNamespaceEnvironmentPlaceholder(expr) {
		return nil, fmt.Errorf("ignore_namespaces entry %s is a pattern and cannot use {env} or $ENV: a pattern is matched against namespace names after substitution, so write the environment out, as in \"/^fixtures_[0-9]+_staging$/\"", quoted)
	}
	if _, err := regexp.Compile(expr); err != nil {
		return nil, fmt.Errorf("ignore_namespaces entry %s is not a valid regular expression: %w", quoted, err)
	}
	// The anchors wrap the parsed expression's canonical form, not the text as
	// written: a quote opened by \Q runs to the end of the text, so wrapped
	// as written it would swallow the closing group and anchor.
	parsed, err := syntax.Parse(expr, syntax.Perl)
	if err != nil {
		return nil, fmt.Errorf("ignore_namespaces entry %s is not a valid regular expression: %w", quoted, err)
	}
	if requiresSlash(parsed) {
		return nil, fmt.Errorf("ignore_namespaces entry %s must be a namespace name or a pattern, not a path: a pattern cannot require a slash, since no namespace name has one", quoted)
	}
	re, err := regexp.Compile(`\A(?:` + parsed.String() + `)\z`)
	if err != nil {
		return nil, fmt.Errorf("ignore_namespaces entry %s is not a valid regular expression: %w", quoted, err)
	}
	return re, nil
}

// checkIgnoreNamespacePatternBudget refuses a config whose distinct pattern
// entries, taken together, are longer than
// maxIgnoreNamespacePatternsTotalBytes. It runs before any pattern compiles,
// so an over-budget config costs nothing to refuse.
func checkIgnoreNamespacePatternBudget(entries []string) error {
	seen := make(map[string]bool)
	total := 0
	for _, entry := range entries {
		if !IsIgnoreNamespacePattern(entry) || seen[entry] {
			continue
		}
		seen[entry] = true
		total += len(entry)
	}
	if total > maxIgnoreNamespacePatternsTotalBytes {
		return fmt.Errorf("ignore_namespaces pattern entries total %d bytes, over the %d-byte limit for all patterns together: remove or shorten pattern entries", total, maxIgnoreNamespacePatternsTotalBytes)
	}
	return nil
}

// ValidateIgnoreNamespaces rejects ignore_namespaces entries that cannot match
// a namespace subdirectory of the schema root: blank entries, entries padded
// with whitespace (namespace keys are never padded, so such an entry would
// silently exclude nothing), entries with path separators, and pattern
// entries that do not compile.
func ValidateIgnoreNamespaces(namespaces []string) error {
	if err := checkIgnoreNamespacePatternBudget(namespaces); err != nil {
		return err
	}
	for _, ns := range namespaces {
		if strings.TrimSpace(ns) == "" {
			return fmt.Errorf("ignore_namespaces entries must not be blank")
		}
		if ns != strings.TrimSpace(ns) {
			return fmt.Errorf("ignore_namespaces entry %q must not have leading or trailing whitespace", ns)
		}
		if IsIgnoreNamespacePattern(ns) {
			if _, err := CompileIgnoreNamespacePattern(ns); err != nil {
				return err
			}
			continue
		}
		if strings.ContainsAny(ns, `/\`) {
			return fmt.Errorf("ignore_namespaces entry %q must be a namespace name, not a path. To match a family of namespaces, wrap a regular expression in slashes, such as \"/^fixtures_[0-9]+_staging$/\"", ns)
		}
	}
	return nil
}
