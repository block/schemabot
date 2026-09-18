package schema

import (
	"fmt"
	"strings"
)

// ValidateIgnoreTables rejects ignore_tables entries that cannot match a live
// table: blank entries, entries padded with whitespace (a target spells a
// table name without padding, so such an entry would silently withhold
// nothing), and entries with path separators, which name a file rather than a
// table.
//
// Entries are literal table names. Unlike ignore_namespaces there is no $ENV
// substitution: an entry is matched against the target's own catalog, exactly
// and case-sensitively.
func ValidateIgnoreTables(tables []string) error {
	for _, name := range tables {
		if strings.TrimSpace(name) == "" {
			return fmt.Errorf("ignore_tables entries must not be blank")
		}
		if name != strings.TrimSpace(name) {
			return fmt.Errorf("ignore_tables entry %q must not have leading or trailing whitespace", name)
		}
		if strings.ContainsAny(name, `/\`) {
			return fmt.Errorf("ignore_tables entry %q must be a table name, not a path", name)
		}
	}
	return nil
}

// UnmatchedIgnoreTables returns the configured ignore_tables entries that
// withheld no live table from the plan — a typo, a case mismatch, or a stale
// entry for a table that no longer exists. Such an entry excludes nothing, so
// callers report the returned values rather than letting the config imply an
// exclusion that is not happening.
//
// withheld is the union of the tables the plan actually withheld, across every
// namespace it covered: an entry that matched in one namespace and not another
// still withheld a table and is not reported.
func UnmatchedIgnoreTables(configured, withheld []string) []string {
	withheldSet := make(map[string]bool, len(withheld))
	for _, name := range withheld {
		withheldSet[name] = true
	}
	var unmatched []string
	for _, name := range configured {
		if !withheldSet[name] {
			unmatched = append(unmatched, name)
		}
	}
	return unmatched
}
