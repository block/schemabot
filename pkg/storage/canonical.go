package storage

import "strings"

// CanonicalKey folds an identity string — repository full name, database
// name, database type, environment — to its canonical single-spelling form.
// Identity strings are the cross-dialect row-identity keys: MySQL's
// utf8mb4_0900_ai_ci storage collation forgives case drift while PostgreSQL
// compares bytes, so every boundary that accepts an identity string folds it
// before matching or persisting. Folding is lowercase-only; accent folding is
// deliberately excluded because identity strings are ASCII in practice.
//
// Deliberately not folded here: lock owner strings ("org/repo#42") are the
// ownership predicate on lock acquire, release, and intent verification, but
// the repository they are derived from is folded at ingress, so owners are
// canonical by construction rather than re-folded at this boundary.
// Deployment names are identity keys too, but they are operator-controlled
// configuration that doubles as routing and schema-directory path components,
// so config validation rejects non-canonical spellings instead of silently
// rewriting them. Table names, SHAs, and GitHub node IDs are case-significant
// or opaque values, not row-identity keys.
func CanonicalKey(s string) string {
	return strings.ToLower(s)
}
