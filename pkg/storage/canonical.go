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
// Deliberately not folded: lock owner strings (audit metadata such as
// "Org/Repo#42", compared byte-wise on both sides of every ownership check),
// deployment names, table names, SHAs, and GitHub node IDs — these are either
// case-significant or opaque values, not row-identity keys.
func CanonicalKey(s string) string {
	return strings.ToLower(s)
}
