package storage

import "strings"

// CanonicalKey folds an identity string — repository full name, database
// name, database type, environment — to its canonical single-spelling form.
// Identity strings are the cross-dialect row-identity keys: MySQL's
// utf8mb4_0900_ai_ci storage collation forgives case drift while PostgreSQL
// compares bytes, so every boundary that accepts an identity string folds it
// before matching or persisting. Caller-supplied API path segments and query
// parameters are such a boundary: a client-typed spelling is folded before it
// reaches storage or a configuration map lookup, so the API resolves the same
// rows and the same configured database regardless of how the caller typed the
// name. Folding is lowercase-only; accent folding is deliberately excluded
// because identity strings are ASCII in practice.
//
// Lock owner strings are the ownership predicate on lock acquire, release,
// and intent verification and must match byte-exactly. GitHub-origin owners
// ("org/repo#42") are canonical by construction from the folded repository;
// the lock API folds a caller-supplied owner ("cli:user@host") with this
// function at acquire and release, and the CLI generates its owner already
// folded so its own ownership checks compare against the stored spelling.
// Deployment names are identity keys too, but they are operator-controlled
// configuration that doubles as routing and schema-directory path components,
// so config validation rejects non-canonical spellings instead of silently
// rewriting them; because every stored deployment name is therefore already
// canonical, a caller-typed deployment is folded at the API read boundary like
// any other identity string. Table names, SHAs, and GitHub node IDs are
// case-significant or opaque values, not row-identity keys.
func CanonicalKey(s string) string {
	return strings.ToLower(s)
}
