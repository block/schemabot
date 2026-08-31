package storage

import "strings"

// CanonicalKey folds an identity string — repository full name, database
// name, database type, environment — to its canonical single-spelling form.
// Identity strings are canonicalized at ingress (webhook payload extraction,
// API request decode, comment-command parsing) and backstopped at the store
// boundary, so byte-wise comparisons and unique indexes behave identically on
// MySQL and PostgreSQL: MySQL's utf8mb4_0900_ai_ci storage collation forgives
// case drift while PostgreSQL compares bytes. Folding is lowercase-only;
// accent folding is deliberately excluded because identity strings are ASCII
// in practice.
func CanonicalKey(s string) string {
	return strings.ToLower(s)
}
