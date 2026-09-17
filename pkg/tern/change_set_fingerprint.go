package tern

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/schema"
)

// ChangeSetFingerprint returns a stable key for a change set: two change sets
// share a fingerprint exactly when CompareChangeSets reports them identical, and
// differ otherwise. It exists so callers can group members that would run the
// same work without comparing every pair.
//
// It is built on the same canonicalized multiset the comparison keys on, which
// is what makes grouping by it sound rather than a textual heuristic. Members of
// one environment are planned by the same differ against the same desired
// schema, so the only thing that can make two members' plans differ is the live
// schema each was diffed against — and any such difference changes the multiset.
// Equal fingerprints therefore mean equal work, not merely similar-looking DDL.
//
// The fingerprint is opaque and not stable across releases: it is a grouping
// key for one rollup, never something to persist, compare across versions, or
// show an operator. Nothing about it identifies which plan it belongs to.
//
// Errors on exactly what the comparison errors on — a malformed change set or
// DDL that cannot be canonicalized — so a caller that cannot fingerprint a
// member treats it as unclassifiable rather than grouping it with members it was
// never compared against.
func ChangeSetFingerprint(dialect schema.Dialect, cs ChangeSet) (string, error) {
	parser, err := ddl.ParserForDialect(dialect)
	if err != nil {
		return "", err
	}
	ms, vschema, err := changeSetMultiset(parser, cs)
	if err != nil {
		return "", fmt.Errorf("fingerprint change set: %w", err)
	}

	// A multiset and a set are both unordered, so the digest has to consume them
	// in an order neither one carries. Sorting the rendered lines is what makes
	// the fingerprint depend on the change set's content and not on the order the
	// engine happened to return it in.
	lines := make([]string, 0, len(ms)+len(vschema))
	for key, count := range ms {
		lines = append(lines, "c"+fingerprintRecord(
			key.namespace, key.shard, key.table, key.operation, key.ddl, strconv.Itoa(count)))
	}
	for ns := range vschema {
		lines = append(lines, "v"+fingerprintRecord(ns))
	}
	sort.Strings(lines)

	digest := sha256.New()
	for _, line := range lines {
		// Record separator, not part of any field: the field separator inside
		// fingerprintRecord already keeps fields from running together, and this
		// keeps two records from doing the same.
		digest.Write([]byte(line))
		digest.Write([]byte{0x1e})
	}
	return hex.EncodeToString(digest.Sum(nil)), nil
}

// fingerprintRecord joins one multiset entry's fields with a separator no field
// can contain, so no pair of distinct entries can render to the same line. A
// namespace, table, or DDL body can contain anything a schema author wrote,
// which is why the separator is a control byte rather than a punctuation
// character: joining on one that a field could contain would let two different
// change sets collide into one fingerprint and be grouped as identical work.
func fingerprintRecord(fields ...string) string {
	return strings.Join(fields, "\x1f")
}
