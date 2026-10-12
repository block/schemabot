package state

import "strings"

// FullKeyRangeShard is the name of the shard that covers a keyspace's whole
// keyrange, which makes it the keyspace's only shard.
const FullKeyRangeShard = "-"

// ShardWorkKey splits a sharded work operation key "namespace/shard/table"
// into its parts. ok is false for any other shape — an empty key (a
// non-sharded apply) or a "namespace/group_finalizer" finalizer key — so
// callers can tell shard work apart from the rest.
func ShardWorkKey(key string) (namespace, shard, table string, ok bool) {
	// Split without a limit so a key with extra segments (e.g.
	// "ns/-40/table/extra") fails the exact-three-parts check rather than folding
	// the remainder into the table and being misclassified as shard work.
	parts := strings.Split(key, OperationKeyDelimiter)
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
		return "", "", "", false
	}
	return parts[0], parts[1], parts[2], true
}

// NamespaceFinalizerKey splits a "namespace/group_finalizer" finalizer
// operation key into its namespace. ok is false for any other shape, including
// the bare "group_finalizer" key a vschema-only plan produces — that shape has
// no shard work alongside it, so it never reaches the sharded layout.
func NamespaceFinalizerKey(key string) (namespace string, ok bool) {
	ns, ok := strings.CutSuffix(key, OperationKeyDelimiter+GroupFinalizerKeySegment)
	if !ok || ns == "" || strings.Contains(ns, OperationKeyDelimiter) {
		return "", false
	}
	return ns, true
}
