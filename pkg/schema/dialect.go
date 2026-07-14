package schema

import "strings"

// Dialect identifies a database family for system-schema classification. It is
// coarser than the per-engine database_type (mysql / vitess / strata all share
// the MySQL dialect) because reserved system schemas are a property of the
// underlying database family, not of the engine that drives it.
type Dialect string

const (
	// DialectMySQL covers MySQL and every MySQL-protocol engine (Vitess, Strata).
	DialectMySQL Dialect = "mysql"
	// DialectPostgres covers PostgreSQL targets.
	DialectPostgres Dialect = "postgres"
)

// schemabotReservedNamespaces are reserved regardless of dialect: SchemaBot's
// own storage schema and its pending-drops quarantine. Any namespace beginning
// with an underscore is also treated as SchemaBot-internal (see the prefix rule
// in IsReservedPullNamespaceForDialect).
var schemabotReservedNamespaces = map[string]struct{}{
	"_pending_drops": {},
	"schemabot":      {},
}

// systemSchemasByDialect maps a dialect to the database-managed schemas that
// must never be treated as user schema for pull discovery.
var systemSchemasByDialect = map[Dialect]map[string]struct{}{
	DialectMySQL: {
		"information_schema": {},
		"innodb":             {},
		"mysql":              {},
		"performance_schema": {},
		"sys":                {},
		"rdsmon":             {},
		"dbadmin":            {},
		"polt":               {},
		"tmp":                {},
		"topo":               {},
	},
	DialectPostgres: {
		"information_schema": {},
		"pg_catalog":         {},
		"pg_toast":           {},
		"rdsadmin":           {},
	},
}

// IsReservedPullNamespaceForDialect reports whether a live namespace should be
// excluded from schema pull discovery and rejected for explicit pull requests,
// for the given database dialect. A namespace is reserved when it is a SchemaBot
// internal namespace, carries the SchemaBot-internal underscore prefix, or is a
// system schema for the dialect (including the Postgres pg_ prefix).
func IsReservedPullNamespaceForDialect(dialect Dialect, namespace string) bool {
	name := strings.ToLower(namespace)
	// Normalize the dialect so a differently-cased value (e.g. "Postgres")
	// cannot silently miss the registry and fail open, treating a reserved
	// system schema as a user namespace.
	dialect = Dialect(strings.ToLower(string(dialect)))

	if _, ok := schemabotReservedNamespaces[name]; ok {
		return true
	}
	if strings.HasPrefix(name, "_") {
		return true
	}
	if _, ok := systemSchemasByDialect[dialect][name]; ok {
		return true
	}
	if dialect == DialectPostgres && strings.HasPrefix(name, "pg_") {
		return true
	}
	return false
}
