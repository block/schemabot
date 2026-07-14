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

// DialectForDatabaseType maps a database_type to its database family. Callers
// must use this instead of converting a database_type string directly to a
// Dialect: "vitess" and "strata" are MySQL-protocol engines that share the
// MySQL dialect, so a direct conversion would produce an unregistered dialect.
// An unrecognized type resolves to DialectMySQL, the conservative default that
// still reserves MySQL system schemas rather than exposing them.
func DialectForDatabaseType(databaseType string) Dialect {
	switch strings.ToLower(databaseType) {
	case "postgres":
		return DialectPostgres
	default:
		return DialectMySQL
	}
}

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
	// The Postgres set is provisional: it covers the always-present system
	// schemas and RDS/Aurora's rdsadmin, but the managed-extension schemas
	// (aws_commons, aws_s3, aws_lambda, aws_ml, ...) are deliberately not
	// enumerated yet. It must be finalized when Postgres pull discovery lands
	// (see the DB-agnostic tracker's Postgres engine slice) so extension-owned
	// schemas are not surfaced as pullable user namespaces.
	DialectPostgres: {
		"information_schema": {},
		"pg_catalog":         {},
		"pg_toast":           {},
		"rdsadmin":           {},
	},
}

// reservedPrefixesByDialect lists namespace prefixes that a dialect reserves for
// database-managed schemas, keeping prefix rules in the registry rather than in
// control flow. The dialect-independent SchemaBot-internal "_" prefix is handled
// separately in IsReservedPullNamespaceForDialect.
var reservedPrefixesByDialect = map[Dialect][]string{
	DialectPostgres: {"pg_"},
}

// IsReservedPullNamespaceForDialect reports whether a live namespace should be
// excluded from schema pull discovery and rejected for explicit pull requests,
// for the given database dialect. A namespace is reserved when it is a SchemaBot
// internal namespace, carries the SchemaBot-internal underscore prefix, or is a
// system schema for the dialect (including the Postgres pg_ prefix).
func IsReservedPullNamespaceForDialect(dialect Dialect, namespace string) bool {
	name := strings.ToLower(namespace)
	// Normalize the dialect and resolve an unregistered value to DialectMySQL so
	// a differently-cased or mis-constructed dialect (e.g. a raw "vitess") cannot
	// silently miss the registry and fail open, treating a reserved system schema
	// as a user namespace. MySQL is the conservative default: the MySQL-protocol
	// engines are the only other database_types today, and they share its dialect.
	dialect = Dialect(strings.ToLower(string(dialect)))
	if _, known := systemSchemasByDialect[dialect]; !known {
		dialect = DialectMySQL
	}

	if _, ok := schemabotReservedNamespaces[name]; ok {
		return true
	}
	if strings.HasPrefix(name, "_") {
		return true
	}
	if _, ok := systemSchemasByDialect[dialect][name]; ok {
		return true
	}
	for _, prefix := range reservedPrefixesByDialect[dialect] {
		if strings.HasPrefix(name, prefix) {
			return true
		}
	}
	return false
}
