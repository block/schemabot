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

// DialectForDatabaseType maps a database_type to its database family for
// system-schema classification. Callers must use this instead of converting a
// database_type string directly to a Dialect: "vitess" and "strata" are
// MySQL-protocol engines that share the MySQL dialect. An unrecognized type is
// returned as its lowercased value — an unregistered dialect — rather than being
// forced to a family, so IsReservedPullNamespaceForDialect classifies it
// conservatively (reserving every known dialect's system schemas) instead of a
// mislabeled target silently exposing another family's system schemas.
func DialectForDatabaseType(databaseType string) Dialect {
	switch strings.ToLower(databaseType) {
	case "mysql", "vitess", "strata":
		return DialectMySQL
	case "postgres":
		return DialectPostgres
	default:
		return Dialect(strings.ToLower(databaseType))
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
	// enumerated yet. It must be finalized when Postgres pull discovery is
	// implemented so extension-owned schemas are not surfaced as pullable user
	// namespaces.
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

	// SchemaBot-internal namespaces are reserved regardless of dialect.
	if _, ok := schemabotReservedNamespaces[name]; ok {
		return true
	}
	if strings.HasPrefix(name, "_") {
		return true
	}

	dialect = Dialect(strings.ToLower(string(dialect)))
	if _, known := systemSchemasByDialect[dialect]; known {
		return isSystemSchemaForDialect(dialect, name)
	}

	// A mis-constructed or unknown dialect (e.g. a raw "vitess" or a mislabeled
	// "postgresql") is classified conservatively: reserve any namespace that is a
	// system schema in ANY known dialect. Over-reserving only excludes a
	// namespace from pull discovery — the safe direction — whereas failing open
	// would expose a real system schema as a user namespace.
	for known := range systemSchemasByDialect {
		if isSystemSchemaForDialect(known, name) {
			return true
		}
	}
	return false
}

// isSystemSchemaForDialect reports whether name is a database-managed schema for
// a single, registered dialect (by exact name or reserved prefix).
func isSystemSchemaForDialect(dialect Dialect, name string) bool {
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
