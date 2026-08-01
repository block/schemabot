package schema

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// IsReservedPullNamespace is the MySQL-dialect classification, so MySQL and
// Vitess pull discovery share a single definition of reserved namespaces.
func TestIsReservedPullNamespaceUsesMySQLDialect(t *testing.T) {
	for _, ns := range []string{
		"mysql", "information_schema", "innodb", "performance_schema", "sys",
		"rdsmon", "dbadmin", "polt", "tmp", "topo", "_pending_drops",
		"schemabot", "_scratch", "orders_production",
	} {
		assert.Equal(t,
			IsReservedPullNamespaceForDialect(DialectMySQL, ns),
			IsReservedPullNamespace(ns),
			"IsReservedPullNamespace should match the MySQL dialect for %q", ns,
		)
	}
}

func TestIsReservedPullNamespaceForDialect(t *testing.T) {
	tests := []struct {
		name      string
		dialect   Dialect
		namespace string
		want      bool
	}{
		// SchemaBot-internal namespaces are reserved for every dialect.
		{name: "pending drops on mysql", dialect: DialectMySQL, namespace: "_pending_drops", want: true},
		{name: "pending drops on postgres", dialect: DialectPostgres, namespace: "_pending_drops", want: true},
		{name: "schemabot storage on postgres", dialect: DialectPostgres, namespace: "schemabot", want: true},
		{name: "underscore prefix on postgres", dialect: DialectPostgres, namespace: "_scratch", want: true},

		// MySQL system schemas are reserved on MySQL but not on Postgres.
		{name: "mysql db on mysql", dialect: DialectMySQL, namespace: "mysql", want: true},
		{name: "innodb on mysql", dialect: DialectMySQL, namespace: "innodb", want: true},
		{name: "mysql db on postgres", dialect: DialectPostgres, namespace: "mysql", want: false},
		{name: "innodb on postgres", dialect: DialectPostgres, namespace: "innodb", want: false},

		// Postgres system schemas are reserved on Postgres but not on MySQL.
		{name: "pg_catalog on postgres", dialect: DialectPostgres, namespace: "pg_catalog", want: true},
		{name: "pg_toast on postgres", dialect: DialectPostgres, namespace: "pg_toast", want: true},
		{name: "rdsadmin on postgres", dialect: DialectPostgres, namespace: "rdsadmin", want: true},
		{name: "pg_temp prefix on postgres", dialect: DialectPostgres, namespace: "pg_temp_3", want: true},
		{name: "uppercase pg_catalog on postgres", dialect: DialectPostgres, namespace: "PG_CATALOG", want: true},
		{name: "pg_catalog on mysql", dialect: DialectMySQL, namespace: "pg_catalog", want: false},
		{name: "pg_temp prefix on mysql", dialect: DialectMySQL, namespace: "pg_temp_3", want: false},

		// A differently-cased dialect value must not fail open: it still
		// classifies system schemas for that dialect.
		{name: "mixed-case postgres dialect matches pg_catalog", dialect: Dialect("Postgres"), namespace: "pg_catalog", want: true},
		{name: "mixed-case postgres dialect matches pg_ prefix", dialect: Dialect("POSTGRES"), namespace: "pg_temp_3", want: true},
		{name: "mixed-case mysql dialect matches innodb", dialect: Dialect("MySQL"), namespace: "innodb", want: true},

		// information_schema exists in both dialects.
		{name: "information_schema on mysql", dialect: DialectMySQL, namespace: "information_schema", want: true},
		{name: "information_schema on postgres", dialect: DialectPostgres, namespace: "information_schema", want: true},

		// Application namespaces are never reserved.
		{name: "app namespace on mysql", dialect: DialectMySQL, namespace: "orders_production", want: false},
		{name: "app namespace on postgres", dialect: DialectPostgres, namespace: "orders_production", want: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, IsReservedPullNamespaceForDialect(tc.dialect, tc.namespace))
		})
	}
}

func TestDialectForDatabaseType(t *testing.T) {
	tests := []struct {
		databaseType string
		want         Dialect
	}{
		{databaseType: "mysql", want: DialectMySQL},
		{databaseType: "vitess", want: DialectMySQL},
		{databaseType: "strata", want: DialectMySQL},
		{databaseType: "postgres", want: DialectPostgres},
		{databaseType: "Postgres", want: DialectPostgres},
		{databaseType: "MYSQL", want: DialectMySQL},
		// Unrecognized types are returned as unregistered dialects (not forced to
		// MySQL) so classification treats them conservatively.
		{databaseType: "postgresql", want: Dialect("postgresql")},
		{databaseType: "unknown", want: Dialect("unknown")},
		{databaseType: "", want: Dialect("")},
	}

	for _, tc := range tests {
		t.Run(tc.databaseType, func(t *testing.T) {
			assert.Equal(t, tc.want, DialectForDatabaseType(tc.databaseType))
		})
	}
}

// An unregistered dialect — a raw database_type cast (e.g. "vitess", "strata")
// or a mislabeled target (e.g. "postgresql") — is classified conservatively:
// classification reserves the union of every known dialect's system schemas
// rather than failing open and exposing a real system schema as a pullable user
// namespace. Application namespaces stay pullable.
func TestIsReservedPullNamespaceForDialectFailsClosed(t *testing.T) {
	for _, raw := range []string{"vitess", "strata", "postgresql", "somethingelse"} {
		for _, ns := range []string{
			// MySQL system schemas.
			"mysql", "information_schema", "innodb", "sys",
			// Postgres system schemas and the pg_ prefix.
			"pg_catalog", "pg_toast", "rdsadmin", "pg_temp_3",
		} {
			assert.True(t,
				IsReservedPullNamespaceForDialect(Dialect(raw), ns),
				"unregistered dialect %q must reserve system schema %q from any family", raw, ns,
			)
		}
		assert.False(t,
			IsReservedPullNamespaceForDialect(Dialect(raw), "orders_production"),
			"unregistered dialect %q must not reserve application namespaces", raw,
		)
	}
}

// A database_type routed through DialectForDatabaseType must be classified
// safely even when it is misspelled: an unknown type resolves to an unregistered
// dialect, and classification then reserves system schemas from every family.
func TestDialectForDatabaseTypeClassifiesUnknownConservatively(t *testing.T) {
	dialect := DialectForDatabaseType("postgresql")
	assert.True(t, IsReservedPullNamespaceForDialect(dialect, "pg_catalog"),
		"a mislabeled postgres type must still reserve pg_catalog")
	assert.True(t, IsReservedPullNamespaceForDialect(dialect, "mysql"),
		"an unregistered dialect reserves every family's system schemas")
	assert.False(t, IsReservedPullNamespaceForDialect(dialect, "orders_production"),
		"application namespaces stay pullable")
}
