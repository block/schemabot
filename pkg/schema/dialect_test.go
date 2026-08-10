package schema

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

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
		{name: "performance_schema on mysql", dialect: DialectMySQL, namespace: "performance_schema", want: true},
		{name: "sys on mysql", dialect: DialectMySQL, namespace: "sys", want: true},
		{name: "rdsmon on mysql", dialect: DialectMySQL, namespace: "rdsmon", want: true},
		{name: "dbadmin on mysql", dialect: DialectMySQL, namespace: "dbadmin", want: true},
		{name: "polt on mysql", dialect: DialectMySQL, namespace: "polt", want: true},
		{name: "tmp on mysql", dialect: DialectMySQL, namespace: "tmp", want: true},
		{name: "topo on mysql", dialect: DialectMySQL, namespace: "topo", want: true},
		{name: "uppercase innodb on mysql", dialect: DialectMySQL, namespace: "INNODB", want: true},
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

		// RDS/Aurora administrative and managed-extension schemas are reserved on
		// Postgres but not on MySQL.
		{name: "rds_tools on postgres", dialect: DialectPostgres, namespace: "rds_tools", want: true},
		{name: "aws_commons on postgres", dialect: DialectPostgres, namespace: "aws_commons", want: true},
		{name: "aws_s3 on postgres", dialect: DialectPostgres, namespace: "aws_s3", want: true},
		{name: "aws_lambda on postgres", dialect: DialectPostgres, namespace: "aws_lambda", want: true},
		{name: "aws_ml on postgres", dialect: DialectPostgres, namespace: "aws_ml", want: true},
		{name: "apg_plan_mgmt on postgres", dialect: DialectPostgres, namespace: "apg_plan_mgmt", want: true},
		{name: "uppercase aws_s3 on postgres", dialect: DialectPostgres, namespace: "AWS_S3", want: true},
		{name: "aws_commons on mysql", dialect: DialectMySQL, namespace: "aws_commons", want: false},
		{name: "apg_plan_mgmt on mysql", dialect: DialectMySQL, namespace: "apg_plan_mgmt", want: false},

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

func TestSupportsFeature(t *testing.T) {
	tests := []struct {
		name         string
		databaseType string
		feature      Feature
		want         bool
	}{
		{name: "mysql deferred cutover", databaseType: "mysql", feature: FeatureDeferredCutover, want: true},
		{name: "vitess deferred cutover", databaseType: "vitess", feature: FeatureDeferredCutover, want: true},
		{name: "strata deferred cutover", databaseType: "strata", feature: FeatureDeferredCutover},
		{name: "postgres deferred cutover", databaseType: "postgres", feature: FeatureDeferredCutover},
		{name: "unknown database type", databaseType: "unknown", feature: FeatureDeferredCutover},
		{name: "unknown feature", databaseType: "mysql", feature: Feature("unknown")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, SupportsFeature(tt.databaseType, tt.feature))
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
