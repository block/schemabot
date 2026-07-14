package schema

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// IsReservedPullNamespace must be the MySQL-dialect classification so existing
// MySQL and Vitess pull discovery is unchanged after routing through the
// dialect registry.
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
