package api

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/schema"
)

// TestPullNamespaces exercises the API-boundary pull gate: requested
// namespaces must be well-formed, concrete, unique, and not reserved for the
// target's dialect. A system or platform-managed schema must be rejected on
// its own dialect, while the same name is a legal user namespace on the other
// dialect.
func TestPullNamespaces(t *testing.T) {
	tests := []struct {
		name       string
		dialect    schema.Dialect
		namespaces []string
		want       []string
		wantErr    string
	}{
		{name: "no namespaces pulls the default namespace", dialect: schema.DialectMySQL, namespaces: nil, want: []string{""}},
		{name: "app namespaces pass through in order", dialect: schema.DialectMySQL, namespaces: []string{"orders", "billing"}, want: []string{"orders", "billing"}},

		// Reserved namespaces are rejected per dialect.
		{name: "mysql system schema rejected on mysql", dialect: schema.DialectMySQL, namespaces: []string{"sys"}, wantErr: "reserved and cannot be pulled"},
		{name: "aws-managed schema rejected on postgres", dialect: schema.DialectPostgres, namespaces: []string{"aws_s3"}, wantErr: "reserved and cannot be pulled"},
		{name: "pg_catalog rejected on postgres", dialect: schema.DialectPostgres, namespaces: []string{"pg_catalog"}, wantErr: "reserved and cannot be pulled"},

		// A namespace reserved on one dialect is a legal user namespace on the other.
		{name: "mysql is a legal user namespace on postgres", dialect: schema.DialectPostgres, namespaces: []string{"mysql"}, want: []string{"mysql"}},
		{name: "aws_s3 is a legal user namespace on mysql", dialect: schema.DialectMySQL, namespaces: []string{"aws_s3"}, want: []string{"aws_s3"}},

		// Shape validation is dialect-independent.
		{name: "empty namespace rejected", dialect: schema.DialectMySQL, namespaces: []string{""}, wantErr: "non-empty"},
		{name: "surrounding whitespace rejected", dialect: schema.DialectMySQL, namespaces: []string{" orders "}, wantErr: "whitespace"},
		{name: "path traversal rejected", dialect: schema.DialectMySQL, namespaces: []string{"a..b"}, wantErr: "single path component"},
		{name: "path separator rejected", dialect: schema.DialectMySQL, namespaces: []string{`a/b`}, wantErr: "single path component"},
		{name: "unresolved env placeholder rejected", dialect: schema.DialectMySQL, namespaces: []string{"orders_$ENV"}, wantErr: "$ENV"},
		{name: "duplicate namespace rejected", dialect: schema.DialectMySQL, namespaces: []string{"orders", "orders"}, wantErr: "duplicate"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := pullNamespaces(tc.dialect, tc.namespaces)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}
