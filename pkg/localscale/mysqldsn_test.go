package localscale

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMySQLDSNSelectsDatabase(t *testing.T) {
	tests := []struct {
		name     string
		dsn      string
		database string
		want     string
	}{
		{
			name:     "database appended to a base DSN naming none",
			dsn:      "vt_dba@unix(/tmp/vt/mysql.sock)/",
			database: "branch_feature_testapp",
			want:     "vt_dba@unix(/tmp/vt/mysql.sock)/branch_feature_testapp",
		},
		{
			name:     "empty database keeps the DSN's own",
			dsn:      "root@tcp(127.0.0.1:15306)/testapp",
			database: "",
			want:     "root@tcp(127.0.0.1:15306)/testapp",
		},
		{
			name:     "database overrides the DSN's own",
			dsn:      "root@tcp(127.0.0.1:15306)/testapp",
			database: "localscale",
			want:     "root@tcp(127.0.0.1:15306)/localscale",
		},
		{
			name:     "existing parameters survive",
			dsn:      "root@tcp(127.0.0.1:15306)/?interpolateParams=true",
			database: "testapp",
			want:     "root@tcp(127.0.0.1:15306)/testapp?interpolateParams=true",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := mysqlDSN(tt.dsn, tt.database)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestMySQLDSNRejectsUnparseableDSN(t *testing.T) {
	_, err := mysqlDSN("root@tcp(127.0.0.1:15306", "testapp")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse MySQL DSN")
}
