//go:build integration

package schema

import (
	"database/sql"
	"strings"
	"testing"

	"github.com/block/spirit/pkg/statement"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/testutil"
)

// TestPostgresSchemaFilesExecuteAndMirrorMySQL executes every embedded
// postgres schema file against a real PostgreSQL server, then verifies each
// table mirrors its MySQL counterpart column-for-column: same column names,
// same nullability, defaults, the declared type mapping, and identical varchar widths.
// The MySQL side is parsed from the embedded files; the PostgreSQL side is
// read back from information_schema after the DDL runs, so the comparison
// covers what the server actually created, not what the file claims.
func TestPostgresSchemaFilesExecuteAndMirrorMySQL(t *testing.T) {
	ctx := t.Context()

	_, db := testutil.StartPostgres(t, "schemabot_test")

	for name, content := range readSchemaDir(t, "postgres") {
		for stmt := range strings.SplitSeq(content, ";") {
			stmt = strings.TrimSpace(stmt)
			if stmt == "" {
				continue
			}
			_, err := db.ExecContext(ctx, stmt)
			require.NoError(t, err, "%s: statement failed:\n%s", name, stmt)
		}
	}

	for name, content := range readSchemaDir(t, "mysql") {
		parsed, err := statement.ParseCreateTable(content)
		require.NoError(t, err, "parse mysql schema file %s", name)

		pgColumns := postgresColumns(t, db, parsed.TableName)
		require.NotEmpty(t, pgColumns, "table %s missing from postgres schema", parsed.TableName)

		mysqlNames := make([]string, 0, len(parsed.Columns))
		for _, col := range parsed.Columns {
			mysqlNames = append(mysqlNames, col.Name)
			pgCol, ok := pgColumns[col.Name]
			if !assert.True(t, ok, "table %s: column %s missing from postgres schema", parsed.TableName, col.Name) {
				continue
			}
			assert.Equal(t, col.Nullable, pgCol.nullable,
				"table %s: column %s nullability differs (mysql nullable=%v)", parsed.TableName, col.Name, col.Nullable)

			wantType, ok := expectedPostgresColumnType(col)
			require.True(t, ok, "table %s: column %s has mysql type %q with no declared postgres mapping", parsed.TableName, col.Name, col.Type)
			assert.Equal(t, wantType, pgCol.dataType,
				"table %s: column %s type differs (mysql type=%q)", parsed.TableName, col.Name, col.Type)

			assert.Equal(t, col.AutoInc, pgCol.identity,
				"table %s: column %s identity differs (mysql auto_increment=%v)",
				parsed.TableName, col.Name, col.AutoInc)

			// PostgreSQL identity columns report no column_default because their
			// sequence is represented by identity metadata (asserted above)
			// rather than a default.
			if !col.AutoInc {
				mysqlDefault := "<nil>"
				if col.Default != nil {
					mysqlDefault = *col.Default
				}
				assert.Equal(t, normalizedMySQLDefault(col), normalizedPostgresDefault(col, pgCol.columnDefault),
					"table %s: column %s default differs (mysql default=%s, postgres default=%v)",
					parsed.TableName, col.Name, mysqlDefault, pgCol.columnDefault)
			}

			if col.Type == "varchar" {
				require.NotNil(t, col.Length, "table %s: mysql varchar column %s has no length", parsed.TableName, col.Name)
				if assert.True(t, pgCol.charMaxLen.Valid, "table %s: postgres column %s has no character maximum length", parsed.TableName, col.Name) {
					assert.Equal(t, int64(*col.Length), pgCol.charMaxLen.Int64,
						"table %s: column %s varchar width differs", parsed.TableName, col.Name)
				}
			}
		}
		pgNames := make([]string, 0, len(pgColumns))
		for colName := range pgColumns {
			pgNames = append(pgNames, colName)
		}
		assert.ElementsMatch(t, mysqlNames, pgNames, "table %s: column sets differ", parsed.TableName)
	}
}

// postgresColumn is one column definition read back from information_schema
// after the DDL ran, so assertions cover what the server actually created.
type postgresColumn struct {
	nullable      bool
	dataType      string
	charMaxLen    sql.NullInt64
	columnDefault sql.NullString
	identity      bool
}

// postgresColumns returns column name → definition for a table in the public
// schema of the connected PostgreSQL database.
func postgresColumns(t *testing.T, db *sql.DB, tableName string) map[string]postgresColumn {
	t.Helper()

	rows, err := db.QueryContext(t.Context(),
		`SELECT column_name, is_nullable, data_type, character_maximum_length, column_default, is_identity
		 FROM information_schema.columns
		 WHERE table_schema = 'public' AND table_name = $1`,
		tableName,
	)
	require.NoError(t, err)
	defer utils.CloseAndLog(rows)

	columns := make(map[string]postgresColumn)
	for rows.Next() {
		var colName, isNullable, isIdentity string
		var col postgresColumn
		require.NoError(t, rows.Scan(&colName, &isNullable, &col.dataType, &col.charMaxLen, &col.columnDefault, &isIdentity))
		col.nullable = isNullable == "YES"
		col.identity = isIdentity == "YES"
		columns[colName] = col
	}
	require.NoError(t, rows.Err())
	return columns
}

// expectedPostgresColumnType returns the information_schema data_type the
// postgres translation of a TiDB-parsed mysql column must produce. This is
// the single declaration of the cross-dialect type mapping: identity bigint
// PKs, bigint for unsigned ints (PostgreSQL has no unsigned types and integer
// would halve the range), boolean for tinyint(1), jsonb for json, and
// zone-less timestamp for zone-less datetime.
func expectedPostgresColumnType(col statement.Column) (string, bool) {
	unsigned := col.Unsigned != nil && *col.Unsigned
	switch col.Type {
	case "tinyint":
		if col.Length == nil || *col.Length != 1 {
			return "", false
		}
		return "boolean", true
	case "int":
		if unsigned {
			return "bigint", true
		}
		return "integer", true
	case "bigint":
		return "bigint", true
	case "varchar":
		return "character varying", true
	case "text":
		return "text", true
	case "datetime":
		return "timestamp without time zone", true
	case "json":
		return "jsonb", true
	default:
		return "", false
	}
}
