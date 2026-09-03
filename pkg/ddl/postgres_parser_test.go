package ddl

import (
	"io/fs"
	"strings"
	"testing"

	pgproto "github.com/pganalyze/pg_query_go/v6"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pgquery "github.com/wasilibs/go-pgquery"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/block/schemabot/pkg/schema"
)

func TestParserForDialect(t *testing.T) {
	t.Run("MySQL family gets the TiDB parser", func(t *testing.T) {
		p, err := ParserForDialect(schema.DialectMySQL)
		require.NoError(t, err)
		assert.IsType(t, tidbStatementParser{}, p)
	})

	t.Run("Postgres gets the libpg_query parser", func(t *testing.T) {
		p, err := ParserForDialect(schema.DialectPostgres)
		require.NoError(t, err)
		assert.IsType(t, postgresStatementParser{}, p)
	})

	t.Run("an unregistered dialect fails closed", func(t *testing.T) {
		p, err := ParserForDialect(schema.Dialect("oracle"))
		require.ErrorContains(t, err, `no statement parser registered for dialect "oracle"`)
		assert.Nil(t, p)
	})
}

func TestPostgresParserSplit(t *testing.T) {
	p := postgresStatementParser{}

	t.Run("empty content yields nil", func(t *testing.T) {
		got, err := p.Split("   \n\t  ")
		require.NoError(t, err)
		assert.Nil(t, got)
	})

	t.Run("splits multiple statements preserving original text", func(t *testing.T) {
		content := `CREATE TABLE users (id BIGINT PRIMARY KEY, email TEXT);

ALTER TABLE users
    ADD COLUMN created_at TIMESTAMPTZ NOT NULL DEFAULT now();
CREATE INDEX idx_users_email ON users (email);`
		got, err := p.Split(content)
		require.NoError(t, err)
		require.Len(t, got, 3)
		assert.Equal(t, "CREATE TABLE users (id BIGINT PRIMARY KEY, email TEXT)", got[0])
		assert.Equal(t, "ALTER TABLE users\n    ADD COLUMN created_at TIMESTAMPTZ NOT NULL DEFAULT now()", got[1])
		assert.Equal(t, "CREATE INDEX idx_users_email ON users (email)", got[2])
	})

	t.Run("handles Postgres-only syntax the MySQL parser rejects", func(t *testing.T) {
		content := `CREATE TABLE events (id BIGSERIAL PRIMARY KEY, payload JSONB);
CREATE INDEX CONCURRENTLY idx_events_payload ON events USING GIN (payload);`
		got, err := p.Split(content)
		require.NoError(t, err)
		require.Len(t, got, 2)
		assert.Contains(t, got[0], "BIGSERIAL")
		assert.Contains(t, got[1], "CONCURRENTLY")
	})

	t.Run("preserves dollar-quoted bodies containing semicolons", func(t *testing.T) {
		content := `CREATE FUNCTION touch() RETURNS trigger AS $$ BEGIN NEW.updated_at = now(); RETURN NEW; END; $$ LANGUAGE plpgsql;
CREATE TABLE t (id INT);`
		got, err := p.Split(content)
		require.NoError(t, err)
		require.Len(t, got, 2)
		assert.Contains(t, got[0], "$$ BEGIN NEW.updated_at = now(); RETURN NEW; END; $$")
		assert.Equal(t, "CREATE TABLE t (id INT)", got[1])
	})

	t.Run("invalid SQL surfaces a bounded parse error", func(t *testing.T) {
		longGarbage := "CREATE GARBAGE " + strings.Repeat("x", 200)
		_, err := p.Split(longGarbage)
		require.ErrorContains(t, err, "failed to parse SQL statements")
		assert.Less(t, len(err.Error()), 250, "parse errors must not embed unbounded statement text")
	})
}

func TestCreateSetStatements(t *testing.T) {
	postgresParser, err := ParserForDialect(schema.DialectPostgres)
	require.NoError(t, err)
	mysqlParser, err := ParserForDialect(schema.DialectMySQL)
	require.NoError(t, err)

	tests := []struct {
		name       string
		parser     StatementParser
		script     string
		want       []string
		wantErrMsg string
	}{
		{
			name:   "single Postgres statement",
			parser: postgresParser,
			script: "ALTER TABLE t ADD COLUMN v text;",
			want:   []string{"ALTER TABLE t ADD COLUMN v text"},
		},
		{
			name:   "Postgres create set",
			parser: postgresParser,
			script: "CREATE TABLE t (id bigint, v text); CREATE INDEX t_v_idx ON t (v); CREATE UNIQUE INDEX t_id_idx ON t (id);",
			want: []string{
				"CREATE TABLE t (id bigint, v text)",
				"CREATE INDEX t_v_idx ON t (v)",
				"CREATE UNIQUE INDEX t_id_idx ON t (id)",
			},
		},
		{
			name:       "index targets another table",
			parser:     postgresParser,
			script:     "CREATE TABLE t (id bigint); CREATE INDEX other_id_idx ON other (id);",
			wantErrMsg: `statement 2 creates an index on table "other", not CREATE TABLE target "t"`,
		},
		{
			name:   "schema-qualified targets match",
			parser: postgresParser,
			script: "CREATE TABLE a.t (id bigint); CREATE INDEX t_id_idx ON a.t (id);",
			want: []string{
				"CREATE TABLE a.t (id bigint)",
				"CREATE INDEX t_id_idx ON a.t (id)",
			},
		},
		{
			name:       "schema-qualified targets differ",
			parser:     postgresParser,
			script:     "CREATE TABLE a.t (id bigint); CREATE INDEX t_id_idx ON b.t (id);",
			wantErrMsg: `statement 2 creates an index on table "b.t", not CREATE TABLE target "a.t"`,
		},
		{
			name:       "qualified and unqualified targets differ",
			parser:     postgresParser,
			script:     "CREATE TABLE a.t (id bigint); CREATE INDEX t_id_idx ON t (id);",
			wantErrMsg: `statement 2 creates an index on table "t", not CREATE TABLE target "a.t"`,
		},
		{
			name:       "alter follows create table",
			parser:     postgresParser,
			script:     "CREATE TABLE t (id bigint); ALTER TABLE t ADD COLUMN v text;",
			wantErrMsg: "statement 2 is ALTER TABLE",
		},
		{
			name:       "two create tables",
			parser:     postgresParser,
			script:     "CREATE TABLE t (id bigint); CREATE TABLE u (id bigint);",
			wantErrMsg: "statement 2 is CREATE TABLE",
		},
		{
			name:       "index first",
			parser:     postgresParser,
			script:     "CREATE INDEX t_id_idx ON t (id); CREATE INDEX t_v_idx ON t (v);",
			wantErrMsg: "statement 1 is CREATE INDEX",
		},
		{
			name:       "DML follows create table",
			parser:     postgresParser,
			script:     "CREATE TABLE t (id bigint); INSERT INTO t (id) VALUES (1);",
			wantErrMsg: "statement 2 is INSERT",
		},
		{
			name:       "empty",
			parser:     postgresParser,
			script:     "  ",
			wantErrMsg: "DDL script contains no statements",
		},
		{
			name:   "single MySQL statement is unchanged",
			parser: mysqlParser,
			script: "  ALTER TABLE `t` ADD COLUMN `v` varchar(20)  ",
			want:   []string{"ALTER TABLE `t` ADD COLUMN `v` varchar(20)"},
		},
		{
			name:       "MySQL multi-statement create set is unsupported",
			parser:     mysqlParser,
			script:     "CREATE TABLE `t` (`id` bigint); CREATE INDEX `t_id_idx` ON `t` (`id`)",
			wantErrMsg: "multi-statement DDL scripts are not supported for this dialect",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := CreateSetStatements(tc.parser, tc.script)
			if tc.wantErrMsg != "" {
				require.ErrorContains(t, err, tc.wantErrMsg)
				assert.Nil(t, got)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}

	t.Run("returns the first statement classification", func(t *testing.T) {
		got, err := ParseCreateSet(postgresParser, "CREATE TABLE a.t (id bigint); CREATE INDEX t_id_idx ON a.t (id)")
		require.NoError(t, err)
		assert.Equal(t, StatementCreateTable, got.Type)
		assert.Equal(t, "t", got.Table)
		assert.Len(t, got.Statements, 2)
	})
}

func TestPostgresParserClassify(t *testing.T) {
	p := postgresStatementParser{}

	tests := []struct {
		name      string
		stmt      string
		wantType  StatementType
		wantTable string
	}{
		{"create table", "CREATE TABLE users (id BIGINT PRIMARY KEY)", StatementCreateTable, "users"},
		{"create table schema-qualified returns bare name", "CREATE TABLE app.users (id BIGINT PRIMARY KEY)", StatementCreateTable, "users"},
		{"alter table", "ALTER TABLE users ADD COLUMN email TEXT", StatementAlterTable, "users"},
		{"alter table rename column is an alteration", "ALTER TABLE users RENAME COLUMN email TO email_address", StatementAlterTable, "users"},
		{"alter table rename constraint is an alteration", "ALTER TABLE users RENAME CONSTRAINT users_pk TO users_pkey", StatementAlterTable, "users"},
		{"alter table rename to is a table rename", "ALTER TABLE users RENAME TO customers", StatementRenameTable, "users"},
		{"drop table", "DROP TABLE users", StatementDropTable, "users"},
		{"drop table schema-qualified returns bare name", "DROP TABLE app.users", StatementDropTable, "users"},
		{"drop table multiple returns the first", "DROP TABLE users, orders", StatementDropTable, "users"},
		{"truncate", "TRUNCATE TABLE users", StatementTruncateTable, "users"},
		{"create index names the indexed table", "CREATE INDEX idx_users_email ON users (email)", StatementCreateIndex, "users"},
		{"drop index has no table name", "DROP INDEX idx_users_email", StatementDropIndex, ""},
		{"create view", "CREATE VIEW active_users AS SELECT id FROM users", StatementCreateView, "active_users"},
		{"insert", "INSERT INTO users (id) VALUES (1)", StatementInsert, "users"},
		{"update", "UPDATE users SET email = 'x'", StatementUpdate, "users"},
		{"delete", "DELETE FROM users WHERE id = 1", StatementDelete, "users"},
		{"select is outside the vocabulary", "SELECT 1", StatementUnknown, ""},
		{"alter view is outside the vocabulary", "ALTER VIEW active_users RENAME TO current_users", StatementUnknown, ""},
		{"drop view is outside the vocabulary", "DROP VIEW active_users", StatementUnknown, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotType, gotTable, err := p.Classify(tc.stmt)
			require.NoError(t, err)
			assert.Equal(t, tc.wantType, gotType, "statement type")
			assert.Equal(t, tc.wantTable, gotTable, "table name")
		})
	}

	t.Run("rejects multi-statement input", func(t *testing.T) {
		_, _, err := p.Classify("DROP TABLE users; CREATE TABLE users (id INT)")
		require.ErrorContains(t, err, "expected a single statement")
		require.ErrorContains(t, err, "split with the parser's Split before classifying")
	})

	t.Run("invalid SQL surfaces a bounded parse error", func(t *testing.T) {
		_, _, err := p.Classify("ALTER GARBAGE " + strings.Repeat("x", 200))
		require.ErrorContains(t, err, "classify statement")
		assert.Less(t, len(err.Error()), 250, "parse errors must not embed unbounded statement text")
	})

	t.Run("empty input reports no classification result", func(t *testing.T) {
		_, _, err := p.Classify("   ")
		require.ErrorContains(t, err, "no classification result")
	})
}

func TestPostgresParserCanonicalize(t *testing.T) {
	p := postgresStatementParser{}

	t.Run("normalizes formatting through the deparser", func(t *testing.T) {
		got := p.Canonicalize("alter   table users\n  add column email text")
		assert.Equal(t, "ALTER TABLE users ADD COLUMN email text", got)
	})

	t.Run("create table round-trips", func(t *testing.T) {
		got := p.Canonicalize("create table users(id bigint primary key, email text not null)")
		assert.Contains(t, got, "CREATE TABLE users")
		assert.Contains(t, got, "bigint")
		assert.Contains(t, got, "NOT NULL")
	})

	t.Run("drop column keeps the explicit COLUMN keyword", func(t *testing.T) {
		got := p.Canonicalize("alter table public.users drop column legacy")
		assert.Equal(t, "ALTER TABLE public.users DROP COLUMN legacy", got)
	})

	t.Run("drop column keyword survives IF EXISTS and CASCADE", func(t *testing.T) {
		got := p.Canonicalize("alter table users drop column if exists legacy cascade")
		assert.Equal(t, "ALTER TABLE users DROP COLUMN IF EXISTS legacy CASCADE", got)
	})

	t.Run("quoted drop column identifiers keep the keyword", func(t *testing.T) {
		got := p.Canonicalize(`alter table users drop column "Legacy"`)
		assert.Equal(t, `ALTER TABLE users DROP COLUMN "Legacy"`, got)
	})

	t.Run("non-column drops are untouched", func(t *testing.T) {
		got := p.Canonicalize("alter table users drop constraint users_pkey, alter column c drop default, alter column d drop not null")
		assert.Equal(t, "ALTER TABLE users DROP CONSTRAINT users_pkey, ALTER COLUMN c DROP DEFAULT, ALTER COLUMN d DROP NOT NULL", got)
	})

	t.Run("mixed column and constraint drops rewrite only the column", func(t *testing.T) {
		got := p.Canonicalize("alter table users drop column legacy, drop constraint users_pkey")
		assert.Equal(t, "ALTER TABLE users DROP COLUMN legacy, DROP CONSTRAINT users_pkey", got)
	})

	t.Run("unparseable input is returned unchanged", func(t *testing.T) {
		in := "THIS IS NOT SQL"
		assert.Equal(t, in, p.Canonicalize(in))
	})

	t.Run("multi-statement input is returned unchanged", func(t *testing.T) {
		in := "DROP TABLE a; DROP TABLE b"
		assert.Equal(t, in, p.Canonicalize(in))
	})
}

func TestPostgresParserCreateTableColumns(t *testing.T) {
	p := postgresStatementParser{}

	columns, err := p.CreateTableColumns(`CREATE TABLE example (
  id bigint,
  "display name" text DEFAULT $$value,with punctuation$$,
  index integer,
  exclude integer,
  PRIMARY KEY (id),
  UNIQUE(id),
  CHECK(id > 0),
  EXCLUDE USING gist (id WITH =)
)`)
	require.NoError(t, err)
	assert.Equal(t, []string{"id", "display name", "index", "exclude"}, columns)

	_, err = p.CreateTableColumns("ALTER TABLE example ADD COLUMN value text")
	require.ErrorContains(t, err, "expected CREATE TABLE statement")
}

func TestPostgresParserSynthesizeAddColumn(t *testing.T) {
	p := postgresStatementParser{}

	tests := []struct {
		name       string
		createDDL  string
		columnName string
		want       string
	}{
		{"plain column", "CREATE TABLE users (id bigint, email text)", "email", "ALTER TABLE users ADD COLUMN email text"},
		{"not null and default", "CREATE TABLE users (id bigint, enabled boolean NOT NULL DEFAULT true)", "enabled", "ALTER TABLE users ADD COLUMN enabled boolean NOT NULL DEFAULT true"},
		{"type modifier", "CREATE TABLE users (name varchar(255))", "name", "ALTER TABLE users ADD COLUMN name varchar(255)"},
		{"timestamp function default", "CREATE TABLE users (updated_at timestamptz DEFAULT now())", "updated_at", "ALTER TABLE users ADD COLUMN updated_at timestamptz DEFAULT now()"},
		{"schema-qualified table", "CREATE TABLE app.users (id bigint)", "id", "ALTER TABLE app.users ADD COLUMN id bigint"},
		{"quoted identifiers", `CREATE TABLE "App"."UserProfiles" ("DisplayName" varchar(255) NOT NULL)`, "DisplayName", `ALTER TABLE "App"."UserProfiles" ADD COLUMN "DisplayName" varchar(255) NOT NULL`},
		{"collation", `CREATE TABLE users (name text COLLATE "C")`, "name", `ALTER TABLE users ADD COLUMN name text COLLATE "C"`},
		{"identity", "CREATE TABLE users (id bigint GENERATED BY DEFAULT AS IDENTITY)", "id", "ALTER TABLE users ADD COLUMN id bigint GENERATED BY DEFAULT AS IDENTITY"},
		{"generated stored", "CREATE TABLE items (qty integer, price numeric, total numeric GENERATED ALWAYS AS (qty * price) STORED)", "total", "ALTER TABLE items ADD COLUMN total numeric GENERATED ALWAYS AS (qty * price) STORED"},
		{"array type", "CREATE TABLE users (tags text[])", "tags", "ALTER TABLE users ADD COLUMN tags text[]"},
		{"storage mode", "CREATE TABLE users (payload bytea STORAGE EXTERNAL)", "payload", "ALTER TABLE users ADD COLUMN payload bytea STORAGE external"},
		{"compression method", "CREATE TABLE users (document text COMPRESSION lz4)", "document", "ALTER TABLE users ADD COLUMN document text COMPRESSION lz4"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := p.SynthesizeAddColumn(tc.createDDL, tc.columnName)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}

	t.Run("table-level constraints are not carried", func(t *testing.T) {
		got, err := p.SynthesizeAddColumn("CREATE TABLE users (id bigint, email text, PRIMARY KEY (id), UNIQUE (email), CHECK (id > 0))", "id")
		require.NoError(t, err)
		assert.Equal(t, "ALTER TABLE users ADD COLUMN id bigint", got)
	})

	t.Run("column not found", func(t *testing.T) {
		_, err := p.SynthesizeAddColumn("CREATE TABLE users (id bigint)", "email")
		require.ErrorContains(t, err, `column "email" not found`)
	})

	t.Run("multiple statements", func(t *testing.T) {
		_, err := p.SynthesizeAddColumn("CREATE TABLE users (id bigint); CREATE TABLE teams (id bigint)", "id")
		require.ErrorContains(t, err, "expected one CREATE TABLE statement, got 2")
	})

	t.Run("not a CREATE TABLE", func(t *testing.T) {
		_, err := p.SynthesizeAddColumn("ALTER TABLE users ADD COLUMN email text", "email")
		require.ErrorContains(t, err, "expected CREATE TABLE statement")
	})

	t.Run("parse failure", func(t *testing.T) {
		_, err := p.SynthesizeAddColumn("CREATE TABLE users (", "email")
		require.ErrorContains(t, err, "parse CREATE TABLE")
	})
}

// Every column of every CREATE TABLE in the embedded storage schema must
// round-trip through synthesis with its declaration intact: the ColumnDef
// carried by the synthesized ALTER must equal the ColumnDef declared in the
// CREATE TABLE, node for node, so no type, default, NOT NULL, identity, or
// collation clause can be silently dropped. This proves faithfulness only; it
// says nothing about whether PostgreSQL would accept the statement on a
// populated table, which is the caller's judgment.
func TestPostgresParserSynthesizeAddColumn_EmbeddedSchemaRoundTrips(t *testing.T) {
	files, err := fs.Glob(schema.PostgresFS, "postgres/*.sql")
	require.NoError(t, err)
	require.NotEmpty(t, files)
	p := postgresStatementParser{}

	for _, file := range files {
		content, err := schema.PostgresFS.ReadFile(file)
		require.NoError(t, err, "read %s", file)
		statements, err := p.Split(string(content))
		require.NoError(t, err, "split %s", file)
		require.NotEmpty(t, statements, "schema file %s", file)

		createTables := 0
		for _, stmt := range statements {
			kind, _, err := p.Classify(stmt)
			require.NoError(t, err, "classify in %s", file)
			if kind != StatementCreateTable {
				continue
			}
			createTables++
			columns, err := p.CreateTableColumns(stmt)
			require.NoError(t, err, "columns in %s", file)

			for _, column := range columns {
				ddl, err := p.SynthesizeAddColumn(stmt, column)
				require.NoError(t, err, "%s column %s", file, column)
				want := columnDefFromCreateTable(t, stmt, column)
				got := columnDefFromAddColumn(t, ddl)
				clearParseLocations(want.ProtoReflect())
				clearParseLocations(got.ProtoReflect())
				assert.True(t, proto.Equal(want, got),
					"%s column %s: synthesized ColumnDef diverges from the CREATE TABLE declaration\nsynthesized: %s\nwant: %v\ngot:  %v",
					file, column, ddl, want, got)
			}
		}
		require.Positive(t, createTables, "schema file %s declares no CREATE TABLE", file)
	}
}

// columnDefFromCreateTable returns the named column's ColumnDef parse node
// from a single CREATE TABLE statement.
func columnDefFromCreateTable(t *testing.T, createDDL, column string) *pgproto.ColumnDef {
	t.Helper()
	parsed, err := pgquery.Parse(createDDL)
	require.NoError(t, err)
	require.Len(t, parsed.GetStmts(), 1)
	create, ok := parsed.GetStmts()[0].GetStmt().GetNode().(*pgproto.Node_CreateStmt)
	require.True(t, ok, "expected CREATE TABLE, got %q", createDDL)
	for _, element := range create.CreateStmt.GetTableElts() {
		def, ok := element.GetNode().(*pgproto.Node_ColumnDef)
		if ok && def.ColumnDef.GetColname() == column {
			return def.ColumnDef
		}
	}
	t.Fatalf("column %q not found in %q", column, createDDL)
	return nil
}

// columnDefFromAddColumn returns the ColumnDef parse node carried by a
// single-command ALTER TABLE ... ADD COLUMN statement.
func columnDefFromAddColumn(t *testing.T, alterDDL string) *pgproto.ColumnDef {
	t.Helper()
	parsed, err := pgquery.Parse(alterDDL)
	require.NoError(t, err, "parse %q", alterDDL)
	require.Len(t, parsed.GetStmts(), 1, "statement %q", alterDDL)
	alter, ok := parsed.GetStmts()[0].GetStmt().GetNode().(*pgproto.Node_AlterTableStmt)
	require.True(t, ok, "expected ALTER TABLE, got %q", alterDDL)
	require.Len(t, alter.AlterTableStmt.GetCmds(), 1, "statement %q", alterDDL)
	cmd, ok := alter.AlterTableStmt.GetCmds()[0].GetNode().(*pgproto.Node_AlterTableCmd)
	require.True(t, ok, "expected ALTER TABLE command in %q", alterDDL)
	require.Equal(t, pgproto.AlterTableType_AT_AddColumn, cmd.AlterTableCmd.GetSubtype(), "statement %q", alterDDL)
	def, ok := cmd.AlterTableCmd.GetDef().GetNode().(*pgproto.Node_ColumnDef)
	require.True(t, ok, "expected a ColumnDef in %q", alterDDL)
	return def.ColumnDef
}

// clearParseLocations recursively zeroes every source-text offset field in a
// parse tree, so trees parsed from different statement texts compare equal on
// structure alone.
func clearParseLocations(m protoreflect.Message) {
	m.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		switch {
		case fd.IsList():
			if fd.Kind() == protoreflect.MessageKind {
				list := v.List()
				for i := 0; i < list.Len(); i++ {
					clearParseLocations(list.Get(i).Message())
				}
			}
		case fd.Kind() == protoreflect.MessageKind:
			clearParseLocations(v.Message())
		case strings.Contains(string(fd.Name()), "location"):
			m.Clear(fd)
		}
		return true
	})
}

// The manual-remediation classifier reads the column's parsed constraints.
// Generated, identity, and unrecognized constraint shapes fail closed, quoted
// identifiers cannot mask a missing DEFAULT, and a function-call DEFAULT fails
// closed because its volatility cannot be proven from the statement alone.
func TestPostgresAddColumnManualReason(t *testing.T) {
	tests := []struct {
		name       string
		createDDL  string
		columnName string
		wantReason string
	}{
		{
			name:       "not null without default",
			createDDL:  "CREATE TABLE settings (id bigint, setting_value text NOT NULL)",
			columnName: "setting_value",
			wantReason: "NOT NULL without a DEFAULT",
		},
		{
			name:       "not null with constant default",
			createDDL:  "CREATE TABLE applies (id bigint, caller varchar(255) DEFAULT '' NOT NULL)",
			columnName: "caller",
		},
		{
			name:       "nullable",
			createDDL:  "CREATE TABLE applies (id bigint, expected_operation_keys jsonb)",
			columnName: "expected_operation_keys",
		},
		{
			name:       "generated stored not null",
			createDDL:  "CREATE TABLE metrics (a bigint, doubled bigint GENERATED ALWAYS AS (a * 2) STORED NOT NULL)",
			columnName: "doubled",
			wantReason: "generated or identity, which rewrites the whole table under an exclusive lock",
		},
		{
			name:       "identity not null",
			createDDL:  "CREATE TABLE metrics (a bigint, seq bigint GENERATED ALWAYS AS IDENTITY NOT NULL)",
			columnName: "seq",
			wantReason: "generated or identity, which rewrites the whole table under an exclusive lock",
		},
		{
			name:       "primary key",
			createDDL:  "CREATE TABLE metrics (a bigint, seq bigint PRIMARY KEY)",
			columnName: "seq",
			wantReason: "constraint CONSTR_PRIMARY",
		},
		{
			name:       "nullable unique",
			createDDL:  "CREATE TABLE metrics (a bigint, external_id bigint UNIQUE)",
			columnName: "external_id",
		},
		{
			name:       "nullable references",
			createDDL:  "CREATE TABLE metrics (a bigint, parent_id bigint REFERENCES parents (id))",
			columnName: "parent_id",
		},
		{
			name:       "quoted identifier containing default",
			createDDL:  `CREATE TABLE odd (id bigint, " default " text NOT NULL)`,
			columnName: " default ",
			wantReason: "NOT NULL without a DEFAULT",
		},
		{
			name:       "sql value function default",
			createDDL:  "CREATE TABLE applies (id bigint, created_at timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL)",
			columnName: "created_at",
		},
		{
			name:       "typecast constant default",
			createDDL:  "CREATE TABLE applies (id bigint, options jsonb DEFAULT '{}'::jsonb NOT NULL)",
			columnName: "options",
		},
		{
			name:       "volatile function default",
			createDDL:  "CREATE TABLE applies (id bigint, external_id uuid DEFAULT gen_random_uuid() NOT NULL)",
			columnName: "external_id",
			wantReason: "volatility cannot be proven",
		},
		{
			name:       "nullable volatile function default",
			createDDL:  "CREATE TABLE applies (id bigint, external_id uuid DEFAULT gen_random_uuid())",
			columnName: "external_id",
			wantReason: "volatility cannot be proven",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			reason, err := PostgresAddColumnManualReason(tc.createDDL, tc.columnName)
			require.NoError(t, err)
			if tc.wantReason == "" {
				assert.Empty(t, reason)
			} else {
				assert.Contains(t, reason, tc.wantReason)
			}
		})
	}

	t.Run("column not found", func(t *testing.T) {
		_, err := PostgresAddColumnManualReason("CREATE TABLE users (id bigint)", "email")
		require.ErrorContains(t, err, `column "email" not found`)
	})
}

func TestPostgresParserCreateIndex(t *testing.T) {
	p := postgresStatementParser{}

	indexName, tableName, unique, err := p.CreateIndex(`CREATE UNIQUE INDEX "event delivery" ON public.webhook_events (provider, delivery_id)`)
	require.NoError(t, err)
	assert.True(t, unique)
	assert.Equal(t, "event delivery", indexName)
	assert.Equal(t, "webhook_events", tableName)

	indexName, tableName, unique, err = p.CreateIndex("CREATE INDEX idx_events_state ON webhook_events (state)")
	require.NoError(t, err)
	assert.False(t, unique)
	assert.Equal(t, "idx_events_state", indexName)
	assert.Equal(t, "webhook_events", tableName)

	indexName, _, unique, err = p.CreateIndex("CREATE TABLE example (id bigint)")
	require.NoError(t, err)
	assert.False(t, unique)
	assert.Empty(t, indexName, "a non-index statement is not an index expectation")
}

// The two parser implementations must diverge where the grammars genuinely
// differ while honoring the same seam contract, so a caller routed through
// ParserForDialect gets dialect-correct behavior from identical inputs.
func TestParsersDivergeOnDialectSpecificSyntax(t *testing.T) {
	pg := postgresStatementParser{}
	tidb := tidbStatementParser{}

	t.Run("Postgres-only DDL parses on pg and fails on tidb", func(t *testing.T) {
		stmt := "CREATE INDEX CONCURRENTLY idx ON events USING GIN (payload)"
		gotType, gotTable, err := pg.Classify(stmt)
		require.NoError(t, err)
		assert.Equal(t, StatementCreateIndex, gotType)
		assert.Equal(t, "events", gotTable)

		_, _, err = tidb.Classify(stmt)
		require.Error(t, err, "the MySQL grammar must not accept Postgres-only syntax")
	})

	t.Run("MySQL-only DDL parses on tidb and fails on pg", func(t *testing.T) {
		stmt := "ALTER TABLE users ADD COLUMN email VARCHAR(255), ALGORITHM=INSTANT"
		gotType, gotTable, err := tidb.Classify(stmt)
		require.NoError(t, err)
		assert.Equal(t, StatementAlterTable, gotType)
		assert.Equal(t, "users", gotTable)

		_, _, err = pg.Classify(stmt)
		require.Error(t, err, "the Postgres grammar must not accept MySQL-only syntax")
	})

	t.Run("both classify shared ANSI DDL identically", func(t *testing.T) {
		stmt := "CREATE TABLE users (id BIGINT PRIMARY KEY)"
		pgType, pgTable, err := pg.Classify(stmt)
		require.NoError(t, err)
		tidbType, tidbTable, err := tidb.Classify(stmt)
		require.NoError(t, err)
		assert.Equal(t, tidbType, pgType)
		assert.Equal(t, tidbTable, pgTable)
	})
}

// CostScalesWithTableSize scopes the plan comment's table-size section: a
// statement reports true when it builds an index, rewrites the table, or
// scans it to validate a constraint or NOT NULL, so a size line renders
// exactly for the changes whose cost grows with the table. Provably
// metadata-only commands stay quiet.
func TestPostgresCostScalesWithTableSize(t *testing.T) {
	p := postgresStatementParser{}

	tests := []struct {
		name string
		stmt string
		want bool
	}{
		{"create index", `CREATE INDEX idx_created_at ON mutes (created_at)`, true},
		{"create unique index", `CREATE UNIQUE INDEX uniq_slug ON mutes (slug)`, true},
		{"add primary key", `ALTER TABLE mutes ADD PRIMARY KEY (id)`, true},
		{"add unique constraint", `ALTER TABLE mutes ADD CONSTRAINT uniq_slug UNIQUE (slug)`, true},
		{"add exclusion constraint", `ALTER TABLE mutes ADD CONSTRAINT excl_range EXCLUDE USING gist (during WITH &&)`, true},
		{"alter column type", `ALTER TABLE mutes ALTER COLUMN count TYPE bigint`, true},
		{"widen varchar", `ALTER TABLE mutes ALTER COLUMN reason TYPE varchar(500)`, true},
		{"set not null", `ALTER TABLE mutes ALTER COLUMN reason SET NOT NULL`, true},
		{"add foreign key", `ALTER TABLE mutes ADD CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users (id)`, true},
		{"add check constraint", `ALTER TABLE mutes ADD CONSTRAINT chk_positive CHECK (count > 0)`, true},
		{"add column with volatile default", `ALTER TABLE mutes ADD COLUMN token uuid DEFAULT gen_random_uuid()`, true},
		{"add column with inline unique", `ALTER TABLE mutes ADD COLUMN slug varchar(64) UNIQUE`, true},
		{"add generated column", `ALTER TABLE mutes ADD COLUMN total int GENERATED ALWAYS AS (a + b) STORED`, true},
		{"add foreign key not valid", `ALTER TABLE mutes ADD CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users (id) NOT VALID`, false},
		{"add check constraint not valid", `ALTER TABLE mutes ADD CONSTRAINT chk_positive CHECK (count > 0) NOT VALID`, false},
		{"add column only", `ALTER TABLE mutes ADD COLUMN reason varchar(255)`, false},
		{"add column with constant default", `ALTER TABLE mutes ADD COLUMN state text DEFAULT 'active'`, false},
		{"add column with cast constant default", `ALTER TABLE mutes ADD COLUMN payload jsonb DEFAULT '{}'::jsonb`, false},
		{"drop column", `ALTER TABLE mutes DROP COLUMN reason`, false},
		{"set default", `ALTER TABLE mutes ALTER COLUMN count SET DEFAULT 0`, false},
		{"drop not null", `ALTER TABLE mutes ALTER COLUMN reason DROP NOT NULL`, false},
		{"drop constraint", `ALTER TABLE mutes DROP CONSTRAINT chk_positive`, false},
		{"create table with primary key", `CREATE TABLE mutes (id BIGINT PRIMARY KEY)`, false},
		{"drop index", `DROP INDEX idx_created_at`, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := p.CostScalesWithTableSize(tt.stmt)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}

	t.Run("unparseable statement is an error", func(t *testing.T) {
		_, err := p.CostScalesWithTableSize("ALTER TABLE ADD INDEX WHAT")
		assert.Error(t, err)
	})

	t.Run("multi-statement input is an error", func(t *testing.T) {
		_, err := p.CostScalesWithTableSize("CREATE INDEX i ON a (c); DROP TABLE b")
		assert.Error(t, err)
	})
}
