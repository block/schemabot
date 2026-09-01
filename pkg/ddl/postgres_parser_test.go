package ddl

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
