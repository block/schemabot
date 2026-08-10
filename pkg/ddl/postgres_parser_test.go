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

	t.Run("unparseable input is returned unchanged", func(t *testing.T) {
		in := "THIS IS NOT SQL"
		assert.Equal(t, in, p.Canonicalize(in))
	})

	t.Run("multi-statement input is returned unchanged", func(t *testing.T) {
		in := "DROP TABLE a; DROP TABLE b"
		assert.Equal(t, in, p.Canonicalize(in))
	})
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
