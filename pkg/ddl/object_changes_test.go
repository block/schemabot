package ddl

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/schema"
)

func col(name string) SchemaObject          { return SchemaObject{Kind: ObjectColumn, Name: name} }
func idx(name string) SchemaObject          { return SchemaObject{Kind: ObjectIndex, Name: name} }
func tbl(name string) SchemaObject          { return SchemaObject{Kind: ObjectTable, Name: name} }
func cons(name string) SchemaObject         { return SchemaObject{Kind: ObjectConstraint, Name: name} }
func part(name string) SchemaObject         { return SchemaObject{Kind: ObjectPartition, Name: name} }
func objs(o ...SchemaObject) []SchemaObject { return o }

func TestTiDBObjectChanges(t *testing.T) {
	p, err := ParserForDialect(schema.DialectMySQL)
	require.NoError(t, err)

	tests := []struct {
		name string
		stmt string
		want ObjectChanges
	}{
		{
			name: "create table names its columns and keys",
			stmt: "CREATE TABLE `Orders` (id bigint, Total int, PRIMARY KEY (id), KEY idx_total (Total), CONSTRAINT fk_c FOREIGN KEY (id) REFERENCES c (id))",
			want: ObjectChanges{
				Added:      objs(tbl("orders"), col("id"), col("total"), idx("primary"), idx("idx_total"), cons("fk_c")),
				Referenced: objs(col("id"), col("total")),
			},
		},
		{
			name: "drop tables",
			stmt: "DROP TABLE a, b",
			want: ObjectChanges{Dropped: objs(tbl("a"), tbl("b"))},
		},
		{
			name: "create index",
			stmt: "CREATE INDEX idx_created_at ON events (created_at)",
			want: ObjectChanges{Added: objs(idx("idx_created_at")), Referenced: objs(col("created_at"))},
		},
		{
			name: "drop index",
			stmt: "DROP INDEX idx_created_at ON events",
			want: ObjectChanges{Dropped: objs(idx("idx_created_at"))},
		},
		{
			name: "alter adds an index",
			stmt: "ALTER TABLE events ADD INDEX idx_created_at (created_at)",
			want: ObjectChanges{Added: objs(idx("idx_created_at")), Referenced: objs(col("created_at"))},
		},
		{
			name: "an index added without a name references the table",
			stmt: "ALTER TABLE events ADD INDEX (created_at)",
			want: ObjectChanges{Referenced: objs(tbl("events"), col("created_at"))},
		},
		{
			name: "keys declared on an added column",
			stmt: "ALTER TABLE events ADD COLUMN code int UNIQUE, ADD COLUMN id2 bigint PRIMARY KEY",
			want: ObjectChanges{Added: objs(col("code"), col("id2"), idx("primary")), Referenced: objs(tbl("events"))},
		},
		{
			name: "a column visibility change modifies that column",
			stmt: "ALTER TABLE events ALTER COLUMN Payload SET INVISIBLE",
			want: ObjectChanges{Modified: objs(col("payload"))},
		},
		{
			name: "an expression index part references the table",
			stmt: "ALTER TABLE events ADD INDEX idx_day ((DATE(created_at)), kind)",
			want: ObjectChanges{Added: objs(idx("idx_day")), Referenced: objs(tbl("events"), col("kind"))},
		},
		{
			name: "a check references the table",
			stmt: "ALTER TABLE events ADD CONSTRAINT chk_total CHECK (total > 0)",
			want: ObjectChanges{Added: objs(cons("chk_total")), Referenced: objs(tbl("events"))},
		},
		{
			name: "alter drops a column and adds another",
			stmt: "ALTER TABLE events DROP COLUMN payload, ADD COLUMN body text",
			want: ObjectChanges{Added: objs(col("body")), Dropped: objs(col("payload"))},
		},
		{
			name: "change column renames",
			stmt: "ALTER TABLE events CHANGE payload body text",
			want: ObjectChanges{Added: objs(col("body")), Dropped: objs(col("payload"))},
		},
		{
			name: "change column under the same name modifies it",
			stmt: "ALTER TABLE events CHANGE payload payload mediumtext",
			want: ObjectChanges{Modified: objs(col("payload"))},
		},
		{
			name: "rename column",
			stmt: "ALTER TABLE events RENAME COLUMN payload TO body",
			want: ObjectChanges{Added: objs(col("body")), Dropped: objs(col("payload"))},
		},
		{
			name: "modify column",
			stmt: "ALTER TABLE events MODIFY payload mediumtext",
			want: ObjectChanges{Modified: objs(col("payload"))},
		},
		{
			name: "rename index",
			stmt: "ALTER TABLE events RENAME INDEX a TO b",
			want: ObjectChanges{Added: objs(idx("b")), Dropped: objs(idx("a"))},
		},
		{
			name: "drop primary key",
			stmt: "ALTER TABLE events DROP PRIMARY KEY",
			want: ObjectChanges{Dropped: objs(idx("primary"))},
		},
		{
			name: "drop foreign key",
			stmt: "ALTER TABLE events DROP FOREIGN KEY fk_c",
			want: ObjectChanges{Dropped: objs(cons("fk_c"))},
		},
		{
			name: "drop check",
			stmt: "ALTER TABLE events DROP CHECK chk_total",
			want: ObjectChanges{Dropped: objs(cons("chk_total"))},
		},
		{
			name: "drop constraint could name a key or a constraint",
			stmt: "ALTER TABLE events DROP CONSTRAINT uq_ref",
			want: ObjectChanges{Dropped: objs(idx("uq_ref"), cons("uq_ref"))},
		},
		{
			name: "drop partitions",
			stmt: "ALTER TABLE events DROP PARTITION p2020, p2021",
			want: ObjectChanges{Dropped: objs(part("p2020"), part("p2021"))},
		},
		{
			name: "table option changes the table",
			stmt: "ALTER TABLE events COMMENT 'audit log'",
			want: ObjectChanges{Modified: objs(tbl("events"))},
		},
		{
			name: "names fold to lower case",
			stmt: "ALTER TABLE events DROP COLUMN Payload, DROP INDEX IDX_A",
			want: ObjectChanges{Dropped: objs(col("payload"), idx("idx_a"))},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := p.ObjectChanges(tt.stmt)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}

	t.Run("rejects more than one statement", func(t *testing.T) {
		_, err := p.ObjectChanges("ALTER TABLE a DROP COLUMN x; ALTER TABLE b DROP COLUMN y")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "expected one statement")
	})
	t.Run("rejects what does not parse", func(t *testing.T) {
		_, err := p.ObjectChanges("this is not SQL")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "parse object changes")
	})
}

func TestPostgresObjectChanges(t *testing.T) {
	p, err := ParserForDialect(schema.DialectPostgres)
	require.NoError(t, err)

	tests := []struct {
		name string
		stmt string
		want ObjectChanges
	}{
		{
			name: "create table names its columns and constraints",
			stmt: "CREATE TABLE orders (id bigint CONSTRAINT orders_pkey PRIMARY KEY, total int, CONSTRAINT total_positive CHECK (total > 0))",
			want: ObjectChanges{
				Added:      objs(tbl("orders"), col("id"), cons("orders_pkey"), col("total"), cons("total_positive")),
				Referenced: objs(tbl("orders")),
			},
		},
		{
			name: "a constraint validated in a step of its own",
			stmt: "ALTER TABLE orders VALIDATE CONSTRAINT orders_customer_fk",
			want: ObjectChanges{Modified: objs(tbl("orders")), Validated: objs(cons("orders_customer_fk"))},
		},
		{
			name: "create index",
			stmt: "CREATE INDEX CONCURRENTLY idx_created_at ON events (created_at) INCLUDE (kind)",
			want: ObjectChanges{Added: objs(idx("idx_created_at")), Referenced: objs(col("created_at"), col("kind"))},
		},
		{
			name: "an index created without a name references the table",
			stmt: "CREATE INDEX ON events (created_at)",
			want: ObjectChanges{Referenced: objs(tbl("events"), col("created_at"))},
		},
		{
			name: "constraints declared on an added column",
			stmt: "ALTER TABLE events ADD COLUMN code int NOT NULL UNIQUE, ADD COLUMN ref int CONSTRAINT fk_ref REFERENCES refs (id)",
			want: ObjectChanges{Added: objs(col("code"), col("ref"), cons("fk_ref")), Referenced: objs(tbl("events"))},
		},
		{
			name: "a table constraint added without a name references the table",
			stmt: "ALTER TABLE events ADD UNIQUE (ref)",
			want: ObjectChanges{Referenced: objs(tbl("events"), col("ref"))},
		},
		{
			name: "a partial index references the table its predicate reads",
			stmt: "CREATE INDEX idx_live ON events (id) WHERE payload IS NOT NULL",
			want: ObjectChanges{Added: objs(idx("idx_live")), Referenced: objs(col("id"), tbl("events"))},
		},
		{
			name: "an exclusion constraint with a predicate references the table",
			stmt: "ALTER TABLE bookings ADD CONSTRAINT no_overlap EXCLUDE USING gist (room WITH =) WHERE (active)",
			want: ObjectChanges{Added: objs(cons("no_overlap")), Referenced: objs(tbl("bookings"))},
		},
		{
			name: "an expression index element references the table",
			stmt: "CREATE INDEX idx_lower_email ON users (lower(email))",
			want: ObjectChanges{Added: objs(idx("idx_lower_email")), Referenced: objs(tbl("users"))},
		},
		{
			name: "drop index",
			stmt: "DROP INDEX CONCURRENTLY public.idx_created_at",
			want: ObjectChanges{Dropped: objs(idx("idx_created_at"))},
		},
		{
			name: "drop tables",
			stmt: "DROP TABLE a, public.b",
			want: ObjectChanges{Dropped: objs(tbl("a"), tbl("b"))},
		},
		{
			name: "alter drops a column and adds another",
			stmt: "ALTER TABLE events DROP COLUMN payload, ADD COLUMN body text",
			want: ObjectChanges{Added: objs(col("body")), Dropped: objs(col("payload"))},
		},
		{
			name: "alter column type",
			stmt: "ALTER TABLE events ALTER COLUMN payload TYPE jsonb USING payload::jsonb",
			want: ObjectChanges{Modified: objs(col("payload"))},
		},
		{
			name: "set not null",
			stmt: "ALTER TABLE events ALTER COLUMN payload SET NOT NULL",
			want: ObjectChanges{Modified: objs(col("payload"))},
		},
		{
			name: "add and drop constraints",
			stmt: "ALTER TABLE events ADD CONSTRAINT uq_ref UNIQUE (ref), DROP CONSTRAINT chk_old",
			want: ObjectChanges{Added: objs(cons("uq_ref")), Dropped: objs(cons("chk_old")), Referenced: objs(col("ref"))},
		},
		{
			name: "a foreign key references its columns",
			stmt: "ALTER TABLE events ADD CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users (id)",
			want: ObjectChanges{Added: objs(cons("fk_user")), Referenced: objs(col("user_id"))},
		},
		{
			name: "rename column",
			stmt: "ALTER TABLE events RENAME COLUMN payload TO body",
			want: ObjectChanges{Added: objs(col("body")), Dropped: objs(col("payload"))},
		},
		{
			name: "rename index",
			stmt: "ALTER INDEX idx_a RENAME TO idx_b",
			want: ObjectChanges{Added: objs(idx("idx_b")), Dropped: objs(idx("idx_a"))},
		},
		{
			name: "table option changes the table",
			stmt: "ALTER TABLE events SET (fillfactor = 70)",
			want: ObjectChanges{Modified: objs(tbl("events"))},
		},
		{
			name: "a statement that names no table object",
			stmt: "CREATE TYPE mood AS ENUM ('ok', 'sad')",
			want: ObjectChanges{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := p.ObjectChanges(tt.stmt)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}

	t.Run("rejects more than one statement", func(t *testing.T) {
		_, err := p.ObjectChanges("DROP TABLE a; DROP TABLE b")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "expected one statement")
	})
}
