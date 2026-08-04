package spirit

import (
	"log/slog"
	"testing"

	"github.com/block/spirit/pkg/migration/check"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// discardLogger is for call sites whose logging is not what the test asserts on.
func discardLogger() *slog.Logger {
	return slog.New(slog.DiscardHandler)
}

// refusalOrdersTable is the current definition routing classifies against, in
// the form SHOW CREATE TABLE reports it.
const refusalOrdersTable = "CREATE TABLE `orders` (\n" +
	"  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n" +
	"  `status` enum('new','shipped','done') NOT NULL DEFAULT 'new',\n" +
	"  `perms` set('read','write','execute') DEFAULT NULL,\n" +
	"  `name` varchar(100) DEFAULT NULL,\n" +
	"  PRIMARY KEY (`id`)\n" +
	") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"

// The engine's refusal verdict is what plan-time execution modes and apply-time
// routing are both built on, and it comes from Spirit rather than from anything
// this repo can see in a diff. This pins the shapes each side of the verdict at
// the unit tier — no MySQL, no Spirit run — so a Spirit bump that moves a
// statement between routed, refused, and unclassifiable is caught here, before
// the integration tests it would also change. The integration tests remain the
// proof that a refused shape really is refused by a live Spirit run.
func TestStatementRefusalContract(t *testing.T) {
	tests := []struct {
		name       string
		stmt       string
		wantReason string
	}{
		{
			name:       "drop primary key",
			stmt:       "ALTER TABLE orders DROP PRIMARY KEY, ADD PRIMARY KEY (name)",
			wantReason: "dropping primary key is not supported",
		},
		{
			name:       "add foreign key",
			stmt:       "ALTER TABLE orders ADD CONSTRAINT fk FOREIGN KEY (id) REFERENCES customers (id)",
			wantReason: "adding foreign key constraints is not supported",
		},
		{
			name:       "explicit algorithm clause",
			stmt:       "ALTER TABLE orders ADD COLUMN b INT, ALGORITHM=INPLACE",
			wantReason: "ALGORITHM=",
		},
		{
			name:       "explicit lock clause",
			stmt:       "ALTER TABLE orders ADD COLUMN b INT, LOCK=NONE",
			wantReason: "LOCK=",
		},
		{
			name:       "enum value reorder",
			stmt:       "ALTER TABLE orders MODIFY COLUMN status ENUM('shipped','new','done') NOT NULL",
			wantReason: `unsafe ENUM value reorder on column "status"`,
		},
		{
			name:       "enum value inserted in the middle",
			stmt:       "ALTER TABLE orders MODIFY COLUMN status ENUM('new','pending','shipped','done') NOT NULL",
			wantReason: `unsafe ENUM value reorder on column "status"`,
		},
		{
			name:       "set member reorder",
			stmt:       "ALTER TABLE orders MODIFY COLUMN perms SET('write','read','execute')",
			wantReason: `unsafe SET value reorder on column "perms"`,
		},
		{
			name:       "enum to numeric conversion",
			stmt:       "ALTER TABLE orders MODIFY COLUMN status INT NOT NULL",
			wantReason: `unsafe ENUM to int(11) type conversion on column "status"`,
		},
		{
			name:       "set to enum conversion",
			stmt:       "ALTER TABLE orders MODIFY COLUMN perms ENUM('read','write')",
			wantReason: `unsafe SET to ENUM type conversion on column "perms"`,
		},
		{
			name: "add column",
			stmt: "ALTER TABLE orders ADD COLUMN shipped_at DATETIME",
		},
		{
			name: "add index",
			stmt: "ALTER TABLE orders ADD INDEX idx_name (name)",
		},
		{
			name: "widen a column",
			stmt: "ALTER TABLE orders MODIFY COLUMN name VARCHAR(255)",
		},
		{
			name: "append enum values",
			stmt: "ALTER TABLE orders MODIFY COLUMN status ENUM('new','shipped','done','lost') NOT NULL",
		},
		{
			name: "drop enum values",
			stmt: "ALTER TABLE orders MODIFY COLUMN status ENUM('new','done') NOT NULL",
		},
		{
			name: "append set members",
			stmt: "ALTER TABLE orders MODIFY COLUMN perms SET('read','write','execute','admin')",
		},
		{
			name: "enum to varchar conversion",
			stmt: "ALTER TABLE orders MODIFY COLUMN status VARCHAR(20) NOT NULL",
		},
		// The shapes below are refused at Spirit's preflight, but MySQL's native
		// DDL — which Spirit attempts first for a single-table change — completes
		// them, so the verdict must stay silent rather than block an apply that
		// succeeds.
		{
			name: "drop and re-add the same column",
			stmt: "ALTER TABLE orders DROP COLUMN name, ADD COLUMN name VARCHAR(100)",
		},
		{
			name: "rename a column onto a freed name",
			stmt: "ALTER TABLE orders RENAME COLUMN name TO label, ADD COLUMN name VARCHAR(100)",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reason, refused, err := check.StatementRefusal(t.Context(), tt.stmt, refusalOrdersTable, discardLogger())
			require.NoError(t, err)
			if tt.wantReason == "" {
				assert.False(t, refused, "statement must route to the engine, not be reported as refused")
				assert.Empty(t, reason)
				return
			}
			require.True(t, refused)
			assert.Contains(t, reason, tt.wantReason)
		})
	}
}

// A statement the engine cannot judge against the definition supplied — here a
// column the definition does not carry, which is what drift between plan and
// apply looks like — is an error, never a refusal. Routing fails the apply on
// it rather than treating it as a refusal and executing the statement directly.
func TestStatementRefusalUnclassifiable(t *testing.T) {
	reason, refused, err := check.StatementRefusal(t.Context(),
		"ALTER TABLE orders MODIFY COLUMN dropped_col ENUM('a','b')", refusalOrdersTable, discardLogger())
	require.Error(t, err)
	assert.False(t, refused)
	assert.Empty(t, reason)
}
