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

// refusalNoPrimaryKeyTable has no primary key, which Spirit cannot copy: every
// ALTER against it is refused at classification time rather than failing
// mid-run at table setup.
const refusalNoPrimaryKeyTable = "CREATE TABLE `orders_log` (\n" +
	"  `note` varchar(100) DEFAULT NULL,\n" +
	"  `name` varchar(100) DEFAULT NULL\n" +
	") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"

// The engine's refusal verdict is what plan-time execution modes and apply-time
// routing are both built on, and it comes from Spirit rather than from anything
// this repo can see in a diff. This pins the shapes each side of the verdict at
// the unit tier — no MySQL, no Spirit run — so a Spirit bump that moves a
// statement between routed, refused, and unclassifiable is caught here, before
// the integration tests it would also change. The integration tests remain the
// proof that a refused shape really is refused by a live Spirit run.
//
// A refused shape added here wants a case in
// TestStatementRefusalPublishesNothingFromTheTarget as well: its reason is
// published to the pull request, and that test is what holds a reason to the
// statement it reports.
func TestStatementRefusalContract(t *testing.T) {
	tests := []struct {
		name       string
		stmt       string
		table      string
		wantReason string
	}{
		{
			name:       "add a column to a table without a primary key",
			stmt:       "ALTER TABLE orders_log ADD COLUMN created_at DATETIME",
			table:      refusalNoPrimaryKeyTable,
			wantReason: "altering a table without a primary key is not supported",
		},
		{
			name:       "widen a column on a table without a primary key",
			stmt:       "ALTER TABLE orders_log MODIFY COLUMN name VARCHAR(255)",
			table:      refusalNoPrimaryKeyTable,
			wantReason: "altering a table without a primary key is not supported",
		},
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
			table := tt.table
			if table == "" {
				table = refusalOrdersTable
			}
			reason, refused, err := check.StatementRefusal(t.Context(), tt.stmt, table, discardLogger())
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

// canaryPrefix marks every value in the canary definitions below that exists
// only on the target. One prefix covers all of them, so a single assertion
// catches a leak from any of them.
const canaryPrefix = "cnry"

// refusalCanaryOrdersTable is refusalOrdersTable with every piece of the
// definition that a statement below does not redeclare replaced by a canary:
// the current ENUM members, the current SET members, an untouched column's
// name, and a default. A reason may name what the statement declares — that
// text is already on the pull request — so only these target-only values
// distinguish the two inputs.
const refusalCanaryOrdersTable = "CREATE TABLE `orders` (\n" +
	"  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n" +
	"  `status` enum('cnry_pending','cnry_settled') NOT NULL DEFAULT 'cnry_pending',\n" +
	"  `perms` set('cnry_read','cnry_write') DEFAULT NULL,\n" +
	"  `cnry_holder` varchar(100) DEFAULT 'cnry_unset',\n" +
	"  PRIMARY KEY (`id`)\n" +
	") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"

// refusalCanaryNoPrimaryKeyTable is the unkeyed definition with canary column
// names, for the refusal that reads the current key set rather than a column.
const refusalCanaryNoPrimaryKeyTable = "CREATE TABLE `orders_log` (\n" +
	"  `cnry_note` varchar(100) DEFAULT NULL,\n" +
	"  `cnry_label` varchar(100) DEFAULT NULL\n" +
	") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"

// Routing marks a refusal reason publishable to the pull request by category:
// the engine's statement-scope checks report the statement back, and the
// statement is already on the PR. The target's own definition is the other
// input to that classification, and nothing from it may reach the reason.
//
// This holds the category to that claim, one statement shape at a time. Each
// statement is classified against a definition whose target-only values are
// canaries the statement never mentions, so a check that reports the current
// definition rather than the submitted one puts a canary in the reason and
// fails here. That reaches a check the engine gains later only when the new
// check fires on one of the shapes below: a refusal keyed on a shape this
// table does not enumerate would interpolate the target with nothing here to
// notice it. A new refusal shape wants a case here alongside its contract
// case.
//
// Every case also asserts the reason names something the statement declares.
// Without it a reason that stopped interpolating its inputs at all, or an
// assertion aimed at the wrong string, would pass while proving nothing.
func TestStatementRefusalPublishesNothingFromTheTarget(t *testing.T) {
	tests := []struct {
		name string
		stmt string
		// table defaults to refusalCanaryOrdersTable.
		table string
		// wantFromStatement is text the reason must carry, and which the
		// statement itself declares.
		wantFromStatement string
	}{
		{
			name:              "ENUM replaced by a numeric type",
			stmt:              "ALTER TABLE orders MODIFY COLUMN status INT NOT NULL",
			wantFromStatement: `"status"`,
		},
		{
			name:              "SET replaced by an ENUM the statement declares",
			stmt:              "ALTER TABLE orders MODIFY COLUMN perms ENUM('draft','final')",
			wantFromStatement: `"perms"`,
		},
		{
			name:              "explicit algorithm clause",
			stmt:              "ALTER TABLE orders ADD COLUMN shipped_at DATETIME, ALGORITHM=INPLACE",
			wantFromStatement: "ALGORITHM=",
		},
		{
			name:              "explicit lock clause",
			stmt:              "ALTER TABLE orders ADD COLUMN shipped_at DATETIME, LOCK=NONE",
			wantFromStatement: "LOCK=",
		},
		{
			name:              "add a foreign key",
			stmt:              "ALTER TABLE orders ADD CONSTRAINT fk FOREIGN KEY (id) REFERENCES customers (id)",
			wantFromStatement: "foreign key",
		},
		{
			name:              "add a column to a table without a primary key",
			stmt:              "ALTER TABLE orders_log ADD COLUMN shipped_at DATETIME",
			table:             refusalCanaryNoPrimaryKeyTable,
			wantFromStatement: "primary key",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			table := tt.table
			if table == "" {
				table = refusalCanaryOrdersTable
			}
			require.Contains(t, table, canaryPrefix, "the definition must carry canaries or this case proves nothing")
			require.NotContains(t, tt.stmt, canaryPrefix, "the statement must not mention a canary or a leak would be indistinguishable")

			reason, refused, err := check.StatementRefusal(t.Context(), tt.stmt, table, discardLogger())
			require.NoError(t, err)
			require.True(t, refused, "the case must reach a refusal, or no reason is classified")
			assert.Contains(t, reason, tt.wantFromStatement,
				"the reason must report the submitted statement")
			assert.NotContains(t, reason, canaryPrefix,
				"the reason carries a value that exists only on the target: %s", reason)
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
