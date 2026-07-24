package ddl

import (
	"log/slog"
	"testing"

	"github.com/block/spirit/pkg/migration/check"
	"github.com/block/spirit/pkg/statement"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEngineRefusalReason(t *testing.T) {
	tests := []struct {
		name               string
		stmt               string
		wantRefused        bool
		wantReason         string
		wantReasonContains string
	}{
		{
			name:        "drop primary key is refused",
			stmt:        "ALTER TABLE `users` DROP PRIMARY KEY",
			wantRefused: true,
			wantReason:  "dropping primary key is not supported",
		},
		{
			name:        "drop primary key among other specs is refused",
			stmt:        "ALTER TABLE `users` DROP PRIMARY KEY, ADD PRIMARY KEY (`id`, `tenant_id`)",
			wantRefused: true,
			wantReason:  "dropping primary key is not supported",
		},
		{
			name:               "explicit algorithm clause is refused",
			stmt:               "ALTER TABLE `users` ADD COLUMN `email` VARCHAR(255), ALGORITHM=INPLACE",
			wantRefused:        true,
			wantReasonContains: "ALGORITHM",
		},
		{
			name:               "explicit lock clause is refused",
			stmt:               "ALTER TABLE `users` ADD COLUMN `email` VARCHAR(255), LOCK=NONE",
			wantRefused:        true,
			wantReasonContains: "LOCK",
		},
		{
			name:        "add foreign key is refused",
			stmt:        "ALTER TABLE `orders` ADD CONSTRAINT `fk_user` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`)",
			wantRefused: true,
			wantReason:  "adding foreign key constraints is not supported",
		},
		{
			name:        "add unnamed foreign key is refused",
			stmt:        "ALTER TABLE `orders` ADD FOREIGN KEY (`user_id`) REFERENCES `users` (`id`)",
			wantRefused: true,
			wantReason:  "adding foreign key constraints is not supported",
		},
		{
			name: "add non-referential constraint runs on the engine",
			stmt: "ALTER TABLE `orders` ADD CONSTRAINT `chk_qty` CHECK (`quantity` > 0)",
		},
		{
			name: "add column runs on the engine",
			stmt: "ALTER TABLE `users` ADD COLUMN `email` VARCHAR(255)",
		},
		{
			name: "add index runs on the engine",
			stmt: "ALTER TABLE `users` ADD INDEX `idx_email` (`email`)",
		},
		{
			name: "modifying a primary-key column runs on the engine",
			stmt: "ALTER TABLE `users` MODIFY COLUMN `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT",
		},
		{
			name: "create table is not an alter and never refused",
			stmt: "CREATE TABLE `users` (`id` BIGINT UNSIGNED AUTO_INCREMENT, PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
		},
		{
			name: "drop table is not an alter and never refused",
			stmt: "DROP TABLE `users`",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reason, refused, err := EngineRefusalReason(tt.stmt)
			require.NoError(t, err)
			assert.Equal(t, tt.wantRefused, refused)
			if tt.wantReason != "" {
				assert.Equal(t, tt.wantReason, reason)
			}
			if tt.wantReasonContains != "" {
				assert.Contains(t, reason, tt.wantReasonContains)
			}
			if refused {
				assert.NotEmpty(t, reason)
			} else {
				assert.Empty(t, reason)
			}
		})
	}
}

// runEngineStatementChecks runs the engine's own statement-scope preflight
// checks against a single statement, returning the engine's refusal error
// (nil when the checks accept the statement).
func runEngineStatementChecks(t *testing.T, stmt string) error {
	t.Helper()
	stmts, err := statement.New(stmt)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	resources := check.Resources{Statement: stmts[0]}
	return check.RunChecks(t.Context(), resources, slog.New(slog.DiscardHandler), check.ScopeStatement)
}

// TestEngineRefusalReasonAgreesWithEngineChecks pins the refusal predicate to
// the engine: every statement this package refuses must also be refused by
// the engine's statement-scope preflight checks, with the same reason. If the
// engine relaxes or rewords one of these checks, this test fails and the
// predicate must be revisited — a refusal claimed here that the engine no
// longer enforces would wrongly block an apply that can succeed.
func TestEngineRefusalReasonAgreesWithEngineChecks(t *testing.T) {
	refused := []string{
		"ALTER TABLE `users` DROP PRIMARY KEY",
		"ALTER TABLE `users` DROP PRIMARY KEY, ADD PRIMARY KEY (`id`, `tenant_id`)",
		"ALTER TABLE `users` ADD COLUMN `email` VARCHAR(255), ALGORITHM=INPLACE",
		"ALTER TABLE `users` ADD COLUMN `email` VARCHAR(255), LOCK=NONE",
		"ALTER TABLE `orders` ADD CONSTRAINT `fk_user` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`)",
		"ALTER TABLE `orders` ADD FOREIGN KEY (`user_id`) REFERENCES `users` (`id`)",
	}
	for _, stmt := range refused {
		t.Run(stmt, func(t *testing.T) {
			reason, isRefused, err := EngineRefusalReason(stmt)
			require.NoError(t, err)
			require.True(t, isRefused)
			checkErr := runEngineStatementChecks(t, stmt)
			require.Error(t, checkErr, "engine statement-scope checks accept a statement this package refuses")
			assert.Equal(t, reason, checkErr.Error())
		})
	}
}

// TestEngineRefusalReasonToleratesFastPathStatements documents the deliberate
// gap between this package's refusals and the engine's statement-scope
// checks: these statements fail a statement-scope check, but the engine
// attempts MySQL's native DDL fast path before preflight checks and the fast
// path can complete them, so the verdict must stay silent rather than claim
// an apply will fail.
func TestEngineRefusalReasonToleratesFastPathStatements(t *testing.T) {
	fastPath := []string{
		"ALTER TABLE `users` DROP COLUMN `email`, ADD COLUMN `email` VARCHAR(255)",
		"ALTER TABLE `users` RENAME COLUMN `email` TO `contact_email`, ADD COLUMN `email` VARCHAR(255)",
	}
	for _, stmt := range fastPath {
		t.Run(stmt, func(t *testing.T) {
			reason, isRefused, err := EngineRefusalReason(stmt)
			require.NoError(t, err)
			assert.False(t, isRefused)
			assert.Empty(t, reason)
			require.Error(t, runEngineStatementChecks(t, stmt),
				"engine statement-scope checks accept this statement, so it no longer documents a tolerated gap — remove it from this corpus")
		})
	}
}

func TestEngineRefusalReasonErrors(t *testing.T) {
	t.Run("unparseable statement", func(t *testing.T) {
		_, _, err := EngineRefusalReason("ALTER TABLE `users` FLUX CAPACITOR")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "parse statement")
	})

	t.Run("multiple statements", func(t *testing.T) {
		_, _, err := EngineRefusalReason("ALTER TABLE `a` ADD COLUMN `x` INT; ALTER TABLE `b` ADD COLUMN `y` INT")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "exactly one statement")
	})
}
