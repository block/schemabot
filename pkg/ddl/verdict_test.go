package ddl

import (
	"testing"

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
