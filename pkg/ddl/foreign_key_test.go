package ddl

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// An engine that rejects foreign keys rejects the statement it is handed, so
// the predicate keys on what the statement declares. Adding or defining a
// constraint counts; dropping one does not, which is what leaves an operator a
// way to remove a legacy constraint on an engine that will not accept new ones.
func TestDeclaresForeignKey(t *testing.T) {
	tests := []struct {
		name string
		stmt string
		want bool
	}{
		{
			name: "ALTER adding a foreign key",
			stmt: "ALTER TABLE `orders` ADD CONSTRAINT `fk_orders_user` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`)",
			want: true,
		},
		{
			name: "ALTER adding an unnamed foreign key",
			stmt: "ALTER TABLE `orders` ADD FOREIGN KEY (`user_id`) REFERENCES `users` (`id`)",
			want: true,
		},
		{
			name: "CREATE TABLE defining a foreign key",
			stmt: "CREATE TABLE `orders` (\n  `id` bigint NOT NULL AUTO_INCREMENT,\n  `user_id` bigint NOT NULL,\n  PRIMARY KEY (`id`),\n  CONSTRAINT `fk_orders_user` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			want: true,
		},
		{
			name: "ALTER adding a parenthesised foreign key",
			stmt: "ALTER TABLE `orders` ADD (CONSTRAINT `fk_orders_user` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`))",
			want: true,
		},
		{
			name: "ALTER adding a column alongside a foreign key",
			stmt: "ALTER TABLE `orders` ADD COLUMN `note` varchar(255) DEFAULT NULL, ADD CONSTRAINT `fk_orders_user` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`)",
			want: true,
		},
		{
			name: "ALTER dropping a foreign key declares none",
			stmt: "ALTER TABLE `orders` DROP FOREIGN KEY `fk_orders_user`",
			want: false,
		},
		{
			name: "ALTER on a table that has one declares none",
			stmt: "ALTER TABLE `orders` ADD COLUMN `note` varchar(255) DEFAULT NULL",
			want: false,
		},
		{
			name: "ALTER adding a non-foreign-key constraint",
			stmt: "ALTER TABLE `orders` ADD CONSTRAINT `uq_orders_ref` UNIQUE (`reference`)",
			want: false,
		},
		{
			name: "CREATE TABLE without one",
			stmt: "CREATE TABLE `orders` (\n  `id` bigint NOT NULL AUTO_INCREMENT,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			want: false,
		},
		{
			name: "DROP TABLE",
			stmt: "DROP TABLE `orders`",
			want: false,
		},
		// MySQL parses a column-level REFERENCES clause and creates no
		// constraint from it, so the statement declares no foreign key and an
		// engine that rejects them has nothing to reject.
		{
			name: "CREATE TABLE with a column-level REFERENCES declares none",
			stmt: "CREATE TABLE `orders` (\n  `id` bigint NOT NULL AUTO_INCREMENT,\n  `user_id` bigint NOT NULL REFERENCES `users` (`id`),\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			want: false,
		},
		{
			name: "ALTER adding a column with a column-level REFERENCES declares none",
			stmt: "ALTER TABLE `orders` ADD COLUMN `user_id` bigint NOT NULL REFERENCES `users` (`id`)",
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := DeclaresForeignKey(tt.stmt)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}

	t.Run("multi-statement input is rejected", func(t *testing.T) {
		_, err := DeclaresForeignKey("ALTER TABLE `a` ADD COLUMN `x` int; ALTER TABLE `b` ADD CONSTRAINT `fk` FOREIGN KEY (`y`) REFERENCES `c` (`id`)")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "exactly one statement")
	})

	t.Run("unparseable input is an error, not a false", func(t *testing.T) {
		_, err := DeclaresForeignKey("ALTER TABLE")
		require.Error(t, err)
	})
}
