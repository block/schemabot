package ddl

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// currentEnumTable is the live table definition the ENUM reorder verdict
// compares planned ALTERs against in these tests.
const currentEnumTable = "CREATE TABLE `enumchk` (\n" +
	"  `id` int NOT NULL AUTO_INCREMENT,\n" +
	"  `status` enum('active','inactive','pending') NOT NULL,\n" +
	"  `flags` set('a','b','c') NOT NULL,\n" +
	"  `name` varchar(255) DEFAULT NULL,\n" +
	"  PRIMARY KEY (`id`)\n" +
	") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"

func TestEnumReorderRefusalReason(t *testing.T) {
	tests := []struct {
		name        string
		stmt        string
		wantRefused bool
	}{
		{
			name:        "reordering existing values is refused",
			stmt:        "ALTER TABLE `enumchk` MODIFY COLUMN `status` ENUM('pending', 'active', 'inactive') NOT NULL",
			wantRefused: true,
		},
		{
			name:        "inserting a new value mid-list is refused",
			stmt:        "ALTER TABLE `enumchk` MODIFY COLUMN `status` ENUM('active', 'suspended', 'inactive', 'pending') NOT NULL",
			wantRefused: true,
		},
		{
			name:        "inserting a new value at the front is refused",
			stmt:        "ALTER TABLE `enumchk` MODIFY COLUMN `status` ENUM('suspended', 'active', 'inactive', 'pending') NOT NULL",
			wantRefused: true,
		},
		{
			name:        "dropping a value and re-adding it elsewhere is refused",
			stmt:        "ALTER TABLE `enumchk` MODIFY COLUMN `status` ENUM('inactive', 'pending', 'active') NOT NULL",
			wantRefused: true,
		},
		{
			name:        "CHANGE COLUMN with a reorder is refused",
			stmt:        "ALTER TABLE `enumchk` CHANGE COLUMN `status` `status` ENUM('pending', 'active', 'inactive') NOT NULL",
			wantRefused: true,
		},
		{
			name:        "CHANGE COLUMN rename with a reorder is refused via the old name",
			stmt:        "ALTER TABLE `enumchk` CHANGE COLUMN `status` `state` ENUM('pending', 'active', 'inactive') NOT NULL",
			wantRefused: true,
		},
		{
			name:        "reorder among multiple alter specs is refused",
			stmt:        "ALTER TABLE `enumchk` ADD COLUMN `email` VARCHAR(255), MODIFY COLUMN `status` ENUM('pending', 'active', 'inactive') NOT NULL",
			wantRefused: true,
		},
		{
			name:        "case-insensitive column lookup still finds the reorder",
			stmt:        "ALTER TABLE `enumchk` MODIFY COLUMN `STATUS` ENUM('pending', 'active', 'inactive') NOT NULL",
			wantRefused: true,
		},
		{
			name: "appending a value at the end runs on the engine",
			stmt: "ALTER TABLE `enumchk` MODIFY COLUMN `status` ENUM('active', 'inactive', 'pending', 'archived') NOT NULL",
		},
		{
			name: "dropping a value from the middle runs on the engine",
			stmt: "ALTER TABLE `enumchk` MODIFY COLUMN `status` ENUM('active', 'pending') NOT NULL",
		},
		{
			name: "dropping a value from the start runs on the engine",
			stmt: "ALTER TABLE `enumchk` MODIFY COLUMN `status` ENUM('inactive', 'pending') NOT NULL",
		},
		{
			name: "dropping multiple values runs on the engine",
			stmt: "ALTER TABLE `enumchk` MODIFY COLUMN `status` ENUM('active') NOT NULL",
		},
		{
			name: "dropping a value and appending a new one runs on the engine",
			stmt: "ALTER TABLE `enumchk` MODIFY COLUMN `status` ENUM('active', 'pending', 'archived') NOT NULL",
		},
		{
			name: "identical element list runs on the engine",
			stmt: "ALTER TABLE `enumchk` MODIFY COLUMN `status` ENUM('active', 'inactive', 'pending') NULL",
		},
		{
			name: "modifying an unrelated non-ENUM column runs on the engine",
			stmt: "ALTER TABLE `enumchk` MODIFY COLUMN `name` VARCHAR(500) DEFAULT NULL",
		},
		{
			name: "adding a column runs on the engine",
			stmt: "ALTER TABLE `enumchk` ADD COLUMN `email` VARCHAR(255)",
		},
		{
			name: "VARCHAR to ENUM conversion is not a reorder",
			stmt: "ALTER TABLE `enumchk` MODIFY COLUMN `name` ENUM('b', 'a') NOT NULL",
		},
		{
			name: "modifying a column absent from the current table stays silent",
			stmt: "ALTER TABLE `enumchk` MODIFY COLUMN `missing` ENUM('b', 'a') NOT NULL",
		},
		{
			name: "SET column reorder is out of scope for the ENUM verdict",
			stmt: "ALTER TABLE `enumchk` MODIFY COLUMN `flags` SET('c', 'b', 'a') NOT NULL",
		},
		{
			name: "create table is not an alter and never refused",
			stmt: "CREATE TABLE `other` (`id` BIGINT UNSIGNED AUTO_INCREMENT, PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
		},
		{
			name: "drop table is not an alter and never refused",
			stmt: "DROP TABLE `enumchk`",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reason, refused, err := EnumReorderRefusalReason(tt.stmt, currentEnumTable)
			require.NoError(t, err)
			assert.Equal(t, tt.wantRefused, refused)
			if refused {
				assert.Contains(t, reason, "unsafe ENUM value reorder")
			} else {
				assert.Empty(t, reason)
			}
		})
	}
}

func TestEnumReorderRefusalReasonEscapedElements(t *testing.T) {
	// Element values containing escaped quotes and commas must compare by
	// their decoded string form, not their SQL-quoted spelling.
	current := "CREATE TABLE `enumchk` (\n" +
		"  `id` int NOT NULL AUTO_INCREMENT,\n" +
		"  `label` enum('it''s','a,b','plain') NOT NULL,\n" +
		"  PRIMARY KEY (`id`)\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"

	reason, refused, err := EnumReorderRefusalReason(
		"ALTER TABLE `enumchk` MODIFY COLUMN `label` ENUM('it''s', 'a,b', 'plain', 'new') NOT NULL", current)
	require.NoError(t, err)
	assert.False(t, refused)
	assert.Empty(t, reason)

	reason, refused, err = EnumReorderRefusalReason(
		"ALTER TABLE `enumchk` MODIFY COLUMN `label` ENUM('a,b', 'it''s', 'plain') NOT NULL", current)
	require.NoError(t, err)
	assert.True(t, refused)
	assert.Contains(t, reason, "unsafe ENUM value reorder")
}

func TestEnumReorderRefusalReasonErrors(t *testing.T) {
	t.Run("unparseable statement errors", func(t *testing.T) {
		_, _, err := EnumReorderRefusalReason("ALTER TABLE", currentEnumTable)
		require.Error(t, err)
	})

	t.Run("multiple statements error", func(t *testing.T) {
		_, _, err := EnumReorderRefusalReason(
			"ALTER TABLE `enumchk` MODIFY COLUMN `status` ENUM('a') NOT NULL; ALTER TABLE `enumchk` ADD COLUMN `x` INT",
			currentEnumTable)
		require.Error(t, err)
	})

	t.Run("malformed current table definition errors when the verdict needs it", func(t *testing.T) {
		_, _, err := EnumReorderRefusalReason(
			"ALTER TABLE `enumchk` MODIFY COLUMN `status` ENUM('pending', 'active') NOT NULL",
			"not a create table")
		require.Error(t, err)
	})
}

func TestIsCompatibleEnumChange(t *testing.T) {
	existing := []string{"a", "b", "c"}
	tests := []struct {
		name string
		new  []string
		want bool
	}{
		{"identical", []string{"a", "b", "c"}, true},
		{"append", []string{"a", "b", "c", "d"}, true},
		{"drop middle", []string{"a", "c"}, true},
		{"drop first", []string{"b", "c"}, true},
		{"drop all but one", []string{"b"}, true},
		{"drop and append", []string{"a", "c", "d"}, true},
		{"swap", []string{"b", "a", "c"}, false},
		{"reverse", []string{"c", "b", "a"}, false},
		{"insert middle", []string{"a", "x", "b", "c"}, false},
		{"insert front", []string{"x", "a", "b", "c"}, false},
		{"move existing to end", []string{"b", "c", "a"}, false},
		{"existing after new", []string{"a", "b", "d", "c"}, false},
		{"repeated existing value", []string{"a", "a", "b"}, false},
		{"all new", []string{"x", "y"}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, isCompatibleEnumChange(existing, tt.new))
		})
	}
}
