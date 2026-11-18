package ddl

import (
	"testing"

	"github.com/block/spirit/pkg/statement"
	"github.com/stretchr/testify/require"
)

func TestCanonicalizeAlterPreservesQuotedIdentifiers(t *testing.T) {
	for _, input := range []string{
		"ALTER TABLE `order``archive` ADD COLUMN `note` INT",
		"ALTER TABLE `tenant``archive`.`orders` ADD COLUMN `note` INT",
		"ALTER TABLE `tenant``archive`.`order``archive` ADD COLUMN `note``text` INT",
	} {
		t.Run(input, func(t *testing.T) {
			before, err := statement.New(input)
			require.NoError(t, err)
			require.Len(t, before, 1)
			output := Canonicalize(input)
			after, err := statement.New(output)
			require.NoError(t, err, "canonical output must remain valid SQL: %s", output)
			require.Len(t, after, 1)
			require.Equal(t, before[0].Schema, after[0].Schema)
			require.Equal(t, before[0].Table, after[0].Table)
			require.Equal(t, before[0].Alter, after[0].Alter)
			require.Equal(t, output, Canonicalize(output), "canonicalization must be idempotent")
		})
	}
}
