package planetscale

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/schema"
)

// A schema file that declares two tables contributes both of them to the
// desired schema, each carrying only its own statement, so the differ plans
// every table the file declares.
func TestParseDesiredSchemas_MultipleTablesInOneFile(t *testing.T) {
	ns := &schema.Namespace{Files: map[string]string{
		"tables.sql": "CREATE TABLE `orders` (`id` bigint NOT NULL, PRIMARY KEY (`id`));\n" +
			"CREATE TABLE `events` (`id` bigint NOT NULL, PRIMARY KEY (`id`));\n",
	}}

	schemas, err := parseDesiredSchemas("commerce", ns)
	require.NoError(t, err)
	require.Len(t, schemas, 2)
	assert.Equal(t, "orders", schemas[0].Name)
	assert.Equal(t, "CREATE TABLE `orders` (`id` bigint NOT NULL, PRIMARY KEY (`id`));", schemas[0].Schema)
	assert.Equal(t, "events", schemas[1].Name)
	assert.Equal(t, "CREATE TABLE `events` (`id` bigint NOT NULL, PRIMARY KEY (`id`));", schemas[1].Schema)
}
