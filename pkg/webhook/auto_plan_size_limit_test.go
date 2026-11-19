package webhook

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
)

func TestAutoPlanSizeOfCountsSchemaFilesUnderConfigDirectories(t *testing.T) {
	configs := []ghclient.DiscoveredConfig{
		{Config: &ghclient.SchemabotConfig{Database: "payments"}, SchemaDir: "schema/payments"},
		{Config: &ghclient.SchemabotConfig{Database: "orders"}, SchemaDir: "schema/orders"},
		{Config: &ghclient.SchemabotConfig{Database: "orders"}, SchemaDir: "schema/orders_replica"},
	}
	files := []ghclient.PRFile{
		{Filename: "schema/orders/users.sql", Status: "modified"},
		{Filename: "schema/orders_replica/users.sql", Status: "modified"},
		{Filename: "schema/payments/ledger/entries.sql", Status: "added"},
		{Filename: "schema/payments/schemabot.yaml", Status: "modified"},
		{Filename: "archive/payments/refunds.sql", PreviousFilename: "schema/payments/refunds.sql", Status: "renamed"},
		{Filename: "schema/shipping/parcels.sql", Status: "added"},
		{Filename: "services/app/Handler.java", Status: "modified"},
	}

	size, databases := autoPlanSizeOf(files, configs)
	assert.Equal(t, api.AutoPlanSize{Databases: 2, SchemaFiles: 4}, size,
		"two databases; the files under their directories, including one renamed out of one")
	assert.Equal(t, []string{"orders", "payments"}, databases)
}

func TestAutoPlanSizeOfCountsEveryPathUnderARootConfig(t *testing.T) {
	configs := []ghclient.DiscoveredConfig{
		{Config: &ghclient.SchemabotConfig{Database: "orders"}, SchemaDir: "."},
	}
	files := []ghclient.PRFile{
		{Filename: "users.sql", Status: "added"},
		{Filename: "nested/accounts.sql", Status: "added"},
		{Filename: "README.md", Status: "modified"},
	}

	size, databases := autoPlanSizeOf(files, configs)
	assert.Equal(t, api.AutoPlanSize{Databases: 1, SchemaFiles: 2}, size)
	assert.Equal(t, []string{"orders"}, databases)
}
