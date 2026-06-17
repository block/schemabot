package webhook

import (
	"testing"

	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/stretchr/testify/assert"
)

func TestPRHasCurrentSchemaBotFiles(t *testing.T) {
	tests := []struct {
		name  string
		files []ghclient.PRFile
		want  bool
	}{
		{
			name: "empty diff",
		},
		{
			name: "non schema file",
			files: []ghclient.PRFile{
				{Filename: "README.md", Status: "modified"},
			},
		},
		{
			name: "added schema file",
			files: []ghclient.PRFile{
				{Filename: "schema/widgets/users.sql", Status: "added"},
			},
			want: true,
		},
		{
			name: "removed schema file still needs normal discovery",
			files: []ghclient.PRFile{
				{Filename: "schema/widgets/users.sql", Status: "removed"},
			},
			want: true,
		},
		{
			name: "removed vschema file still needs normal discovery",
			files: []ghclient.PRFile{
				{Filename: "schema/widgets/vschema.json", Status: "removed"},
			},
			want: true,
		},
		{
			name: "modified config file needs normal discovery",
			files: []ghclient.PRFile{
				{Filename: "schema/schemabot.yaml", Status: "modified"},
			},
			want: true,
		},
		{
			name: "removed config alone is not a current managed schema change",
			files: []ghclient.PRFile{
				{Filename: "schema/schemabot.yaml", Status: "removed"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, prHasCurrentSchemaBotFiles(tt.files))
		})
	}
}

func TestSchemaChangeReconciliationItemsUseApplyStateOverStaleCheckStatus(t *testing.T) {
	items := schemaChangeReconciliationItems([]schemaChangeReconciliationRecord{
		{
			check: &storage.Check{
				DatabaseName: "orders",
				Environment:  "staging",
				Status:       checkStatusInProgress,
			},
			apply: &storage.Apply{
				ApplyIdentifier: "apply-1234",
				State:           state.Apply.Completed,
			},
		},
	})

	if assert.Len(t, items, 1) {
		assert.False(t, items[0].InProgress)
		assert.Equal(t, state.Apply.Completed, items[0].State)
	}
}
