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

// A rename that vacates a schema path stops the default branch declaring that
// file's tables, exactly as a deletion does, so a command on a PR carrying only
// such a rename must go through normal config discovery instead of refreshing
// the checks as passing.
func TestPRHasCurrentSchemaBotFilesCountsRenamesThatVacateSchema(t *testing.T) {
	tests := []struct {
		name string
		file ghclient.PRFile
	}{
		{"renamed to a disabled name", ghclient.PRFile{Filename: "schema/widgets/users.sql.disabled", PreviousFilename: "schema/widgets/users.sql", Status: "renamed"}},
		{"renamed to a non-schema file", ghclient.PRFile{Filename: "docs/users.md", PreviousFilename: "schema/widgets/users.sql", Status: "renamed"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.True(t, prHasCurrentSchemaBotFiles([]ghclient.PRFile{tt.file}))
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
