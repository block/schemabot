package spirit

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// Spirit's own lines and errors reach an operator through the apply log stream
// and through the reason a schema change failed. At that point the text is
// SchemaBot's, so it has to use SchemaBot's word for the thing — without
// mangling the identifiers in the same line, which an operator needs to be able
// to read back exactly.
func TestOperatorText(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "run-start line loses the library's phrasing",
			in:   "Starting spirit migration",
			want: "Starting schema change",
		},
		{
			name: "plural keeps its number",
			in:   "attempting to acquire advisory lock (GET_LOCK) to prevent concurrent migrations",
			want: "attempting to acquire advisory lock (GET_LOCK) to prevent concurrent schema changes",
		},
		{
			name: "capitalization carries over",
			in:   "Migration status: copying rows",
			want: "Schema change status: copying rows",
		},
		{
			name: "mid-sentence error text",
			in:   "migration successful but failed to drop old table",
			want: "schema change successful but failed to drop old table",
		},
		{
			name: "identifier with the word inside it is left alone",
			in:   "table schema_migrations is not eligible",
			want: "table schema_migrations is not eligible",
		},
		{
			name: "quoted table named for the word is left alone",
			in:   "copying rows into `migrations`",
			want: "copying rows into `migrations`",
		},
		{
			name: "a quoted name and a sentence in the same line",
			in:   "spirit migration failed on table `migrations`",
			want: "schema change failed on table `migrations`",
		},
		{
			name: "text without the word is returned unchanged",
			in:   "apply complete",
			want: "apply complete",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, operatorText(tt.in))
		})
	}
}

// The failure message an operator reads is the one recorded on the running
// schema change, so the rewrite has to happen where that message is set rather
// than at each of the many call sites that build the error.
func TestSetSchemaChangeFailed_RecordsOperatorVocabulary(t *testing.T) {
	eng := New(Config{})
	eng.runningSchemaChange = &runningSchemaChange{}

	eng.setSchemaChangeFailed(assertableError("run Spirit: migration failed: table is busy"))

	assert.Equal(t, "run Spirit: schema change failed: table is busy", eng.runningSchemaChange.errorMessage)
}

type assertableError string

func (e assertableError) Error() string { return string(e) }
