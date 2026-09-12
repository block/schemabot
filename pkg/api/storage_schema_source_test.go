package api

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func validStorageSchemaFiles() map[string]string {
	return map[string]string{
		"applies.sql": "CREATE TABLE `applies` (`id` BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY)",
		"checks.sql":  "CREATE TABLE `checks` (`id` BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY)",
	}
}

// A supplied schema is validated before it reads a database, because the
// failure it prevents is silent: a half-read file set diffs cleanly and reports
// the tables it is missing as surplus, which is a report full of statements
// that destroy the storage schema the operator was about to extend.
func TestStorageSchemaFromFiles_Validation(t *testing.T) {
	tests := []struct {
		name        string
		description string
		files       map[string]string
		wantErr     string
	}{
		{
			name:        "a directory of .sql files, one per table",
			description: "the schema files of release v1.4.0",
			files:       validStorageSchemaFiles(),
		},
		{
			name:    "no description to attribute the answer to",
			files:   validStorageSchemaFiles(),
			wantErr: "needs a description saying where it came from",
		},
		{
			name:        "no files at all",
			description: "the schema files in ./empty",
			files:       map[string]string{},
			wantErr:     "would report every existing storage table as surplus",
		},
		{
			name:        "a path instead of a file name",
			description: "the schema files in ./checkout",
			files:       map[string]string{"mysql/applies.sql": "CREATE TABLE `applies` (`id` BIGINT UNSIGNED PRIMARY KEY)"},
			wantErr:     "is a path, not a file name",
		},
		{
			name:        "a file that is not .sql",
			description: "the schema files in ./checkout",
			files:       map[string]string{"applies.txt": "CREATE TABLE `applies` (`id` BIGINT UNSIGNED PRIMARY KEY)"},
			wantErr:     "is not a .sql file",
		},
		{
			name:        "a file with no table in it",
			description: "the schema files in ./checkout",
			files:       map[string]string{"applies.sql": "   \n"},
			wantErr:     "is empty",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			desired, err := StorageSchemaFromFiles(tc.description, tc.files)
			if tc.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.description, desired.Describe())
			assert.Equal(t, tc.files, desired.Files)
		})
	}
}

// The embedded source is attributed to the version that embedded it, and says
// so without one rather than leaving the report's origin blank.
func TestEmbeddedStorageSchema(t *testing.T) {
	assert.Equal(t, "the schema embedded in v1.2.3", EmbeddedStorageSchema("v1.2.3").Describe())
	assert.Equal(t, "the schema embedded in this binary", EmbeddedStorageSchema("").Describe())
	assert.Empty(t, EmbeddedStorageSchema("v1.2.3").Files, "the embedded source carries no files of its own")
	assert.Equal(t, "the schema embedded in this binary", (*StorageSchemaSource)(nil).Describe(),
		"no source at all is the embedded schema")
}

// The responder's version stamp names the binary that answered, and never
// relabels a schema the caller supplied: a report claiming a release's answer
// came from the answering binary's files is the one way this report can mislead
// rather than merely be wrong.
func TestStorageSchemaReport_AttributeTo(t *testing.T) {
	embedded := &StorageSchemaReport{SchemaSource: embeddedStorageSchemaDescription}
	embedded.AttributeTo("v1.2.3")
	assert.Equal(t, "the schema embedded in v1.2.3", embedded.SchemaSource)
	assert.Equal(t, "v1.2.3", embedded.Version)

	unstamped := &StorageSchemaReport{}
	unstamped.AttributeTo("v1.2.3")
	assert.Equal(t, "the schema embedded in v1.2.3", unstamped.SchemaSource)

	supplied := &StorageSchemaReport{SchemaSource: "the schema files of release v1.4.0"}
	supplied.AttributeTo("v1.2.3")
	assert.Equal(t, "the schema files of release v1.4.0", supplied.SchemaSource,
		"the schema the diff used outranks the version of the binary that read it")
	assert.Equal(t, "v1.2.3", supplied.Version)
}

// Each differ consumes the source in the shape its convergence already uses: a
// namespaced file map on MySQL, sorted table names and contents keyed by table
// on PostgreSQL. A supplied set and the embedded set travel the same path, so
// neither differ has a second way to read a schema.
func TestStorageSchemaSource_PerDialectShapes(t *testing.T) {
	desired, err := StorageSchemaFromFiles("the schema files of release v1.4.0", validStorageSchemaFiles())
	require.NoError(t, err)

	mysqlFiles, err := desired.mysqlSchemaFiles()
	require.NoError(t, err)
	require.Contains(t, mysqlFiles, storageSchemaNamespace)
	assert.Len(t, mysqlFiles[storageSchemaNamespace].Files, 2)
	assert.Contains(t, mysqlFiles[storageSchemaNamespace].Files["applies.sql"], "CREATE TABLE `applies`")

	tables, contents, err := desired.postgresSchemaFiles()
	require.NoError(t, err)
	assert.Equal(t, []string{"applies", "checks"}, tables, "tables come out in the order a convergence runs them")
	assert.Contains(t, contents["checks"], "CREATE TABLE `checks`")

	// No source falls through to the embedded files, which is what a boot reads.
	embeddedMySQL, err := (*StorageSchemaSource)(nil).mysqlSchemaFiles()
	require.NoError(t, err)
	assert.NotEmpty(t, embeddedMySQL[storageSchemaNamespace].Files)
	embeddedTables, _, err := (*StorageSchemaSource)(nil).postgresSchemaFiles()
	require.NoError(t, err)
	assert.NotEmpty(t, embeddedTables)

	// Only the embedded source may carry no files. A source that names a
	// release and carries none is refused rather than answered from this
	// binary's own schema under that release's name — whether its file set is
	// empty or was never set at all.
	embedded, err := EmbeddedStorageSchema("v1.2.3").mysqlSchemaFiles()
	require.NoError(t, err)
	assert.NotEmpty(t, embedded[storageSchemaNamespace].Files)

	for _, fileless := range []*StorageSchemaSource{
		{Description: "the schema files of release v1.4.0"},
		{Description: "the schema files of release v1.4.0", Files: map[string]string{}},
	} {
		_, err := fileless.mysqlSchemaFiles()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no schema files supplied for the schema files of release v1.4.0")
		_, _, err = fileless.postgresSchemaFiles()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no schema files supplied for the schema files of release v1.4.0")
	}
}
