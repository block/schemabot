package api

import (
	"fmt"
	"maps"
	"path"
	"sort"
	"strings"

	"github.com/block/schemabot/pkg/schema"
)

// The desired side of a storage schema diff is an input, and naming it is part
// of the answer.
//
// The live side is never in question: it is read from the catalog of the one
// database the request addresses. The desired side is, because the question an
// operator asks during a deploy is not always "does this storage match the
// binary that is running". Deploying a later release asks "does this storage
// match the release I am about to roll", and the running binary cannot answer
// that from files it does not carry.
//
// So the schema may come from elsewhere — a checkout on disk, a published
// release — and wherever it came from, the report says so. Two diffs of one
// database against two releases give different answers, both correct, and a
// report that did not name its desired side would leave an operator holding
// the wrong one with no way to tell.
//
// A supplied schema is a diff-only input. ApplyStorageSchema takes no source
// and has no way to accept one: a convergence runs the schema of the binary
// running it, or "apply is what a boot does" — the property that makes this
// usable as a pre-deploy step — stops being true (AV-9).

// StorageSchemaSource is the desired schema of a diff, with the words the
// report uses to attribute it.
type StorageSchemaSource struct {
	// Description says where the schema came from, as a report renders it
	// ("the schema embedded in v1.2.3", "the schema files in ./schema/mysql").
	Description string
	// Files is the schema, as file name → file contents. Nil means the embedded
	// files of this binary, read the same way a boot reads them.
	Files map[string]string
}

// embeddedStorageSchemaDescription attributes a diff to the running binary's
// own files when nothing said otherwise.
const embeddedStorageSchemaDescription = "the schema embedded in this binary"

// EmbeddedStorageSchema is the desired schema a boot would converge to: the
// files embedded in this binary. The version is attribution only — it names
// whose files these are and is never used to find them, because a version that
// disagrees with the files it labels is exactly the failure this whole surface
// exists to avoid.
func EmbeddedStorageSchema(version string) *StorageSchemaSource {
	description := embeddedStorageSchemaDescription
	if v := strings.TrimSpace(version); v != "" {
		description = fmt.Sprintf("the schema embedded in %s", v)
	}
	return &StorageSchemaSource{Description: description}
}

// StorageSchemaFromFiles is a desired schema supplied by the caller, validated
// before anything reads a database with it.
//
// Validation is strict and happens up front because the failure it prevents is
// a quiet one. A file set that is missing half its tables still diffs cleanly
// and reports the missing ones as surplus — a report full of statements that
// destroy the storage schema an operator was about to extend. Refusing an
// unreadable set at the door turns that into an error naming the file.
func StorageSchemaFromFiles(description string, files map[string]string) (*StorageSchemaSource, error) {
	description = strings.TrimSpace(description)
	if description == "" {
		return nil, fmt.Errorf("a supplied storage schema needs a description saying where it came from, so the report can attribute the answer to it")
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("no schema files supplied for %s: a diff against an empty schema would report every existing storage table as surplus", description)
	}
	names := make([]string, 0, len(files))
	for name := range files {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if err := validateStorageSchemaFileName(name); err != nil {
			return nil, fmt.Errorf("schema file %q from %s: %w", name, description, err)
		}
		if strings.TrimSpace(files[name]) == "" {
			return nil, fmt.Errorf("schema file %q from %s is empty: an empty file declares no table, so the table it is named for would be reported as surplus", name, description)
		}
	}
	return &StorageSchemaSource{Description: description, Files: files}, nil
}

// validateStorageSchemaFileName holds the file set to the shape both
// convergences already assume: a flat directory of .sql files, one per storage
// table. The PostgreSQL convergence derives a table name from the file name, so
// a nested path or a stray extension there is not a cosmetic problem — it
// becomes a table nothing declares.
func validateStorageSchemaFileName(name string) error {
	if name == "" {
		return fmt.Errorf("has no name")
	}
	if name != path.Base(name) || strings.ContainsAny(name, `/\`) {
		return fmt.Errorf("is a path, not a file name; a storage schema is a flat directory of .sql files")
	}
	if !strings.HasSuffix(name, ".sql") {
		return fmt.Errorf("is not a .sql file; a storage schema is a flat directory of .sql files, one per storage table")
	}
	if strings.TrimSuffix(name, ".sql") == "" {
		return fmt.Errorf("names no table")
	}
	return nil
}

// AttributeTo stamps the report with the SchemaBot version of the binary that
// produced it, and names that version as the desired schema's origin when the
// desired schema was that binary's own embedded files.
//
// It is a stamp applied after the fact because only the caller knows its own
// version, and a version is deliberately not an input to the diff itself. A
// schema the caller supplied keeps its own attribution: the answer came from
// those files, and relabelling them with the responder's version is exactly the
// misattribution the report's wording exists to prevent.
func (r *StorageSchemaReport) AttributeTo(version string) {
	if r == nil {
		return
	}
	r.Version = version
	if r.SchemaSource == "" || r.SchemaSource == embeddedStorageSchemaDescription {
		r.SchemaSource = EmbeddedStorageSchema(version).Description
	}
}

// Describe is the report's attribution for this source.
func (s *StorageSchemaSource) Describe() string {
	if s == nil || strings.TrimSpace(s.Description) == "" {
		return embeddedStorageSchemaDescription
	}
	return s.Description
}

// mysqlSchemaFiles renders the source the way the MySQL differ consumes it —
// the same SchemaFiles shape ensureMySQLSchema builds, under the same
// namespace, so a supplied set and the embedded set travel one code path.
func (s *StorageSchemaSource) mysqlSchemaFiles() (schema.SchemaFiles, error) {
	if s == nil || len(s.Files) == 0 {
		return readEmbeddedSchemaFiles()
	}
	files := make(map[string]string, len(s.Files))
	maps.Copy(files, s.Files)
	return schema.SchemaFiles{storageSchemaNamespace: &schema.Namespace{Files: files}}, nil
}

// postgresSchemaFiles renders the source the way the PostgreSQL convergence
// consumes it: sorted table names, and contents keyed by table rather than by
// file. The table name is the file's base name, which is the same invariant the
// embedded reader relies on and the schema parity tests pin.
func (s *StorageSchemaSource) postgresSchemaFiles() ([]string, map[string]string, error) {
	if s == nil || len(s.Files) == 0 {
		return readEmbeddedPostgresSchemaFiles()
	}
	tables := make([]string, 0, len(s.Files))
	files := make(map[string]string, len(s.Files))
	for name, content := range s.Files {
		table := strings.TrimSuffix(name, ".sql")
		if existing, ok := files[table]; ok && existing != content {
			return nil, nil, fmt.Errorf("two schema files from %s declare table %q with different contents; one file per storage table", s.Describe(), table)
		}
		files[table] = content
		tables = append(tables, table)
	}
	sort.Strings(tables)
	return tables, files, nil
}
