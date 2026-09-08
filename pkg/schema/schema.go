// Package schema provides embedded SQL schema files and shared schema types
// for SchemaBot storage. Each storage table has one file per dialect
// directory; see mysql/ and postgres/.
package schema

import "embed"

// MySQLFS contains the embedded SQL schema files for SchemaBot's own storage tables.
// These are bundled into the binary so the app can bootstrap its database at startup.
//
//go:embed mysql/*.sql
var MySQLFS embed.FS

// PostgresFS contains the embedded SQL schema files for SchemaBot's own
// storage tables on PostgreSQL. Each file holds one CREATE TABLE statement
// followed by its CREATE INDEX statements, separated by semicolons — plain
// statements only, with no comments, dollar-quoted blocks, or E-strings (the
// schema lint tests pin this, and the statement-level lints in this package
// parse the files line-by-line assuming it). The files mirror the MySQL
// schema table-for-table and column-for-column; updated_at stamping is the
// application's responsibility on PostgreSQL (there is no
// ON UPDATE CURRENT_TIMESTAMP equivalent and no trigger is installed).
//
//go:embed postgres/*.sql
var PostgresFS embed.FS

// SchemaFiles maps namespace names to their file contents.
// The namespace key is engine-specific:
//   - MySQL: schema name
//   - Vitess: keyspace name (e.g. "commerce", "customers")
//   - PostgreSQL: schema name (e.g. "public", "app")
type SchemaFiles map[string]*Namespace

// Namespace contains all declarative files for a single schema namespace
// (MySQL schema/database, Vitess keyspace, or PostgreSQL schema).
// The engine interprets the files based on its conventions:
//   - Spirit: reads *.sql files as CREATE TABLE statements
//   - PlanetScale: reads *.sql files as CREATE TABLE statements + vschema.json
type Namespace struct {
	Files map[string]string // filename → content
}
