// Package schema provides embedded SQL schema files and shared schema types
// for SchemaBot storage.
//
// Tables:
//   - locks: Deployment locks
//   - checks: Schema check tracking
//   - settings: Runtime settings
//   - plans: Schema change plans
//   - tasks: Schema change tasks
//   - vitess_apply_data: Vitess-specific per-apply deploy metadata
//   - vitess_tasks: Vitess-specific non-DDL tasks (VSchema, routing rules)
package schema

import "embed"

// MySQLFS contains the embedded SQL schema files for SchemaBot's own storage tables.
// These are bundled into the binary so the app can bootstrap its database at startup.
//
//go:embed mysql/*.sql
var MySQLFS embed.FS

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
