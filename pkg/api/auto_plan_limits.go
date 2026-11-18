package api

import "fmt"

// Defaults for AutoPlanLimitsConfig. They are sized so that a PR making an
// ordinary schema change, even one that spans a few databases or adds a new
// database's whole schema, plans as it always has, while a PR that rewrites
// schema across many databases at once — a bulk rename or a generated
// reformat — is stopped before it reads any schema content.
const (
	DefaultAutoPlanMaxDatabases   = 10
	DefaultAutoPlanMaxSchemaFiles = 100
)

// AutoPlanLimitsConfig bounds the schema one automatic plan covers. A PR is
// over the limit when it changes schema in more databases than MaxDatabases,
// or when it changes schema in more than one database and more schema files
// than MaxSchemaFiles. A PR that changes one database is never over the limit:
// one database is what an explicit `schemabot plan -d <database>` plans, so
// splitting cannot make it smaller.
type AutoPlanLimitsConfig struct {
	// MaxDatabases is the most databases one automatic plan covers.
	// Defaults to DefaultAutoPlanMaxDatabases.
	MaxDatabases int `yaml:"max_databases,omitempty"`

	// MaxSchemaFiles is the most changed schema files one automatic plan
	// covers when the PR spans more than one database.
	// Defaults to DefaultAutoPlanMaxSchemaFiles.
	MaxSchemaFiles int `yaml:"max_schema_files,omitempty"`
}

// AutoPlanSize is what one automatic plan would cover: the databases it plans
// and the schema files the PR changes in them.
type AutoPlanSize struct {
	Databases   int
	SchemaFiles int
}

// DatabaseLimit returns the configured database limit, or its default.
func (c AutoPlanLimitsConfig) DatabaseLimit() int {
	if c.MaxDatabases == 0 {
		return DefaultAutoPlanMaxDatabases
	}
	return c.MaxDatabases
}

// SchemaFileLimit returns the configured schema file limit, or its default.
func (c AutoPlanLimitsConfig) SchemaFileLimit() int {
	if c.MaxSchemaFiles == 0 {
		return DefaultAutoPlanMaxSchemaFiles
	}
	return c.MaxSchemaFiles
}

// TooManyDatabases reports whether size spans more databases than one
// automatic plan covers.
func (c AutoPlanLimitsConfig) TooManyDatabases(size AutoPlanSize) bool {
	return size.Databases > c.DatabaseLimit()
}

// TooManySchemaFiles reports whether size changes more schema files than one
// automatic plan covers. It never holds for a single database, which an
// explicit plan covers whatever its size.
func (c AutoPlanLimitsConfig) TooManySchemaFiles(size AutoPlanSize) bool {
	return size.Databases > 1 && size.SchemaFiles > c.SchemaFileLimit()
}

// Exceeded reports whether one automatic plan should refuse size.
func (c AutoPlanLimitsConfig) Exceeded(size AutoPlanSize) bool {
	return c.TooManyDatabases(size) || c.TooManySchemaFiles(size)
}

func (c AutoPlanLimitsConfig) validate() error {
	if c.MaxDatabases < 0 {
		return fmt.Errorf("auto_plan_limits.max_databases must not be negative, got %d", c.MaxDatabases)
	}
	if c.MaxSchemaFiles < 0 {
		return fmt.Errorf("auto_plan_limits.max_schema_files must not be negative, got %d", c.MaxSchemaFiles)
	}
	return nil
}
