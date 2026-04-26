// Package lint provides schema linting using Spirit's lint framework.
// It detects unsafe changes (DROP TABLE, DROP COLUMN, etc.) that require
// escalated review and the --allow-unsafe flag.
package lint

import (
	"fmt"
	"sync"

	spiritlint "github.com/block/spirit/pkg/lint"
	"github.com/block/spirit/pkg/statement"
	"github.com/block/spirit/pkg/table"

	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/engine"
)

var spiritLintMu sync.Mutex

// Config holds configuration for schema linting.
type Config struct {
	// AllowUnsafe disables blocking on unsafe changes.
	// When false (default), unsafe changes require --allow-unsafe flag.
	AllowUnsafe bool

	// AllowedPKTypes are the allowed primary key types.
	// Default: "bigint,binary,varbinary"
	AllowedPKTypes string

	// AllowedCharsets are the allowed character sets.
	// Default: "utf8mb4,binary"
	AllowedCharsets string

	// IgnoreTables are tables to skip during linting.
	IgnoreTables []string
}

// DefaultConfig returns the default linting configuration.
func DefaultConfig() Config {
	return Config{
		AllowUnsafe:     false,
		AllowedPKTypes:  "bigint,binary,varbinary",
		AllowedCharsets: "utf8mb4,binary",
		IgnoreTables:    []string{"schema_version"},
	}
}

// Linter provides schema linting using Spirit's lint framework.
type Linter struct {
	config Config
}

// New creates a new linter with default configuration.
func New() *Linter {
	return &Linter{config: DefaultConfig()}
}

// NewWithConfig creates a new linter with custom configuration.
func NewWithConfig(cfg Config) *Linter {
	return &Linter{config: cfg}
}

// Result represents a lint finding.
type Result struct {
	Table    string // Table name affected
	Column   string // Column name if applicable
	Linter   string // Name of the linter
	Message  string // Human-readable description
	Severity string // "warning" or "error"
	IsUnsafe bool   // True if this is an unsafe/destructive change
}

// LintStatements analyzes DDL statements for lint issues and unsafe changes.
// It returns warnings, whether any unsafe changes were detected, and any error.
func (l *Linter) LintStatements(ddlStatements []string) ([]Result, bool, error) {
	// Parse DDL statements into AbstractStatements for Spirit
	var changes []*statement.AbstractStatement
	for _, stmt := range ddlStatements {
		parsed, err := statement.New(stmt)
		if err != nil {
			return nil, false, fmt.Errorf("failed to parse DDL statement: %w", err)
		}
		changes = append(changes, parsed...)
	}

	if len(changes) == 0 {
		return nil, false, nil
	}

	// Build Spirit lint config and run linters
	spiritConfig := l.buildSpiritConfig()
	violations, err := runSpiritLinters(nil, changes, spiritConfig)
	if err != nil {
		return nil, false, fmt.Errorf("failed to run linters: %w", err)
	}

	// Convert violations to Results
	var results []Result
	hasUnsafe := false
	for _, v := range violations {
		r := l.convertViolation(v)
		if r.IsUnsafe {
			hasUnsafe = true
		}
		results = append(results, r)
	}

	return results, hasUnsafe, nil
}

// LintSchema analyzes CREATE TABLE statements for lint issues.
// This checks the schema itself (PK types, charsets, etc.).
func (l *Linter) LintSchema(schemaFiles map[string]string) ([]Result, error) {
	// Parse CREATE TABLE statements
	var createTables []*statement.CreateTable
	for filename, content := range schemaFiles {
		stmts, err := ddl.SplitStatements(content)
		if err != nil {
			return nil, fmt.Errorf("failed to split statements in %s: %w", filename, err)
		}
		for _, stmt := range stmts {
			// ParseCreateTable returns an error for non-CREATE TABLE statements;
			// we intentionally skip those since we only lint table definitions here.
			ct, err := statement.ParseCreateTable(stmt)
			if err != nil {
				continue
			}
			createTables = append(createTables, ct)
		}
	}
	if len(createTables) == 0 {
		return nil, nil
	}

	// Build Spirit lint config
	spiritConfig := l.buildSpiritConfig()

	// Run Spirit linters
	violations, err := runSpiritLinters(createTables, nil, spiritConfig)
	if err != nil {
		return nil, err
	}

	// Convert to Results
	var results []Result
	for _, v := range violations {
		results = append(results, l.convertViolation(v))
	}
	return results, nil
}

// SpiritConfig returns the Spirit lint.Config derived from our Config.
// This is used by callers that need to pass a Spirit config directly
// (e.g., to lint.PlanChanges).
func (l *Linter) SpiritConfig() *spiritlint.Config {
	cfg := l.buildSpiritConfig()
	return &cfg
}

// buildSpiritConfig creates a Spirit lint.Config from our Config.
func (l *Linter) buildSpiritConfig() spiritlint.Config {
	ignoreTables := make(map[string]bool)
	for _, t := range l.config.IgnoreTables {
		ignoreTables[t] = true
	}

	allowUnsafeStr := "false"
	if l.config.AllowUnsafe {
		allowUnsafeStr = "true"
	}

	return spiritlint.Config{
		Settings: map[string]map[string]string{
			"primary_key": {
				"allowedTypes": l.config.AllowedPKTypes,
			},
			"allow_charset": {
				"charsets": l.config.AllowedCharsets,
			},
			"unsafe": {
				"allowUnsafe": allowUnsafeStr,
			},
			"invisible_index_before_drop": {
				// Treat DROP INDEX without making invisible first as an error (requires --allow-unsafe)
				"raiseError": "true",
			},
		},
		IgnoreTables: ignoreTables,
	}
}

// PlanChanges serializes calls into Spirit's linter registry. Spirit registers
// singleton linter instances, and configurable linters mutate instance fields
// while applying config.
func PlanChanges(current, desired []table.TableSchema, diffOpts *statement.DiffOptions, lintConfig *spiritlint.Config) (*spiritlint.Plan, error) {
	spiritLintMu.Lock()
	defer spiritLintMu.Unlock()
	return spiritlint.PlanChanges(current, desired, diffOpts, lintConfig)
}

func runSpiritLinters(existingSchema []*statement.CreateTable, changes []*statement.AbstractStatement, config spiritlint.Config) ([]spiritlint.Violation, error) {
	spiritLintMu.Lock()
	defer spiritLintMu.Unlock()
	return spiritlint.RunLinters(existingSchema, changes, config)
}

// convertViolation converts a Spirit violation to our Result type.
func (l *Linter) convertViolation(v spiritlint.Violation) Result {
	linterName := ""
	if v.Linter != nil {
		linterName = v.Linter.Name()
	}

	table := ""
	column := ""
	if v.Location != nil {
		table = v.Location.Table
		if v.Location.Column != nil {
			column = *v.Location.Column
		}
	}

	// Determine severity string
	severity := "warning"
	if v.Severity == spiritlint.SeverityError {
		severity = "error"
	}

	// Violations with SeverityError are considered unsafe and require --allow-unsafe.
	// This includes:
	// - "unsafe" linter violations (DROP TABLE, DROP COLUMN, etc.)
	// - "invisible_index_before_drop" with raiseError=true (DROP INDEX without making invisible)
	isUnsafe := v.Severity == spiritlint.SeverityError

	return Result{
		Table:    table,
		Column:   column,
		Linter:   linterName,
		Message:  v.Message,
		Severity: severity,
		IsUnsafe: isUnsafe,
	}
}

// ToEngineWarnings converts lint Results to engine.LintViolation format.
func ToEngineWarnings(results []Result) []engine.LintViolation {
	warnings := make([]engine.LintViolation, len(results))
	for i, r := range results {
		warnings[i] = engine.LintViolation{
			Table:    r.Table,
			Column:   r.Column,
			Linter:   r.Linter,
			Message:  r.Message,
			Severity: r.Severity,
		}
	}
	return warnings
}
