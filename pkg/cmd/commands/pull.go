package commands

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/block/schemabot/pkg/local"
	"github.com/block/schemabot/pkg/secrets"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/utils"

	"gopkg.in/yaml.v3"
)

// PullCmd pulls the current schema from a live MySQL database and writes
// one .sql file per table plus a schemabot.yaml config file.
type PullCmd struct {
	DSN         string `required:"" help:"MySQL DSN (e.g., root:pass@tcp(localhost:3306)/mydb). Supports env: and file: prefixes."`
	OutputDir   string `short:"o" help:"Output directory for schema files." default:"."`
	Database    string `short:"d" help:"Override database name (default: extracted from DSN)."`
	Environment string `short:"e" help:"Environment name for this connection." default:"staging"`
	Type        string `short:"t" help:"Database type." default:"mysql" enum:"mysql"`
}

// Run executes the pull command.
func (cmd *PullCmd) Run(g *Globals) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	dsn, err := secrets.Resolve(cmd.DSN, "")
	if err != nil {
		return fmt.Errorf("resolve DSN: %w", err)
	}
	if dsn == "" {
		return fmt.Errorf("DSN is empty after resolution")
	}

	dbName := cmd.Database
	if dbName == "" {
		dbName = ExtractDatabaseFromDSN(dsn)
		if dbName == "" {
			return fmt.Errorf("could not extract database name from DSN — use --database to specify it")
		}
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer utils.CloseAndLog(db)

	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("connect to %s: %w", dbName, err)
	}

	tables, err := table.LoadSchemaFromDB(ctx, db,
		table.WithoutUnderscoreTables,
		table.WithStrippedAutoIncrement,
	)
	if err != nil {
		return fmt.Errorf("load schema from %s: %w", dbName, err)
	}

	// Create output directory
	if err := os.MkdirAll(cmd.OutputDir, 0755); err != nil {
		return fmt.Errorf("create output directory %s: %w", cmd.OutputDir, err)
	}

	// Write .sql files
	var written []string
	for _, t := range tables {
		filename := t.Name + ".sql"
		path := filepath.Join(cmd.OutputDir, filename)

		// Write the raw SHOW CREATE TABLE output — this is already canonical MySQL format.
		// AUTO_INCREMENT is stripped by the WithStrippedAutoIncrement filter above.
		content := strings.TrimRight(t.Schema, "\n") + ";\n"
		if err := os.WriteFile(path, []byte(content), 0644); err != nil {
			return fmt.Errorf("write %s: %w", path, err)
		}
		written = append(written, filename)
	}

	sort.Strings(written)

	// Generate schemabot.yaml (database + type only, no connection details)
	configPath := filepath.Join(cmd.OutputDir, "schemabot.yaml")
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		cfg := CLIConfig{
			Database: dbName,
			Type:     cmd.Type,
		}
		data, err := yaml.Marshal(cfg)
		if err != nil {
			return fmt.Errorf("marshal schemabot.yaml: %w", err)
		}
		if err := os.WriteFile(configPath, data, 0644); err != nil {
			return fmt.Errorf("write schemabot.yaml: %w", err)
		}
		fmt.Printf("Created %s\n", configPath)
	} else {
		slog.Debug("schemabot.yaml already exists, skipping generation", "path", configPath)
	}

	// Upsert database environment into ~/.schemabot/config.yaml local section
	if err := local.UpsertLocalEnvironment(dbName, cmd.Type, cmd.Environment, local.LocalEnvironment{
		DSN: cmd.DSN,
	}); err != nil {
		slog.Warn("failed to update local config", "error", err)
	} else {
		fmt.Printf("Updated ~/.schemabot/config.yaml: %s/%s\n", dbName, cmd.Environment)
	}

	absDir, _ := filepath.Abs(cmd.OutputDir)
	fmt.Printf("Pulled %d tables from %s to %s\n", len(written), dbName, absDir)
	for _, f := range written {
		fmt.Printf("  %s\n", f)
	}

	fmt.Printf("\nNext steps:\n")
	fmt.Printf("  Edit a .sql file in %s\n", cmd.OutputDir)
	fmt.Printf("  schemabot plan -s %s  # see the diff\n", cmd.OutputDir)
	fmt.Printf("  schemabot apply -s %s # apply changes\n", cmd.OutputDir)

	return nil
}

// ExtractDatabaseFromDSN extracts the database name from a MySQL DSN.
// DSN format: [user[:password]@][net[(addr)]]/dbname[?param=value]
func ExtractDatabaseFromDSN(dsn string) string {
	// Find the last '/' that separates address from database
	idx := strings.LastIndex(dsn, "/")
	if idx < 0 {
		return ""
	}
	rest := dsn[idx+1:]
	// Strip query parameters
	if qIdx := strings.Index(rest, "?"); qIdx >= 0 {
		rest = rest[:qIdx]
	}
	return rest
}
