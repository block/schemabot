package commands

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/block/schemabot/pkg/lint"
)

// FixLintCmd auto-fixes lint issues in schema files.
type FixLintCmd struct {
	SchemaDir string `short:"s" required:"" help:"Schema directory with .sql files" name:"schema_dir"`
	DryRun    bool   `help:"Preview fixes without writing files" name:"dry-run"`
}

// Run executes the fix-lint command.
func (cmd *FixLintCmd) Run(g *Globals) error {
	// Read all .sql files from the schema directory
	files, err := readSchemaFiles(cmd.SchemaDir)
	if err != nil {
		return fmt.Errorf("read schema files: %w", err)
	}

	if len(files) == 0 {
		fmt.Println("No .sql files found in schema directory.")
		return nil
	}

	// Run the fixer
	fixer := lint.NewFixer()
	result, err := fixer.FixFiles(files)
	if err != nil {
		return fmt.Errorf("fix files: %w", err)
	}

	// Report results
	if result.TotalFixed == 0 && len(result.UnfixableIssues) == 0 {
		fmt.Println("✓ No lint issues found.")
		return nil
	}

	// Show fixed issues
	if result.TotalFixed > 0 {
		if cmd.DryRun {
			fmt.Printf("Would fix %d issue(s):\n", result.TotalFixed)
		} else {
			fmt.Printf("✅ Fixed %d issue(s):\n", result.TotalFixed)
		}

		for _, fr := range result.Files {
			if fr.Changed {
				for _, fix := range fr.Fixes {
					fmt.Printf("  - [%s] %s\n", fr.Filename, fix)
				}

				// Write fixed file (unless dry-run)
				if !cmd.DryRun {
					filePath := filepath.Join(cmd.SchemaDir, fr.Filename)
					if err := os.WriteFile(filePath, []byte(fr.FixedSQL), 0644); err != nil {
						return fmt.Errorf("write %s: %w", fr.Filename, err)
					}
				}
			}
		}
		fmt.Println()
	}

	// Show unfixable issues
	if len(result.UnfixableIssues) > 0 {
		fmt.Printf("❌ %d issue(s) require manual fix:\n", len(result.UnfixableIssues))
		for _, issue := range result.UnfixableIssues {
			loc := issue.Table
			if issue.Column != "" {
				loc = issue.Table + "." + issue.Column
			}
			fmt.Printf("  - [%s] %s\n", loc, issue.Message)
		}
		fmt.Println()
	}

	if cmd.DryRun && result.TotalFixed > 0 {
		fmt.Println("Run without --dry-run to apply fixes.")
	} else if result.TotalFixed > 0 {
		fmt.Println("Run 'schemabot plan' to see full validation results.")
	}

	// Exit with error if there are unfixable issues (for CI)
	if len(result.UnfixableIssues) > 0 {
		return fmt.Errorf("%d unfixable issue(s) found", len(result.UnfixableIssues))
	}

	return nil
}

// readSchemaFiles reads all .sql files from a directory.
func readSchemaFiles(dir string) (map[string]string, error) {
	files := make(map[string]string)

	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}

		content, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", entry.Name(), err)
		}

		files[entry.Name()] = string(content)
	}

	return files, nil
}
