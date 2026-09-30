package commands

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/block/schemabot/pkg/cmd/client"
	"github.com/block/schemabot/pkg/cmd/cliname"
	"github.com/block/schemabot/pkg/glyph"
	"github.com/block/schemabot/pkg/lint"
	"github.com/block/schemabot/pkg/schema"
)

// FixLintCmd auto-fixes lint issues in schema files.
type FixLintCmd struct {
	SchemaDir string `short:"s" required:"" help:"Schema directory with .sql files" name:"schema_dir"`
	DryRun    bool   `help:"Preview fixes without writing files" name:"dry-run"`
}

// Run executes the fix-lint command.
func (cmd *FixLintCmd) Run(g *Globals) error {
	// Read the .sql files with the same layout rules plan uses: flat files in
	// the schema directory, or one level of namespace subdirectories.
	files, err := readSchemaFiles(cmd.SchemaDir)
	if err != nil {
		return fmt.Errorf("read schema files: %w", err)
	}

	// A schema directory with nothing to fix is almost always a wrong path, so
	// fail rather than report success on files that were never read.
	if len(files) == 0 {
		return fmt.Errorf("no .sql files found in %s", cmd.SchemaDir)
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
					filePath := filepath.Join(cmd.SchemaDir, filepath.FromSlash(fr.Filename))
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
		fmt.Printf(glyph.Failed+" %d issue(s) require manual fix:\n", len(result.UnfixableIssues))
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
		fmt.Printf("Run '%s plan' to see full validation results.\n", cliname.Name())
	}

	// Exit with error if there are unfixable issues (for CI)
	if len(result.UnfixableIssues) > 0 {
		return fmt.Errorf("%d unfixable issue(s) found", len(result.UnfixableIssues))
	}

	return nil
}

// readSchemaFiles reads the .sql files under dir, keyed by their
// slash-separated path relative to dir ("users.sql" for a flat layout,
// "orders/users.sql" for a namespaced one). Other schema files such as
// vschema.json are not SQL and have nothing for the fixer to rewrite. A layout
// plan rejects, such as flat files beside namespace subdirectories, is
// rejected here with the same error, so fix-lint never rewrites files plan
// would refuse to read.
func readSchemaFiles(dir string) (map[string]string, error) {
	all, err := client.ReadSchemaFilesByPath(dir)
	if err != nil {
		return nil, err
	}
	if _, _, err := schema.GroupFilesByNamespace(all, filepath.Base(dir), "", nil); err != nil {
		return nil, err
	}

	files := make(map[string]string, len(all))
	for relPath, content := range all {
		if !strings.HasSuffix(relPath, ".sql") {
			continue
		}
		files[relPath] = content
	}
	return files, nil
}
