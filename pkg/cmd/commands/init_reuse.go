package commands

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"

	"github.com/block/schemabot/pkg/schema"
)

// Snapshot the user's files before verification. Publication later compares
// against the same snapshot, so edits made during setup are not overwritten.
func stageExistingInitSchema(root, stage, database, engine, environment string, namespaces []string) ([]string, error) {
	snapshot, err := initSchemaSnapshot(root)
	if err != nil {
		return nil, err
	}
	if _, exists := snapshot["schemabot.yaml"]; !exists {
		return nil, fmt.Errorf("schemabot.yaml not found in %q; choose a schema directory with a valid configuration", root)
	}
	for relative, content := range snapshot {
		path := filepath.Join(stage, relative)
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			return nil, err
		}
		if err := os.WriteFile(path, []byte(content), 0600); err != nil {
			return nil, err
		}
	}
	cfg, err := LoadCLIConfig(stage)
	if err != nil {
		return nil, err
	}
	if cfg.Database != database || cfg.Type != engine {
		return nil, fmt.Errorf("existing schema configuration names a different database or engine")
	}
	files, _, err := schema.GroupFilesByNamespace(snapshot, filepath.Base(root), environment, cfg.IgnoreNamespaces)
	if err != nil {
		return nil, err
	}
	actual := make([]string, 0, len(files))
	for namespace := range files {
		actual = append(actual, namespace)
	}
	selected := slices.Clone(namespaces)
	slices.Sort(actual)
	slices.Sort(selected)
	if !slices.Equal(actual, selected) {
		return nil, fmt.Errorf("existing schema namespaces %v do not match selected scope %v", actual, selected)
	}
	return cfg.IgnoreNamespaces, nil
}

// Existing files are never adopted implicitly without a SchemaBot declaration.
func validateInitSchemaDestination(root string) error {
	entries, err := os.ReadDir(root)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read schema directory: %w", err)
	}
	if len(entries) == 0 {
		return nil
	}
	if _, err := LoadCLIConfig(root); err != nil {
		return fmt.Errorf("choose an empty --schema-dir or provide a valid schemabot.yaml to reuse your files: %w", err)
	}
	return nil
}
