package schema

import (
	"fmt"
	"path"
	"strings"
)

// GroupFilesByNamespace groups schema files by namespace using their relative paths.
// Two layouts are supported:
//
//  1. Flat layout — SQL files directly in the schema directory. The namespace
//     key is defaultNamespace (the schema directory name = the MySQL database name).
//
//     schema/aurora_coffeeshop_exemplar/    → defaultNamespace = "aurora_coffeeshop_exemplar"
//     ├── schemabot.yaml
//     ├── baristas.sql                      → namespace: "aurora_coffeeshop_exemplar"
//     └── customers.sql                     → namespace: "aurora_coffeeshop_exemplar"
//
//  2. Subdirectory layout — SQL files in named subdirectories. Each subdirectory
//     name becomes the namespace key. defaultNamespace is not used.
//
//     schema/
//     ├── schemabot.yaml
//     ├── payments/
//     │   └── transactions.sql              → namespace: "payments"
//     └── payments_audit/
//     └── audit_log.sql                 → namespace: "payments_audit"
//
// Mixed layouts (both flat files and subdirectories) are rejected as ambiguous.
//
// Both the CLI (ReadSchemaFiles) and webhook (groupFilesByNamespace) call this
// function with the schema directory's base name as defaultNamespace:
//   - CLI:     filepath.Base(dir)       e.g. "aurora_coffeeshop_exemplar"
//   - Webhook: path.Base(schemaPath)    e.g. "aurora_coffeeshop_exemplar"
//
// The input map keys are relative paths (e.g., "users.sql" or "payments/users.sql")
// and values are file contents. Only .sql files and vschema.json are included;
// other files (like schemabot.yaml) are skipped.
func GroupFilesByNamespace(files map[string]string, defaultNamespace string) (SchemaFiles, error) {
	result := make(SchemaFiles)
	var hasFlatFile, hasNamespacedFile bool

	for relativePath, content := range files {
		filename := path.Base(relativePath)

		// Skip non-schema files
		if !strings.HasSuffix(filename, ".sql") && filename != "vschema.json" {
			continue
		}

		namespace := path.Dir(relativePath)
		if namespace == "." || namespace == "" {
			namespace = defaultNamespace
			hasFlatFile = true
		} else {
			hasNamespacedFile = true
		}

		if result[namespace] == nil {
			result[namespace] = &Namespace{Files: make(map[string]string)}
		}
		result[namespace].Files[filename] = content
	}

	// Reject mixed flat + namespaced files
	if hasFlatFile && hasNamespacedFile {
		return nil, fmt.Errorf("schema directory has both flat files and namespace subdirectories — use one layout or the other")
	}

	return result, nil
}
