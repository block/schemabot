package planetscale

import (
	"context"
	"fmt"
	"maps"
	"strings"
	"sync"
	"time"

	ps "github.com/planetscale/planetscale-go/planetscale"

	"github.com/block/spirit/pkg/statement"
	"github.com/block/spirit/pkg/table"
	"golang.org/x/sync/errgroup"

	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/lint"
	"github.com/block/schemabot/pkg/schema"
)

// Plan computes the schema changes needed by diffing current schema against desired.
// For each keyspace in the schema files, it fetches the current schema and uses
// Spirit's PlanChanges to diff and lint in a single pass.
func (e *Engine) Plan(ctx context.Context, req *engine.PlanRequest) (*engine.PlanResult, error) {
	e.logger.Info("computing plan",
		"database", req.Database,
		"schema_files", len(req.SchemaFiles),
	)

	client, err := e.getClient(req.Credentials)
	if err != nil {
		return nil, fmt.Errorf("get planetscale client: %w", err)
	}

	org := credOrg(req.Credentials)
	branch := mainBranch(req.Credentials)

	// Sort keyspaces for deterministic order
	keyspaces := sortedKeyspaces(req.SchemaFiles)

	// Prefer the PlanetScale schema API when safe schema changes are enabled,
	// and use vtgate only when they are not.
	currentSchema, err := e.fetchPlanSchema(ctx, client, org, req.Database, branch, req.Credentials, keyspaces)
	if err != nil {
		return nil, fmt.Errorf("fetch current schema: %w", err)
	}

	// Diff and lint per keyspace in parallel using Spirit's PlanChanges.
	type keyspaceResult struct {
		change     engine.SchemaChange
		violations []engine.LintViolation
		schemas    map[string]string // keyspace.table -> CREATE TABLE
		hasChanges bool
	}

	var mu sync.Mutex
	results := make(map[string]*keyspaceResult, len(keyspaces))
	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(20)

	for _, keyspace := range keyspaces {
		ks := keyspace
		g.Go(func() error {
			ns := req.SchemaFiles[ks]

			tableChanges, vschemaChanged, diffErr := e.diffKeyspace(gCtx, client, org, req.Database, branch, ks, ns, currentSchema)
			if diffErr != nil {
				return diffErr
			}

			sc := engine.SchemaChange{
				Namespace:    ks,
				Metadata:     make(map[string]string),
				TableChanges: tableChanges,
			}

			if vschemaChanged {
				currentVSchemaRaw := ""
				currentVSchema, _ := client.GetKeyspaceVSchema(gCtx, &ps.GetKeyspaceVSchemaRequest{
					Organization: org, Database: req.Database, Branch: branch, Keyspace: ks,
				})
				if currentVSchema != nil {
					currentVSchemaRaw = currentVSchema.Raw
				}
				sc.Metadata["vschema_changed"] = "true"
				sc.Metadata["vschema"] = VSchemaDiff(currentVSchemaRaw, ns.Files["vschema.json"])
			}

			var currentTableSchemas []table.TableSchema
			if tables, ok := currentSchema[ks]; ok {
				currentTableSchemas = append(currentTableSchemas, tables...)
			}
			desiredTableSchemas, _ := parseDesiredSchemas(ks, ns)
			plan, _ := lint.PlanChanges(currentTableSchemas, desiredTableSchemas, nil, e.linter.SpiritConfig())

			schemas := make(map[string]string)
			if tables, ok := currentSchema[ks]; ok {
				for _, t := range tables {
					schemas[ks+"."+t.Name] = t.Schema
				}
			}

			var violations []engine.LintViolation
			if plan != nil {
				for _, pc := range plan.Changes {
					for _, v := range pc.Violations {
						violations = append(violations, engine.LintViolation{
							Table:    pc.TableName,
							Linter:   v.Linter.Name(),
							Message:  v.Message,
							Severity: strings.ToLower(v.Severity.String()),
						})
					}
				}
			}

			mu.Lock()
			results[ks] = &keyspaceResult{
				change:     sc,
				violations: violations,
				schemas:    schemas,
				hasChanges: len(sc.TableChanges) > 0 || sc.Metadata["vschema_changed"] == "true",
			}
			mu.Unlock()
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}

	// Collect results in deterministic keyspace order, deduplicating lint violations.
	var changes []engine.SchemaChange
	var lintViolations []engine.LintViolation
	seenLint := make(map[string]bool)
	originalSchema := make(map[string]string)
	for _, ks := range keyspaces {
		r := results[ks]
		if r == nil {
			continue
		}
		maps.Copy(originalSchema, r.schemas)
		for _, v := range r.violations {
			key := v.Table + "\x00" + v.Message
			if !seenLint[key] {
				seenLint[key] = true
				lintViolations = append(lintViolations, v)
			}
		}
		if r.hasChanges {
			changes = append(changes, r.change)
		}
	}

	if len(changes) == 0 {
		return &engine.PlanResult{
			PlanID:    fmt.Sprintf("plan-%d", time.Now().UnixNano()),
			NoChanges: true,
		}, nil
	}

	return &engine.PlanResult{
		PlanID:         fmt.Sprintf("plan-%d", time.Now().UnixNano()),
		Changes:        changes,
		LintViolations: lintViolations,
		OriginalSchema: originalSchema,
	}, nil
}

// parseDesiredSchemas parses CREATE TABLE statements from schema files in a namespace,
// returning table schemas suitable for diffing against current state. Skips vschema.json
// and non-.sql files.
func parseDesiredSchemas(keyspace string, ns *schema.Namespace) ([]table.TableSchema, error) {
	var schemas []table.TableSchema
	for filename, content := range ns.Files {
		if filename == "vschema.json" || !strings.HasSuffix(filename, ".sql") {
			continue
		}
		stmts, err := ddl.SplitStatements(content)
		if err != nil {
			return nil, fmt.Errorf("split SQL for keyspace %s: %w", keyspace, err)
		}
		for _, stmt := range stmts {
			ct, err := statement.ParseCreateTable(stmt)
			if err != nil {
				return nil, fmt.Errorf("parse desired schema in keyspace %s/%s: %w", keyspace, filename, err)
			}
			if err := ddl.ValidateCreateTable(ct); err != nil {
				return nil, fmt.Errorf("SQL usage error in keyspace %s/%s: %w", keyspace, filename, err)
			}
			schemas = append(schemas, table.TableSchema{
				Name:   ct.TableName,
				Schema: stmt,
			})
		}
	}
	return schemas, nil
}
