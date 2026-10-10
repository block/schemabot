package planetscale

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"

	"github.com/block/spirit/pkg/statement"
	"github.com/block/spirit/pkg/table"
	"golang.org/x/sync/errgroup"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/psclient"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/vschema"
)

// vschemaDeletionsMetadata detects structural removals between the current and
// desired VSchema and encodes them for plan change metadata. Removals gate the
// apply behind the unsafe opt-in: deleting a vindex, a table routing entry, or
// a column-vindex association changes Vitess query routing the moment the
// VSchema lands, so it is as dangerous as destructive DDL. Returns "" when the
// change removes nothing.
func vschemaDeletionsMetadata(currentRaw, desired string) (string, error) {
	deletions, err := vschema.Deletions(currentRaw, desired)
	if err != nil {
		return "", err
	}
	if len(deletions) == 0 {
		return "", nil
	}
	converted := make([]apitypes.VSchemaDeletion, len(deletions))
	for i, d := range deletions {
		converted[i] = apitypes.VSchemaDeletion{Kind: d.Kind, Name: d.Name, Reason: d.Reason}
	}
	return apitypes.EncodeVSchemaDeletions(converted)
}

// vschemaMutationsMetadata detects in-place routing changes between the
// current and desired VSchema and encodes them for plan change metadata.
// Mutations gate the apply behind the unsafe opt-in just like removals: a
// same-name vindex whose type, params, or owner changes, a keyspace whose
// sharded or require_explicit_routing flag flips, or a table whose type,
// pin, primary vindex, reference source, or auto-increment changes alters
// Vitess query routing, lookup maintenance, or id generation the moment the
// VSchema lands. liveTables names the tables on the target so a sequence
// given to a live table through a new entry in an unsharded keyspace is
// gated too. Returns "" when nothing changes in place.
func vschemaMutationsMetadata(currentRaw, desired string, liveTables []string) (string, error) {
	mutations, err := vschema.Mutations(currentRaw, desired, liveTables)
	if err != nil {
		return "", err
	}
	if len(mutations) == 0 {
		return "", nil
	}
	converted := make([]apitypes.VSchemaMutation, len(mutations))
	for i, m := range mutations {
		converted[i] = apitypes.VSchemaMutation{Kind: m.Kind, Name: m.Name, Reason: m.Reason}
	}
	return apitypes.EncodeVSchemaMutations(converted)
}

// liveTableNames returns the names of the tables the target currently holds
// in one keyspace.
func liveTableNames(tables []table.TableSchema) []string {
	names := make([]string, 0, len(tables))
	for _, t := range tables {
		names = append(names, t.Name)
	}
	return names
}

// Plan computes the schema changes needed by diffing current schema against desired.
// For each keyspace in the schema files, it fetches the current schema and uses
// Spirit's PlanChanges to diff and lint in a single pass.
func (e *Engine) Plan(ctx context.Context, req *engine.PlanRequest) (*engine.PlanResult, error) {
	r := *req
	r.Database = e.resolveDatabase(req.Credentials, req.Database)
	req = &r

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

	// The config withholds named live tables from the planner, so a table no
	// schema file declares is not proposed for DROP TABLE. Applied before the
	// diff below, per keyspace, and disclosed on the result: the diff never
	// sees a withheld table, so without the disclosure it would be
	// indistinguishable from an unchanged one.
	ignored, err := engine.NewIgnoredTables(req.IgnoreTables)
	if err != nil {
		return nil, fmt.Errorf("plan database %s: %w", req.Database, err)
	}
	exemptTables, err := e.withholdIgnoredTables(ignored, req, keyspaces, currentSchema)
	if err != nil {
		return nil, err
	}

	// Diff and lint per keyspace in parallel using Spirit's PlanChanges. A
	// failing keyspace does not cancel the others: every keyspace runs to
	// completion, so when several keyspaces fail the plan reports the first
	// failure in keyspace order, the same one on every run, rather than
	// whichever goroutine finished first.
	var mu sync.Mutex
	results := make(map[string]*keyspaceResult, len(keyspaces))
	failures := make(map[string]error, len(keyspaces))
	var g errgroup.Group
	g.SetLimit(20)

	for _, keyspace := range keyspaces {
		ks := keyspace
		g.Go(func() error {
			result, err := e.planKeyspace(ctx, client, org, req.Database, branch, ks, req.SchemaFiles[ks], currentSchema)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				failures[ks] = err
				return err
			}
			results[ks] = result
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		for _, ks := range keyspaces {
			if failure := failures[ks]; failure != nil {
				return nil, failure
			}
		}
		return nil, err
	}

	// Collect results in deterministic keyspace order, deduplicating lint violations.
	var changes []engine.SchemaChange
	var lintViolations []engine.LintViolation
	seenLint := make(map[string]bool)
	for _, ks := range keyspaces {
		r := results[ks]
		if r == nil {
			continue
		}
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
		// The exemption travels on a no-changes plan too: this is exactly where
		// a reviewer needs to tell a withheld live table from an unchanged one.
		return &engine.PlanResult{
			PlanID:       engine.NewPlanID(),
			NoChanges:    true,
			ExemptTables: exemptTables,
		}, nil
	}

	return &engine.PlanResult{
		PlanID:         engine.NewPlanID(),
		Changes:        changes,
		LintViolations: lintViolations,
		ExemptTables:   exemptTables,
	}, nil
}

// keyspaceResult is one keyspace's share of a plan.
type keyspaceResult struct {
	change     engine.SchemaChange
	violations []engine.LintViolation
	hasChanges bool
}

// planKeyspace diffs and lints one keyspace's desired schema files against its
// current schema and builds the keyspace's SchemaChange, including the VSchema
// diff and the metadata that gates VSchema removals and in-place mutations.
func (e *Engine) planKeyspace(ctx context.Context, client psclient.PSClient, org, database, branch, ks string, ns *schema.Namespace, currentSchema map[string][]table.TableSchema) (*keyspaceResult, error) {
	diff, diffErr := e.diffKeyspace(ctx, client, org, database, branch, ks, ns, currentSchema)
	if diffErr != nil {
		return nil, diffErr
	}
	vschemaChanged, currentVSchemaRaw := diff.vschemaChanged, diff.currentVSchemaRaw

	sc := engine.SchemaChange{
		Namespace:    ks,
		Metadata:     make(map[string]string),
		TableChanges: diff.tableChanges,
	}
	if tables, ok := currentSchema[ks]; ok {
		sc.OriginalFiles = make(map[string]string, len(tables)+1)
		for _, t := range tables {
			sc.OriginalFiles[t.Name+".sql"] = t.Schema
		}
	}

	if vschemaChanged {
		sc.Metadata["vschema_changed"] = "true"
		sc.Metadata["vschema"] = vschema.Diff(currentVSchemaRaw, ns.Files["vschema.json"])
		deletionsMeta, delErr := vschemaDeletionsMetadata(currentVSchemaRaw, ns.Files["vschema.json"])
		if delErr != nil {
			return nil, fmt.Errorf("detect VSchema deletions for keyspace %s: %w", ks, delErr)
		}
		if deletionsMeta != "" {
			sc.Metadata[apitypes.VSchemaDeletionsMetadataKey] = deletionsMeta
		}
		mutationsMeta, mutErr := vschemaMutationsMetadata(currentVSchemaRaw, ns.Files["vschema.json"], liveTableNames(currentSchema[ks]))
		if mutErr != nil {
			return nil, fmt.Errorf("detect VSchema mutations for keyspace %s: %w", ks, mutErr)
		}
		if mutationsMeta != "" {
			sc.Metadata[apitypes.VSchemaMutationsMetadataKey] = mutationsMeta
		}
		if strings.TrimSpace(currentVSchemaRaw) == "" {
			currentVSchemaRaw = "{}"
		}
		if sc.OriginalFiles == nil {
			sc.OriginalFiles = make(map[string]string, 1)
		}
		sc.OriginalFiles["vschema.json"] = currentVSchemaRaw
	}
	if len(sc.TableChanges) > 0 || vschemaChanged {
		sc.OriginalFilesCaptured = true
		if sc.OriginalFiles == nil {
			sc.OriginalFiles = make(map[string]string)
		}
	}

	return &keyspaceResult{
		change:     sc,
		violations: diff.violations,
		hasChanges: len(sc.TableChanges) > 0 || sc.Metadata["vschema_changed"] == "true",
	}, nil
}

// withholdIgnoredTables removes the live tables the config's ignore_tables
// withholds from each keyspace's current schema, in place, and returns the
// plan's disclosure of what it actually withheld. A keyspace whose schema
// files declare a withheld table is refused: the declaration and the ignore
// contradict each other, and the diff would resolve that contradiction by
// proposing to create a table that already exists.
func (e *Engine) withholdIgnoredTables(ignored engine.IgnoredTables, req *engine.PlanRequest, keyspaces []string, currentSchema map[string][]table.TableSchema) ([]*engine.ExemptTables, error) {
	if ignored.Empty() {
		return nil, nil
	}
	var exemptTables []*engine.ExemptTables
	for _, ks := range keyspaces {
		ns := req.SchemaFiles[ks]
		if ns == nil {
			return nil, fmt.Errorf("plan keyspace %q: schema files are required", ks)
		}
		declared, err := parseDesiredSchemas(ks, ns)
		if err != nil {
			return nil, err
		}
		names := make([]string, len(declared))
		for i, ts := range declared {
			names[i] = ts.Name
		}
		if err := ignored.RefuseDeclared(ks, names); err != nil {
			return nil, err
		}

		live, ok := currentSchema[ks]
		if !ok {
			continue
		}
		kept := make([]table.TableSchema, 0, len(live))
		var withheld []string
		for _, ts := range live {
			if ignored.Withholds(ts.Name) {
				withheld = append(withheld, ts.Name)
				continue
			}
			kept = append(kept, ts)
		}
		if len(withheld) == 0 {
			continue
		}
		currentSchema[ks] = kept
		// The plan discloses this too; the log is where an operator tracing
		// "why does the plan not mention table X" finds the answer without a
		// plan comment in front of them.
		e.logger.Info("live tables withheld from the plan by ignore_tables",
			"database", req.Database, "keyspace", ks, "tables", withheld)
		exemptTables = append(exemptTables, ignored.Exemption(ks, withheld))
	}
	return exemptTables, nil
}

// parseDesiredSchemas parses CREATE TABLE statements from schema files in a namespace,
// returning table schemas suitable for diffing against current state. Skips vschema.json
// and non-.sql files. A keyspace is diffed as one set, so a table two schema
// files declare is refused; files are read in sorted order so the refusal names
// the same pair on every run.
func parseDesiredSchemas(keyspace string, ns *schema.Namespace) ([]table.TableSchema, error) {
	var schemas []table.TableSchema
	var declared ddl.TableDeclarations
	for _, filename := range slices.Sorted(maps.Keys(ns.Files)) {
		content := ns.Files[filename]
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
			if err := declared.Declare(keyspace+"/"+filename, ct.TableName); err != nil {
				return nil, err
			}
			schemas = append(schemas, table.TableSchema{
				Name:   ct.TableName,
				Schema: stmt,
			})
		}
	}
	return schemas, nil
}
