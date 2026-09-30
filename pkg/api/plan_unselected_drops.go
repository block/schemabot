package api

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/block/schemabot/pkg/ddl"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/routing"
	"github.com/block/schemabot/pkg/schema"
)

// refuseDropsOfUnselectedTables refuses a plan that proposes dropping a table a
// namespace declares when the member's targets entry does not select that
// namespace. The configuration places that namespace on another target, so the
// schema files never asked for the table's removal: a drop of it means the data
// plane diffed the whole target without honoring the unselected namespaces it
// was sent, which a build that predates the field does by discarding it. The
// check reads the plan that came back rather than trusting the data plane to
// have refused, so it holds on every data plane build.
//
// It is a refusal, not an unsafe change awaiting an opt-in: --allow-unsafe
// accepts drops the schema files ask for, and these are drops they do not.
//
// declared is the request's full declared namespace set, before it was narrowed
// to the member's selection. The unselected namespaces' files are parsed only
// when the plan proposes a drop, so a plan with none costs nothing, and a file
// that cannot be parsed fails the plan rather than letting a drop through
// unchecked.
func (s *Service) refuseDropsOfUnselectedTables(req PlanRequest, declared map[string]*ternv1.SchemaFiles, unselected []string, member routing.ExecutionTarget, changes []*ternv1.SchemaChange, shards []*ternv1.ShardPlan) error {
	if len(unselected) == 0 {
		return nil
	}
	dropped := plannedTableDrops(changes, shards)
	if len(dropped) == 0 {
		return nil
	}
	declaredBy, err := tablesDeclaredBy(declared, unselected, member.DatabaseType)
	if err != nil {
		return fmt.Errorf("check planned drops of database %q environment %q target %q against its unselected namespaces: %w",
			req.Database, req.Environment, member.Target, err)
	}
	var refused []string
	namespaces := map[string]bool{}
	for _, table := range dropped {
		namespace, ok := declaredBy[table]
		if !ok {
			continue
		}
		refused = append(refused, table)
		namespaces[namespace] = true
	}
	if len(refused) == 0 {
		return nil
	}
	s.logger.Error("plan proposes dropping tables that namespaces the target's entry does not select declare",
		"database", req.Database,
		"environment", req.Environment,
		"deployment", member.Deployment,
		"target", member.Target,
		"repository", req.Repository,
		"tables", refused,
		"unselected_namespaces", unselected)
	return &UnselectedTableDropError{
		Deployment: member.Deployment,
		Target:     member.Target,
		Tables:     refused,
		Namespaces: slices.Sorted(maps.Keys(namespaces)),
	}
}

// UnselectedTableDropError reports a plan that proposes dropping tables that
// namespaces the member's targets entry does not select declare.
type UnselectedTableDropError struct {
	Deployment string
	Target     string
	// Tables are the refused drops, sorted.
	Tables []string
	// Namespaces are the unselected namespaces declaring them, sorted.
	Namespaces []string
}

func (e *UnselectedTableDropError) Error() string {
	return fmt.Sprintf(
		"the plan from deployment %q target %q proposes dropping %s, which namespaces [%s] declare and this target's entry does not select. The schema files do not ask for these drops, so the plan is refused, --allow-unsafe included. Upgrade that deployment to a build that supports selecting namespaces per target",
		e.Deployment, e.Target, quotedTableList(e.Tables), strings.Join(e.Namespaces, ", "))
}

// plannedTableDrops returns every table a plan proposes dropping, from its
// namespace changes and its per-shard changes, sorted and deduplicated.
func plannedTableDrops(changes []*ternv1.SchemaChange, shards []*ternv1.ShardPlan) []string {
	var dropped []string
	collect := func(tableChanges []*ternv1.TableChange) {
		for _, tc := range tableChanges {
			if tc != nil && tc.ChangeType == ternv1.ChangeType_CHANGE_TYPE_DROP {
				dropped = append(dropped, tc.TableName)
			}
		}
	}
	for _, change := range changes {
		if change != nil {
			collect(change.TableChanges)
		}
	}
	for _, shard := range shards {
		if shard != nil {
			collect(shard.Changes)
		}
	}
	slices.Sort(dropped)
	return slices.Compact(dropped)
}

// tablesDeclaredBy maps each table the given namespaces' schema files create to
// the first of those namespaces, in sorted order, that declares it. Only SQL
// files declare tables; other schema artifacts such as a VSchema are skipped.
func tablesDeclaredBy(declared map[string]*ternv1.SchemaFiles, namespaces []string, databaseType string) (map[string]string, error) {
	parser, err := ddl.ParserForDialect(schema.DialectForDatabaseType(databaseType))
	if err != nil {
		return nil, fmt.Errorf("statement parser for database type %q: %w", databaseType, err)
	}
	declaredBy := map[string]string{}
	for _, namespace := range slices.Sorted(slices.Values(namespaces)) {
		files := declared[namespace].GetFiles()
		for _, name := range slices.Sorted(maps.Keys(files)) {
			if !strings.HasSuffix(name, ".sql") {
				continue
			}
			stmts, err := parser.Split(files[name])
			if err != nil {
				return nil, fmt.Errorf("split schema file %s/%s: %w", namespace, name, err)
			}
			for _, stmt := range stmts {
				stmtType, table, err := parser.Classify(stmt)
				if err != nil {
					return nil, fmt.Errorf("classify statement in schema file %s/%s: %w", namespace, name, err)
				}
				if stmtType != ddl.StatementCreateTable {
					continue
				}
				if _, seen := declaredBy[table]; !seen {
					declaredBy[table] = namespace
				}
			}
		}
	}
	return declaredBy, nil
}
