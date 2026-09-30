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

// refuseDropsOfUnselectedTables refuses a plan that proposes dropping a table in
// a namespace the member's targets entry does not select. The configuration
// places that namespace on another target, so the schema files never asked for
// the table's removal: a drop of it means the data plane diffed the whole target
// without honoring the unselected namespaces it was sent, which a build that
// predates the field does by discarding it. The check reads the plan that came
// back rather than trusting the data plane to have refused, so it holds on every
// data plane build.
//
// A drop is judged by the namespace the plan attributes it to, not by its table
// name, because the namespaces of a sharded family all declare the same tables:
// a drop the plan places in a selected namespace passes even when an unselected
// namespace declares a table of that name, and a drop it places in any other
// namespace is refused whatever the table. A drop the plan attributes to no
// namespace cannot be placed, so it fails closed on its name: it is refused when
// an unselected namespace declares that table.
//
// It is a refusal, not an unsafe change awaiting an opt-in: --allow-unsafe
// accepts drops the schema files ask for, and these are drops they do not.
//
// declared is the request's full declared namespace set, before it was narrowed
// to the member's selection. The unselected namespaces' files are parsed only
// when the plan proposes a drop it does not attribute, so any other plan costs
// nothing, and a file that cannot be parsed fails the plan rather than letting a
// drop through unchecked.
func (s *Service) refuseDropsOfUnselectedTables(req PlanRequest, declared map[string]*ternv1.SchemaFiles, unselected []string, member routing.ExecutionTarget, changes []*ternv1.SchemaChange, shards []*ternv1.ShardPlan) error {
	if len(unselected) == 0 {
		return nil
	}
	drops := plannedTableDrops(changes, shards)
	if len(drops) == 0 {
		return nil
	}
	refused := map[string]bool{}
	namespaces := map[string]bool{}
	var unattributed []string
	for _, drop := range drops {
		if len(drop.namespaces) == 0 {
			unattributed = append(unattributed, drop.table)
			continue
		}
		for _, namespace := range drop.namespaces {
			if slices.Contains(member.Namespaces, namespace) {
				continue
			}
			refused[drop.table] = true
			namespaces[namespace] = true
		}
	}
	if len(unattributed) > 0 {
		declaredBy, err := tablesDeclaredBy(declared, unselected, member.DatabaseType)
		if err != nil {
			return fmt.Errorf("check planned drops of database %q environment %q target %q against its unselected namespaces: %w",
				req.Database, req.Environment, member.Target, err)
		}
		for _, table := range unattributed {
			namespace, ok := declaredBy[table]
			if !ok {
				continue
			}
			refused[table] = true
			namespaces[namespace] = true
		}
	}
	if len(refused) == 0 {
		return nil
	}
	tables := slices.Sorted(maps.Keys(refused))
	s.logger.Error("plan proposes dropping tables in namespaces the target's entry does not select",
		"database", req.Database,
		"environment", req.Environment,
		"deployment", member.Deployment,
		"target", member.Target,
		"repository", req.Repository,
		"tables", tables,
		"refused_namespaces", slices.Sorted(maps.Keys(namespaces)),
		"selected_namespaces", member.Namespaces,
		"unselected_namespaces", unselected)
	return &UnselectedTableDropError{
		Deployment: member.Deployment,
		Target:     member.Target,
		Tables:     tables,
		Namespaces: slices.Sorted(maps.Keys(namespaces)),
	}
}

// UnselectedTableDropError reports a plan that proposes dropping tables in
// namespaces the member's targets entry does not select.
type UnselectedTableDropError struct {
	Deployment string
	Target     string
	// Tables are the refused drops, sorted.
	Tables []string
	// Namespaces are the unselected namespaces the refused drops belong to,
	// sorted: the namespace the plan attributes a drop to, or for a drop it
	// attributes to none, the unselected namespace declaring the table.
	Namespaces []string
}

func (e *UnselectedTableDropError) Error() string {
	return fmt.Sprintf(
		"the plan from deployment %q target %q proposes dropping %s in namespaces [%s], which this target's entry does not select. The schema files do not ask for these drops, so the plan is refused, --allow-unsafe included. Upgrade that deployment to a build that supports selecting namespaces per target",
		e.Deployment, e.Target, quotedTableList(e.Tables), strings.Join(e.Namespaces, ", "))
}

// plannedTableDrop is one table a plan proposes dropping, with every namespace
// the plan attributes that drop to: the table change's own namespace and that of
// the namespace change or shard plan carrying it. It has none when the plan
// attributes the drop to no namespace.
type plannedTableDrop struct {
	table      string
	namespaces []string
}

// plannedTableDrops returns every table drop a plan proposes, from its
// namespace changes and its per-shard changes.
func plannedTableDrops(changes []*ternv1.SchemaChange, shards []*ternv1.ShardPlan) []plannedTableDrop {
	var drops []plannedTableDrop
	collect := func(carrier string, tableChanges []*ternv1.TableChange) {
		for _, tc := range tableChanges {
			if tc == nil || tc.ChangeType != ternv1.ChangeType_CHANGE_TYPE_DROP {
				continue
			}
			var namespaces []string
			for _, namespace := range []string{tc.Namespace, carrier} {
				if namespace != "" && !slices.Contains(namespaces, namespace) {
					namespaces = append(namespaces, namespace)
				}
			}
			drops = append(drops, plannedTableDrop{table: tc.TableName, namespaces: namespaces})
		}
	}
	for _, change := range changes {
		if change != nil {
			collect(change.Namespace, change.TableChanges)
		}
	}
	for _, shard := range shards {
		if shard != nil {
			collect(shard.Namespace, shard.Changes)
		}
	}
	return drops
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
