package api

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/block/schemabot/pkg/ddl"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/routing"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/storage"
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
// A drop the plan attributes to a namespace the member does not select is
// refused whatever the table. A drop the plan attributes to a selected
// namespace passes on that attribution alone only where the engine located the
// dropped table in the namespace it names (engineLocatesDroppedTables), because
// the namespaces of a sharded family all declare the same tables and a name
// match would refuse their legitimate drops. Every other drop, one the plan
// attributes to no namespace or one whose attribution the engine inferred, is
// judged by its name as well and fails closed: it is refused when an
// unselected namespace declares that table.
//
// It is a refusal, not an unsafe change awaiting an opt-in: --allow-unsafe
// accepts drops the schema files ask for, and these are drops they do not.
//
// declared is the request's full declared namespace set, before it was narrowed
// to the member's selection. The unselected namespaces' files are parsed only
// when the plan proposes a drop judged by name, so any other plan costs nothing,
// and a file that cannot be parsed fails the plan rather than letting a drop
// through unchecked.
func (s *Service) refuseDropsOfUnselectedTables(req PlanRequest, declared map[string]*ternv1.SchemaFiles, unselected []string, member routing.ExecutionTarget, changes []*ternv1.SchemaChange, shards []*ternv1.ShardPlan) error {
	if len(unselected) == 0 {
		return nil
	}
	drops := plannedTableDrops(changes, shards)
	if len(drops) == 0 {
		return nil
	}
	locatesDrops := engineLocatesDroppedTables(member.DatabaseType)
	// placed holds the drops the plan itself attributes to an unselected
	// namespace; named holds those refused only because an unselected
	// namespace declares a table of that name.
	placed := map[string]bool{}
	named := map[string]bool{}
	namespaces := map[string]bool{}
	var judgedByName []string
	for _, drop := range drops {
		for _, namespace := range drop.namespaces {
			if slices.Contains(member.Namespaces, namespace) {
				continue
			}
			placed[drop.table] = true
			namespaces[namespace] = true
		}
		// Only a located attribution clears a drop: one the plan attributes to
		// no namespace, or whose namespace the engine inferred, is also judged
		// by name.
		attributionClears := locatesDrops && len(drop.namespaces) > 0
		if !attributionClears {
			judgedByName = append(judgedByName, drop.table)
		}
	}
	if len(judgedByName) > 0 {
		declaredBy, err := tablesDeclaredBy(declared, unselected, member.DatabaseType)
		if err != nil {
			return &UnselectedTableDropCheckError{Database: req.Database, Environment: req.Environment, Target: member.Target, Err: err}
		}
		for _, table := range judgedByName {
			namespace, ok := declaredBy[table]
			if !ok {
				continue
			}
			named[table] = true
			namespaces[namespace] = true
		}
	}
	if len(placed) == 0 && len(named) == 0 {
		return nil
	}
	refused := maps.Clone(placed)
	maps.Copy(refused, named)
	tables := slices.Sorted(maps.Keys(refused))
	// A refusal is the plan's deterministic answer for this configuration, not
	// a server fault, so it is a warning; the caller reports it.
	s.logger.Warn("plan proposes dropping tables in namespaces the target's entry does not select",
		"database", req.Database,
		"environment", req.Environment,
		"deployment", member.Deployment,
		"target", member.Target,
		"repository", req.Repository,
		"tables", tables,
		"refused_namespaces", slices.Sorted(maps.Keys(namespaces)),
		"refused_by_name", slices.Sorted(maps.Keys(named)),
		"selected_namespaces", member.Namespaces,
		"unselected_namespaces", unselected)
	return &UnselectedTableDropError{
		Deployment:  member.Deployment,
		Target:      member.Target,
		Tables:      tables,
		Namespaces:  slices.Sorted(maps.Keys(namespaces)),
		MatchedName: len(placed) == 0,
	}
}

// engineLocatesDroppedTables reports whether an engine's plan attributes a
// dropped table to the namespace it found that table in, so the attribution can
// clear the drop on its own. A Vitess plan is diffed per keyspace and a
// PostgreSQL plan per schema, so each names where the table lives. The MySQL
// engine diffs a database-scoped target as one unit and attributes a dropped
// table, which no file it was sent defines, to the only namespace it was sent:
// a data plane that discarded the unselected namespaces attributes their live
// tables to the selected one. Its attribution, and that of an engine not listed
// here, is inferred, so those drops are judged by name as well.
func engineLocatesDroppedTables(databaseType string) bool {
	switch databaseType {
	case storage.DatabaseTypeVitess, storage.DatabaseTypePostgres:
		return true
	default:
		return false
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
	// sorted: the namespace the plan attributes a drop to, or for a drop judged
	// by name, the unselected namespace declaring the table.
	Namespaces []string
	// MatchedName is set when every refused drop was refused by name alone: the
	// plan placed none of them in an unselected namespace, but an unselected
	// namespace declares a table of each name. The engine cannot say whose
	// table such a drop is, so it is either a data plane planning another
	// target's tables or a legitimate drop whose name collides with them.
	MatchedName bool
}

func (e *UnselectedTableDropError) Error() string {
	if e.MatchedName {
		return fmt.Sprintf(
			"the plan from deployment %q target %q proposes dropping %s, and namespaces [%s], which this target's entry does not select, declare tables of the same name. This target's engine does not report which namespace a dropped table belongs to, so SchemaBot cannot tell a drop of this target's own table from a drop of one of those namespaces' tables, and refuses the plan, --allow-unsafe included. Either that deployment predates selecting namespaces per target and planned those namespaces' tables, or the drop is intended and its table name collides with a table those namespaces still declare, which this target cannot drop while they do",
			e.Deployment, e.Target, quotedTableList(e.Tables), strings.Join(e.Namespaces, ", "))
	}
	return fmt.Sprintf(
		"the plan from deployment %q target %q proposes dropping %s in namespaces [%s], which this target's entry does not select. The schema files do not ask for these drops, so the plan is refused, --allow-unsafe included. Upgrade that deployment to a build that supports selecting namespaces per target",
		e.Deployment, e.Target, quotedTableList(e.Tables), strings.Join(e.Namespaces, ", "))
}

// UnselectedTableDropCheckError reports a plan whose drops could not be checked
// against the namespaces the member's targets entry does not select, because an
// unselected namespace's schema file could not be parsed. It is a check failure
// rather than a verdict on the drops, and it fails the plan closed.
type UnselectedTableDropCheckError struct {
	Database    string
	Environment string
	Target      string
	Err         error
}

func (e *UnselectedTableDropCheckError) Error() string {
	return fmt.Sprintf("check planned drops of database %q environment %q target %q against its unselected namespaces: %v",
		e.Database, e.Environment, e.Target, e.Err)
}

func (e *UnselectedTableDropCheckError) Unwrap() error { return e.Err }

// UnselectedTableDropRefused reports whether a plan was refused because it
// proposed dropping tables in namespaces the member's targets entry does not
// select, or because those drops could not be checked. Like a namespace
// placement refusal, every plan of the environment reproduces it until the
// configuration, the schema files or the planning deployment changes, and the
// refused environment has no plan to fold, so a caller that gates a merge on
// the plan must fail that environment's check closed on it.
func UnselectedTableDropRefused(err error) bool {
	if _, ok := errors.AsType[*UnselectedTableDropError](err); ok {
		return true
	}
	_, ok := errors.AsType[*UnselectedTableDropCheckError](err)
	return ok
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
