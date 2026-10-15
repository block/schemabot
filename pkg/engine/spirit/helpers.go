package spirit

import (
	"fmt"
	"maps"
	"slices"
	"sort"
	"strings"

	"github.com/block/mysql"
	"github.com/block/spirit/pkg/table"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/schema"
)

// uniqueTableList joins table names into a comma-separated list, dropping
// duplicates while preserving order. A combined ALTER statement can carry
// several statements against the same table; the runner logger's table attr
// should name each table once.
func uniqueTableList(tables []string) string {
	seen := make(map[string]bool, len(tables))
	unique := make([]string, 0, len(tables))
	for _, t := range tables {
		if seen[t] {
			continue
		}
		seen[t] = true
		unique = append(unique, t)
	}
	return strings.Join(unique, ", ")
}

// parseDSN extracts connection info from a MySQL DSN using the mysql driver's parser.
func parseDSN(dsn string) (host, username, password, database string, err error) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return "", "", "", "", fmt.Errorf("parse DSN: %w", err)
	}
	return cfg.Addr, cfg.User, cfg.Passwd, cfg.DBName, nil
}

// groupChangesByNamespace builds the plan's per-namespace SchemaChanges.
// Spirit operates on a single database, but table changes are grouped by the
// namespace they belong to (from SchemaFiles keys) for consistency with
// multi-namespace engines like PlanetScale. declaredIn maps each desired
// table to the namespace whose schema file declares it. The result is in
// namespace order, so the same input yields the same plan. A namespace with
// no table change gets no SchemaChange, so an empty change list yields an
// empty result. database names the database the request is diffed against,
// for the error when a dropped table cannot be attributed.
func groupChangesByNamespace(changes []engine.TableChange, currentSchema []table.TableSchema, declaredIn map[string]string, sf schema.SchemaFiles, database string) ([]engine.SchemaChange, error) {
	changesByNS := make(map[string][]engine.TableChange)
	for _, tc := range changes {
		ns, err := namespaceForTable(tc.Table, declaredIn, sf, database)
		if err != nil {
			return nil, fmt.Errorf("namespace lookup for table %q: %w", tc.Table, err)
		}
		changesByNS[ns] = append(changesByNS[ns], tc)
	}
	originalFilesByNS := make(map[string]map[string]string, len(changesByNS))
	for ns := range changesByNS {
		originalFilesByNS[ns] = map[string]string{}
	}
	// Originals are captured only for the namespaces that have a SchemaChange;
	// a live table in a namespace the plan does not change has no SchemaChange
	// to carry it.
	for _, ts := range currentSchema {
		ns, err := namespaceForTable(ts.Name, declaredIn, sf, database)
		if err != nil {
			return nil, fmt.Errorf("namespace lookup for original table %q: %w", ts.Name, err)
		}
		if files, ok := originalFilesByNS[ns]; ok {
			files[ts.Name+".sql"] = ts.Schema
		}
	}
	schemaChanges := make([]engine.SchemaChange, 0, len(changesByNS))
	for _, ns := range slices.Sorted(maps.Keys(changesByNS)) {
		schemaChanges = append(schemaChanges, engine.SchemaChange{
			Namespace:             ns,
			TableChanges:          changesByNS[ns],
			OriginalFiles:         originalFilesByNS[ns],
			OriginalFilesCaptured: true,
		})
	}
	return schemaChanges, nil
}

// namespaceForTable returns the namespace a table belongs to: the namespace
// whose schema file declares it, as recorded in declaredIn while the desired
// schema was parsed. A table is declared by at most one schema file, so the
// recorded namespace is the only candidate, and the answer depends on the
// declarations alone, never on the order a map is walked or on a file's name.
//
// The namespace is the per-table progress-matching key, so it must be
// deterministic. A table no schema file declares is one the plan drops: its
// declaring file was removed or emptied. With exactly one namespace the
// request is that namespace's database, so the drop belongs to it. With two
// or more, every namespace is diffed against the same database, so the live
// table is in all of their schemas at once, and the request no longer carries
// the removed file that said which namespace held it. No namespace can be
// proven to own the drop, so this refuses with the remedy rather than picking
// one. A namespace-free target DSN plans each namespace against its own
// database, which attributes every drop exactly.
func namespaceForTable(table string, declaredIn map[string]string, sf schema.SchemaFiles, database string) (string, error) {
	if nsName, ok := declaredIn[table]; ok {
		return nsName, nil
	}
	switch len(sf) {
	case 0:
		return "", fmt.Errorf("table %q in database %q has no schema namespace: the request carries no schema files", table, database)
	case 1:
		for nsName := range sf {
			return nsName, nil
		}
	}
	return "", fmt.Errorf("cannot attribute DROP TABLE %q to a schema namespace: namespaces %v are all diffed against database %q and none of them declares the table; use a target DSN that names no database, so each namespace is diffed against its own database, or keep this database's schema files in one namespace directory", table, namespaceNames(sf), database)
}

// namespaceNames returns the sorted namespace keys for inclusion in error
// messages so operators can see which namespaces were searched.
func namespaceNames(sf schema.SchemaFiles) []string {
	names := make([]string, 0, len(sf))
	for nsName := range sf {
		names = append(names, nsName)
	}
	sort.Strings(names)
	return names
}
