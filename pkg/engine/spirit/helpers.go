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
// namespace order, so the same input yields the same plan.
func groupChangesByNamespace(changes []engine.TableChange, currentSchema []table.TableSchema, declaredIn map[string]string, sf schema.SchemaFiles) ([]engine.SchemaChange, error) {
	changesByNS := make(map[string][]engine.TableChange)
	for _, tc := range changes {
		ns, err := namespaceForTable(tc.Table, declaredIn, sf)
		if err != nil {
			return nil, fmt.Errorf("namespace lookup for table %q: %w", tc.Table, err)
		}
		changesByNS[ns] = append(changesByNS[ns], tc)
	}
	originalFilesByNS := make(map[string]map[string]string, len(changesByNS))
	for ns := range changesByNS {
		originalFilesByNS[ns] = map[string]string{}
	}
	if len(sf) == 1 {
		for ns := range sf {
			for _, ts := range currentSchema {
				originalFilesByNS[ns][ts.Name+".sql"] = ts.Schema
			}
		}
	} else {
		for _, ts := range currentSchema {
			ns, err := namespaceForTable(ts.Name, declaredIn, sf)
			if err != nil {
				return nil, fmt.Errorf("namespace lookup for original table %q: %w", ts.Name, err)
			}
			if _, ok := originalFilesByNS[ns]; ok {
				originalFilesByNS[ns][ts.Name+".sql"] = ts.Schema
			}
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
// deterministic. A table no schema file declares — for example a DROP TABLE
// plan, where the table no longer has a defining statement — can only be
// attributed when exactly one namespace exists. With two or more namespaces
// it cannot be attributed to a single one, so this returns an error rather
// than an arbitrary map key.
func namespaceForTable(table string, declaredIn map[string]string, sf schema.SchemaFiles) (string, error) {
	if nsName, ok := declaredIn[table]; ok {
		return nsName, nil
	}
	if len(sf) == 1 {
		for nsName := range sf {
			return nsName, nil
		}
	}
	return "", fmt.Errorf("no namespace defines table %q among %d schema namespaces %v", table, len(sf), namespaceNames(sf))
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
