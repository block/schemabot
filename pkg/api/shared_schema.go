package api

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/engine"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/routing"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/storage"
)

// SharedSchemaDatabase is one database's side of a schema it shares with
// another database on the same target: the schema files it plans for an
// environment, and the ignore_tables entries that keep its plans away from the
// tables the other database declares there.
type SharedSchemaDatabase struct {
	Database string
	// ConfigPath is the repo-relative schemabot.yaml the database was read
	// from, which is where a missing ignore_tables entry has to be added.
	ConfigPath   string
	SchemaFiles  map[string]*ternv1.SchemaFiles
	IgnoreTables []string
}

// SharedSchemaPeers returns, sorted, the other databases of this server that
// can share a schema with database in environment: databases of the same type
// that accept schema changes from repo and whose targets in that environment
// include a rollout member (deployment and target) that database also has.
// Whether two of them actually share a namespace on that member depends on
// their schema files, which CheckSharedSchemaIgnoreLists reads.
//
// A database that does not configure the environment shares nothing in it. A
// peer whose targets cannot be resolved is an error rather than a database
// skipped, because skipping it would pass a pair the check could not compare.
func (c *ServerConfig) SharedSchemaPeers(repo, database, environment string) ([]string, error) {
	if c == nil {
		return nil, fmt.Errorf("server config is nil")
	}
	databases := c.DatabaseConfigs()
	self, ok := databases[database]
	if !ok {
		return nil, &DatabaseNotConfiguredError{Database: database}
	}
	selfTargets, err := c.ResolveDatabaseTargets(database, environment)
	if err != nil {
		return nil, fmt.Errorf("resolve targets of database %q in environment %q: %w", database, environment, err)
	}
	selfMembers := make(map[string]bool, len(selfTargets))
	for _, t := range selfTargets {
		selfMembers[t.MemberID()] = true
	}

	var peers []string
	for _, name := range slices.Sorted(maps.Keys(databases)) {
		if name == database {
			continue
		}
		peer := databases[name]
		if peer.Type != self.Type {
			continue
		}
		if len(peer.AllowedRepos) > 0 && !repoAllowed(peer.AllowedRepos, repo) {
			continue
		}
		if _, ok := peer.Environments[environment]; !ok {
			continue
		}
		peerTargets, err := c.ResolveDatabaseTargets(name, environment)
		if err != nil {
			return nil, fmt.Errorf("resolve targets of database %q in environment %q, to tell whether it shares a target with %q: %w", name, environment, database, err)
		}
		if slices.ContainsFunc(peerTargets, func(t routing.ExecutionTarget) bool { return selfMembers[t.MemberID()] }) {
			peers = append(peers, name)
		}
	}
	return peers, nil
}

// CheckSharedSchemaIgnoreLists refuses a database whose schema files and
// ignore_tables disagree with a peer's about a namespace the two hold on the
// same rollout member. Two databases that split one schema between them each
// declare their own tables and must list the other's in ignore_tables: a table
// one declares and the other does not ignore is proposed as a DROP TABLE by
// the other's next plan, and a table both declare has two owners whose plans
// can undo each other. Either is returned as a *SharedSchemaIgnoreListsError
// naming the entry or file to fix.
//
// The check runs both ways, so it holds whichever of the two databases a pull
// request changes. It reads only the schema files, never the target, so it
// holds before any apply has created the table.
func (c *ServerConfig) CheckSharedSchemaIgnoreLists(environment string, self SharedSchemaDatabase, peers []SharedSchemaDatabase) error {
	if len(peers) == 0 {
		return nil
	}
	dbConfig := c.Database(self.Database)
	if dbConfig == nil {
		return &DatabaseNotConfiguredError{Database: self.Database}
	}
	parser, err := ddl.ParserForDialect(schema.DialectForDatabaseType(dbConfig.Type))
	if err != nil {
		return fmt.Errorf("statement parser for database type %q: %w", dbConfig.Type, err)
	}
	selfTargets, err := c.ResolveDatabaseTargets(self.Database, environment)
	if err != nil {
		return fmt.Errorf("resolve targets of database %q in environment %q: %w", self.Database, environment, err)
	}
	selfSide, err := newSharedSchemaSide(self, parser)
	if err != nil {
		return err
	}

	report := &SharedSchemaIgnoreListsError{Environment: environment}
	for _, peer := range peers {
		peerTargets, err := c.ResolveDatabaseTargets(peer.Database, environment)
		if err != nil {
			return fmt.Errorf("resolve targets of database %q in environment %q: %w", peer.Database, environment, err)
		}
		peerSide, err := newSharedSchemaSide(peer, parser)
		if err != nil {
			return err
		}
		for _, shared := range sharedNamespaces(selfTargets, self.SchemaFiles, peerTargets, peer.SchemaFiles, plansEachNamespaceApart(dbConfig.Type)) {
			report.compare(shared, selfSide, peerSide)
		}
	}
	if report.empty() {
		return nil
	}
	report.sort()
	return report
}

// sharedNamespace is a pair of namespaces, one from each database, that one
// rollout member can plan as the same set of tables: the same namespace, or,
// on an engine that can plan a target as one unit, any two namespaces the two
// databases hold there.
type sharedNamespace struct {
	selfNamespace string
	peerNamespace string
	target        string
}

// plansEachNamespaceApart reports whether every target of a database type
// plans each namespace as its own set of tables, so two databases can collide
// only in a namespace they both declare. A Vitess plan is diffed per keyspace
// and a PostgreSQL plan per schema. A MySQL target whose DSN names a database
// diffs every namespace it is sent against that one database, and only the
// data plane knows its DSN, so a MySQL pair is compared across namespaces for
// the ignore_tables entries each needs. An engine not listed here is compared
// the same cautious way.
func plansEachNamespaceApart(databaseType string) bool {
	switch databaseType {
	case storage.DatabaseTypeVitess, storage.DatabaseTypePostgres:
		return true
	default:
		return false
	}
}

// sharedNamespaces returns each pair of namespaces the two databases declare
// and hold on a rollout member they both have, once per pair, naming the first
// such member's target in member order. With apart set, a pair is one
// namespace both declare; otherwise it is any namespace of each.
func sharedNamespaces(selfTargets []routing.ExecutionTarget, selfFiles map[string]*ternv1.SchemaFiles, peerTargets []routing.ExecutionTarget, peerFiles map[string]*ternv1.SchemaFiles, apart bool) []sharedNamespace {
	peerByMember := make(map[string]routing.ExecutionTarget, len(peerTargets))
	for _, t := range peerTargets {
		peerByMember[t.MemberID()] = t
	}
	seen := map[[2]string]bool{}
	var shared []sharedNamespace
	for _, selfTarget := range selfTargets {
		peerTarget, ok := peerByMember[selfTarget.MemberID()]
		if !ok {
			continue
		}
		for _, selfNamespace := range slices.Sorted(maps.Keys(selfFiles)) {
			if !selfTarget.Holds(selfNamespace) {
				continue
			}
			for _, peerNamespace := range slices.Sorted(maps.Keys(peerFiles)) {
				if apart && peerNamespace != selfNamespace {
					continue
				}
				pair := [2]string{selfNamespace, peerNamespace}
				if seen[pair] || !peerTarget.Holds(peerNamespace) {
					continue
				}
				seen[pair] = true
				shared = append(shared, sharedNamespace{selfNamespace: selfNamespace, peerNamespace: peerNamespace, target: selfTarget.Target})
			}
		}
	}
	return shared
}

// sharedSchemaSide is one database of a pair, parsed for comparison.
type sharedSchemaSide struct {
	SharedSchemaDatabase
	ignored engine.IgnoredTables
	// declared maps each namespace to the tables its schema files create, as
	// the files spell them, sorted.
	declared map[string][]string
}

func newSharedSchemaSide(db SharedSchemaDatabase, parser ddl.StatementParser) (sharedSchemaSide, error) {
	ignored, err := engine.NewIgnoredTables(db.IgnoreTables)
	if err != nil {
		return sharedSchemaSide{}, fmt.Errorf("ignore_tables of database %q: %w", db.Database, err)
	}
	declared := make(map[string][]string, len(db.SchemaFiles))
	for namespace, files := range db.SchemaFiles {
		tables, err := tablesCreatedIn(parser, namespace, files)
		if err != nil {
			return sharedSchemaSide{}, fmt.Errorf("schema files of database %q: %w", db.Database, err)
		}
		declared[namespace] = tables
	}
	return sharedSchemaSide{SharedSchemaDatabase: db, ignored: ignored, declared: declared}, nil
}

// tablesCreatedIn returns the tables a namespace's SQL files create, sorted.
// Other schema artifacts, such as a VSchema, declare no tables.
func tablesCreatedIn(parser ddl.StatementParser, namespace string, files *ternv1.SchemaFiles) ([]string, error) {
	var tables []string
	for name, content := range files.GetFiles() {
		if !strings.HasSuffix(name, ".sql") {
			continue
		}
		stmts, err := parser.Split(content)
		if err != nil {
			return nil, fmt.Errorf("split schema file %s/%s: %w", namespace, name, err)
		}
		for _, stmt := range stmts {
			stmtType, table, err := parser.Classify(stmt)
			if err != nil {
				return nil, fmt.Errorf("classify statement in schema file %s/%s: %w", namespace, name, err)
			}
			if stmtType == ddl.StatementCreateTable {
				tables = append(tables, table)
			}
		}
	}
	slices.Sort(tables)
	return slices.Compact(tables), nil
}

// MissingIgnoreEntry is a table one database declares in a shared namespace
// that the other database's ignore_tables does not withhold.
type MissingIgnoreEntry struct {
	// Database is the database whose ignore_tables needs the entry, and
	// ConfigPath its schemabot.yaml.
	Database   string
	ConfigPath string
	// DeclaredBy is the database whose schema files declare Tables, spelled
	// as the files spell them.
	DeclaredBy string
	// Namespace is where DeclaredBy declares Tables, and OwnerNamespace the
	// namespace of Database that the target can plan as the same tables. The
	// two differ only on an engine that can plan a target as one unit.
	Namespace      string
	OwnerNamespace string
	Target         string
	Tables         []string
	// Entries are the names ignore_tables has to withhold for Tables: each
	// declared spelling it misses, and the lower-case spelling a target that
	// folds table names stores the table under.
	Entries []string
}

// DoublyDeclaredTables are tables both databases declare in one namespace
// they share.
type DoublyDeclaredTables struct {
	Databases [2]string
	Namespace string
	Target    string
	Tables    []string
}

// SharedSchemaIgnoreListsError is the refusal CheckSharedSchemaIgnoreLists
// returns. Its message is meant for the plan comment: it names each entry to
// add and the file to add it to.
type SharedSchemaIgnoreListsError struct {
	Environment string
	Missing     []MissingIgnoreEntry
	Doubled     []DoublyDeclaredTables
}

func (e *SharedSchemaIgnoreListsError) empty() bool {
	return len(e.Missing) == 0 && len(e.Doubled) == 0
}

// compare records what the pair gets wrong about one shared namespace, in both
// directions. Names are compared the way the engines withhold them: an
// ignore_tables entry must name the table exactly or match it as a pattern. A
// table both declare is matched ignoring case, which errs toward refusing on a
// target that folds table names.
//
// A table both declare is refused only within one namespace. Across two
// namespaces it is the same table only on a target whose connection names one
// database, and only the data plane knows the connection. On a target whose
// connection names none, each namespace is its own database and the two
// tables are separate, so the refusal could not be fixed. A table only one
// declares is still compared across namespaces: listing it in the other's
// ignore_tables is harmless when the tables are separate and prevents a
// DROP TABLE when they are not.
func (e *SharedSchemaIgnoreListsError) compare(shared sharedNamespace, self, peer sharedSchemaSide) {
	selfTables := self.declared[shared.selfNamespace]
	peerTables := peer.declared[shared.peerNamespace]
	selfFolded := foldedTableSet(selfTables)
	peerFolded := foldedTableSet(peerTables)

	var doubled []string
	if shared.selfNamespace == shared.peerNamespace {
		for _, table := range selfTables {
			if peerFolded[declaredTableKey(table)] {
				doubled = append(doubled, table)
			}
		}
	}
	if len(doubled) > 0 {
		e.Doubled = append(e.Doubled, DoublyDeclaredTables{
			Databases: [2]string{self.Database, peer.Database},
			Namespace: shared.selfNamespace,
			Target:    shared.target,
			Tables:    doubled,
		})
	}
	e.addMissing(shared.target, shared.selfNamespace, self, shared.peerNamespace, peer, peerFolded)
	e.addMissing(shared.target, shared.peerNamespace, peer, shared.selfNamespace, self, selfFolded)
}

// addMissing records the tables declarer declares in the shared namespace that
// owner neither declares nor ignores. A table counts as ignored only when the
// entries withhold it under both its declared spelling and its lower-case
// one: an engine withholds by the name the catalog holds, and a target that
// folds table names holds the table in lower case. Entries match case
// exactly, so an entry naming only the declared spelling would leave the
// folded table to be dropped.
func (e *SharedSchemaIgnoreListsError) addMissing(target, declarerNamespace string, declarer sharedSchemaSide, ownerNamespace string, owner sharedSchemaSide, ownerFolded map[string]bool) {
	var tables, entries []string
	for _, table := range declarer.declared[declarerNamespace] {
		if ownerFolded[declaredTableKey(table)] {
			continue
		}
		unwithheld := unwithheldSpellings(owner.ignored, table)
		if len(unwithheld) == 0 {
			continue
		}
		tables = append(tables, table)
		entries = append(entries, unwithheld...)
	}
	if len(tables) == 0 {
		return
	}
	e.Missing = append(e.Missing, MissingIgnoreEntry{
		Database:       owner.Database,
		ConfigPath:     owner.ConfigPath,
		DeclaredBy:     declarer.Database,
		Namespace:      declarerNamespace,
		OwnerNamespace: ownerNamespace,
		Target:         target,
		Tables:         tables,
		Entries:        entries,
	})
}

// unwithheldSpellings returns the spellings of table, as declared and in
// lower case, that ignored does not withhold.
func unwithheldSpellings(ignored engine.IgnoredTables, table string) []string {
	var spellings []string
	for _, spelling := range slices.Compact([]string{table, strings.ToLower(table)}) {
		if !ignored.Withholds(spelling) {
			spellings = append(spellings, spelling)
		}
	}
	return spellings
}

func (e *SharedSchemaIgnoreListsError) sort() {
	slices.SortFunc(e.Missing, func(a, b MissingIgnoreEntry) int {
		return strings.Compare(a.Database+"\x00"+a.Namespace+"\x00"+a.OwnerNamespace+"\x00"+a.DeclaredBy, b.Database+"\x00"+b.Namespace+"\x00"+b.OwnerNamespace+"\x00"+b.DeclaredBy)
	})
	slices.SortFunc(e.Doubled, func(a, b DoublyDeclaredTables) int {
		return strings.Compare(a.Namespace+"\x00"+a.Databases[1], b.Namespace+"\x00"+b.Databases[1])
	})
}

func (e *SharedSchemaIgnoreListsError) Error() string {
	var parts []string
	for _, m := range e.Missing {
		config := m.ConfigPath
		if config == "" {
			config = fmt.Sprintf("the schemabot.yaml of %q", m.Database)
		}
		var part string
		if m.OwnerNamespace == m.Namespace {
			part = fmt.Sprintf(
				"%q declares %s in namespace %q, which %q shares on target %q, but %s does not ignore %s, so a plan of %q would drop %s. Add %s to ignore_tables in %s",
				m.DeclaredBy, quotedTables(m.Tables), m.Namespace, m.Database, m.Target,
				config, pronoun(m.Tables), m.Database, pronoun(m.Tables), quotedTables(m.Entries), config)
		} else {
			part = fmt.Sprintf(
				"%q declares %s in namespace %q on target %q, where %q plans namespace %q, and a target whose connection names one database plans both as that database. %s does not ignore %s, so a plan of %q would drop %s. Add %s to ignore_tables in %s",
				m.DeclaredBy, quotedTables(m.Tables), m.Namespace, m.Target, m.Database, m.OwnerNamespace,
				config, pronoun(m.Tables), m.Database, pronoun(m.Tables), quotedTables(m.Entries), config)
		}
		if !slices.Equal(m.Entries, m.Tables) {
			part += ". Entries match case exactly, and a target that folds table names stores each table in lower case, so the lower-case spelling needs an entry too"
		}
		parts = append(parts, part)
	}
	for _, d := range e.Doubled {
		parts = append(parts, fmt.Sprintf(
			"%q and %q both declare %s in namespace %q on target %q. Keep each table's schema file in one of them and list the table in the other's ignore_tables",
			d.Databases[0], d.Databases[1], quotedTables(d.Tables), d.Namespace, d.Target))
	}
	return strings.Join(parts, "; ")
}

func foldedTableSet(tables []string) map[string]bool {
	set := make(map[string]bool, len(tables))
	for _, table := range tables {
		set[declaredTableKey(table)] = true
	}
	return set
}

func quotedTables(tables []string) string {
	quoted := make([]string, len(tables))
	for i, table := range tables {
		quoted[i] = fmt.Sprintf("%q", table)
	}
	return strings.Join(quoted, ", ")
}

func pronoun(tables []string) string {
	if len(tables) == 1 {
		return "it"
	}
	return "them"
}
