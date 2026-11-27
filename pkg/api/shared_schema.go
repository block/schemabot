package api

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/engine"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/routing"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/tern"
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
// Whether two of them actually plan one schema on that member depends on their
// schema files and on the schemas the data plane resolves their namespaces to,
// which CheckSharedSchemaIgnoreLists compares.
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

// NamespaceResolver reports the schema each namespace of a database addresses
// on one of its targets: the schema a plan of that namespace connects to. Only
// the data plane knows it, since it depends on the target's connection and
// schema overrides.
type NamespaceResolver interface {
	ResolveNamespaces(ctx context.Context, database, environment string, target routing.ExecutionTarget, namespaces []string) (NamespacePlacement, error)
}

// NamespacePlacement is where a database's namespaces land on one target.
type NamespacePlacement struct {
	// Scope is what the schemas belong to below the target, such as the
	// PostgreSQL database a connection opens. Schemas of two databases are
	// one only when their scopes match.
	Scope string
	// Schemas maps each namespace to the schema it addresses.
	Schemas map[string]string
}

// CheckSharedSchemaIgnoreLists refuses a database whose schema files and
// ignore_tables disagree with a peer's about a schema the two plan on the same
// rollout member. Two databases that split one schema between them each
// declare their own tables and must list the other's in ignore_tables:
//   - a table one declares and the other neither declares nor ignores is
//     proposed as a DROP TABLE by the other's next plan;
//   - a table both declare has two owners whose plans can undo each other;
//   - a table one declares there and the other declares in a namespace stored
//     in another schema cannot be ignored at all, because ignore_tables covers
//     every namespace of a database.
//
// Each is returned as a *SharedSchemaIgnoreListsError naming what to fix.
//
// Namespaces are compared by the schema the data plane resolves them to on the
// member, not by their keys. A MySQL target whose connection names a database
// plans every namespace as that database, a target with a schema override
// plans its one namespace under the override's name, and any other target
// plans each namespace as a schema of its own name. A resolution that fails,
// including one refused by a data plane too old to answer, fails the check,
// since the pair cannot be compared without it.
//
// The check runs both ways, so it holds whichever of the two databases a pull
// request changes. It reads the schema files rather than the target's tables,
// so it holds before any apply has created a table.
func (c *ServerConfig) CheckSharedSchemaIgnoreLists(ctx context.Context, environment string, self SharedSchemaDatabase, peers []SharedSchemaDatabase, resolver NamespaceResolver) error {
	if len(peers) == 0 {
		return nil
	}
	if resolver == nil {
		return fmt.Errorf("namespace resolver is required to compare database %q with the databases it shares a target with", self.Database)
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

	schemas := &memberSchemas{resolver: resolver, environment: environment, resolved: map[string]NamespacePlacement{}}
	// A MySQL-family target can fold schema names to lower case, so there
	// two spellings are compared as one schema; PostgreSQL keeps them apart.
	foldSchemaNames := schema.DialectForDatabaseType(dbConfig.Type) != schema.DialectPostgres
	report := &SharedSchemaIgnoreListsError{Environment: environment, reported: map[string]bool{}}
	for _, peer := range peers {
		peerTargets, err := c.ResolveDatabaseTargets(peer.Database, environment)
		if err != nil {
			return fmt.Errorf("resolve targets of database %q in environment %q: %w", peer.Database, environment, err)
		}
		peerSide, err := newSharedSchemaSide(peer, parser)
		if err != nil {
			return err
		}
		for _, member := range sharedMembers(selfTargets, peerTargets) {
			selfHeld := heldNamespaces(member.self, self.SchemaFiles)
			peerHeld := heldNamespaces(member.peer, peer.SchemaFiles)
			if len(selfHeld) == 0 || len(peerHeld) == 0 {
				continue
			}
			selfSchemas, err := schemas.of(ctx, self.Database, member.self, selfHeld)
			if err != nil {
				return err
			}
			peerSchemas, err := schemas.of(ctx, peer.Database, member.peer, peerHeld)
			if err != nil {
				return err
			}
			report.compare(member.self.Target, foldSchemaNames, selfSide, selfSchemas, peerSide, peerSchemas)
		}
	}
	if report.empty() {
		return nil
	}
	report.sort()
	return report
}

// sharedMember is one rollout member two databases both have, with each
// database's target on it. The two targets have the same identity but can
// hold different namespaces.
type sharedMember struct {
	self routing.ExecutionTarget
	peer routing.ExecutionTarget
}

// sharedMembers returns the rollout members both databases have, in the order
// of self's targets.
func sharedMembers(selfTargets, peerTargets []routing.ExecutionTarget) []sharedMember {
	peerByMember := make(map[string]routing.ExecutionTarget, len(peerTargets))
	for _, t := range peerTargets {
		peerByMember[t.MemberID()] = t
	}
	var members []sharedMember
	for _, selfTarget := range selfTargets {
		if peerTarget, ok := peerByMember[selfTarget.MemberID()]; ok {
			members = append(members, sharedMember{self: selfTarget, peer: peerTarget})
		}
	}
	return members
}

// heldNamespaces returns, sorted, the namespaces of files that target holds.
func heldNamespaces(target routing.ExecutionTarget, files map[string]*ternv1.SchemaFiles) []string {
	var held []string
	for _, namespace := range slices.Sorted(maps.Keys(files)) {
		if target.Holds(namespace) {
			held = append(held, namespace)
		}
	}
	return held
}

// memberSchemas resolves a database's namespaces on a member once per check,
// since the database is compared there with every peer that shares it.
type memberSchemas struct {
	resolver    NamespaceResolver
	environment string
	resolved    map[string]NamespacePlacement
}

// of returns the schema each of namespaces addresses on target. A resolution
// that leaves a namespace without a schema is an error: the namespace could
// not be compared, and comparing the rest would pass a pair the check did not
// see whole.
func (m *memberSchemas) of(ctx context.Context, database string, target routing.ExecutionTarget, namespaces []string) (NamespacePlacement, error) {
	key := database + "\x00" + target.MemberID()
	if placement, ok := m.resolved[key]; ok {
		return placement, nil
	}
	failed := func(cause error) error {
		return &NamespaceResolutionError{
			Database:    database,
			Environment: m.environment,
			Deployment:  target.Deployment,
			Target:      target.Target,
			Namespaces:  namespaces,
			Cause:       cause,
		}
	}
	placement, err := m.resolver.ResolveNamespaces(ctx, database, m.environment, target, namespaces)
	if err != nil {
		return NamespacePlacement{}, failed(err)
	}
	for _, namespace := range namespaces {
		if placement.Schemas[namespace] == "" {
			return NamespacePlacement{}, failed(fmt.Errorf("data plane resolved no schema for namespace %q", namespace))
		}
	}
	m.resolved[key] = placement
	return placement, nil
}

// NamespaceResolutionError is a failure to learn which schemas a database's
// namespaces address on one of its targets. The shared schema check cannot
// compare the database there, so it fails rather than passing the pair. Its
// message carries the data plane's error and is meant for server logs; a
// public surface should show a fixed line built from the identifiers instead.
type NamespaceResolutionError struct {
	Database    string
	Environment string
	Deployment  string
	Target      string
	Namespaces  []string
	Cause       error
}

func (e *NamespaceResolutionError) Error() string {
	return fmt.Sprintf("resolve the schemas that namespaces %v of database %q address on target %q of deployment %q in environment %q: %v",
		e.Namespaces, e.Database, e.Target, e.Deployment, e.Environment, e.Cause)
}

func (e *NamespaceResolutionError) Unwrap() error { return e.Cause }

// DataPlaneTooOld reports whether the data plane predates namespace
// resolution, which upgrading it fixes. A data plane whose engine cannot
// resolve namespaces is not too old, and upgrading it does not help.
func (e *NamespaceResolutionError) DataPlaneTooOld() bool {
	return errors.Is(e.Cause, tern.ErrResolveNamespacesUnimplemented)
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
			return nil, &schemaFileParseError{message: fmt.Sprintf("split schema file %s/%s: %v", namespace, name, err), err: err}
		}
		for _, stmt := range stmts {
			stmtType, table, err := parser.Classify(stmt)
			if err != nil {
				return nil, &schemaFileParseError{message: fmt.Sprintf("classify statement in schema file %s/%s: %v", namespace, name, err), err: err}
			}
			if stmtType == ddl.StatementCreateTable {
				tables = append(tables, table)
			}
		}
	}
	slices.Sort(tables)
	return slices.Compact(tables), nil
}

// MissingIgnoreEntry is a table one database declares in a schema both plan
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
	// namespace of Database that Target stores in the same Schema.
	Namespace      string
	OwnerNamespace string
	Schema         string
	Target         string
	Tables         []string
	// Entries are the names ignore_tables has to withhold for Tables: each
	// declared spelling it misses, and the lower-case spelling a target that
	// folds table names stores the table under.
	Entries []string
}

// DoublyDeclaredTables are tables both databases declare in one schema they
// both plan. Namespaces are each database's namespace for that schema, in the
// order of Databases.
type DoublyDeclaredTables struct {
	Databases  [2]string
	Namespaces [2]string
	Schema     string
	Target     string
	Tables     []string
}

// TableDeclaredElsewhere is a table one database declares in a schema both
// plan, which the other database declares in a namespace stored in another
// schema. The other database's plan of the shared schema would drop the
// table, and ignore_tables cannot withhold it there without also withholding
// the table the database declares itself.
type TableDeclaredElsewhere struct {
	// Database is the database whose plan would drop Table, and
	// OwnerDeclaredIn the namespace where it declares a table of that name.
	Database        string
	OwnerDeclaredIn string
	// DeclaredBy declares Table in Namespace, which Target stores in Schema
	// together with OwnerNamespace of Database.
	DeclaredBy     string
	Namespace      string
	OwnerNamespace string
	Schema         string
	Target         string
	Table          string
}

// SharedSchemaIgnoreListsError is the refusal CheckSharedSchemaIgnoreLists
// returns. Its message is meant for the plan comment: it names each entry to
// add and the file to add it to, or the table to move when no entry can help.
type SharedSchemaIgnoreListsError struct {
	Environment       string
	Missing           []MissingIgnoreEntry
	Doubled           []DoublyDeclaredTables
	DeclaredElsewhere []TableDeclaredElsewhere
	// reported records each finding once, so a pair that shares several
	// members is told about a table once, on the first member in target
	// order.
	reported map[string]bool
}

func (e *SharedSchemaIgnoreListsError) empty() bool {
	return len(e.Missing) == 0 && len(e.Doubled) == 0 && len(e.DeclaredElsewhere) == 0
}

// firstReport reports whether a finding is new, and records it. Tables are
// keyed as the files spell them, so two spellings of one name are each
// reported: an ignore_tables entry matches case exactly, so each spelling
// needs its own.
func (e *SharedSchemaIgnoreListsError) firstReport(kind, database, other, table string) bool {
	key := kind + "\x00" + database + "\x00" + other + "\x00" + table
	if e.reported[key] {
		return false
	}
	e.reported[key] = true
	return true
}

// compare records what the pair gets wrong on one member, in both directions,
// for each schema both databases plan there. Names are compared the way the
// engines withhold them: an ignore_tables entry must name the table exactly or
// match it as a pattern. Whether two declarations are one table is decided
// ignoring case, which errs toward refusing on a target that folds table
// names.
func (e *SharedSchemaIgnoreListsError) compare(target string, foldSchemaNames bool, self sharedSchemaSide, selfPlacement NamespacePlacement, peer sharedSchemaSide, peerPlacement NamespacePlacement) {
	selfBySchema := namespacesBySchema(selfPlacement, foldSchemaNames)
	peerBySchema := namespacesBySchema(peerPlacement, foldSchemaNames)
	for _, identity := range slices.Sorted(maps.Keys(selfBySchema)) {
		peerSchema, ok := peerBySchema[identity]
		if !ok {
			continue
		}
		selfSchema := selfBySchema[identity]
		shared := sharedSchema{name: selfSchema.name, target: target}
		e.addDoubled(shared, self, selfSchema.namespaces, peer, peerSchema.namespaces)
		e.addMissing(shared, self, selfSchema.namespaces, peer, peerSchema.namespaces)
		e.addMissing(shared, peer, peerSchema.namespaces, self, selfSchema.namespaces)
	}
}

// sharedSchema is one schema two databases both plan on a member's target.
type sharedSchema struct {
	name   string
	target string
}

// resolvedSchema is one schema a database's namespaces land in on a target:
// its name as the data plane spells it, and the namespaces stored there.
type resolvedSchema struct {
	name       string
	namespaces []string
}

// namespacesBySchema inverts a placement, listing each schema's namespaces
// sorted. Schemas are keyed by their scope and name, with the name folded
// when the target may fold schema names, so two databases' schemas match
// whenever the target could store them as one.
func namespacesBySchema(placement NamespacePlacement, foldSchemaNames bool) map[string]resolvedSchema {
	bySchema := map[string]resolvedSchema{}
	for _, namespace := range slices.Sorted(maps.Keys(placement.Schemas)) {
		name := placement.Schemas[namespace]
		identityName := name
		if foldSchemaNames {
			identityName = schema.CaseFoldKey(name)
		}
		identity := placement.Scope + "\x00" + identityName
		resolved := bySchema[identity]
		if resolved.name == "" {
			resolved.name = name
		}
		resolved.namespaces = append(resolved.namespaces, namespace)
		bySchema[identity] = resolved
	}
	return bySchema
}

// addDoubled records the tables both databases declare in the shared schema.
func (e *SharedSchemaIgnoreListsError) addDoubled(shared sharedSchema, self sharedSchemaSide, selfNamespaces []string, peer sharedSchemaSide, peerNamespaces []string) {
	for _, selfNamespace := range selfNamespaces {
		for _, peerNamespace := range peerNamespaces {
			peerFolded := foldedTableSet(peer.declared[peerNamespace])
			var doubled []string
			for _, table := range self.declared[selfNamespace] {
				if peerFolded[sharedTableKey(table)] && e.firstReport("doubled", self.Database, peer.Database, table) {
					doubled = append(doubled, table)
				}
			}
			if len(doubled) == 0 {
				continue
			}
			e.Doubled = append(e.Doubled, DoublyDeclaredTables{
				Databases:  [2]string{self.Database, peer.Database},
				Namespaces: [2]string{selfNamespace, peerNamespace},
				Schema:     shared.name,
				Target:     shared.target,
				Tables:     doubled,
			})
		}
	}
}

// addMissing records the tables declarer declares in the shared schema that
// owner does not declare there. A table owner declares in another schema is
// one ignore_tables cannot withhold. Any other counts as ignored only when the
// entries withhold it under both its declared spelling and its lower-case
// one: an engine withholds by the name the catalog holds, and a target that
// folds table names holds the table in lower case. Entries match case
// exactly, so an entry naming only the declared spelling would leave the
// folded table to be dropped.
func (e *SharedSchemaIgnoreListsError) addMissing(shared sharedSchema, declarer sharedSchemaSide, declarerNamespaces []string, owner sharedSchemaSide, ownerNamespaces []string) {
	ownerHere := map[string]bool{}
	for _, namespace := range ownerNamespaces {
		maps.Copy(ownerHere, foldedTableSet(owner.declared[namespace]))
	}
	ownerAnywhere := map[string]string{}
	for _, namespace := range slices.Sorted(maps.Keys(owner.declared)) {
		for _, table := range owner.declared[namespace] {
			if _, ok := ownerAnywhere[sharedTableKey(table)]; !ok {
				ownerAnywhere[sharedTableKey(table)] = namespace
			}
		}
	}
	ownerNamespace := ownerNamespaces[0]
	for _, declarerNamespace := range declarerNamespaces {
		var tables, entries []string
		for _, table := range declarer.declared[declarerNamespace] {
			if ownerHere[sharedTableKey(table)] {
				continue
			}
			if declaredIn, ok := ownerAnywhere[sharedTableKey(table)]; ok {
				if e.firstReport("elsewhere", owner.Database, declarer.Database, table) {
					e.DeclaredElsewhere = append(e.DeclaredElsewhere, TableDeclaredElsewhere{
						Database:        owner.Database,
						OwnerDeclaredIn: declaredIn,
						DeclaredBy:      declarer.Database,
						Namespace:       declarerNamespace,
						OwnerNamespace:  ownerNamespace,
						Schema:          shared.name,
						Target:          shared.target,
						Table:           table,
					})
				}
				continue
			}
			unwithheld := unwithheldSpellings(owner.ignored, table)
			if len(unwithheld) == 0 || !e.firstReport("missing", owner.Database, declarer.Database, table) {
				continue
			}
			tables = append(tables, table)
			entries = append(entries, unwithheld...)
		}
		if len(tables) == 0 {
			continue
		}
		e.Missing = append(e.Missing, MissingIgnoreEntry{
			Database:       owner.Database,
			ConfigPath:     owner.ConfigPath,
			DeclaredBy:     declarer.Database,
			Namespace:      declarerNamespace,
			OwnerNamespace: ownerNamespace,
			Schema:         shared.name,
			Target:         shared.target,
			Tables:         tables,
			Entries:        entries,
		})
	}
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
		return strings.Compare(a.Namespaces[0]+"\x00"+a.Namespaces[1]+"\x00"+a.Databases[1], b.Namespaces[0]+"\x00"+b.Namespaces[1]+"\x00"+b.Databases[1])
	})
	slices.SortFunc(e.DeclaredElsewhere, func(a, b TableDeclaredElsewhere) int {
		return strings.Compare(a.Database+"\x00"+a.Namespace+"\x00"+a.Table, b.Database+"\x00"+b.Namespace+"\x00"+b.Table)
	})
}

func (e *SharedSchemaIgnoreListsError) Error() string {
	var parts []string
	for _, m := range e.Missing {
		config := m.ConfigPath
		if config == "" {
			config = fmt.Sprintf("the schemabot.yaml of %q", m.Database)
		}
		part := fmt.Sprintf("%s, but %s does not ignore %s, so a plan of %q would drop %s. Add %s to ignore_tables in %s",
			sharedPlacement(m.DeclaredBy, quotedTables(m.Tables), m.Namespace, m.Database, m.OwnerNamespace, m.Schema, m.Target),
			config, pronoun(m.Tables), m.Database, pronoun(m.Tables), quotedTables(m.Entries), config)
		if !slices.Equal(m.Entries, m.Tables) {
			part += ". Entries match case exactly, and a target that folds table names stores each table in lower case, so the lower-case spelling needs an entry too"
		}
		parts = append(parts, part)
	}
	for _, d := range e.Doubled {
		var where string
		if d.Namespaces[0] == d.Namespaces[1] {
			where = fmt.Sprintf("in namespace %q on target %q", d.Namespaces[0], d.Target)
		} else {
			where = fmt.Sprintf("on target %q, in namespaces %q and %q, which the target stores in one schema %q", d.Target, d.Namespaces[0], d.Namespaces[1], d.Schema)
		}
		parts = append(parts, fmt.Sprintf(
			"%q and %q both declare %s %s. Keep each table's schema file in one of them and list the table in the other's ignore_tables",
			d.Databases[0], d.Databases[1], quotedTables(d.Tables), where))
	}
	for _, d := range e.DeclaredElsewhere {
		parts = append(parts, fmt.Sprintf(
			"%s. %q also declares %q in namespace %q, and ignore_tables applies to every namespace of a database, so no entry can keep the table out of a plan of namespace %q. Rename one of the two tables, or store the two namespaces in different schemas",
			sharedPlacement(d.DeclaredBy, fmt.Sprintf("%q", d.Table), d.Namespace, d.Database, d.OwnerNamespace, d.Schema, d.Target),
			d.Database, d.Table, d.OwnerDeclaredIn, d.OwnerNamespace))
	}
	return strings.Join(parts, "; ")
}

// sharedPlacement says where declarer's tables sit relative to owner's plan:
// in a namespace owner shares, or in a namespace of another name that the
// target stores in the same schema as owner's.
func sharedPlacement(declarer, tables, namespace, owner, ownerNamespace, schemaName, target string) string {
	if namespace == ownerNamespace {
		return fmt.Sprintf("%q declares %s in namespace %q, which %q shares on target %q", declarer, tables, namespace, owner, target)
	}
	return fmt.Sprintf("%q declares %s in namespace %q on target %q, where %q plans namespace %q, and the target stores both namespaces in schema %q",
		declarer, tables, namespace, target, owner, ownerNamespace, schemaName)
}

func foldedTableSet(tables []string) map[string]bool {
	set := make(map[string]bool, len(tables))
	for _, table := range tables {
		set[sharedTableKey(table)] = true
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

// sharedTableKey is the key under which two declarations are one table: the
// same case folding the engines use to refuse a declared table that
// ignore_tables also withholds, so the check and the plan agree.
func sharedTableKey(table string) string {
	return schema.CaseFoldKey(table)
}
