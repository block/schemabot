package api

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/routing"
	"github.com/block/schemabot/pkg/tern"
)

// sharedSchemaConfig splits one schema between two databases: shop owns the
// unsharded tables, which live in shard 0's schema, and shop_sharded owns the
// sharded tables in every shard's schema. The other databases share nothing
// with them: a different target, a different repository, a different engine,
// and the same target name behind a different deployment.
const sharedSchemaConfig = `
tern_deployments:
  us:
    production: tern-us:9090
    staging: tern-us-staging:9090
  eu:
    production: tern-eu:9090
databases:
  shop:
    type: mysql
    allowed_repos: [octo/shop]
    environments:
      production:
        deployment: us
        targets:
          - target: shop-001
            namespaces: [shop_0]
  shop_sharded:
    type: mysql
    allowed_repos: [octo/shop]
    environments:
      production:
        deployment: us
        targets:
          - target: shop-001
            namespaces: [shop_0]
          - target: shop-002
            namespaces: [shop_1]
      staging:
        deployment: us
        target: shop-staging
  billing:
    type: mysql
    environments:
      production:
        deployment: us
        target: billing-001
  catalog:
    type: mysql
    allowed_repos: [octo/catalog]
    environments:
      production:
        deployment: us
        target: shop-001
  analytics:
    type: postgres
    environments:
      production:
        deployment: us
        target: shop-001
  shop_eu:
    type: mysql
    environments:
      production:
        deployment: eu
        target: shop-001
`

func parseSharedSchemaConfig(t *testing.T) *ServerConfig {
	t.Helper()
	cfg, err := ParseServerConfig([]byte(sharedSchemaConfig))
	require.NoError(t, err)
	return cfg
}

func createTables(tables ...string) *ternv1.SchemaFiles {
	files := map[string]string{}
	for _, table := range tables {
		files[table+".sql"] = "CREATE TABLE `" + table + "` (`id` bigint NOT NULL, PRIMARY KEY (`id`));"
	}
	return &ternv1.SchemaFiles{Files: files}
}

func shopSide(ignore []string, tables ...string) SharedSchemaDatabase {
	return SharedSchemaDatabase{
		Database:     "shop",
		ConfigPath:   "db/shop/schemabot.yaml",
		SchemaFiles:  map[string]*ternv1.SchemaFiles{"shop_0": createTables(tables...)},
		IgnoreTables: ignore,
	}
}

func shopShardedSide(ignore []string, tables ...string) SharedSchemaDatabase {
	return SharedSchemaDatabase{
		Database:   "shop_sharded",
		ConfigPath: "db/shop_sharded/schemabot.yaml",
		SchemaFiles: map[string]*ternv1.SchemaFiles{
			"shop_0": createTables(tables...),
			"shop_1": createTables(tables...),
		},
		IgnoreTables: ignore,
	}
}

// Two databases share a schema when their targets in the environment include
// the same rollout member. A target of the same name behind another
// deployment, a database of another engine, one that accepts no changes from
// the repository, and one that does not configure the environment share
// nothing.
func TestSharedSchemaPeers(t *testing.T) {
	cfg := parseSharedSchemaConfig(t)

	peers, err := cfg.SharedSchemaPeers("octo/shop", "shop", "production")
	require.NoError(t, err)
	assert.Equal(t, []string{"shop_sharded"}, peers)

	peers, err = cfg.SharedSchemaPeers("octo/shop", "shop_sharded", "production")
	require.NoError(t, err)
	assert.Equal(t, []string{"shop"}, peers)

	peers, err = cfg.SharedSchemaPeers("octo/shop", "shop_sharded", "staging")
	require.NoError(t, err)
	assert.Empty(t, peers, "shop does not configure staging")

	peers, err = cfg.SharedSchemaPeers("octo/shop", "billing", "production")
	require.NoError(t, err)
	assert.Empty(t, peers, "billing's target is its own")

	peers, err = cfg.SharedSchemaPeers("octo/catalog", "catalog", "production")
	require.NoError(t, err)
	assert.Empty(t, peers, "shop and shop_sharded accept no changes from octo/catalog")

	peers, err = cfg.SharedSchemaPeers("octo/shop", "shop_eu", "production")
	require.NoError(t, err)
	assert.Empty(t, peers, "shop_eu's shop-001 is behind another deployment, so it is another member")

	peers, err = cfg.SharedSchemaPeers("octo/shop", "analytics", "production")
	require.NoError(t, err)
	assert.Empty(t, peers, "a database of another engine shares no schema")
}

// A pair whose ignore lists name exactly each other's tables passes, and so
// does one whose entries withhold the other's tables by pattern.
func TestCheckSharedSchemaIgnoreLists_AgreeingListsPass(t *testing.T) {
	cfg := parseSharedSchemaConfig(t)

	shop := shopSide([]string{"orders", "order_items"}, "sequences")
	sharded := shopShardedSide([]string{"sequences"}, "orders", "order_items")
	require.NoError(t, cfg.CheckSharedSchemaIgnoreLists(t.Context(), "production", shop, []SharedSchemaDatabase{sharded}, &targetSchemas{}))
	require.NoError(t, cfg.CheckSharedSchemaIgnoreLists(t.Context(), "production", sharded, []SharedSchemaDatabase{shop}, &targetSchemas{}))

	byPattern := shopSide([]string{"/order.*/"}, "sequences")
	require.NoError(t, cfg.CheckSharedSchemaIgnoreLists(t.Context(), "production", byPattern, []SharedSchemaDatabase{sharded}, &targetSchemas{}))
}

// A pull request adds a sharded table without listing it in shop's
// ignore_tables. Checked from either database, the refusal names the table,
// the database that needs the entry, and the file to add it to: left alone, a
// later plan of shop proposes dropping the table from shard 0.
func TestCheckSharedSchemaIgnoreLists_MissingEntryIsRefusedFromEitherSide(t *testing.T) {
	cfg := parseSharedSchemaConfig(t)

	shop := shopSide([]string{"orders"}, "sequences")
	sharded := shopShardedSide([]string{"sequences"}, "orders", "refunds")

	for name, check := range map[string]func() error{
		"planning shop": func() error {
			return cfg.CheckSharedSchemaIgnoreLists(t.Context(), "production", shop, []SharedSchemaDatabase{sharded}, &targetSchemas{})
		},
		"planning shop_sharded": func() error {
			return cfg.CheckSharedSchemaIgnoreLists(t.Context(), "production", sharded, []SharedSchemaDatabase{shop}, &targetSchemas{})
		},
	} {
		t.Run(name, func(t *testing.T) {
			err := check()
			var mismatch *SharedSchemaIgnoreListsError
			require.True(t, errors.As(err, &mismatch), "want a SharedSchemaIgnoreListsError, got %v", err)
			assert.Equal(t, []MissingIgnoreEntry{{
				Database:       "shop",
				ConfigPath:     "db/shop/schemabot.yaml",
				DeclaredBy:     "shop_sharded",
				Namespace:      "shop_0",
				OwnerNamespace: "shop_0",
				Schema:         "shop_0",
				Target:         "shop-001",
				Tables:         []string{"refunds"},
				Entries:        []string{"refunds"},
			}}, mismatch.Missing)
			assert.Empty(t, mismatch.Doubled)
			assert.Equal(t,
				`"shop_sharded" declares "refunds" in namespace "shop_0", which "shop" shares on target "shop-001", but db/shop/schemabot.yaml does not ignore it, so a plan of "shop" would drop it. Add "refunds" to ignore_tables in db/shop/schemabot.yaml`,
				err.Error())
		})
	}
}

// Missing entries on both sides are each named, with their own file.
func TestCheckSharedSchemaIgnoreLists_MissingEntriesOnBothSides(t *testing.T) {
	cfg := parseSharedSchemaConfig(t)

	shop := shopSide([]string{"orders"}, "sequences", "settings")
	sharded := shopShardedSide(nil, "orders", "refunds")

	err := cfg.CheckSharedSchemaIgnoreLists(t.Context(), "production", shop, []SharedSchemaDatabase{sharded}, &targetSchemas{})
	var mismatch *SharedSchemaIgnoreListsError
	require.True(t, errors.As(err, &mismatch), "want a SharedSchemaIgnoreListsError, got %v", err)
	require.Len(t, mismatch.Missing, 2)
	assert.Equal(t, "shop", mismatch.Missing[0].Database)
	assert.Equal(t, []string{"refunds"}, mismatch.Missing[0].Tables)
	assert.Equal(t, "shop_sharded", mismatch.Missing[1].Database)
	assert.Equal(t, "db/shop_sharded/schemabot.yaml", mismatch.Missing[1].ConfigPath)
	assert.Equal(t, []string{"sequences", "settings"}, mismatch.Missing[1].Tables)
	assert.Contains(t, err.Error(), `Add "sequences", "settings" to ignore_tables in db/shop_sharded/schemabot.yaml`)
}

// A table both databases declare in the shared namespace has two owners whose
// plans can undo each other's changes, whatever the ignore lists say. The
// match ignores case, so a second spelling of the same table is caught too.
func TestCheckSharedSchemaIgnoreLists_TableDeclaredByBoth(t *testing.T) {
	cfg := parseSharedSchemaConfig(t)

	shop := shopSide([]string{"orders"}, "sequences", "Orders")
	sharded := shopShardedSide([]string{"sequences"}, "orders")

	err := cfg.CheckSharedSchemaIgnoreLists(t.Context(), "production", shop, []SharedSchemaDatabase{sharded}, &targetSchemas{})
	var mismatch *SharedSchemaIgnoreListsError
	require.True(t, errors.As(err, &mismatch), "want a SharedSchemaIgnoreListsError, got %v", err)
	assert.Empty(t, mismatch.Missing)
	assert.Equal(t, []DoublyDeclaredTables{{
		Databases:  [2]string{"shop", "shop_sharded"},
		Namespaces: [2]string{"shop_0", "shop_0"},
		Schema:     "shop_0",
		Target:     "shop-001",
		Tables:     []string{"Orders"},
	}}, mismatch.Doubled)
	assert.Contains(t, err.Error(), `"shop" and "shop_sharded" both declare "Orders" in namespace "shop_0" on target "shop-001"`)
}

// Only namespaces both databases hold on a shared member are compared. shop
// holds shard 0 alone, so shard 1's tables, which only shop_sharded holds,
// need no entry in shop's ignore_tables.
func TestCheckSharedSchemaIgnoreLists_UnsharedNamespacesAreNotCompared(t *testing.T) {
	cfg := parseSharedSchemaConfig(t)

	shop := shopSide([]string{"orders"}, "sequences")
	sharded := shopShardedSide([]string{"sequences"}, "orders")
	sharded.SchemaFiles["shop_1"] = createTables("orders", "shard_one_only")

	require.NoError(t, cfg.CheckSharedSchemaIgnoreLists(t.Context(), "production", shop, []SharedSchemaDatabase{sharded}, &targetSchemas{}))
}

// A schema file the parser cannot read fails the check instead of being
// skipped, since a skipped file declares nothing and would pass the pair.
func TestCheckSharedSchemaIgnoreLists_UnparseableFileFails(t *testing.T) {
	cfg := parseSharedSchemaConfig(t)

	shop := shopSide(nil, "sequences")
	sharded := shopShardedSide([]string{"sequences"}, "orders")
	sharded.SchemaFiles["shop_0"].Files["broken.sql"] = "CREATE TABLE ("

	err := cfg.CheckSharedSchemaIgnoreLists(t.Context(), "production", shop, []SharedSchemaDatabase{sharded}, &targetSchemas{})
	require.Error(t, err)
	var mismatch *SharedSchemaIgnoreListsError
	assert.False(t, errors.As(err, &mismatch), "a parse failure is not a list mismatch: %v", err)
	assert.Contains(t, err.Error(), `schema files of database "shop_sharded"`)
	assert.Contains(t, err.Error(), "shop_0/broken.sql")
}

// sameNameEverywhereConfig gives every cluster the same schema name. The
// unsharded tables and shard 0 share ledger_production on ledger-001, and
// the shards on ledger-002 and ledger-003 hold only sharded tables. A third
// table set lives on ledger-004 under the same schema name. None of the
// targets select namespaces, so each holds every namespace its files declare.
const sameNameEverywhereConfig = `
tern_deployments:
  us:
    production: tern-us:9090
databases:
  ledger:
    type: mysql
    environments:
      production:
        deployment: us
        target: ledger-001
  ledger_sharded:
    type: mysql
    environments:
      production:
        deployment: us
        targets: [ledger-002, ledger-003, ledger-001]
  ledger_feed:
    type: mysql
    environments:
      production:
        deployment: us
        target: ledger-004
`

func ledgerSide(database string, ignore []string, tables ...string) SharedSchemaDatabase {
	return SharedSchemaDatabase{
		Database:     database,
		ConfigPath:   "db/" + database + "/schemabot.yaml",
		SchemaFiles:  map[string]*ternv1.SchemaFiles{"ledger_production": createTables(tables...)},
		IgnoreTables: ignore,
	}
}

// When one schema name is used on every cluster, the cluster decides which
// databases share it. The unsharded and sharded databases share it on
// ledger-001, where the check names the missing entry. The third table set
// uses the same schema name on a cluster of its own, so it shares nothing and
// needs no entries.
func TestCheckSharedSchemaIgnoreLists_SameSchemaNameOnEveryCluster(t *testing.T) {
	cfg, err := ParseServerConfig([]byte(sameNameEverywhereConfig))
	require.NoError(t, err)

	peers, err := cfg.SharedSchemaPeers("octo/ledger", "ledger", "production")
	require.NoError(t, err)
	assert.Equal(t, []string{"ledger_sharded"}, peers)
	peers, err = cfg.SharedSchemaPeers("octo/ledger", "ledger_feed", "production")
	require.NoError(t, err)
	assert.Empty(t, peers, "ledger_feed's cluster holds no other database's tables")

	ledger := ledgerSide("ledger", []string{"entries"}, "accounts")
	sharded := ledgerSide("ledger_sharded", []string{"accounts"}, "entries", "holds")

	err = cfg.CheckSharedSchemaIgnoreLists(t.Context(), "production", ledger, []SharedSchemaDatabase{sharded}, &targetSchemas{})
	var mismatch *SharedSchemaIgnoreListsError
	require.True(t, errors.As(err, &mismatch), "want a SharedSchemaIgnoreListsError, got %v", err)
	assert.Equal(t, []MissingIgnoreEntry{{
		Database:       "ledger",
		ConfigPath:     "db/ledger/schemabot.yaml",
		DeclaredBy:     "ledger_sharded",
		Namespace:      "ledger_production",
		OwnerNamespace: "ledger_production",
		Schema:         "ledger_production",
		Target:         "ledger-001",
		Tables:         []string{"holds"},
		Entries:        []string{"holds"},
	}}, mismatch.Missing)

	sharded.IgnoreTables = []string{"accounts"}
	ledger.IgnoreTables = []string{"entries", "holds"}
	require.NoError(t, cfg.CheckSharedSchemaIgnoreLists(t.Context(), "production", ledger, []SharedSchemaDatabase{sharded}, &targetSchemas{}))
}

// A target that folds table names stores a table declared as Refunds under
// refunds, and the engine withholds by that name. Entries match case
// exactly, so an entry naming only Refunds leaves the stored table to be
// dropped. The check asks for the lower-case spelling as well, and passes
// once an entry covers both spellings.
func TestCheckSharedSchemaIgnoreLists_EntryMustCoverTheLowerCaseSpelling(t *testing.T) {
	cfg := parseSharedSchemaConfig(t)
	sharded := shopShardedSide([]string{"sequences"}, "orders", "Refunds")

	for name, tc := range map[string]struct {
		ignore  []string
		entries []string
	}{
		"no entry":                 {ignore: []string{"orders"}, entries: []string{"Refunds", "refunds"}},
		"declared spelling only":   {ignore: []string{"orders", "Refunds"}, entries: []string{"refunds"}},
		"case-sensitive pattern":   {ignore: []string{"orders", "/Ref.*/"}, entries: []string{"refunds"}},
		"lower-case spelling only": {ignore: []string{"orders", "refunds"}, entries: []string{"Refunds"}},
	} {
		t.Run(name, func(t *testing.T) {
			err := cfg.CheckSharedSchemaIgnoreLists(t.Context(), "production", shopSide(tc.ignore, "sequences"), []SharedSchemaDatabase{sharded}, &targetSchemas{})
			var mismatch *SharedSchemaIgnoreListsError
			require.True(t, errors.As(err, &mismatch), "want a SharedSchemaIgnoreListsError, got %v", err)
			require.Len(t, mismatch.Missing, 1)
			assert.Equal(t, []string{"Refunds"}, mismatch.Missing[0].Tables)
			assert.Equal(t, tc.entries, mismatch.Missing[0].Entries)
			assert.Contains(t, err.Error(), "Add "+quotedTables(tc.entries)+" to ignore_tables in db/shop/schemabot.yaml")
		})
	}

	for name, ignore := range map[string][]string{
		"both spellings":           {"orders", "Refunds", "refunds"},
		"case-insensitive pattern": {"orders", "/(?i)refunds/"},
	} {
		t.Run(name, func(t *testing.T) {
			require.NoError(t, cfg.CheckSharedSchemaIgnoreLists(t.Context(), "production", shopSide(ignore, "sequences"), []SharedSchemaDatabase{sharded}, &targetSchemas{}))
		})
	}
}

// sharedTargetConfig puts two databases of each engine on one target, each
// under its own namespace key.
const sharedTargetConfig = `
tern_deployments:
  us:
    production: tern-us:9090
databases:
  shop:
    type: mysql
    environments:
      production:
        deployment: us
        target: shop-001
  shop_sharded:
    type: mysql
    environments:
      production:
        deployment: us
        target: shop-001
  catalog:
    type: postgres
    environments:
      production:
        deployment: us
        target: catalog-001
  catalog_search:
    type: postgres
    environments:
      production:
        deployment: us
        target: catalog-001
`

func postgresCreateTables(tables ...string) *ternv1.SchemaFiles {
	files := map[string]string{}
	for _, table := range tables {
		files[table+".sql"] = "CREATE TABLE " + table + " (id bigint PRIMARY KEY);"
	}
	return &ternv1.SchemaFiles{Files: files}
}

func namespacedSide(database, namespace string, ignore []string, tables ...string) SharedSchemaDatabase {
	return SharedSchemaDatabase{
		Database:     database,
		ConfigPath:   "db/" + database + "/schemabot.yaml",
		SchemaFiles:  map[string]*ternv1.SchemaFiles{namespace: createTables(tables...)},
		IgnoreTables: ignore,
	}
}

// targetSchemas stands in for the data planes. A member listed in pinned has
// a connection that names one database, which every namespace sent to it
// addresses. Any other namespace addresses a schema of its own name.
type targetSchemas struct {
	pinned map[string]string
	err    error
	// omit names a namespace the resolution leaves out of its answer.
	omit  string
	calls []string
}

func (r *targetSchemas) ResolveNamespaces(_ context.Context, database, _ string, target routing.ExecutionTarget, namespaces []string) (map[string]string, error) {
	r.calls = append(r.calls, fmt.Sprintf("%s@%s:%v", database, target.MemberID(), namespaces))
	if r.err != nil {
		return nil, r.err
	}
	schemas := make(map[string]string, len(namespaces))
	for _, namespace := range namespaces {
		switch {
		case namespace == r.omit:
		case r.pinned[target.MemberID()] != "":
			schemas[namespace] = r.pinned[target.MemberID()]
		default:
			schemas[namespace] = namespace
		}
	}
	return schemas, nil
}

func pinnedTo(schemaName string) *targetSchemas {
	return &targetSchemas{pinned: map[string]string{"us/shop-001": schemaName}}
}

func requireListsMismatch(t *testing.T, err error) *SharedSchemaIgnoreListsError {
	t.Helper()
	var mismatch *SharedSchemaIgnoreListsError
	require.True(t, errors.As(err, &mismatch), "want a SharedSchemaIgnoreListsError, got %v", err)
	return mismatch
}

// Two MySQL databases on one target keep their tables under different
// namespace keys, shop and shop_sharded. Whether those keys are one schema is
// the target's answer: a connection that names a database stores both in it,
// and one that names none stores each in a schema of its own name. The check
// asks the data plane and compares by the answer. On the pinned target a
// table both declare has two owners and a table only one declares needs an
// entry in the other's ignore_tables. On the unpinned target the same files
// are two separate sets of tables and pass as they are.
func TestCheckSharedSchemaIgnoreLists_ComparesTheSchemaTheTargetResolves(t *testing.T) {
	cfg, err := ParseServerConfig([]byte(sharedTargetConfig))
	require.NoError(t, err)

	t.Run("a pinned schema refuses a table both declare under different keys", func(t *testing.T) {
		shop := namespacedSide("shop", "shop", nil, "users")
		sharded := namespacedSide("shop_sharded", "shop_sharded", nil, "users")
		resolver := pinnedTo("shop_main")

		err := cfg.CheckSharedSchemaIgnoreLists(t.Context(), "production", shop, []SharedSchemaDatabase{sharded}, resolver)
		mismatch := requireListsMismatch(t, err)
		assert.Empty(t, mismatch.Missing)
		assert.Equal(t, []DoublyDeclaredTables{{
			Databases:  [2]string{"shop", "shop_sharded"},
			Namespaces: [2]string{"shop", "shop_sharded"},
			Schema:     "shop_main",
			Target:     "shop-001",
			Tables:     []string{"users"},
		}}, mismatch.Doubled)
		assert.Equal(t,
			`"shop" and "shop_sharded" both declare "users" on target "shop-001", in namespaces "shop" and "shop_sharded", which the target stores in one schema "shop_main". Keep each table's schema file in one of them and list the table in the other's ignore_tables`,
			err.Error())
		assert.Equal(t, []string{"shop@us/shop-001:[shop]", "shop_sharded@us/shop-001:[shop_sharded]"}, resolver.calls,
			"each database's namespaces are resolved once, on its own target")
	})

	t.Run("an unpinned target allows a table each declares under its own key", func(t *testing.T) {
		shop := namespacedSide("shop", "shop", nil, "users")
		sharded := namespacedSide("shop_sharded", "shop_sharded", nil, "users")

		require.NoError(t, cfg.CheckSharedSchemaIgnoreLists(t.Context(), "production", shop, []SharedSchemaDatabase{sharded}, &targetSchemas{}))
	})

	t.Run("a pinned schema refuses missing entries under different keys", func(t *testing.T) {
		shop := namespacedSide("shop", "shop", nil, "settings")
		sharded := namespacedSide("shop_sharded", "shop_sharded", nil, "orders")

		err := cfg.CheckSharedSchemaIgnoreLists(t.Context(), "production", shop, []SharedSchemaDatabase{sharded}, pinnedTo("shop_main"))
		mismatch := requireListsMismatch(t, err)
		assert.Equal(t, []MissingIgnoreEntry{
			{
				Database:       "shop",
				ConfigPath:     "db/shop/schemabot.yaml",
				DeclaredBy:     "shop_sharded",
				Namespace:      "shop_sharded",
				OwnerNamespace: "shop",
				Schema:         "shop_main",
				Target:         "shop-001",
				Tables:         []string{"orders"},
				Entries:        []string{"orders"},
			},
			{
				Database:       "shop_sharded",
				ConfigPath:     "db/shop_sharded/schemabot.yaml",
				DeclaredBy:     "shop",
				Namespace:      "shop",
				OwnerNamespace: "shop_sharded",
				Schema:         "shop_main",
				Target:         "shop-001",
				Tables:         []string{"settings"},
				Entries:        []string{"settings"},
			},
		}, mismatch.Missing)
		assert.Contains(t, err.Error(),
			`"shop_sharded" declares "orders" in namespace "shop_sharded" on target "shop-001", where "shop" plans namespace "shop", and the target stores both namespaces in schema "shop_main", but db/shop/schemabot.yaml does not ignore it, so a plan of "shop" would drop it. Add "orders" to ignore_tables in db/shop/schemabot.yaml`)

		shop.IgnoreTables = []string{"orders"}
		sharded.IgnoreTables = []string{"settings"}
		require.NoError(t, cfg.CheckSharedSchemaIgnoreLists(t.Context(), "production", shop, []SharedSchemaDatabase{sharded}, pinnedTo("shop_main")))
	})

	t.Run("an unpinned target needs no entries across different keys", func(t *testing.T) {
		shop := namespacedSide("shop", "shop", nil, "settings")
		sharded := namespacedSide("shop_sharded", "shop_sharded", nil, "orders")

		require.NoError(t, cfg.CheckSharedSchemaIgnoreLists(t.Context(), "production", shop, []SharedSchemaDatabase{sharded}, &targetSchemas{}))
	})

	t.Run("postgres schemas under different keys share nothing", func(t *testing.T) {
		catalog := namespacedSide("catalog", "catalog", nil)
		catalog.SchemaFiles["catalog"] = postgresCreateTables("products")
		search := namespacedSide("catalog_search", "search", nil)
		search.SchemaFiles["search"] = postgresCreateTables("products", "terms")

		require.NoError(t, cfg.CheckSharedSchemaIgnoreLists(t.Context(), "production", catalog, []SharedSchemaDatabase{search}, &targetSchemas{}))
	})
}

// shop declares users in its north namespace and settings in south, and
// shop_sharded declares its own users in east, all on one target. When the
// target stores each namespace in a schema of its own name, the three are
// separate and the pair passes: shop is never asked to ignore users, an entry
// its own north declaration would refuse. When the target's connection names
// one database, all three are that database, so users has two owners and
// shop_sharded's plan would drop settings.
func TestCheckSharedSchemaIgnoreLists_NamespacesThatMayOrMayNotBeOneSchema(t *testing.T) {
	cfg, err := ParseServerConfig([]byte(sharedTargetConfig))
	require.NoError(t, err)
	shop := SharedSchemaDatabase{
		Database:   "shop",
		ConfigPath: "db/shop/schemabot.yaml",
		SchemaFiles: map[string]*ternv1.SchemaFiles{
			"north": createTables("users"),
			"south": createTables("settings"),
		},
	}
	sharded := namespacedSide("shop_sharded", "east", nil, "users")

	t.Run("unpinned", func(t *testing.T) {
		require.NoError(t, cfg.CheckSharedSchemaIgnoreLists(t.Context(), "production", shop, []SharedSchemaDatabase{sharded}, &targetSchemas{}))
		require.NoError(t, cfg.CheckSharedSchemaIgnoreLists(t.Context(), "production", sharded, []SharedSchemaDatabase{shop}, &targetSchemas{}))
	})

	t.Run("pinned", func(t *testing.T) {
		err := cfg.CheckSharedSchemaIgnoreLists(t.Context(), "production", shop, []SharedSchemaDatabase{sharded}, pinnedTo("shop_main"))
		mismatch := requireListsMismatch(t, err)
		assert.Equal(t, []DoublyDeclaredTables{{
			Databases:  [2]string{"shop", "shop_sharded"},
			Namespaces: [2]string{"north", "east"},
			Schema:     "shop_main",
			Target:     "shop-001",
			Tables:     []string{"users"},
		}}, mismatch.Doubled)
		require.Len(t, mismatch.Missing, 1)
		assert.Equal(t, "shop_sharded", mismatch.Missing[0].Database)
		assert.Equal(t, "south", mismatch.Missing[0].Namespace)
		assert.Equal(t, []string{"settings"}, mismatch.Missing[0].Tables)
		assert.Empty(t, mismatch.DeclaredElsewhere)
	})
}

// shop and shop_sharded both keep tables in namespace shop on one target,
// which stores it in a schema of that name, and shop also has an archive
// namespace there, stored in a schema of its own. shop_sharded declares users
// in shop, and shop declares a users table of its own in the archive. A plan
// of shop's namespace shop would drop shop_sharded's users, and no
// ignore_tables entry can stop it: an entry naming users would also withhold
// the archive's users, which the planner refuses. The check says so and names
// the remedy instead of asking for an entry the planner would reject.
func TestCheckSharedSchemaIgnoreLists_TableDeclaredInAnotherSchemaCannotBeIgnored(t *testing.T) {
	cfg, err := ParseServerConfig([]byte(sharedTargetConfig))
	require.NoError(t, err)
	shop := SharedSchemaDatabase{
		Database:   "shop",
		ConfigPath: "db/shop/schemabot.yaml",
		SchemaFiles: map[string]*ternv1.SchemaFiles{
			"shop":    createTables("settings"),
			"archive": createTables("users"),
		},
	}
	sharded := namespacedSide("shop_sharded", "shop", []string{"settings"}, "users")

	err = cfg.CheckSharedSchemaIgnoreLists(t.Context(), "production", shop, []SharedSchemaDatabase{sharded}, &targetSchemas{})
	mismatch := requireListsMismatch(t, err)
	assert.Empty(t, mismatch.Missing)
	assert.Empty(t, mismatch.Doubled)
	assert.Equal(t, []TableDeclaredElsewhere{{
		Database:        "shop",
		OwnerDeclaredIn: "archive",
		DeclaredBy:      "shop_sharded",
		Namespace:       "shop",
		OwnerNamespace:  "shop",
		Schema:          "shop",
		Target:          "shop-001",
		Table:           "users",
	}}, mismatch.DeclaredElsewhere)
	assert.Equal(t,
		`"shop_sharded" declares "users" in namespace "shop", which "shop" shares on target "shop-001". "shop" also declares "users" in namespace "archive", and ignore_tables applies to every namespace of a database, so no entry can keep the table out of a plan of namespace "shop". Rename one of the two tables, or store the two namespaces in different schemas`,
		err.Error())
}

// The check cannot compare a pair without knowing which schemas their
// namespaces are, so a resolution that fails, including the refusal of a data
// plane too old to answer, and one that leaves a namespace out, fail the
// check. Neither is a list mismatch: the operator fixes the data plane, not
// the schema files.
func TestCheckSharedSchemaIgnoreLists_UnresolvedNamespacesFailClosed(t *testing.T) {
	cfg, err := ParseServerConfig([]byte(sharedTargetConfig))
	require.NoError(t, err)
	shop := namespacedSide("shop", "shop", []string{"orders"}, "settings")
	sharded := namespacedSide("shop_sharded", "shop_sharded", []string{"settings"}, "orders")

	for name, tc := range map[string]struct {
		resolver *targetSchemas
		database string
		want     string
		tooOld   bool
	}{
		"a data plane too old to answer": {
			resolver: &targetSchemas{err: fmt.Errorf("upgrade it: %w", tern.ErrResolveNamespacesUnsupported)},
			database: "shop",
			want:     `resolve the schemas that namespaces [shop] of database "shop" address on target "shop-001" of deployment "us" in environment "production": upgrade it: namespace resolution unsupported`,
			tooOld:   true,
		},
		"a resolution that fails": {
			resolver: &targetSchemas{err: errors.New("connection refused")},
			database: "shop",
			want:     `resolve the schemas that namespaces [shop] of database "shop" address on target "shop-001" of deployment "us" in environment "production": connection refused`,
		},
		"a namespace left unresolved": {
			resolver: &targetSchemas{omit: "shop_sharded"},
			database: "shop_sharded",
			want:     `resolve the schemas that namespaces [shop_sharded] of database "shop_sharded" address on target "shop-001" of deployment "us" in environment "production": data plane resolved no schema for namespace "shop_sharded"`,
		},
	} {
		t.Run(name, func(t *testing.T) {
			err := cfg.CheckSharedSchemaIgnoreLists(t.Context(), "production", shop, []SharedSchemaDatabase{sharded}, tc.resolver)
			require.EqualError(t, err, tc.want)
			var mismatch *SharedSchemaIgnoreListsError
			assert.False(t, errors.As(err, &mismatch), "an unresolved namespace is not a list mismatch: %v", err)
			resolution, ok := errors.AsType[*NamespaceResolutionError](err)
			require.True(t, ok, "want a NamespaceResolutionError, got %v", err)
			assert.Equal(t, tc.database, resolution.Database)
			assert.Equal(t, "us", resolution.Deployment)
			assert.Equal(t, "shop-001", resolution.Target)
			assert.Equal(t, tc.tooOld, resolution.DataPlaneTooOld())
		})
	}

	t.Run("no resolver", func(t *testing.T) {
		err := cfg.CheckSharedSchemaIgnoreLists(t.Context(), "production", shop, []SharedSchemaDatabase{sharded}, nil)
		require.ErrorContains(t, err, `namespace resolver is required to compare database "shop"`)
	})
}
