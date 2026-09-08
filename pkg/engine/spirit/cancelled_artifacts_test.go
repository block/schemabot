package spirit

import (
	"strings"
	"testing"

	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
)

func TestEngine_ReleaseCancelledArtifacts_RejectsIncompleteRequests(t *testing.T) {
	tests := []struct {
		name    string
		req     *engine.ReleaseArtifactsRequest
		wantErr string
	}{
		{
			name:    "nil request",
			req:     nil,
			wantErr: "release artifacts request is required",
		},
		{
			name:    "no credentials",
			req:     &engine.ReleaseArtifactsRequest{Database: "shop"},
			wantErr: "DSN credentials required",
		},
		{
			name:    "empty DSN",
			req:     &engine.ReleaseArtifactsRequest{Database: "shop", Credentials: &engine.Credentials{}},
			wantErr: "DSN credentials required",
		},
		{
			name: "no database in the DSN or the request",
			req: &engine.ReleaseArtifactsRequest{
				Credentials: &engine.Credentials{DSN: "user:pass@tcp(127.0.0.1:3306)/"},
			},
			wantErr: "database is required",
		},
		{
			// An empty table list is refused rather than run as the no-op it
			// resembles. Nothing derives from no tables, so the only artifacts
			// left in scope are the schema-scoped pair, and a release that
			// reached them would be deciding the fate of the schema's shared
			// checkpoint and deferred cutover gate on behalf of no named table.
			name: "no tables",
			req: &engine.ReleaseArtifactsRequest{
				Database:    "shop",
				Credentials: &engine.Credentials{DSN: "user:pass@tcp(127.0.0.1:3306)/shop"},
			},
			wantErr: "tables are required",
		},
	}

	eng := New(Config{})
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := eng.ReleaseCancelledArtifacts(t.Context(), tt.req)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
			assert.Nil(t, result)
		})
	}
}

// The artifact names must come from Spirit's own helpers so they match what a
// run actually created, including the deterministic truncation Spirit applies
// to keep a name within MySQL's identifier limit.
func TestArtifactNames_MatchSpiritNaming(t *testing.T) {
	longTable := strings.Repeat("a", 70)
	tables := []string{"orders", longTable}

	data := dataBearingArtifacts(tables)
	assert.Equal(t, []string{
		utils.NewTableName("orders"),
		utils.OldTableName("orders"),
		utils.NewTableName(longTable),
		utils.OldTableName(longTable),
	}, data)

	metadata := perTableMetadataArtifacts(tables)
	assert.Equal(t, []string{
		utils.CheckpointTableName("orders"),
		utils.CheckpointTableName(longTable),
	}, metadata)

	for _, name := range append(data, metadata...) {
		assert.LessOrEqual(t, len(name), utils.MaxTableNameLength,
			"artifact name %q must fit MySQL's identifier limit", name)
	}
}

// The split between the two artifact classes is what the guard is built on: an
// artifact derived from a table the request names belongs to the cancelled
// schema change and nothing else, while the schema-scoped pair belongs to
// whichever schema change in the schema owns it. Every name must sit on exactly
// one side of that line, and `isSchemaScopedArtifact` must agree with the list,
// or a shared artifact reaches the reclaim path as if it were a derived one.
func TestArtifactClasses_SeparateDerivedFromSchemaScoped(t *testing.T) {
	assert.Empty(t, dataBearingArtifacts(nil))
	assert.Empty(t, perTableMetadataArtifacts(nil))

	assert.Equal(t,
		[]string{sharedCheckpointTable, deferredCutoverSentinelTable},
		schemaScopedArtifacts())
	for _, name := range schemaScopedArtifacts() {
		assert.True(t, isSchemaScopedArtifact(name),
			"schema-scoped artifact %q must be recognised as one", name)
	}

	for _, table := range []string{"orders", "_orders", strings.Repeat("a", 70)} {
		derived := append(dataBearingArtifacts([]string{table}), perTableMetadataArtifacts([]string{table})...)
		for _, name := range derived {
			assert.False(t, isSchemaScopedArtifact(name),
				"artifact %q derived from %q must not be treated as schema-scoped", name, table)
		}
	}
}

// The shapes the release guard recognises must be the shapes Spirit's naming
// actually produces. Restating them is what lets the guard drift: a suffix
// Spirit renamed would leave the guard refusing every artifact carrying it,
// turning a release into a failure rather than a wrong drop — safe, but broken.
func TestArtifactNameShapes_MatchSpiritNaming(t *testing.T) {
	const table = "orders"

	assert.Equal(t, artifactPrefix+table+shadowTableSuffix, utils.NewTableName(table))
	assert.Equal(t, artifactPrefix+table+cutoverOriginalSuffix, utils.OldTableName(table))
	assert.Equal(t, artifactPrefix+table+perTableCheckpointSuffix, utils.CheckpointTableName(table))
}

// Nothing reaches a DROP or a RENAME unless it is shaped like an artifact name.
// The guard stands between a name and the DDL, so a name that arrives there
// without having been derived — a table name that skipped the derivation, an
// operator-supplied string, a future path into this file — is refused rather
// than executed. Truncation cannot defeat it: Spirit reserves room for the
// suffix, so a derived name carries the shape at any table-name length.
func TestVerifyReleasableArtifacts_RefusesAnythingNotAnArtifactName(t *testing.T) {
	allowed := []string{
		sharedCheckpointTable,
		deferredCutoverSentinelTable,
	}
	for _, table := range []string{"orders", "_orders", strings.Repeat("a", 200)} {
		allowed = append(allowed, dataBearingArtifacts([]string{table})...)
		allowed = append(allowed, utils.CheckpointTableName(table))
	}
	assert.NoError(t, verifyReleasableArtifacts("shop", allowed),
		"every name a release derives must pass its own guard")

	refused := []string{
		"orders",                  // the live table itself
		"orders_new",              // an artifact's suffix without its prefix
		"_orders",                 // an artifact's prefix without its suffix
		"_spirit_sentinel_backup", // a name that only starts like a schema-level table
		"users",
		"*",
		"",
	}
	for _, name := range refused {
		err := verifyReleasableArtifacts("shop", []string{name})
		require.Error(t, err, "release must refuse %q", name)
		assert.Contains(t, err.Error(), "shop."+name,
			"the refusal must name what it refused so an operator can see it")
	}

	// One bad name refuses the whole set: a release is checked before its first
	// statement, so it cannot drop half its names and then discover the rest.
	require.Error(t, verifyReleasableArtifacts("shop",
		append(append([]string{}, allowed...), "orders")))
}

// Every name a release reclaims is derived from a table name, never passed
// through, and no derivation returns the table it was given. That is what keeps
// a release off the live table it was asked about, whatever that table is
// called — including names that already look like artifacts.
//
// It does not put a release beyond reach of some *other* table: Spirit's naming
// truncates, so two long table names sharing a prefix derive the same artifact
// names. Nothing in a name records which table produced it, so that case is
// covered by the caller's precondition — no schema change live in the schema —
// rather than by anything here.
func TestArtifactNames_NeverNameTheTableTheyDeriveFrom(t *testing.T) {
	tables := []string{
		"orders",
		"_orders",
		"_orders_new",
		strings.Repeat("a", 70),
	}

	// Only the derived names are checked. The two schema-level names are
	// constants, not derivations, and they are Spirit's own — a schema change
	// never targets a table called either.
	for _, table := range tables {
		artifacts := append(dataBearingArtifacts([]string{table}), utils.CheckpointTableName(table))
		for _, name := range artifacts {
			assert.NotEqual(t, table, name,
				"artifact name derived from %q is that same table", table)
			assert.True(t, strings.HasPrefix(name, "_"),
				"artifact name %q must carry the leading underscore that marks it as derived", name)
		}
	}
}
