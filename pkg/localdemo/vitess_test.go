package localdemo

import (
	"encoding/json"
	"sort"
	"strings"
	"testing"

	"github.com/block/schemabot/pkg/ddl"
	"github.com/stretchr/testify/require"
)

// A released CLI provisions the sample from the image published for its own
// release, so the PlanetScale API the engine speaks moves on both sides at
// once. A development build has no published image of its own and follows the
// default branch instead.
func TestSampleImageFollowsReleaseVersion(t *testing.T) {
	require.Equal(t, localscaleRepository+":v1.2.3", localscaleImage("v1.2.3"))
	require.Equal(t, localscaleRepository+":v0.1.89", localscaleImage("v0.1.89"))
	for _, version := range []string{"", "dev", "v1.2.3-rc1", "v1.2", "84ae863"} {
		require.Equal(t, localscaleRepository+":latest", localscaleImage(version), version)
	}
}

// The sample's cluster configuration is what makes it worth running: one
// sharded keyspace, so an apply reports progress per shard, seeded from the
// directory the sample's schema is copied into.
func TestSampleClusterConfigIsSharded(t *testing.T) {
	encoded, err := json.Marshal(localscaleConfig())
	require.NoError(t, err)
	var config struct {
		Organizations map[string]struct {
			Databases map[string]struct {
				Keyspaces []struct {
					Name   string `json:"name"`
					Shards int    `json:"shards"`
				} `json:"keyspaces"`
			} `json:"databases"`
		} `json:"organizations"`
		SchemaDir string `json:"schema_dir"`
	}
	require.NoError(t, json.Unmarshal(encoded, &config))

	require.Equal(t, localscaleSchemaDir, config.SchemaDir)
	require.Len(t, config.Organizations, 1)
	databases := config.Organizations[sampleOrganization].Databases
	require.Len(t, databases, 1)
	keyspaces := databases[sampleVitessDatabase].Keyspaces
	require.Len(t, keyspaces, 1)
	require.Equal(t, sampleVitessDatabase, keyspaces[0].Name)
	require.Equal(t, SampleShards, keyspaces[0].Shards)
	require.Greater(t, SampleShards, 1, "a single-shard sample would not show per-shard progress")
}

// LocalScale applies the keyspace's vschema and then its tables, so the two
// have to describe the same schema: a vindex on a table nothing creates leaves
// the keyspace routing queries to a table that is not there.
func TestSampleSeedVSchemaCoversEveryTable(t *testing.T) {
	seed := vitessSeed()
	raw, ok := seed["vschema.json"]
	require.True(t, ok, "the sample keyspace needs a vschema to be sharded")
	var vschema struct {
		Sharded  bool `json:"sharded"`
		Vindexes map[string]struct {
			Type string `json:"type"`
		} `json:"vindexes"`
		Tables map[string]struct {
			ColumnVindexes []struct {
				Column string `json:"column"`
				Name   string `json:"name"`
			} `json:"column_vindexes"`
		} `json:"tables"`
	}
	require.NoError(t, json.Unmarshal([]byte(raw), &vschema))
	require.True(t, vschema.Sharded)

	var created []string
	for file, content := range seed {
		if !strings.HasSuffix(file, ".sql") {
			continue
		}
		statementType, table, err := ddl.ClassifyStatement(content)
		require.NoError(t, err, file)
		require.Equal(t, ddl.StatementCreateTable, statementType, file)
		require.Equal(t, strings.TrimSuffix(file, ".sql"), table, "each seed file is named for the table it creates")
		created = append(created, table)
	}

	var routed []string
	for table, spec := range vschema.Tables {
		routed = append(routed, table)
		require.NotEmpty(t, spec.ColumnVindexes, table)
		for _, vindex := range spec.ColumnVindexes {
			require.Contains(t, vschema.Vindexes, vindex.Name, table)
			require.NotEmpty(t, vindex.Column, table)
		}
	}
	sort.Strings(created)
	sort.Strings(routed)
	require.Equal(t, created, routed)
}
