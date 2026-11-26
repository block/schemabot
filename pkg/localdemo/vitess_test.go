package localdemo

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/block/mysql"
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

// Every proxy port the cluster allocates from has to be published, and on
// loopback alone. A port left unpublished would make the API hand the engine an
// address only the container can reach, and an apply would fail at "ping
// branch" after the plan had already succeeded.
func TestSampleProxyPortMapRequiresEveryPublishedPort(t *testing.T) {
	full := sampleContainerWithProxyPorts(50000)
	ports, err := sampleProxyPortMap(full)
	require.NoError(t, err)
	require.Len(t, ports, sampleProxyPorts)
	require.Equal(t, 50000, ports[sampleProxyPortStart])
	require.Equal(t, 50000+sampleProxyPorts-1, ports[sampleProxyPortStart+sampleProxyPorts-1])

	missing := sampleContainerWithProxyPorts(50000)
	delete(missing.NetworkSettings.Ports, strconv.Itoa(sampleProxyPortStart+3)+"/tcp")
	_, err = sampleProxyPortMap(missing)
	require.ErrorContains(t, err, strconv.Itoa(sampleProxyPortStart+3))

	exposed := sampleContainerWithProxyPorts(50000)
	exposed.NetworkSettings.Ports[strconv.Itoa(sampleProxyPortStart+1)+"/tcp"][0].HostIP = "0.0.0.0"
	_, err = sampleProxyPortMap(exposed)
	require.ErrorContains(t, err, "loopback")
}

// The sample reads its vtgate endpoint from the cluster and translates it to
// the host, so the engine queries shards and schema over an address it can
// reach. The port the cluster reports is its own; only the published one works.
func TestSampleVtgateDSNTranslatesToThePublishedPort(t *testing.T) {
	var posted map[int]int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/admin/proxy-port-map":
			require.NoError(t, json.NewDecoder(r.Body).Decode(&posted))
			_, err := w.Write([]byte(`{"ok":true}`))
			require.NoError(t, err)
		case "/admin/edges":
			_, err := fmt.Fprintf(w, `{%q:"0.0.0.0:%d"}`, sampleOrganization+"/"+sampleVitessDatabase, sampleProxyPortStart+2)
			require.NoError(t, err)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	ports, err := sampleProxyPortMap(sampleContainerWithProxyPorts(50000))
	require.NoError(t, err)
	require.NoError(t, postSampleProxyPortMap(t.Context(), server.URL, ports))
	require.Equal(t, ports, posted)

	dsn, err := sampleVtgateDSN(t.Context(), server.URL, ports)
	require.NoError(t, err)
	require.Equal(t, "root@tcp(127.0.0.1:50002)/"+sampleVitessDatabase, dsn)

	cfg, err := mysql.ParseDSN(dsn)
	require.NoError(t, err)
	require.Equal(t, sampleVitessDatabase, cfg.DBName, "an environment's DSN has to name a database")
}

// A vtgate endpoint outside the published range is reported rather than turned
// into a DSN that cannot be dialed.
func TestSampleVtgateDSNRejectsAnUnpublishedPort(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, err := fmt.Fprintf(w, `{%q:"0.0.0.0:3306"}`, sampleOrganization+"/"+sampleVitessDatabase)
		require.NoError(t, err)
	}))
	defer server.Close()
	ports, err := sampleProxyPortMap(sampleContainerWithProxyPorts(50000))
	require.NoError(t, err)
	_, err = sampleVtgateDSN(t.Context(), server.URL, ports)
	require.ErrorContains(t, err, "outside the range it publishes")
}

// The proxy ports are published as one block at a reserved offset, so two
// projects can each run a sample instead of competing for fixed ports.
func TestReserveHostPortRangeIsContiguousAndFree(t *testing.T) {
	start, err := reserveHostPortRange(t.Context(), 8)
	require.NoError(t, err)
	var cfg net.ListenConfig
	for port := start; port < start+8; port++ {
		listener, err := cfg.Listen(t.Context(), "tcp4", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)))
		require.NoError(t, err, "port %d was reported free", port)
		require.NoError(t, listener.Close())
	}
}

// sampleContainerWithProxyPorts describes a container publishing the sample's
// whole proxy range on loopback, starting at hostStart.
func sampleContainerWithProxyPorts(hostStart int) container {
	var c container
	c.NetworkSettings.Ports = map[string][]struct{ HostIP, HostPort string }{}
	for i := range sampleProxyPorts {
		c.NetworkSettings.Ports[strconv.Itoa(sampleProxyPortStart+i)+"/tcp"] = []struct{ HostIP, HostPort string }{
			{HostIP: "127.0.0.1", HostPort: strconv.Itoa(hostStart + i)},
		}
	}
	return c
}
