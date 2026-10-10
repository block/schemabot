package etre

import (
	"context"
	"fmt"
	"testing"

	"github.com/block/mysql"
	"github.com/square/etre"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/inventory"
)

// fakeWriterProbe reports a fixed status per connection address and records
// which addresses were probed.
type fakeWriterProbe struct {
	byAddr map[string]inventory.WriterStatus
	errs   map[string]error
	probed []string
}

func (f *fakeWriterProbe) ProbeWriter(_ context.Context, dsn string) (inventory.WriterStatus, error) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return inventory.WriterStatus{}, fmt.Errorf("parse probed dsn: %w", err)
	}
	f.probed = append(f.probed, cfg.Addr)
	if err := f.errs[cfg.Addr]; err != nil {
		return inventory.WriterStatus{}, err
	}
	status, ok := f.byAddr[cfg.Addr]
	if !ok {
		return inventory.WriterStatus{}, fmt.Errorf("unexpected probe of %s", cfg.Addr)
	}
	return status, nil
}

func pairEntities() []etre.Entity {
	return []etre.Entity{
		{"_id": "id-blue", "writer_endpoint": "orders.example"},
		{"_id": "id-green", "writer_endpoint": "orders-green.example"},
	}
}

func newWriterProbeResolver(t *testing.T, entities []etre.Entity, probe *fakeWriterProbe) *EtreResolver {
	t.Helper()
	return newEtreResolverForTest(t, nil, entities, EtreResolverConfig{
		TargetLabel: "dsid",
		EnvLabel:    "env",
		HostField:   "writer_endpoint",
		WriterProbe: probe,
	})
}

func resolvedAddr(t *testing.T, target *inventory.Target) string {
	t.Helper()
	cfg, err := mysql.ParseDSN(target.DSN)
	require.NoError(t, err)
	return cfg.Addr
}

// A target whose inventory records both sides of a replicated pair resolves to
// the side that accepts writes, whichever order the inventory lists them in.
func TestEtreResolverWriterProbeChoosesTheWriter(t *testing.T) {
	tests := []struct {
		name   string
		byAddr map[string]inventory.WriterStatus
		want   string
	}{
		{
			name: "original side is the writer",
			byAddr: map[string]inventory.WriterStatus{
				"orders.example:3306":       {Writable: true, ServerID: "uuid-blue"},
				"orders-green.example:3306": {ReadOnlyReason: "read_only=1", ServerID: "uuid-green", SourceIDs: []string{"uuid-blue"}},
			},
			want: "orders.example:3306",
		},
		{
			name: "standby side is the writer after a switchover",
			byAddr: map[string]inventory.WriterStatus{
				"orders.example:3306":       {ReadOnlyReason: "read_only=1", ServerID: "uuid-blue", SourceIDs: []string{"uuid-green"}},
				"orders-green.example:3306": {Writable: true, ServerID: "uuid-green"},
			},
			want: "orders-green.example:3306",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			probe := &fakeWriterProbe{byAddr: tt.byAddr}
			r := newWriterProbeResolver(t, pairEntities(), probe)

			target, err := r.ResolveTarget(t.Context(), inventory.Request{Target: "orders-dsid", DatabaseType: "mysql", Environment: "staging"})
			require.NoError(t, err)
			assert.Equal(t, tt.want, resolvedAddr(t, target))
			assert.Equal(t, "orders-dsid", target.Target)
			assert.ElementsMatch(t, []string{"orders.example:3306", "orders-green.example:3306"}, probe.probed)
		})
	}
}

// When the matches cannot be narrowed to one proven writer, the target does not
// resolve, and the error names each candidate by its inventory id.
func TestEtreResolverWriterProbeRefusesWithoutOneProvenWriter(t *testing.T) {
	tests := []struct {
		name    string
		byAddr  map[string]inventory.WriterStatus
		errs    map[string]error
		wantErr []string
	}{
		{
			name: "both sides writable",
			byAddr: map[string]inventory.WriterStatus{
				"orders.example:3306":       {Writable: true, ServerID: "uuid-blue"},
				"orders-green.example:3306": {Writable: true, ServerID: "uuid-green"},
			},
			wantErr: []string{"2 candidates accept writes", "id-blue writable", "id-green writable"},
		},
		{
			name: "neither side writable",
			byAddr: map[string]inventory.WriterStatus{
				"orders.example:3306":       {ReadOnlyReason: "read_only=1", ServerID: "uuid-blue"},
				"orders-green.example:3306": {ReadOnlyReason: "read_only=1", ServerID: "uuid-green"},
			},
			wantErr: []string{"no candidate accepts writes"},
		},
		{
			name: "read-only side is not a replica of the writer",
			byAddr: map[string]inventory.WriterStatus{
				"orders.example:3306":       {Writable: true, ServerID: "uuid-blue"},
				"orders-green.example:3306": {ReadOnlyReason: "read_only=1", ServerID: "uuid-green"},
			},
			wantErr: []string{"read-only candidate id-green does not replicate from writable candidate id-blue"},
		},
		{
			name: "one side cannot be probed",
			byAddr: map[string]inventory.WriterStatus{
				"orders.example:3306": {Writable: true, ServerID: "uuid-blue"},
			},
			errs:    map[string]error{"orders-green.example:3306": fmt.Errorf("dial tcp: i/o timeout")},
			wantErr: []string{"probe writer candidate id-green", "i/o timeout"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newWriterProbeResolver(t, pairEntities(), &fakeWriterProbe{byAddr: tt.byAddr, errs: tt.errs})

			target, err := r.ResolveTarget(t.Context(), inventory.Request{Target: "orders-dsid", DatabaseType: "mysql", Environment: "staging"})
			require.Error(t, err)
			assert.Nil(t, target)
			assert.Contains(t, err.Error(), `resolve target "orders-dsid"`)
			for _, want := range tt.wantErr {
				assert.Contains(t, err.Error(), want)
			}
			assert.NotContains(t, err.Error(), "orders.example", "errors name candidates by inventory id, not endpoint")
		})
	}
}

// A single match resolves exactly as it does without a probe: nothing is
// probed, because there is nothing to choose between.
func TestEtreResolverWriterProbeSkipsASingleMatch(t *testing.T) {
	probe := &fakeWriterProbe{}
	r := newWriterProbeResolver(t, []etre.Entity{{"_id": "id-blue", "writer_endpoint": "orders.example"}}, probe)

	target, err := r.ResolveTarget(t.Context(), inventory.Request{Target: "orders-dsid", DatabaseType: "mysql", Environment: "staging"})
	require.NoError(t, err)
	assert.Equal(t, "orders.example:3306", resolvedAddr(t, target))
	assert.Empty(t, probe.probed)
}

// A selector broad enough to match more candidates than the probe checks is
// refused before any connection is opened.
func TestEtreResolverWriterProbeRefusesTooManyMatches(t *testing.T) {
	entities := make([]etre.Entity, 0, maxWriterCandidates+1)
	for i := range maxWriterCandidates + 1 {
		entities = append(entities, etre.Entity{"_id": fmt.Sprintf("id-%d", i), "writer_endpoint": fmt.Sprintf("orders-%d.example", i)})
	}
	probe := &fakeWriterProbe{}
	r := newWriterProbeResolver(t, entities, probe)

	_, err := r.ResolveTarget(t.Context(), inventory.Request{Target: "orders-dsid", DatabaseType: "mysql", Environment: "staging"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), fmt.Sprintf("%d etre entities matched, more than the %d a writer probe will check", maxWriterCandidates+1, maxWriterCandidates))
	assert.Empty(t, probe.probed)
}

// Without a probe, two matches stay ambiguous: nothing could choose between
// them, so the lookup refuses.
func TestEtreResolverWithoutWriterProbeRefusesTwoMatches(t *testing.T) {
	r := newEtreResolverForTest(t, nil, pairEntities(), EtreResolverConfig{
		TargetLabel: "dsid",
		HostField:   "writer_endpoint",
	})

	_, err := r.ResolveTarget(t.Context(), inventory.Request{Target: "orders-dsid"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "expected exactly one")
}
