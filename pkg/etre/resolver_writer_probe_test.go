package etre

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/block/mysql"
	"github.com/square/etre"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/inventory"
)

// fakeWriterProbe reports a fixed status per connection address and records
// which addresses were probed. The resolver probes candidates concurrently, so
// the record is guarded.
type fakeWriterProbe struct {
	byAddr map[string]inventory.WriterStatus
	errs   map[string]error
	mu     sync.Mutex
	probed []string
}

func (f *fakeWriterProbe) ProbeWriter(_ context.Context, dsn string) (inventory.WriterStatus, error) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return inventory.WriterStatus{}, fmt.Errorf("parse probed dsn: %w", err)
	}
	f.mu.Lock()
	f.probed = append(f.probed, cfg.Addr)
	f.mu.Unlock()
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
			errs:    map[string]error{"orders-green.example:3306": fmt.Errorf("dial tcp orders-green.example:3306: i/o timeout")},
			wantErr: []string{"writer candidate id-green could not be probed; see server logs"},
		},
		{
			name: "one side is too old to probe",
			byAddr: map[string]inventory.WriterStatus{
				"orders.example:3306": {Writable: true, ServerID: "uuid-blue"},
			},
			errs: map[string]error{"orders-green.example:3306": &inventory.UnsupportedServerError{
				Reason: "the server does not support SHOW REPLICA STATUS, which needs MySQL 8.0.22 or later",
				Err:    fmt.Errorf("orders-green.example:3306: Error 1064: You have an error in your SQL syntax"),
			}},
			wantErr: []string{"writer candidate id-green could not be probed: the server does not support SHOW REPLICA STATUS, which needs MySQL 8.0.22 or later"},
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
			for _, endpoint := range []string{"orders.example", "orders-green.example"} {
				assert.NotContains(t, err.Error(), endpoint, "errors name candidates by inventory id, not endpoint")
			}
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

// barrierWriterProbe answers only once every expected probe is in flight, so a
// resolver that probed one candidate at a time would never get an answer.
type barrierWriterProbe struct {
	byAddr  map[string]inventory.WriterStatus
	arrived chan struct{}
	release chan struct{}
	want    int
	once    sync.Once
}

func (b *barrierWriterProbe) ProbeWriter(ctx context.Context, dsn string) (inventory.WriterStatus, error) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return inventory.WriterStatus{}, fmt.Errorf("parse probed dsn: %w", err)
	}
	b.arrived <- struct{}{}
	if len(b.arrived) == b.want {
		b.once.Do(func() { close(b.release) })
	}
	select {
	case <-b.release:
		return b.byAddr[cfg.Addr], nil
	case <-ctx.Done():
		return inventory.WriterStatus{}, fmt.Errorf("probe of %s was never joined by the other candidates: %w", cfg.Addr, ctx.Err())
	}
}

// The candidates are probed at the same time, so a target whose candidates are
// slow to answer waits for the slowest one rather than for all of them in turn.
func TestEtreResolverWriterProbeProbesCandidatesConcurrently(t *testing.T) {
	probe := &barrierWriterProbe{
		byAddr: map[string]inventory.WriterStatus{
			"orders.example:3306":       {Writable: true, ServerID: "uuid-blue"},
			"orders-green.example:3306": {ReadOnlyReason: "read_only=1", ServerID: "uuid-green", SourceIDs: []string{"uuid-blue"}},
		},
		arrived: make(chan struct{}, 2),
		release: make(chan struct{}),
		want:    2,
	}
	r := newEtreResolverForTest(t, nil, pairEntities(), EtreResolverConfig{
		TargetLabel: "dsid",
		EnvLabel:    "env",
		HostField:   "writer_endpoint",
		WriterProbe: probe,
	})

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	target, err := r.ResolveTarget(ctx, inventory.Request{Target: "orders-dsid", DatabaseType: "mysql", Environment: "staging"})
	require.NoError(t, err)
	assert.Equal(t, "orders.example:3306", resolvedAddr(t, target))
}

// A candidate without an Etre id is named by its position, and the position is
// taken after ordering the candidates by host, so the same server keeps the
// same name whichever order the inventory returned them in.
func TestEtreResolverWriterProbeNamesCandidatesStably(t *testing.T) {
	blue := etre.Entity{"writer_endpoint": "orders.example"}
	green := etre.Entity{"writer_endpoint": "orders-green.example"}
	probe := &fakeWriterProbe{byAddr: map[string]inventory.WriterStatus{
		"orders.example:3306":       {ReadOnlyReason: "read_only=1", ServerID: "uuid-blue"},
		"orders-green.example:3306": {ReadOnlyReason: "innodb_read_only=1", ServerID: "uuid-green"},
	}}
	for _, entities := range [][]etre.Entity{{blue, green}, {green, blue}} {
		r := newWriterProbeResolver(t, entities, probe)
		_, err := r.ResolveTarget(t.Context(), inventory.Request{Target: "orders-dsid", DatabaseType: "mysql", Environment: "staging"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "match 1 read-only: innodb_read_only=1; match 2 read-only: read_only=1")
	}
}
