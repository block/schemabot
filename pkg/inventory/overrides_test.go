package inventory

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type overrideBackend func(context.Context, Request) (*Target, error)

func (f overrideBackend) ResolveTarget(ctx context.Context, req Request) (*Target, error) {
	return f(ctx, req)
}

func sampleOverride() TargetOverride {
	return TargetOverride{Environment: "production", Target: "cluster-002", DatabaseType: "mysql", SchemaOverrides: map[string]string{"orders_production": "orders_sandbox"}}
}

func TestOverrideValidation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*TargetOverride)
	}{
		{"environment absent", func(e *TargetOverride) { e.Environment = "" }},
		{"environment uppercase", func(e *TargetOverride) { e.Environment = "Production" }},
		{"target absent", func(e *TargetOverride) { e.Target = "" }},
		{"target malformed", func(e *TargetOverride) { e.Target = "cluster/*" }},
		{"type absent", func(e *TargetOverride) { e.DatabaseType = "" }},
		{"type uppercase", func(e *TargetOverride) { e.DatabaseType = "MySQL" }},
		{"vitess", func(e *TargetOverride) { e.DatabaseType = "vitess" }},
		{"strata", func(e *TargetOverride) { e.DatabaseType = "strata" }},
		{"empty mapping", func(e *TargetOverride) { e.SchemaOverrides = nil }},
		{"multiple mappings", func(e *TargetOverride) { e.SchemaOverrides["another"] = "another" }},
		{"bad canonical", func(e *TargetOverride) { e.SchemaOverrides = map[string]string{"bad-name": "physical"} }},
		{"bad physical", func(e *TargetOverride) { e.SchemaOverrides["orders_production"] = "" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			entry := sampleOverride()
			tc.change(&entry)
			require.Error(t, ValidateTargetOverrides([]TargetOverride{entry}))
		})
	}
	entry := sampleOverride()
	require.ErrorContains(t, ValidateTargetOverrides([]TargetOverride{entry, entry}), "duplicate")
	entry.DatabaseType = "postgres"
	require.NoError(t, ValidateTargetOverrides([]TargetOverride{entry}))
}

func TestOverrideResolution(t *testing.T) {
	original := &Target{Target: "cluster-002", DatabaseType: "mysql", DSN: "user:secret@tcp(host:3306)/", Metadata: map[string]string{"key": "value"}, TableOwner: "owner"}
	req := Request{Target: original.Target, Environment: "production"} // omitted caller type must still be checked
	entry := sampleOverride()
	calls := 0
	underlying := overrideBackend(func(ctx context.Context, got Request) (*Target, error) {
		calls++
		assert.Equal(t, req, got)
		assert.Same(t, t.Context(), ctx)
		return original, nil
	})
	resolver, err := NewOverrideResolver(underlying, []TargetOverride{entry}, slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	entry.SchemaOverrides["orders_production"] = "changed_input"
	resolved, err := resolver.ResolveTarget(t.Context(), req)
	require.NoError(t, err)
	assert.Equal(t, "orders_sandbox", resolved.SchemaOverrides["orders_production"])
	assert.Equal(t, original.DSN, resolved.DSN)
	assert.Equal(t, original.TableOwner, resolved.TableOwner)
	resolved.SchemaOverrides["orders_production"] = "changed_output"
	resolved.Metadata["key"] = "changed_output"
	again, err := resolver.ResolveTarget(t.Context(), req)
	require.NoError(t, err)
	assert.Equal(t, "orders_sandbox", again.SchemaOverrides["orders_production"])
	assert.Equal(t, "value", original.Metadata["key"])
	assert.Empty(t, original.SchemaOverrides)
	assert.Equal(t, 2, calls)
	for _, mutate := range []func(){func() { original.Target = "wrong" }, func() { original.Target = req.Target; original.DatabaseType = "postgres" }, func() {
		original.DatabaseType = "mysql"
		original.SchemaOverrides = map[string]string{"orders_production": "orders_sandbox"}
	}} {
		mutate()
		_, err = resolver.ResolveTarget(t.Context(), req)
		require.Error(t, err)
	}
}

func TestOverrideExactMatchAndErrors(t *testing.T) {
	target := &Target{Target: "other", DatabaseType: "mysql"}
	backend := overrideBackend(func(context.Context, Request) (*Target, error) { return target, nil })
	resolver, err := NewOverrideResolver(backend, []TargetOverride{sampleOverride()}, nil)
	require.NoError(t, err)
	for _, req := range []Request{{Target: "cluster-002", Environment: "staging"}, {Target: "cluster-001", Environment: "production"}, {Target: "cluster-002"}} {
		result, err := resolver.ResolveTarget(t.Context(), req)
		require.NoError(t, err)
		assert.Same(t, target, result)
	}
	for _, failure := range []error{context.Canceled, errors.New("not found"), errors.New("ambiguous"), errors.New("credentials")} {
		resolver, err = NewOverrideResolver(overrideBackend(func(context.Context, Request) (*Target, error) { return nil, failure }), []TargetOverride{sampleOverride()}, nil)
		require.NoError(t, err)
		_, err = resolver.ResolveTarget(t.Context(), Request{Target: "cluster-002", Environment: "production"})
		assert.Same(t, failure, err)
	}
}

type overrideCapabilities struct {
	Resolver
	failure error
}

func (r overrideCapabilities) DatabaseType() string { return "mysql" }
func (r overrideCapabilities) Enumerate(context.Context) ([]ProbeRequest, error) {
	return []ProbeRequest{{Target: "unmapped", DatabaseType: "mysql", Environment: "production"}}, r.failure
}
func (r overrideCapabilities) UnenumerableDatabaseTypes() []string { return []string{"postgres"} }

func TestOverrideCapabilities(t *testing.T) {
	basic := overrideBackend(func(context.Context, Request) (*Target, error) { return nil, nil })
	full := overrideCapabilities{Resolver: basic}
	router, err := NewTypeRoutingResolver(map[string]Resolver{"mysql": full, "vitess": basic})
	require.NoError(t, err)
	for _, tc := range []struct {
		name              string
		backend           Resolver
		enumerates, typed bool
	}{
		{"basic", basic, false, false}, {"single", full, true, true}, {"router", router, true, false},
		{"type only", struct {
			Resolver
			TypeReporter
		}{basic, full}, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			wrapped, err := NewOverrideResolver(tc.backend, []TargetOverride{sampleOverride()}, nil)
			require.NoError(t, err)
			enumeration, ok := wrapped.(Enumerator)
			assert.Equal(t, tc.enumerates, ok)
			if ok {
				expected, err := tc.backend.(Enumerator).Enumerate(t.Context())
				require.NoError(t, err)
				actual, err := enumeration.Enumerate(t.Context())
				require.NoError(t, err)
				assert.Equal(t, expected, actual)
				assert.Equal(t, tc.backend.(Enumerator).UnenumerableDatabaseTypes(), enumeration.UnenumerableDatabaseTypes())
			}
			reporter, ok := wrapped.(TypeReporter)
			assert.Equal(t, tc.typed, ok)
			if ok {
				assert.Equal(t, "mysql", reporter.DatabaseType())
			}
		})
	}
	full.failure = errors.New("enumeration failed")
	wrapped, err := NewOverrideResolver(full, []TargetOverride{sampleOverride()}, nil)
	require.NoError(t, err)
	_, err = wrapped.(Enumerator).Enumerate(t.Context())
	assert.Same(t, full.failure, err)
}

func TestOverrideStaticConflict(t *testing.T) {
	static, err := NewStaticResolver(StaticConfig{Targets: map[string]StaticTarget{"cluster-002": {DatabaseType: "mysql", DSN: "root@tcp(localhost:3306)/", SchemaOverrides: sampleOverride().SchemaOverrides}}})
	require.NoError(t, err)
	_, err = NewOverrideResolver(static, []TargetOverride{sampleOverride()}, nil)
	require.ErrorContains(t, err, "already has static")
}

func TestOverrideNoopAndNilBackend(t *testing.T) {
	backend := &overrideCapabilities{}
	resolver, err := NewOverrideResolver(backend, nil, nil)
	require.NoError(t, err)
	assert.Same(t, backend, resolver)
	_, err = NewOverrideResolver(nil, []TargetOverride{sampleOverride()}, nil)
	require.ErrorContains(t, err, "requires a backend")
	resolver, err = NewOverrideResolver(overrideBackend(func(context.Context, Request) (*Target, error) { return nil, nil }), []TargetOverride{sampleOverride()}, nil)
	require.NoError(t, err)
	_, err = resolver.ResolveTarget(t.Context(), Request{Target: "cluster-002", Environment: "production"})
	require.ErrorContains(t, err, "returned no target")
}

func TestOverrideLogsExcludeConnectionSecrets(t *testing.T) {
	var output bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&output, &slog.HandlerOptions{Level: slog.LevelDebug}))
	backend := overrideBackend(func(context.Context, Request) (*Target, error) {
		return &Target{Target: "cluster-002", DatabaseType: "mysql", DSN: "user:dsn-secret@tcp(host)/", Metadata: map[string]string{"password": "metadata-secret"}}, nil
	})
	resolver, err := NewOverrideResolver(backend, []TargetOverride{sampleOverride()}, logger)
	require.NoError(t, err)
	_, err = resolver.ResolveTarget(t.Context(), Request{Target: "cluster-002", Environment: "production"})
	require.NoError(t, err)
	assert.Contains(t, output.String(), "target schema override configured")
	assert.Contains(t, output.String(), "target schema override applied")
	assert.Contains(t, output.String(), "orders_sandbox")
	assert.NotContains(t, output.String(), "dsn-secret")
	assert.NotContains(t, output.String(), "metadata-secret")
}
