package inventory

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"regexp"
)

// TargetOverride attaches an operator-owned mapping to one exact environment
// and target. DatabaseType asserts the resolved engine; it does not route it.
type TargetOverride struct {
	Environment     string            `yaml:"environment"`
	Target          string            `yaml:"target"`
	DatabaseType    string            `yaml:"type"`
	SchemaOverrides map[string]string `yaml:"schema_overrides"`
}

type overrideKey struct{ environment, target string }

var overrideEnvironmentPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]*$`)
var overrideTargetPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]*$`)

// ValidateTargetOverrides validates configuration without consulting inventory.
// A nonexistent target can only be detected by resolving it during rollout.
func ValidateTargetOverrides(entries []TargetOverride) error {
	seen := make(map[overrideKey]bool, len(entries))
	for i, entry := range entries {
		prefix := fmt.Sprintf("overrides[%d] environment %q target %q", i, entry.Environment, entry.Target)
		if !overrideEnvironmentPattern.MatchString(entry.Environment) {
			return fmt.Errorf("%s: environment must match %s", prefix, overrideEnvironmentPattern)
		}
		if !overrideTargetPattern.MatchString(entry.Target) {
			return fmt.Errorf("%s: target must match %s", prefix, overrideTargetPattern)
		}
		key := overrideKey{entry.Environment, entry.Target}
		if seen[key] {
			return fmt.Errorf("%s: duplicate environment and target", prefix)
		}
		seen[key] = true
		if len(entry.SchemaOverrides) != 1 {
			return fmt.Errorf("%s: schema_overrides requires exactly one mapping", prefix)
		}
		if err := ValidateSchemaOverrides(entry.DatabaseType, entry.SchemaOverrides); err != nil {
			return fmt.Errorf("%s: %w", prefix, err)
		}
	}
	return nil
}

type overrideResolver struct {
	underlying Resolver
	entries    map[overrideKey]TargetOverride
	logger     *slog.Logger
}

// NewOverrideResolver snapshots validated mappings and preserves precisely the
// wrapped resolver's optional capabilities. It performs no discovery or secret
// lookup, and never substitutes another request on failure.
func NewOverrideResolver(underlying Resolver, entries []TargetOverride, logger *slog.Logger) (Resolver, error) {
	if underlying == nil {
		return nil, fmt.Errorf("override resolver requires a backend")
	}
	if err := ValidateTargetOverrides(entries); err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		return underlying, nil
	}
	if logger == nil {
		logger = slog.Default()
	}
	r := &overrideResolver{underlying: underlying, entries: make(map[overrideKey]TargetOverride, len(entries)), logger: logger}
	for _, entry := range entries {
		// Static entries are known without resolving credentials. Refuse two
		// mapping owners before startup, even when their mappings agree.
		if static, ok := underlying.(*StaticResolver); ok && static != nil {
			if target, exists := static.targets[entry.Target]; exists {
				if len(target.schemaOverrides) != 0 {
					return nil, fmt.Errorf("override target %q already has static schema_overrides", entry.Target)
				}
				if target.databaseType != entry.DatabaseType {
					return nil, fmt.Errorf("override target %q type %q does not match static type %q", entry.Target, entry.DatabaseType, target.databaseType)
				}
			}
		}
		entry.SchemaOverrides = maps.Clone(entry.SchemaOverrides)
		r.entries[overrideKey{entry.Environment, entry.Target}] = entry
	}
	for _, entry := range entries {
		logger.Info("target schema override configured", "environment", entry.Environment, "target", entry.Target, "database_type", entry.DatabaseType, "schema_overrides", entry.SchemaOverrides)
	}
	// Embedding only the capabilities actually present avoids advertising a
	// fabricated enumeration or single engine on a multi-engine router.
	enumerator, enumerable := underlying.(Enumerator)
	reporter, typed := underlying.(TypeReporter)
	switch {
	case enumerable && typed:
		return struct {
			Resolver
			Enumerator
			TypeReporter
		}{r, enumerator, reporter}, nil
	case enumerable:
		return struct {
			Resolver
			Enumerator
		}{r, enumerator}, nil
	case typed:
		return struct {
			Resolver
			TypeReporter
		}{r, reporter}, nil
	default:
		return r, nil
	}
}

func (r *overrideResolver) ResolveTarget(ctx context.Context, req Request) (*Target, error) {
	target, err := r.underlying.ResolveTarget(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("resolve target %q in environment %q: %w", req.Target, req.Environment, err)
	}
	entry, ok := r.entries[overrideKey{req.Environment, req.Target}]
	if !ok {
		return target, nil
	}
	if target == nil {
		return nil, fmt.Errorf("override target %q: backend returned no target", req.Target)
	}
	if target.Target != entry.Target {
		return nil, fmt.Errorf("override target %q: backend returned target %q", entry.Target, target.Target)
	}
	if target.DatabaseType != entry.DatabaseType {
		return nil, fmt.Errorf("override target %q: resolved type %q does not match configured type %q", entry.Target, target.DatabaseType, entry.DatabaseType)
	}
	if len(target.SchemaOverrides) != 0 {
		return nil, fmt.Errorf("override target %q already has schema_overrides", entry.Target)
	}
	result := *target
	result.Metadata = maps.Clone(target.Metadata)
	result.SchemaOverrides = maps.Clone(entry.SchemaOverrides)
	r.logger.DebugContext(ctx, "target schema override applied", "environment", entry.Environment, "target", entry.Target, "database_type", entry.DatabaseType, "schema_overrides", entry.SchemaOverrides)
	return &result, nil
}
