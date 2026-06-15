package inventory

import (
	"context"
	"fmt"
	"maps"
	"strings"

	"github.com/go-sql-driver/mysql"

	"github.com/block/schemabot/pkg/secrets"
)

// StaticConfig configures a resolver backed by static target entries.
type StaticConfig struct {
	Targets map[string]StaticTarget `yaml:"targets"`
}

// StaticTarget is a static connection entry for one target.
type StaticTarget struct {
	DatabaseType string            `yaml:"type"`
	DSN          string            `yaml:"dsn"`
	Metadata     map[string]string `yaml:"metadata,omitempty"`
}

// StaticResolver resolves targets from static configuration.
type StaticResolver struct {
	targets map[string]Target
}

var _ Resolver = (*StaticResolver)(nil)

// NewStaticResolver creates a static target resolver.
func NewStaticResolver(config StaticConfig) (*StaticResolver, error) {
	if len(config.Targets) == 0 {
		return nil, fmt.Errorf("static target resolver requires at least one target")
	}
	targets := make(map[string]Target, len(config.Targets))
	for target, entry := range config.Targets {
		if target == "" {
			return nil, fmt.Errorf("static target resolver contains an empty target key")
		}
		resolved, err := resolveStaticTarget(target, entry)
		if err != nil {
			return nil, err
		}
		targets[target] = *resolved
	}
	return &StaticResolver{targets: targets}, nil
}

// ResolveTarget resolves one target from static configuration.
func (r *StaticResolver) ResolveTarget(_ context.Context, req Request) (*Target, error) {
	if r == nil {
		return nil, fmt.Errorf("static target resolver is nil")
	}
	if req.Target == "" {
		return nil, fmt.Errorf("target is required")
	}
	entry, ok := r.targets[req.Target]
	if !ok {
		return nil, fmt.Errorf("target %q is not configured", req.Target)
	}
	if strings.TrimSpace(req.DatabaseType) != "" {
		requestedType := canonicalDatabaseType(req.DatabaseType)
		if requestedType != entry.DatabaseType {
			return nil, fmt.Errorf("target %q is configured for database type %q, not %q", req.Target, entry.DatabaseType, requestedType)
		}
	}
	return &Target{
		Target:       entry.Target,
		DatabaseType: entry.DatabaseType,
		DSN:          entry.DSN,
		Metadata:     maps.Clone(entry.Metadata),
	}, nil
}

func resolveStaticTarget(target string, entry StaticTarget) (*Target, error) {
	if entry.DatabaseType == "" {
		return nil, fmt.Errorf("target %q missing type", target)
	}
	databaseType := canonicalDatabaseType(entry.DatabaseType)
	if databaseType == "" {
		return nil, fmt.Errorf("target %q missing type", target)
	}
	if entry.DSN == "" {
		return nil, fmt.Errorf("target %q missing dsn", target)
	}
	dsn, err := secrets.Resolve(entry.DSN, "")
	if err != nil {
		return nil, fmt.Errorf("resolve DSN for target %q: %w", target, err)
	}
	if dsn == "" {
		return nil, fmt.Errorf("target %q resolved an empty DSN", target)
	}
	if err := validateResolvedStaticTargetDSN(target, databaseType, dsn); err != nil {
		return nil, err
	}
	return &Target{
		Target:       target,
		DatabaseType: databaseType,
		DSN:          dsn,
		Metadata:     maps.Clone(entry.Metadata),
	}, nil
}

func canonicalDatabaseType(databaseType string) string {
	return strings.ToLower(strings.TrimSpace(databaseType))
}

func validateResolvedStaticTargetDSN(target, databaseType, dsn string) error {
	if databaseType != "mysql" {
		return nil
	}
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return fmt.Errorf("parse MySQL DSN for target %q: %w", target, err)
	}
	if cfg.DBName != "" {
		return fmt.Errorf("target %q MySQL DSN must not include a database name; the request supplies the namespace", target)
	}
	return nil
}
