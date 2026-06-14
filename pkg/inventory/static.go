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
	targets map[string]StaticTarget
}

var _ Resolver = (*StaticResolver)(nil)

// NewStaticResolver creates a static target resolver.
func NewStaticResolver(config StaticConfig) (*StaticResolver, error) {
	if len(config.Targets) == 0 {
		return nil, fmt.Errorf("static target resolver requires at least one target")
	}
	targets := make(map[string]StaticTarget, len(config.Targets))
	for target, entry := range config.Targets {
		if target == "" {
			return nil, fmt.Errorf("static target resolver contains an empty target key")
		}
		if err := validateStaticTarget(target, entry); err != nil {
			return nil, err
		}
		targets[target] = StaticTarget{
			DatabaseType: entry.DatabaseType,
			DSN:          entry.DSN,
			Metadata:     maps.Clone(entry.Metadata),
		}
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
	if req.DatabaseType != "" && req.DatabaseType != entry.DatabaseType {
		return nil, fmt.Errorf("target %q is configured for database type %q, not %q", req.Target, entry.DatabaseType, req.DatabaseType)
	}
	dsn, err := secrets.Resolve(entry.DSN, "")
	if err != nil {
		return nil, fmt.Errorf("resolve DSN for target %q: %w", req.Target, err)
	}
	if dsn == "" {
		return nil, fmt.Errorf("target %q resolved an empty DSN", req.Target)
	}
	if err := validateResolvedStaticTargetDSN(req.Target, entry.DatabaseType, dsn); err != nil {
		return nil, err
	}
	return &Target{
		Target:       req.Target,
		DatabaseType: entry.DatabaseType,
		DSN:          dsn,
		Metadata:     maps.Clone(entry.Metadata),
	}, nil
}

func validateStaticTarget(target string, entry StaticTarget) error {
	if entry.DatabaseType == "" {
		return fmt.Errorf("target %q missing type", target)
	}
	if entry.DSN == "" {
		return fmt.Errorf("target %q missing dsn", target)
	}
	return nil
}

func validateResolvedStaticTargetDSN(target, databaseType, dsn string) error {
	if !strings.EqualFold(databaseType, "mysql") {
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
