package etre

import (
	"context"
	"fmt"
	"maps"
	"strings"

	"github.com/block/schemabot/pkg/inventory"
)

// EtreResolverConfig configures an inventory.Resolver backed by Etre.
//
// It is engine-agnostic: the Etre lookup produces an endpoint host and
// attributes, an injected CredentialResolver produces the credentials, and an
// injected ConnectionAssembler turns those into the engine-specific connection.
// Resolving a different engine through Etre means supplying a different
// assembler, not writing a new resolver.
type EtreResolverConfig struct {
	// Client is the Etre query client, bound to the entity type that records
	// this engine's clusters.
	Client *Client

	// TargetLabel is the Etre label the request's opaque target matches.
	TargetLabel string

	// Labels are fixed equality predicates added to every lookup (for example a
	// region label that disambiguates a target present in multiple regions).
	Labels map[string]string

	// EnvLabel, when set, adds the request environment as a selector predicate.
	EnvLabel string

	// HostField is the entity field holding the connection host, passed to the
	// assembler. It may be empty for engines that connect by attribute rather
	// than host (the assembler validates what it needs).
	HostField string

	// AttributeFields are entity fields surfaced to the credential resolver and
	// the assembler as attributes (for example an account id for assume-role, or
	// an organization for a Vitess connection).
	AttributeFields []string

	// Credentials resolves the credentials, independently of the endpoint.
	Credentials inventory.CredentialResolver

	// Assembler turns the resolved endpoint and credentials into the
	// engine-specific connection, and determines the resolved target's type.
	Assembler inventory.ConnectionAssembler
}

// EtreResolver resolves targets through Etre, delegating the engine-specific
// connection assembly to a ConnectionAssembler.
type EtreResolver struct {
	cfg EtreResolverConfig
}

var _ inventory.Resolver = (*EtreResolver)(nil)

// NewEtreResolver validates the config and builds a resolver.
func NewEtreResolver(cfg EtreResolverConfig) (*EtreResolver, error) {
	switch {
	case cfg.Client == nil:
		return nil, fmt.Errorf("etre client is required")
	case cfg.TargetLabel == "":
		return nil, fmt.Errorf("target label is required")
	case cfg.Credentials == nil:
		return nil, fmt.Errorf("credential resolver is required")
	case cfg.Assembler == nil:
		return nil, fmt.Errorf("connection assembler is required")
	}
	// Fixed labels must not collide with the target or env predicates, which
	// would let a misconfigured label silently override them and resolve the
	// wrong entity.
	for label := range cfg.Labels {
		if label == cfg.TargetLabel {
			return nil, fmt.Errorf("fixed label %q collides with the target label", label)
		}
		if cfg.EnvLabel != "" && label == cfg.EnvLabel {
			return nil, fmt.Errorf("fixed label %q collides with the env label", label)
		}
	}
	return &EtreResolver{cfg: cfg}, nil
}

// ResolveTarget looks the target up in Etre for its endpoint and attributes,
// resolves credentials, and delegates engine-specific connection assembly. It
// fails closed: lookup ambiguity (via the Etre client), credential failure, or
// an assembler rejecting an incomplete endpoint are all errors.
func (r *EtreResolver) ResolveTarget(ctx context.Context, req inventory.Request) (*inventory.Target, error) {
	if req.Target == "" {
		return nil, fmt.Errorf("target is required")
	}
	// Fail closed when the request's type does not match the engine this
	// resolver serves, rather than returning a connection for a different engine.
	if dbType := strings.TrimSpace(req.DatabaseType); dbType != "" && !strings.EqualFold(dbType, r.cfg.Assembler.DatabaseType()) {
		return nil, fmt.Errorf("target %q requested database type %q but this resolver serves %q", req.Target, dbType, r.cfg.Assembler.DatabaseType())
	}

	selector := map[string]string{r.cfg.TargetLabel: req.Target}
	maps.Copy(selector, r.cfg.Labels)
	// When an env label is configured the lookup must be environment-scoped;
	// resolving without an environment could match a different environment's
	// entity, so fail closed rather than drop the predicate.
	if r.cfg.EnvLabel != "" {
		if req.Environment == "" {
			return nil, fmt.Errorf("target %q requires an environment because env label %q is configured", req.Target, r.cfg.EnvLabel)
		}
		selector[r.cfg.EnvLabel] = req.Environment
	}

	entity, err := r.cfg.Client.QueryOne(ctx, selector)
	if err != nil {
		return nil, fmt.Errorf("resolve target %q: %w", req.Target, err)
	}

	host := StringField(entity, r.cfg.HostField)

	var attrs map[string]string
	if len(r.cfg.AttributeFields) > 0 {
		attrs = make(map[string]string, len(r.cfg.AttributeFields))
		for _, field := range r.cfg.AttributeFields {
			attrs[field] = StringField(entity, field)
		}
	}

	creds, err := r.cfg.Credentials.ResolveCredentials(ctx, req, attrs)
	if err != nil {
		return nil, fmt.Errorf("resolve credentials for target %q: %w", req.Target, err)
	}

	dsn, metadata, err := r.cfg.Assembler.Assemble(host, attrs, creds)
	if err != nil {
		return nil, fmt.Errorf("assemble connection for target %q: %w", req.Target, err)
	}

	return &inventory.Target{
		Target:       req.Target,
		DatabaseType: r.cfg.Assembler.DatabaseType(),
		DSN:          dsn,
		Metadata:     metadata,
	}, nil
}
