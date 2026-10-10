package etre

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/square/etre"

	"github.com/block/schemabot/pkg/inventory"
)

// maxWriterCandidates bounds how many matches a writer probe will connect to.
// Both sides of a replicated pair, plus a cross-region standby, fit inside it;
// a selector broad enough to exceed it is misconfigured, and probing every match
// would open a connection to each on every resolution.
const maxWriterCandidates = 4

// writerProbeTimeout bounds probing one candidate, dial and queries together.
const writerProbeTimeout = 10 * time.Second

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

	// TableOwner is the PostgreSQL role new tables are created as on every
	// target this resolver serves. Optional: empty creates them as the
	// connected role. Only valid for postgres.
	TableOwner string

	// WriterProbe, when set, lets a lookup that matches more than one entity
	// resolve to the one that accepts writes, as inventory.SelectWriter decides.
	// When nil, more than one match is an error.
	WriterProbe inventory.WriterProbe

	// Logger receives the writer probe's decisions. Defaults to slog.Default().
	Logger *slog.Logger
}

// EtreResolver resolves targets through Etre, delegating the engine-specific
// connection assembly to a ConnectionAssembler.
type EtreResolver struct {
	cfg EtreResolverConfig

	// writers remembers the last writer chosen per environment and target, only
	// to decide how loudly to log the next choice.
	writers sync.Map
}

var (
	_ inventory.Resolver     = (*EtreResolver)(nil)
	_ inventory.TypeReporter = (*EtreResolver)(nil)
)

// DatabaseType names the one engine this resolver serves: the type its
// assembler builds connections for, and the type ResolveTarget refuses to
// deviate from.
func (r *EtreResolver) DatabaseType() string {
	return r.cfg.Assembler.DatabaseType()
}

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
	if err := inventory.ValidateTableOwner(cfg.Assembler.DatabaseType(), cfg.TableOwner); err != nil {
		return nil, fmt.Errorf("etre resolver: %w", err)
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
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &EtreResolver{cfg: cfg}, nil
}

// ResolveTarget looks the target up in Etre for its endpoint and attributes,
// resolves credentials, and delegates engine-specific connection assembly. It
// fails closed: lookup ambiguity (via the Etre client), credential failure, or
// an assembler rejecting an incomplete endpoint are all errors.
//
// With a WriterProbe configured, several matches are not ambiguous by
// themselves: each is probed, and the target resolves only when
// inventory.SelectWriter finds exactly one writer that every other match
// replicates from. Anything less is still an error.
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

	entities, err := r.lookup(ctx, selector)
	if err != nil {
		return nil, fmt.Errorf("resolve target %q: %w", req.Target, err)
	}
	if len(entities) == 1 {
		conn, err := r.connect(ctx, req, entities[0])
		if err != nil {
			return nil, err
		}
		return r.target(req, conn), nil
	}
	conn, err := r.resolveWriter(ctx, req, entities)
	if err != nil {
		return nil, err
	}
	return r.target(req, conn), nil
}

// lookup returns the entities matching selector. Without a writer probe it
// requires exactly one, since nothing could choose among several; with one it
// returns every match for resolveWriter to choose from.
func (r *EtreResolver) lookup(ctx context.Context, selector map[string]string) ([]etre.Entity, error) {
	if r.cfg.WriterProbe == nil {
		entity, err := r.cfg.Client.QueryOne(ctx, selector)
		if err != nil {
			return nil, err
		}
		return []etre.Entity{entity}, nil
	}
	return r.cfg.Client.Query(ctx, selector)
}

// connection is one entity turned into an engine connection.
type connection struct {
	entityID string
	host     string
	dsn      string
	metadata map[string]string
}

// connect turns one resolved entity into a connection: it reads the endpoint
// and attributes, resolves credentials, and delegates assembly.
func (r *EtreResolver) connect(ctx context.Context, req inventory.Request, entity etre.Entity) (connection, error) {
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
		return connection{}, fmt.Errorf("resolve credentials for target %q: %w", req.Target, err)
	}

	dsn, metadata, err := r.cfg.Assembler.Assemble(host, attrs, creds)
	if err != nil {
		return connection{}, fmt.Errorf("assemble connection for target %q: %w", req.Target, err)
	}
	return connection{host: host, dsn: dsn, metadata: metadata}, nil
}

// resolveWriter connects to every matching entity, probes each, and returns the
// one inventory.SelectWriter chooses. Any candidate it cannot connect to or
// probe fails the resolution, because the remaining candidates cannot then be
// proven to hold the only writer.
//
// The candidates are probed concurrently, so a slow candidate costs one probe
// timeout rather than one per candidate. They are ordered by Etre id, then
// host, first, so a candidate named by its position keeps its name from one
// resolution to the next whatever order the query returned.
func (r *EtreResolver) resolveWriter(ctx context.Context, req inventory.Request, entities []etre.Entity) (connection, error) {
	if len(entities) > maxWriterCandidates {
		return connection{}, fmt.Errorf("resolve target %q: %d etre entities matched, more than the %d a writer probe will check; narrow the selector", req.Target, len(entities), maxWriterCandidates)
	}

	ordered := slices.Clone(entities)
	slices.SortStableFunc(ordered, func(a, b etre.Entity) int {
		return cmp.Or(
			cmp.Compare(StringField(a, "_id"), StringField(b, "_id")),
			cmp.Compare(StringField(a, r.cfg.HostField), StringField(b, r.cfg.HostField)),
		)
	})

	results := make([]probedCandidate, len(ordered))
	var wg sync.WaitGroup
	for i, entity := range ordered {
		wg.Go(func() { results[i] = r.probeCandidate(ctx, req, entity, i) })
	}
	wg.Wait()

	conns := make([]connection, 0, len(results))
	candidates := make([]inventory.WriterCandidate, 0, len(results))
	for _, result := range results {
		if result.err != nil {
			return connection{}, result.err
		}
		conns = append(conns, result.conn)
		candidates = append(candidates, inventory.WriterCandidate{ID: result.conn.entityID, Status: result.status})
	}

	chosen, err := inventory.SelectWriter(candidates)
	if err != nil {
		r.cfg.Logger.Warn("etre: no single writer among the matched entities; refusing to resolve the target",
			"target", req.Target, "environment", req.Environment, "candidates", candidateLogAttrs(conns, candidates), "error", err)
		return connection{}, fmt.Errorf("resolve target %q: %w", req.Target, err)
	}
	r.logWriterChoice(req, conns[chosen], conns, candidates)
	return conns[chosen], nil
}

// probedCandidate is one candidate's connection and what probing it found, or
// the resolution error it ends in.
type probedCandidate struct {
	conn   connection
	status inventory.WriterStatus
	err    error
}

// probeCandidate connects to one matched entity and probes it.
func (r *EtreResolver) probeCandidate(ctx context.Context, req inventory.Request, entity etre.Entity, index int) probedCandidate {
	id := candidateID(entity, index)
	conn, err := r.connect(ctx, req, entity)
	if err != nil {
		return probedCandidate{err: fmt.Errorf("writer candidate %s: %w", id, err)}
	}
	conn.entityID = id
	status, err := r.probe(ctx, conn)
	if err != nil {
		r.cfg.Logger.Warn("etre: writer probe failed; refusing to resolve the target",
			"target", req.Target, "environment", req.Environment, "candidate", conn.entityID, "host", conn.host, "error", err)
		return probedCandidate{err: probeFailure(req, conn, err)}
	}
	return probedCandidate{conn: conn, status: status}
}

// probeFailure is the resolution error for a candidate that could not be
// probed. The probe error can carry the candidate's endpoint (a dial error
// names the host), and a resolution error can reach a PR comment, so the raw
// error stays in the server log and only a reason the probe wrote for display
// is shown.
func probeFailure(req inventory.Request, conn connection, err error) error {
	var unsupported *inventory.UnsupportedServerError
	if errors.As(err, &unsupported) {
		return fmt.Errorf("resolve target %q: writer candidate %s could not be probed: %s", req.Target, conn.entityID, unsupported.Reason)
	}
	return fmt.Errorf("resolve target %q: writer candidate %s could not be probed; see server logs", req.Target, conn.entityID)
}

// candidateID names a matched entity in errors and logs by its Etre id, which
// identifies the record without revealing its endpoint. An entity without one
// is named by its position among the matches.
func candidateID(entity etre.Entity, index int) string {
	if id := StringField(entity, "_id"); id != "" {
		return id
	}
	return fmt.Sprintf("match %d", index+1)
}

// probe runs the writer probe against one candidate under its own deadline.
func (r *EtreResolver) probe(ctx context.Context, conn connection) (inventory.WriterStatus, error) {
	ctx, cancel := context.WithTimeout(ctx, writerProbeTimeout)
	defer cancel()
	return r.cfg.WriterProbe.ProbeWriter(ctx, conn.dsn)
}

// logWriterChoice records the chosen writer. Resolution runs on every routed
// request, so an unchanged choice logs at debug and only a change of writer for
// the target, such as a switchover between two drives, logs at info.
func (r *EtreResolver) logWriterChoice(req inventory.Request, writer connection, conns []connection, candidates []inventory.WriterCandidate) {
	key := req.Environment + "/" + req.Target
	previous, seen := r.writers.Swap(key, writer.entityID)
	attrs := []any{"target", req.Target, "environment", req.Environment, "writer", writer.entityID, "writer_host", writer.host, "candidates", candidateLogAttrs(conns, candidates)}
	switch {
	case !seen:
		r.cfg.Logger.Info("etre: resolved the writer among matched entities", attrs...)
	case previous != writer.entityID:
		r.cfg.Logger.Info("etre: the writer among matched entities changed", append(attrs, "previous_writer", previous)...)
	default:
		r.cfg.Logger.Debug("etre: resolved the same writer among matched entities", attrs...)
	}
}

// candidateLogAttrs records each candidate's host and observed state, so the
// decision can be checked from the log alone.
func candidateLogAttrs(conns []connection, candidates []inventory.WriterCandidate) []map[string]any {
	attrs := make([]map[string]any, 0, len(candidates))
	for i, c := range candidates {
		attrs = append(attrs, map[string]any{
			"id":               c.ID,
			"host":             conns[i].host,
			"writable":         c.Status.Writable,
			"read_only_reason": c.Status.ReadOnlyReason,
			"server_id":        c.Status.ServerID,
			"source_ids":       c.Status.SourceIDs,
		})
	}
	return attrs
}

// target builds the resolved target from the chosen connection.
func (r *EtreResolver) target(req inventory.Request, conn connection) *inventory.Target {
	return &inventory.Target{
		Target:       req.Target,
		DatabaseType: r.cfg.Assembler.DatabaseType(),
		DSN:          conn.dsn,
		Metadata:     conn.metadata,
		TableOwner:   r.cfg.TableOwner,
	}
}
