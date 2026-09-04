package tern

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/engine"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/schema"
)

// driftChangeKey identifies a single table DDL change for drift comparison. Two
// changes are the same iff they target the same namespace, shard, and table with
// the same operation and canonicalized DDL. The shard is part of the key because
// a sharded engine emits one change set per shard and the same table repeats
// across shards: keying without it would conflate a change on one shard with a
// different change on another.
//
// The shard-aware comparison relies on the engine emitting a stable shard
// identifier (engine.SchemaChange.Shard.Name) equal to the value the dispatch
// fan-out keyed its per-shard operations on. Both sides of the multiset read
// that same field — the dispatch's TargetShards is rebuilt from it and the
// re-plan reads it directly — so a legitimate shard-scoped apply is compared
// symmetrically rather than refused. This guard couples "what may be applied" to
// "what re-planning live-vs-desired reproduces": a future reconciliation path
// that applies repair DDL derived some other way (e.g. shard-to-shard
// comparison or a stored repair a live-vs-desired re-plan would not recompute)
// must route through a normal reviewed plan or add an explicit reconciliation
// mode here, or it would trip the guard.
type driftChangeKey struct {
	namespace string
	shard     string
	table     string
	operation string
	ddl       string
}

// driftChangeMultiset counts table DDL changes by key so duplicate changes are
// compared exactly (set equality would silently tolerate a duplicated change).
type driftChangeMultiset map[driftChangeKey]int

// driftRecoveryHint gives a blocked operator the defined next step when the
// guard fails closed: the reviewed plan no longer matches live schema, so it
// must be regenerated against the current schema and re-reviewed before
// re-applying. Without this an operator only learns what drifted, not what to do.
const driftRecoveryHint = "to recover, re-plan against the current live schema to refresh the reviewed plan, then re-apply"

// verifyMaterializedPlanMatchesLiveSchema fails closed unless the reviewed DDL a
// dispatch carries exactly matches what this deployment would independently plan
// against its own live schema. A non-primary deployment never planned locally,
// so materializing the primary's reviewed DDL could silently replay it against a
// deployment whose schema has drifted; recomputing the local diff and requiring
// an exact match keeps non-primary drift from being applied unreviewed.
//
// The comparison is shard-aware. A sharded engine's work is dispatched one
// apply_operation per shard, so a request that carries a target shard is scoped
// to that single shard: the reviewed DDL is compared against this deployment's
// re-plan restricted to the same shard. A request with no target shard is a
// whole-deployment (or non-sharded) apply, compared against the re-plan's
// non-sharded changes.
func (c *LocalClient) verifyMaterializedPlanMatchesLiveSchema(ctx context.Context, req *ternv1.ApplyRequest, schemaFiles schema.SchemaFiles) error {
	shardScoped := len(req.TargetShards) > 0
	targetShard := ""
	if shardScoped {
		shard, err := dispatchTargetShard(req.TargetShards)
		if err != nil {
			return fmt.Errorf("drift guard: %w", err)
		}
		targetShard = shard
	}

	result, err := c.planWithEngine(ctx, &ternv1.PlanRequest{
		Database:    c.config.Database,
		Type:        c.config.Type,
		Environment: req.Environment,
		Target:      req.Target,
	}, c.config.Database, schemaFiles)
	if err != nil {
		return fmt.Errorf("recompute local plan: %w", err)
	}

	recomputed, err := c.driftMultisetFromPlanResult(result, shardScoped, targetShard)
	if err != nil {
		return fmt.Errorf("recomputed plan: %w", err)
	}
	dispatched, err := c.driftMultisetFromApplyRequest(req.DdlChanges, targetShard)
	if err != nil {
		return fmt.Errorf("dispatched plan: %w", err)
	}
	if err := compareDriftMultisets(recomputed, dispatched); err != nil {
		return fmt.Errorf("local schema has drifted from the reviewed plan (database %q, target %q): %w; %s", c.config.Database, req.Target, err, driftRecoveryHint)
	}

	// VSchema changes are namespace-level, not shard-scoped, and travel on the
	// whole-deployment dispatch — a shard-scoped DDL dispatch never carries them
	// (VSchema is applied by a separate task-less finalizer). So parity is only
	// meaningful for a whole-deployment materialize.
	//
	// The two sides bridge different representations of "vschema changed": the
	// re-plan reads the engine's Metadata["vschema_changed"], the dispatch reads
	// the proto CHANGE_TYPE_VSCHEMA. They agree today; any divergence drops a
	// namespace from one set, which trips parity in the fail-closed direction.
	if !shardScoped {
		if err := compareVSchemaParity(vschemaNamespacesFromPlanResult(c, result), vschemaNamespacesFromApplyRequest(c, req.DdlChanges)); err != nil {
			return fmt.Errorf("local vschema has drifted from the reviewed plan (database %q, target %q): %w; %s", c.config.Database, req.Target, err, driftRecoveryHint)
		}
	}
	return nil
}

// driftMultisetFromPlanResult builds the table DDL multiset this deployment
// would plan against its own live schema, restricted to the dispatch's shard
// scope. A shard-scoped dispatch covers exactly one shard, so other shards'
// remaining changes are not part of this comparison; a whole-deployment dispatch
// covers only the non-sharded changes. VSchema changes carry no table DDL and
// are compared separately, so they are excluded here.
func (c *LocalClient) driftMultisetFromPlanResult(result *engine.PlanResult, shardScoped bool, targetShard string) (driftChangeMultiset, error) {
	parser, err := c.statementParser()
	if err != nil {
		return nil, err
	}
	ms := driftChangeMultiset{}
	for _, sc := range result.Changes {
		shard := sc.ShardName()
		if shardScoped {
			if shard != targetShard {
				continue
			}
		} else if sc.Sharded() {
			continue
		}
		ns := c.planNamespace(sc.Namespace)
		for _, tc := range sc.TableChanges {
			canon, err := canonicalDDLForDrift(parser, tc.DDL)
			if err != nil {
				return nil, fmt.Errorf("table %q: %w", tc.Table, err)
			}
			ms[driftChangeKey{ns, shard, tc.Table, ddl.StatementTypeToOp(tc.Operation), canon}]++
		}
	}
	return ms, nil
}

// driftMultisetFromApplyRequest builds the table DDL multiset the dispatch
// request carries as the reviewed, authoritative plan. The dispatched changes
// are flat (the TableChange proto carries no shard), so they are keyed to the
// dispatch's target shard, which is "" for a whole-deployment apply. VSchema
// changes are compared separately and excluded here. Nil entries are corrupt
// input and fail closed.
func (c *LocalClient) driftMultisetFromApplyRequest(changes []*ternv1.TableChange, targetShard string) (driftChangeMultiset, error) {
	parser, err := c.statementParser()
	if err != nil {
		return nil, err
	}
	ms := driftChangeMultiset{}
	for _, ch := range changes {
		if ch == nil {
			return nil, fmt.Errorf("dispatch request carried a nil table change")
		}
		if ch.ChangeType == ternv1.ChangeType_CHANGE_TYPE_VSCHEMA {
			continue
		}
		op, err := materializedTableChangeOperation(parser, ch)
		if err != nil {
			return nil, err
		}
		canon, err := canonicalDDLForDrift(parser, ch.Ddl)
		if err != nil {
			return nil, fmt.Errorf("table %q: %w", ch.TableName, err)
		}
		ms[driftChangeKey{c.planNamespace(ch.Namespace), targetShard, ch.TableName, op, canon}]++
	}
	return ms, nil
}

// statementParser returns the DDL parser for this deployment's database type,
// so drift comparisons classify and canonicalize DDL with the target's own
// grammar rather than assuming every target speaks MySQL. An unregistered
// dialect is an error the drift guard fails closed on.
func (c *LocalClient) statementParser() (ddl.StatementParser, error) {
	p, err := ddl.ParserForDialect(schema.DialectForDatabaseType(c.config.Type))
	if err != nil {
		return nil, fmt.Errorf("resolve statement parser for database type %q: %w", c.config.Type, err)
	}
	return p, nil
}

// canonicalDDLForDrift normalizes a DDL statement, or a greenfield create set,
// for comparison and fails closed if it cannot be parsed or is not actually
// DDL. The parser's Canonicalize returns the input unchanged on a parse
// failure, so such input would otherwise compare by raw text and could mask
// drift — Classify errors reject it first. Classify also rejects
// multi-statement input; the one multi-statement shape drift admits is a
// greenfield create set (a CREATE TABLE followed by CREATE INDEX statements on
// that table), canonicalized statement by statement so equivalent spellings
// in the same order compare equal.
func canonicalDDLForDrift(p ddl.StatementParser, raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", fmt.Errorf("empty DDL")
	}
	if stmtType, _, err := p.Classify(raw); err == nil {
		return canonicalDriftStatement(p, raw, stmtType)
	}
	createSet, err := ddl.ParseCreateSet(p, raw)
	if err != nil {
		return "", fmt.Errorf("DDL rejected by the statement parser: %w", err)
	}
	canonical := make([]string, 0, len(createSet.Statements))
	for i, statement := range createSet.Statements {
		stmtType := createSet.Type
		if i > 0 {
			stmtType = ddl.StatementCreateIndex
		}
		c, err := canonicalDriftStatement(p, statement, stmtType)
		if err != nil {
			return "", err
		}
		canonical = append(canonical, c)
	}
	return strings.Join(canonical, ";\n"), nil
}

// canonicalDriftStatement canonicalizes one statement that must be DDL. Two
// rejection causes get distinct messages because they call for different
// remedies: DML (e.g. INSERT) has no place in a schema change and should be
// removed from it, while a statement outside the shared DDL vocabulary is SQL
// the comparison has no name for and cannot verify.
func canonicalDriftStatement(p ddl.StatementParser, statement string, stmtType ddl.StatementType) (string, error) {
	if stmtType == ddl.StatementUnknown {
		return "", fmt.Errorf("statement classified outside the shared DDL vocabulary; drift cannot verify it")
	}
	if !stmtType.IsDDL() {
		return "", fmt.Errorf("expected a DDL statement, got %s", stmtType)
	}
	return p.Canonicalize(statement), nil
}

// diffDriftMultisets returns the changes each side of a comparison holds that
// the other does not, rendered and sorted for a message. Both are empty exactly
// when the two multisets are equal.
//
// The set arithmetic is shared, the vocabulary is not: what a difference means
// depends on what is being compared, and a caller comparing two dispatches has
// no "reviewed" side to name. Callers phrase their own message from these.
func diffDriftMultisets(recomputed, dispatched driftChangeMultiset) (onlyInDispatched, onlyInRecomputed []string) {
	for key, want := range dispatched {
		if recomputed[key] < want {
			onlyInDispatched = append(onlyInDispatched, formatDriftKey(key))
		}
	}
	for key, have := range recomputed {
		if have > dispatched[key] {
			onlyInRecomputed = append(onlyInRecomputed, formatDriftKey(key))
		}
	}
	sort.Strings(onlyInDispatched)
	sort.Strings(onlyInRecomputed)
	return onlyInDispatched, onlyInRecomputed
}

// compareDriftMultisets reports drift unless the recomputed and dispatched table
// DDL multisets are exactly equal.
func compareDriftMultisets(recomputed, dispatched driftChangeMultiset) error {
	missing, unexpected := diffDriftMultisets(recomputed, dispatched)
	if len(missing) == 0 && len(unexpected) == 0 {
		return nil
	}
	return fmt.Errorf("reviewed changes this deployment would not plan: %v; changes this deployment would plan that were not reviewed: %v", missing, unexpected)
}

// formatDriftKey renders a drift key for an operator-facing message. The
// canonicalized DDL is included because the multiset keys on it: two changes for
// the same namespace/shard/table/operation that differ only in DDL must render
// differently or the message would list identical-looking entries on both sides
// and hide what actually drifted. The shard is shown only when set so
// non-sharded messages stay uncluttered.
func formatDriftKey(k driftChangeKey) string {
	return fmt.Sprintf("%s (%s)", formatDriftLocation(k), k.ddl)
}

// formatDriftLocation renders the namespace/shard/table/operation of a drift key
// without its DDL, for messages that present the reviewed and re-planned DDL
// separately. The shard is shown only when set so non-sharded messages stay
// uncluttered.
func formatDriftLocation(k driftChangeKey) string {
	loc := fmt.Sprintf("%s.%s", k.namespace, k.table)
	if k.shard != "" {
		loc = fmt.Sprintf("%s[%s].%s", k.namespace, k.shard, k.table)
	}
	return fmt.Sprintf("%s/%s", loc, k.operation)
}

// vschemaNamespacesFromPlanResult returns the namespaces the recomputed plan
// detected a vschema change for.
func vschemaNamespacesFromPlanResult(c *LocalClient, result *engine.PlanResult) map[string]bool {
	out := map[string]bool{}
	for _, sc := range result.Changes {
		if sc.Metadata["vschema_changed"] == "true" {
			out[c.planNamespace(sc.Namespace)] = true
		}
	}
	return out
}

// vschemaNamespacesFromApplyRequest returns the namespaces the dispatch request
// carries a vschema change for.
func vschemaNamespacesFromApplyRequest(c *LocalClient, changes []*ternv1.TableChange) map[string]bool {
	out := map[string]bool{}
	for _, ch := range changes {
		if ch != nil && ch.ChangeType == ternv1.ChangeType_CHANGE_TYPE_VSCHEMA {
			out[c.planNamespace(ch.Namespace)] = true
		}
	}
	return out
}

// compareVSchemaParity reports drift unless the recomputed and dispatched sets
// of vschema-changed namespaces are identical.
func compareVSchemaParity(recomputed, dispatched map[string]bool) error {
	var missing, unexpected []string
	for ns := range dispatched {
		if !recomputed[ns] {
			missing = append(missing, ns)
		}
	}
	for ns := range recomputed {
		if !dispatched[ns] {
			unexpected = append(unexpected, ns)
		}
	}
	if len(missing) == 0 && len(unexpected) == 0 {
		return nil
	}
	sort.Strings(missing)
	sort.Strings(unexpected)
	return fmt.Errorf("reviewed vschema changes this deployment would not plan: %v; vschema changes this deployment would plan that were not reviewed: %v", missing, unexpected)
}
