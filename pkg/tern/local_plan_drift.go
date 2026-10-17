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
	"github.com/block/schemabot/pkg/storage"
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

// replannedChange is one change as this deployment's own re-plan produced it:
// the statement its engine emitted against this target, the execution-mode
// verdict on how that statement would run here, and whether its engine judged
// the statement unsafe.
type replannedChange struct {
	ddl          string
	mode         string
	reason       string
	unsafe       bool
	unsafeReason string
}

// replannedChangeOf reads the re-planned facts back off a plan row's change,
// so a change carried from the plan row is stamped exactly as the plan row
// was.
func replannedChangeOf(tc storage.TableChange) replannedChange {
	return replannedChange{ddl: tc.DDL, mode: tc.ExecutionMode, reason: tc.ModeReason, unsafe: tc.IsUnsafe, unsafeReason: tc.UnsafeReason}
}

// stampOnto writes the re-planned facts onto a change. The statement and
// execution-mode verdict are this target's and replace what the change
// carried. The unsafe flag is only ever tightened: a re-plan that judges the
// statement unsafe marks the change unsafe, so the unsafe-change admission
// gate requires the opt-in for it, and a re-plan that judges it safe leaves a
// change already marked unsafe as it is.
func (r replannedChange) stampOnto(tc *storage.TableChange) {
	tc.DDL = r.ddl
	tc.ExecutionMode = r.mode
	tc.ModeReason = r.reason
	if !r.unsafe {
		return
	}
	tc.IsUnsafe = true
	if r.unsafeReason != "" {
		tc.UnsafeReason = r.unsafeReason
	}
}

// replannedChanges records, per change, what this deployment's re-plan
// produced, keyed exactly as the drift comparison keyed the changes (so
// targetShard is the shard every key carries: "" for a whole-deployment
// apply). All three parts are properties of a target, not of the reviewed
// text. The verdict: the same statement can be executable on the primary's
// target and refused on this one (a table above the direct-execution size
// bound here, a grant missing here). The statement: an engine that qualifies
// DDL with the physical schema it planned against writes the primary's schema
// into the dispatched text, and this target's copy of that schema can carry a
// different name. The unsafe flag: whether the statement destroys data is
// judged by this target's engine against this target's schema. The
// dispatched changes carry none of them, so the re-plan is the only place
// they exist.
type replannedChanges struct {
	targetShard string
	byChange    map[driftChangeKey]replannedChange
}

// record keeps the re-planned change for a key. A key can repeat when the
// re-plan holds the same change twice; a blocked verdict then wins because
// admitting a plan one copy would refuse is the failure this exists to
// prevent, and an unsafe flag wins for the same reason: the kept copy takes
// the unsafe flag and reason of the other copy when only that one is unsafe.
// Otherwise the first seen stands — the copies canonicalize equal by
// construction of the key, so either statement is this target's own.
func (r replannedChanges) record(key driftChangeKey, change replannedChange) {
	existing, seen := r.byChange[key]
	if !seen {
		r.byChange[key] = change
		return
	}
	kept, other := existing, change
	if change.mode == engine.ExecutionModeBlocked && existing.mode != engine.ExecutionModeBlocked {
		kept, other = change, existing
	}
	if other.unsafe && !kept.unsafe {
		kept.unsafe, kept.unsafeReason = true, other.unsafeReason
	}
	r.byChange[key] = kept
}

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
// A mismatch is permanent. A redispatch carries the same reviewed DDL against
// the same drifted schema, so no retry can clear it; only a re-plan that
// refreshes the reviewed plan can, so the apply fails at once with that hint
// instead of spending its retry budget. A failure to compute the re-plan
// itself stays retryable, since it says nothing about the schema.
//
// On a match it returns this deployment's re-planned changes, keyed the same
// way the comparison was: the statement this target's engine emitted for each
// reviewed change and its execution-mode verdict. The dispatch is built from
// task rows, which carry the primary's text and no verdict, so the re-plan is
// the only source of both "would this target refuse the statement" and "how
// does this target spell it" — and the materialized plan must carry them for
// the admission gate to have anything to refuse and for the engine to run a
// statement that names this target. Returning them from here rather than
// re-planning again keeps one engine call serving the comparison and the
// materialized plan.
//
// The comparison is shard-aware. A sharded engine's work is dispatched one
// apply_operation per shard, so a request that carries a target shard is scoped
// to that single shard: the reviewed DDL is compared against this deployment's
// re-plan restricted to the same shard. A request with no target shard is a
// whole-deployment (or non-sharded) apply, compared against the re-plan's
// non-sharded changes.
func (c *LocalClient) verifyMaterializedPlanMatchesLiveSchema(ctx context.Context, req *ternv1.ApplyRequest, schemaFiles schema.SchemaFiles) (replannedChanges, error) {
	shardScoped := len(req.TargetShards) > 0
	targetShard := ""
	if shardScoped {
		shard, err := dispatchTargetShard(req.TargetShards)
		if err != nil {
			return replannedChanges{}, fmt.Errorf("drift guard: %w", err)
		}
		targetShard = shard
	}

	result, err := c.replanForDispatch(ctx, req, schemaFiles)
	if err != nil {
		return replannedChanges{}, err
	}

	recomputed, replanned, err := c.driftMultisetFromPlanResult(result, shardScoped, targetShard)
	if err != nil {
		return replannedChanges{}, fmt.Errorf("recomputed plan: %w", err)
	}
	dispatched, err := c.driftMultisetFromApplyRequest(req.DdlChanges, targetShard)
	if err != nil {
		return replannedChanges{}, fmt.Errorf("dispatched plan: %w", err)
	}
	if err := compareDriftMultisets(recomputed, dispatched); err != nil {
		return replannedChanges{}, engine.NewPermanentError("local schema has drifted from the reviewed plan (database %q, target %q): %w; %s", c.config.Database, req.Target, err, driftRecoveryHint)
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
	//
	// A finalize the engine asked for travels the same way and is held to the
	// same parity: the re-plan must still ask to finalize exactly the
	// namespaces the reviewed plan did, so a finalizer whose reason went away
	// fails closed instead of finalizing a keyspace nobody reviewed.
	if !shardScoped {
		if err := compareVSchemaParity(vschemaNamespacesFromPlanResult(c, result), vschemaNamespacesFromApplyRequest(c, req.DdlChanges)); err != nil {
			return replannedChanges{}, engine.NewPermanentError("local vschema has drifted from the reviewed plan (database %q, target %q): %w; %s", c.config.Database, req.Target, err, driftRecoveryHint)
		}
		if err := compareFinalizeParity(finalizeNamespacesFromPlanResult(c, result), finalizeNamespacesFromApplyRequest(c, req.DdlChanges)); err != nil {
			return replannedChanges{}, engine.NewPermanentError("local keyspace finalization has drifted from the reviewed plan (database %q, target %q): %w; %s", c.config.Database, req.Target, err, driftRecoveryHint)
		}
	}
	return replanned, nil
}

// replanForDispatch recomputes this deployment's plan of the dispatch's
// reviewed schema files against its own live schema, under the settings the
// reviewed plan was made with.
func (c *LocalClient) replanForDispatch(ctx context.Context, req *ternv1.ApplyRequest, schemaFiles schema.SchemaFiles) (*engine.PlanResult, error) {
	result, err := c.planWithEngine(ctx, &ternv1.PlanRequest{
		Database:    c.config.Database,
		Type:        c.config.Type,
		Environment: req.Environment,
		Target:      req.Target,
		// The re-plan must be asked to withhold what the reviewed plan was
		// asked to withhold. A withheld table has no declaring file, so a
		// re-plan that sees it proposes dropping it — a change the reviewed
		// DDL cannot contain, which would fail this guard on every apply of a
		// plan that withheld anything. The dispatch carries the whole config
		// the plan was reviewed under rather than the subset that matched
		// where it was planned, so an entry naming a table only this
		// deployment holds withholds it here too.
		IgnoreTables: req.GetIgnoreTables(),
		// The guard compares the reviewed DDL against this re-plan, so the
		// re-plan has to be judged under the dispatch's own policy. Under a
		// different one a statement the reviewed plan routed directly comes
		// back blocked, and the guard reads that as drift.
		DirectExecution: req.GetDirectExecution(),
	}, c.config.Database, schemaFiles)
	if err != nil {
		return nil, fmt.Errorf("recompute local plan: %w", err)
	}
	return result, nil
}

// verifyAttachedShardDispatch holds a shard-scoped dispatch that attaches to
// an existing apply to the same rule as the dispatch that created it: the
// reviewed change it carries must be what this deployment would plan for that
// shard and table against its own live schema, or the attach fails closed.
// The plan row the attach runs from was checked against the creating
// dispatch's shard only, so without this a sibling shard that drifted would
// run the reviewed DDL unchecked. On a match the scope's changes take this
// shard's re-planned statement, execution-mode verdict and unsafe flag, as a
// materialized plan's do.
//
// The comparison is restricted to the tables the dispatch names. A dispatch
// carries one table's changes for one shard, and the shard's other tables are
// other operations' work — some already run, some not yet attached — so they
// are no evidence about this one.
//
// A dispatch that carries no schema files has no reviewed schema to re-plan
// against, and attaches against the plan row as it is: a change no re-plan has
// judged keeps the mark that makes admission require the unsafe opt-in for it.
func (c *LocalClient) verifyAttachedShardDispatch(ctx context.Context, req *ternv1.ApplyRequest, scope *dispatchScope) error {
	if len(req.SchemaFiles) == 0 {
		c.logger.Debug("shard dispatch carries no schema files to re-plan against; attaching against the plan row as it is",
			"plan_id", req.PlanId,
			"shard", scope.shard)
		return nil
	}
	schemaFiles, err := c.normalizeSchemaFiles(protoToSchemaFiles(req.SchemaFiles))
	if err != nil {
		return fmt.Errorf("normalize schema files: %w", err)
	}
	result, err := c.replanForDispatch(ctx, req, schemaFiles)
	if err != nil {
		return err
	}
	recomputed, replanned, err := c.driftMultisetFromPlanResult(result, true, scope.shard)
	if err != nil {
		return fmt.Errorf("recomputed plan: %w", err)
	}
	dispatched, err := c.driftMultisetFromApplyRequest(req.DdlChanges, scope.shard)
	if err != nil {
		return fmt.Errorf("dispatched plan: %w", err)
	}
	if err := compareDriftMultisets(recomputed.restrictedToTablesOf(dispatched), dispatched); err != nil {
		return engine.NewPermanentError("local schema has drifted from the reviewed plan (database %q, target %q, shard %q): %w; %s", c.config.Database, req.Target, scope.shard, err, driftRecoveryHint)
	}
	parser, err := c.statementParser()
	if err != nil {
		return err
	}
	if err := restoreDispatchedUnsafeVerdicts(req.DdlChanges, scope.ddlChanges); err != nil {
		return err
	}
	if err := c.stampReplannedTables(parser, scope.ddlChanges, replanned); err != nil {
		return fmt.Errorf("re-planned changes: %w", err)
	}
	return nil
}

// restoreDispatchedUnsafeVerdicts puts back, on each change this shard's
// re-plan is about to judge, the unsafe verdict the dispatch itself carried.
// The scope's changes were already marked by dispatchScopeForApply: unsafe with
// a "could not be confirmed" reason where no re-planned copy existed, or with
// the verdict of whichever sibling's re-plan the plan row holds. Neither is a
// judgement of this shard's change, and stamping only ever tightens the flag,
// so they would outlive the re-plan that now judges it. Restoring the
// dispatched verdict first lets the re-plan's own verdict be the one the
// change carries — safe when it judged the change safe, unsafe with its own
// reason when it judged it unsafe — while a flag the reviewed plan itself
// carried is kept. A change the engine blocks is not stamped and keeps the
// mark it has.
func restoreDispatchedUnsafeVerdicts(dispatched []*ternv1.TableChange, changes []storage.TableChange) error {
	if len(dispatched) != len(changes) {
		return fmt.Errorf("shard dispatch carries %d changes but its scope holds %d", len(dispatched), len(changes))
	}
	for i := range changes {
		if changes[i].EngineBlocked() {
			continue
		}
		changes[i].IsUnsafe = dispatched[i].GetIsUnsafe()
		changes[i].UnsafeReason = dispatched[i].GetUnsafeReason()
	}
	return nil
}

// restrictedToTablesOf keeps only the changes to a namespace and table the
// other multiset holds a change for.
func (ms driftChangeMultiset) restrictedToTablesOf(other driftChangeMultiset) driftChangeMultiset {
	type namespaceTable struct{ namespace, table string }
	tables := map[namespaceTable]bool{}
	for key := range other {
		tables[namespaceTable{key.namespace, key.table}] = true
	}
	out := driftChangeMultiset{}
	for key, n := range ms {
		if tables[namespaceTable{key.namespace, key.table}] {
			out[key] = n
		}
	}
	return out
}

// driftMultisetFromPlanResult builds the table DDL multiset this deployment
// would plan against its own live schema, restricted to the dispatch's shard
// scope, together with the re-planned change (this target's statement and
// execution-mode verdict) for each key. A
// shard-scoped dispatch covers exactly one shard, so other shards' remaining
// changes are not part of this comparison; a whole-deployment dispatch covers
// only the non-sharded changes. VSchema changes carry no table DDL and are
// compared separately, so they are excluded here.
func (c *LocalClient) driftMultisetFromPlanResult(result *engine.PlanResult, shardScoped bool, targetShard string) (driftChangeMultiset, replannedChanges, error) {
	parser, err := c.statementParser()
	if err != nil {
		return nil, replannedChanges{}, err
	}
	ms := driftChangeMultiset{}
	replanned := replannedChanges{targetShard: targetShard, byChange: map[driftChangeKey]replannedChange{}}
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
				return nil, replannedChanges{}, fmt.Errorf("table %q: %w", tc.Table, err)
			}
			key := driftChangeKey{ns, shard, tc.Table, ddl.StatementTypeToOp(tc.Operation), canon}
			ms[key]++
			replanned.record(key, replannedChange{ddl: tc.DDL, mode: tc.ExecutionMode, reason: tc.ModeReason, unsafe: tc.IsUnsafe, unsafeReason: tc.UnsafeReason})
		}
	}
	return ms, replanned, nil
}

// stampReplannedChanges rewrites the materialized table changes as this
// deployment's re-plan produced them, so the plan describes the apply as this
// target will run it rather than as the primary reviewed it.
//
// The statement becomes this target's engine-emitted text, on every engine.
// The drift comparison has already proven it the same change as the reviewed
// one under canonicalization; what canonicalization set aside is the spelling
// — quoting, whitespace, and for an engine that qualifies DDL with the
// physical schema it planned against, the primary's schema name. The last is
// the one that matters for correctness: materializing the reviewed text would
// carry that name here, and the engine refuses a statement naming a schema
// other than the one the apply addresses. The others change the stored bytes
// without changing what runs, so the plan and task rows of one deployment
// read as that deployment's engine renders them rather than byte-for-byte as
// the primary's. The resume path makes the same substitution when it
// re-derives a task's statement.
//
// The verdict becomes this target's, so the admission gate judges the plan as
// this target would run it. A dispatch carries no verdict by design: it is
// built from task rows, and the task-to-wire conversion leaves the
// execution-mode fields out because they travel one way, out of the engine
// that judged them (see tablechange_convert.go). Should a dispatch
// nevertheless arrive already blocked, that refusal is kept: a verdict is only
// ever tightened here, never relaxed, and a change kept blocked keeps the
// reviewed text it will never run. Every materialized change has a re-planned
// counterpart because the drift comparison already required the two multisets
// to match key for key.
//
// The unsafe flag is tightened the same way. The dispatch does not carry the
// flag, and an operation the statement classifies as an alter can still drop
// data (a dropped column), so without the re-plan's judgement the
// unsafe-change admission gate would admit it without the opt-in.
func (c *LocalClient) stampReplannedChanges(namespaces map[string]*storage.NamespacePlanData, replanned replannedChanges) error {
	parser, err := c.statementParser()
	if err != nil {
		return err
	}
	for _, nsData := range namespaces {
		if err := c.stampReplannedTables(parser, nsData.Tables, replanned); err != nil {
			return err
		}
	}
	return nil
}

// stampReplannedTables stamps each change with its re-planned counterpart,
// matched under the drift key the comparison admitted it on.
func (c *LocalClient) stampReplannedTables(parser ddl.StatementParser, tables []storage.TableChange, replanned replannedChanges) error {
	for i := range tables {
		tc := &tables[i]
		if tc.EngineBlocked() {
			continue
		}
		canon, err := canonicalDDLForDrift(parser, tc.DDL)
		if err != nil {
			return fmt.Errorf("table %q: %w", tc.Table, err)
		}
		change, ok := replanned.byChange[driftChangeKey{c.planNamespace(tc.Namespace), replanned.targetShard, tc.Table, tc.Operation, canon}]
		if !ok {
			return fmt.Errorf("table %q: no re-planned change for one the drift comparison matched", tc.Table)
		}
		change.stampOnto(tc)
	}
	return nil
}

// dispatchScopeForApply derives the scope a dispatch's task rows are built
// from, carrying the plan row's copy of each change into it so the task rows
// and the plan row of one apply hold the same statement and verdict.
//
// A whole-deployment dispatch already builds its scope from the plan row, so
// the stamped changes reach the tasks as they are. A shard-scoped dispatch
// builds its scope from the dispatched changes instead (the control plane's
// per-shard fan-out owns that scope), which would carry the primary's text
// onto the task rows even though the plan row was just materialized with this
// target's re-planned text. The substitution here closes that gap: each
// dispatched change is matched to its counterpart in the plan row under the
// drift key — the same equivalence the drift comparison admitted it on — and
// takes the counterpart's statement and execution-mode verdict, and its unsafe
// flag where the counterpart is marked unsafe. The counterparts come from the
// dispatched shard's own plan row where the plan judged a namespace per shard
// (see shardDispatchCounterparts), so a shard whose changes diverge from its
// siblings' is matched to its own text and verdict.
//
// A change with no counterpart was never re-planned here. The plan row was
// materialized by a sibling dispatch of the same plan and holds only that
// dispatch's changes, and a later dispatch resolves to the stored row without
// a re-plan, so nothing on this deployment has judged whether the change
// destroys data. Such a change keeps the dispatched statement — the reviewed
// text, the primary's rendering — and is marked unsafe with a reason saying
// its verdict could not be confirmed here, so the unsafe-change admission
// gate requires the opt-in for it rather than admitting it on the absence of
// a verdict. The substitution never widens what runs, and the operation key a
// shard-scoped dispatch is stamped with names the namespace, shard and table
// only, so the control plane's echo check, which derives the key from the
// dispatched text, is unaffected.
func (c *LocalClient) dispatchScopeForApply(plan *storage.Plan, req *ternv1.ApplyRequest) (dispatchScope, error) {
	scope, err := deriveDispatchScope(plan, req)
	if err != nil {
		return dispatchScope{}, err
	}
	if scope.shard == "" {
		return scope, nil
	}
	parser, err := c.statementParser()
	if err != nil {
		return dispatchScope{}, err
	}
	stored := map[driftChangeKey][]storage.TableChange{}
	for _, tc := range c.shardDispatchCounterparts(plan, scope.shard) {
		canon, err := canonicalDDLForDrift(parser, tc.DDL)
		if err != nil {
			return dispatchScope{}, fmt.Errorf("plan table %q: %w", tc.Table, err)
		}
		key := driftChangeKey{c.planNamespace(tc.Namespace), scope.shard, tc.Table, tc.Operation, canon}
		stored[key] = append(stored[key], tc)
	}
	for i := range scope.ddlChanges {
		ch := &scope.ddlChanges[i]
		canon, err := canonicalDDLForDrift(parser, ch.DDL)
		if err != nil {
			return dispatchScope{}, fmt.Errorf("dispatch change for table %q: %w", ch.Table, err)
		}
		key := driftChangeKey{c.planNamespace(ch.Namespace), scope.shard, ch.Table, ch.Operation, canon}
		matches := stored[key]
		if len(matches) == 0 {
			c.logger.Warn("dispatched change has no counterpart in the plan row; the task keeps the dispatched statement and admission will require allow_unsafe for it",
				"plan_id", req.PlanId,
				"database", c.config.Database,
				"shard", scope.shard,
				"namespace", ch.Namespace,
				"table", ch.Table,
				"operation", ch.Operation)
			markUnconfirmedUnsafe(ch)
			continue
		}
		// Consume the match so a duplicated change pairs one to one.
		stored[key] = matches[1:]
		replannedChangeOf(matches[0]).stampOnto(ch)
	}
	return scope, nil
}

// unconfirmedUnsafeReason is the unsafe reason a dispatched change carries
// when this deployment holds no re-planned copy of it to judge it by.
const unconfirmedUnsafeReason = "its unsafe verdict could not be confirmed on this deployment, which holds no re-planned copy of this shard's change"

// markUnconfirmedUnsafe fails a change with no re-planned verdict closed: it
// is marked unsafe so admission requires the opt-in for it. A reason the
// change already carries is kept, since it names a judgement that was made.
func markUnconfirmedUnsafe(tc *storage.TableChange) {
	tc.IsUnsafe = true
	if tc.UnsafeReason == "" {
		tc.UnsafeReason = unconfirmedUnsafeReason
	}
}

// shardDispatchCounterparts returns the plan's copies of the changes a dispatch
// scoped to shard can be matched against. A namespace the plan judged per
// shard lists each table once at namespace level, with the text and verdict of
// whichever shard was planned first, while every shard's own changes live on
// its shard row; so for such a namespace the counterparts are the dispatched
// shard's row, and a shard whose changes diverge from its siblings' is matched
// to its own text and verdict rather than to another shard's. Every other
// namespace, including every namespace of a plan materialized from a dispatch
// (which carries no shard rows), contributes its namespace-level changes.
func (c *LocalClient) shardDispatchCounterparts(plan *storage.Plan, shard string) []storage.TableChange {
	carriedByShards := map[string]bool{}
	for _, sp := range plan.Shards {
		if len(sp.Changes) > 0 {
			carriedByShards[c.planNamespace(sp.Namespace)] = true
		}
	}
	var counterparts []storage.TableChange
	for _, tc := range plan.FlatDDLChanges() {
		if carriedByShards[c.planNamespace(tc.Namespace)] {
			continue
		}
		counterparts = append(counterparts, tc)
	}
	for _, sp := range plan.Shards {
		if sp.Shard != shard {
			continue
		}
		for _, tc := range sp.Changes {
			if tc.Namespace == "" {
				tc.Namespace = sp.Namespace
			}
			counterparts = append(counterparts, tc)
		}
	}
	return counterparts
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
// DDL. The parser's canonicalizers return the input unchanged on a parse
// failure, so such input would otherwise compare by raw text and could mask
// drift — Classify errors reject it first. Classify also rejects
// multi-statement input; the one multi-statement shape drift admits is a
// greenfield create set (a CREATE TABLE followed by CREATE INDEX statements on
// that table), canonicalized statement by statement so equivalent spellings
// in the same order compare equal. Admission already fixes the type of every
// statement in the set, so each one is read back from the set rather than
// classified again before the DDL gates below judge it.
func canonicalDDLForDrift(p ddl.StatementParser, raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", fmt.Errorf("empty DDL")
	}
	// An atomic operation is compared as one ordered sequence. Its engine owns
	// grammar admission; ordinary statement and create-set parsing remain strict.
	// RLS retains its physical target: a dispatch does not prove the source
	// deployment’s namespace mapping, so it cannot authorize schema erasure.
	if rls, ok := p.(interface{ CanonicalRowSecurity(string) (string, error) }); ok {
		if canonical, err := rls.CanonicalRowSecurity(raw); err == nil {
			return canonical, nil
		}
	}
	if stmtType, _, err := p.Classify(raw); err == nil {
		return canonicalDriftStatement(p, raw, stmtType)
	}
	// Classify accepts one statement only, so a multi-statement script reaches
	// ParseCreateSet, and its admitted shapes are what keep drift closed: a
	// CREATE TABLE followed only by CREATE INDEX statements on that table. Any
	// other sequence is refused at the first statement outside that shape, so
	// a trailing statement cannot hide behind the classification of the first.
	createSet, err := ddl.ParseCreateSet(p, raw)
	if err != nil {
		return "", fmt.Errorf("DDL rejected by the statement parser: %w", err)
	}
	canonical := make([]string, 0, len(createSet.Statements))
	for i, statement := range createSet.Statements {
		c, err := canonicalDriftStatement(p, statement, createSet.StatementType(i))
		if err != nil {
			return "", fmt.Errorf("statement %d of create set: %w", i+1, err)
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
	// Every drift key carries the change's canonical namespace, so the schema
	// qualifier the engine writes into the DDL is the physical schema of the
	// target that planned it — noise that differs between targets mapping the
	// same namespace to differently named schemas, never signal. That holds
	// for every relation the statement names in that same schema, not only
	// the one it changes: a foreign key's target there is qualified by the
	// same namespace mapping applied to the same desired schema, so two
	// targets can only disagree on it by disagreeing on the mapping, which
	// the key's own namespace already trusts. A reference into any other
	// schema is outside that mapping and stays qualified, so targets that
	// point at different schemas there still diverge.
	return p.CanonicalizeUnqualified(statement), nil
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
		if ch != nil && ch.ChangeType == ternv1.ChangeType_CHANGE_TYPE_VSCHEMA && !dispatchChangeFinalizesOnly(ch) {
			out[c.planNamespace(ch.Namespace)] = true
		}
	}
	return out
}

// finalizeNamespacesFromPlanResult returns the namespaces the recomputed plan
// asks to finalize.
func finalizeNamespacesFromPlanResult(c *LocalClient, result *engine.PlanResult) map[string]bool {
	out := map[string]bool{}
	for _, sc := range result.Changes {
		if sc.NeedsFinalizer() {
			out[c.planNamespace(sc.Namespace)] = true
		}
	}
	return out
}

// finalizeNamespacesFromApplyRequest returns the namespaces the dispatch
// request asks to finalize.
func finalizeNamespacesFromApplyRequest(c *LocalClient, changes []*ternv1.TableChange) map[string]bool {
	out := map[string]bool{}
	for _, ch := range changes {
		if ch != nil && ch.ChangeType == ternv1.ChangeType_CHANGE_TYPE_VSCHEMA && ch.Metadata[engine.MetadataNeedsFinalizer] == "true" {
			out[c.planNamespace(ch.Namespace)] = true
		}
	}
	return out
}

// compareVSchemaParity reports drift unless the recomputed and dispatched sets
// of vschema-changed namespaces are identical.
func compareVSchemaParity(recomputed, dispatched map[string]bool) error {
	missing, unexpected := namespaceSetDifference(recomputed, dispatched)
	if len(missing) == 0 && len(unexpected) == 0 {
		return nil
	}
	return fmt.Errorf("reviewed vschema changes this deployment would not plan: %v; vschema changes this deployment would plan that were not reviewed: %v", missing, unexpected)
}

// compareFinalizeParity reports drift unless the recomputed and dispatched sets
// of namespaces the engine asks to finalize are identical.
func compareFinalizeParity(recomputed, dispatched map[string]bool) error {
	missing, unexpected := namespaceSetDifference(recomputed, dispatched)
	if len(missing) == 0 && len(unexpected) == 0 {
		return nil
	}
	return fmt.Errorf("reviewed keyspace finalizations this deployment would not plan: %v; keyspace finalizations this deployment would plan that were not reviewed: %v", missing, unexpected)
}

// namespaceSetDifference returns, sorted, the dispatched namespaces the
// recomputed set lacks (missing) and the recomputed namespaces the dispatch
// lacks (unexpected).
func namespaceSetDifference(recomputed, dispatched map[string]bool) (missing, unexpected []string) {
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
	sort.Strings(missing)
	sort.Strings(unexpected)
	return missing, unexpected
}
