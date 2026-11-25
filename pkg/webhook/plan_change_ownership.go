package webhook

import (
	"context"
	"maps"
	"slices"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/ddl"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/webhook/templates"
)

// annotateAttributedChanges fills in the plan comment's attributed changes: the
// tables the plan would destroy something on that SchemaBot cannot vouch for as
// this pull request's to destroy.
//
// Attribution matches objects, not just tables. Stored task history names the
// table each task changed and keeps the statement it ran, so a table is
// attributed to an open pull request only when one of the plan's destructive
// changes undoes what that pull request's statements did to it
// (templates.UndoesAttributedWork): dropping a column it added, for example,
// but not dropping a column it never touched beside an index it added.
// Whatever the match cannot read is attributed. Over-attributing is the safe
// direction: it annotates a destructive change rather than leaving one
// unremarked.
//
// SchemaBot's workflow is schema-first — a pull request applies its schema
// change and merges afterwards — so between those two moments the live database
// carries a table no merged tree describes. A plan is a full declarative diff
// of the planning pull request's schema files against that live database, so
// the table reads as one to drop. Stored task history knows better: it records
// which pull request last changed each table.
//
// Reconciling the database to the declared schema stays the operator's call —
// that is what declarative means — so the annotation informs rather than
// blocks, and the plan still offers its apply command.
//
// The lookup fails toward ownership. A storage failure, or a pull-request state
// lookup that fails, annotates the drop as unresolved rather than letting it
// render as a drop with nothing said about it.
//
// rollout is the rollout preview the comment renders, nil when it renders the
// primary plan alone. A target that runs its own plan can destroy something
// on a table the primary plan leaves alone, so the tables every rendered
// target plan destroys something on are looked up as well.
func (h *Handler) annotateAttributedChanges(ctx context.Context, client *ghclient.InstallationClient, data *templates.PlanCommentData, planResp *apitypes.PlanResponse, rollout *templates.DeploymentDriftData, repo string, pr int, environment string) {
	if data == nil {
		return
	}
	statements := plannedDestructiveStatements(planResp)
	for table, stmts := range targetPlanDestructiveStatements(rollout) {
		statements[table] = append(statements[table], stmts...)
	}
	if len(statements) == 0 {
		return
	}
	tables := slices.Sorted(maps.Keys(statements))
	parser, err := ddl.ParserForDialect(schema.DialectForDatabaseType(data.DatabaseType))
	if err != nil {
		h.logger.Warn("planned destructive changes are attributed by table alone: no statement parser serves the database type",
			"repo", repo, "pr", pr, "database", data.Database, "database_type", data.DatabaseType,
			"environment", environment, "error", err)
		parser = nil
	}
	lookup := ownerWorkLookup{h: h, parser: parser, tasks: map[ownerKey][]*storage.Task{}}
	for _, table := range tables {
		ref := storage.TableRef{
			Database:     data.Database,
			DatabaseType: data.DatabaseType,
			Environment:  environment,
			TableName:    table,
		}
		data.AttributedChanges = append(data.AttributedChanges,
			h.classifyDestructiveChange(ctx, client, &lookup, ref, repo, pr, table, statements[table])...)
	}
}

// classifyDestructiveChange decides whether one table's destructive changes
// must be annotated, and how they should read. statements are the planned
// destructive statements on the table. It returns one entry for every open
// pull request whose work one of them undoes, most recent first, since
// different open pull requests can own different objects on the same table and
// each finding carries the note of the owners it undoes. It returns nothing
// only when the lookup positively established that no other open pull request
// did anything to the table that the plan would undo.
func (h *Handler) classifyDestructiveChange(ctx context.Context, client *ghclient.InstallationClient, lookup *ownerWorkLookup, ref storage.TableRef, repo string, pr int, table string, statements []string) []templates.AttributedChangeData {
	owners, err := h.service.Storage().Tasks().FindTableOwners(ctx, ref)
	if err != nil {
		h.logger.Error("planned destructive change's ownership is unresolved: table-ownership lookup failed",
			"repo", repo, "pr", pr, "database", ref.Database, "database_type", ref.DatabaseType,
			"environment", ref.Environment, "table", table, "error", err)
		metrics.RecordPlanChangeOwnership(ctx, repo, ref.Database, ref.Environment, "storage_error")
		return []templates.AttributedChangeData{{Table: table, Unresolved: true}}
	}

	planned := make([]*ddl.ObjectChanges, len(statements))
	for i, stmt := range statements {
		planned[i] = templates.ReadPlannedChange(table, stmt, lookup.parser)
	}
	var checked int
	var attributedOwners []templates.AttributedChangeData
	for _, owner := range owners {
		if owner.Repository == repo && owner.PullRequest == pr {
			// The planning pull request's own earlier apply. Dropping what it
			// itself created is a change it is proposing.
			continue
		}
		checked++
		info, err := client.FetchPullRequest(ctx, owner.Repository, owner.PullRequest)
		if err != nil {
			h.logger.Error("planned destructive change's ownership is unresolved: owning pull request's state could not be read",
				"repo", repo, "pr", pr, "database", ref.Database, "database_type", ref.DatabaseType,
				"environment", ref.Environment, "table", table,
				"owner_repo", owner.Repository, "owner_pr", owner.PullRequest, "error", err)
			metrics.RecordPlanChangeOwnership(ctx, repo, ref.Database, ref.Environment, "pr_state_error")
			return []templates.AttributedChangeData{{Table: table, Unresolved: true}}
		}
		if info.IsClosed() {
			h.logger.Debug("planned destructive change is not attributed to this owner: its pull request is closed",
				"repo", repo, "pr", pr, "database", ref.Database, "environment", ref.Environment,
				"table", table, "owner_repo", owner.Repository, "owner_pr", owner.PullRequest,
				"owner_merged", info.Merged)
			continue
		}
		attributed := templates.AttributedChangeData{
			Table:        table,
			Repository:   owner.Repository,
			PullRequest:  owner.PullRequest,
			OwnerChanges: lookup.ownerChanges(ctx, ref, owner, repo, pr),
		}
		if !slices.ContainsFunc(planned, func(change *ddl.ObjectChanges) bool {
			return templates.UndoesAttributedWork(attributed, change)
		}) {
			h.logger.Debug("planned destructive change is not attributed to this owner: its open pull request changed other objects on the table",
				"repo", repo, "pr", pr, "database", ref.Database, "environment", ref.Environment,
				"table", table, "owner_repo", owner.Repository, "owner_pr", owner.PullRequest)
			continue
		}
		h.logger.Warn("planned destructive change is attributed to another open pull request: it undoes that pull request's change to this table",
			"repo", repo, "pr", pr, "database", ref.Database, "database_type", ref.DatabaseType,
			"environment", ref.Environment, "table", table,
			"owner_repo", owner.Repository, "owner_pr", owner.PullRequest)
		attributedOwners = append(attributedOwners, attributed)
	}
	if len(attributedOwners) > 0 {
		metrics.RecordPlanChangeOwnership(ctx, repo, ref.Database, ref.Environment, "owned")
		return attributedOwners
	}

	h.logger.Debug("planned destructive change is this pull request's to make: it undoes no other open pull request's change to the table",
		"repo", repo, "pr", pr, "database", ref.Database, "environment", ref.Environment, "table", table,
		"other_pull_requests_checked", checked)
	metrics.RecordPlanChangeOwnership(ctx, repo, ref.Database, ref.Environment, "unowned")
	return nil
}

// plannedDestructiveStatements returns, keyed by table, the statements of the
// plan's changes that destroy something on the table — a dropped table,
// column, or index. Each change is judged by the same predicate --allow-unsafe
// is gated on, over the same view: both the namespace-level changes and the
// per-shard ones are read, since a sharded plan can carry a destructive change
// on individual shards that the collapsed view omits, and one confined to a
// single shard is still destructive. The changes that create a table
// (createsTable) are left out: a table the plan creates holds no other pull
// request's work to attribute. Its unsafe findings still require the opt-in,
// so the annotated tables are a subset of the gated ones.
func plannedDestructiveStatements(planResp *apitypes.PlanResponse) map[string][]string {
	statements := map[string][]string{}
	if planResp == nil {
		return statements
	}
	add := func(t *apitypes.TableChangeResponse) {
		if t.TableName == "" {
			return
		}
		if _, unsafe := t.UnsafeChange(); !unsafe || createsTable(t.ChangeType) {
			return
		}
		if !slices.Contains(statements[t.TableName], t.DDL) {
			statements[t.TableName] = append(statements[t.TableName], t.DDL)
		}
	}
	for _, sc := range planResp.Changes {
		if sc == nil {
			continue
		}
		for _, t := range sc.TableChanges {
			add(t)
		}
	}
	for _, sp := range planResp.Shards {
		if sp == nil {
			continue
		}
		for _, t := range sp.Changes {
			add(t)
		}
	}
	return statements
}

// createsTable reports whether a change of this type creates its table. Such a
// change can carry an unsafe finding, such as a lint error on one of its
// columns, but it destroys nothing: the table is not on the target yet. So it
// is never something another pull request applied showing up as one to undo,
// and the attribution notice leaves it out. Every other unsafe change keeps
// the notice, since over-attributing is the safe direction.
func createsTable(changeType string) bool {
	return ddl.OpToStatementType(changeType) == ddl.StatementCreateTable
}

// targetPlanDestructiveStatements returns, keyed by table, the statements of
// the rendered target plans' changes that destroy something beyond the primary
// plan. They are the unsafe changes the unsafe gate counts for those plans
// (templates.TargetPlanUnsafeChanges), so a table a target's plan drops, or
// drops a column or index on, is looked up the way the primary plan's are. A
// VSchema change is not a table's, and a change that creates its table
// destroys nothing, so both are left out as the primary plan's are.
func targetPlanDestructiveStatements(rollout *templates.DeploymentDriftData) map[string][]string {
	statements := map[string][]string{}
	for _, change := range templates.TargetPlanUnsafeChanges(rollout) {
		if change.Table == "" || change.ChangeType == apitypes.VSchemaChangeType || createsTable(change.ChangeType) {
			continue
		}
		if !slices.Contains(statements[change.Table], change.DDL) {
			statements[change.Table] = append(statements[change.Table], change.DDL)
		}
	}
	return statements
}

// ownerKey names one pull request in stored task history.
type ownerKey struct {
	repository  string
	pullRequest int
}

// ownerWorkLookup reads what other pull requests' stored statements did to a
// table, loading each pull request's tasks once per plan comment.
type ownerWorkLookup struct {
	h      *Handler
	parser ddl.StatementParser
	tasks  map[ownerKey][]*storage.Task
}

// ownerChanges merges what the owner's stored statements did to the table in
// ref. It returns nil, so that every planned change on the table is
// attributed, when the owner's tasks cannot be read, when no parser serves
// the database type, or when any of the owner's statements on the table does
// not parse.
func (l *ownerWorkLookup) ownerChanges(ctx context.Context, ref storage.TableRef, owner storage.TableOwner, repo string, pr int) *ddl.ObjectChanges {
	logAttrs := []any{"repo", repo, "pr", pr, "database", ref.Database, "database_type", ref.DatabaseType,
		"environment", ref.Environment, "table", ref.TableName,
		"owner_repo", owner.Repository, "owner_pr", owner.PullRequest}
	if l.parser == nil {
		return nil
	}
	key := ownerKey{repository: owner.Repository, pullRequest: owner.PullRequest}
	tasks, loaded := l.tasks[key]
	if !loaded {
		var err error
		tasks, err = l.h.service.Storage().Tasks().GetByPR(ctx, owner.Repository, owner.PullRequest)
		if err != nil {
			l.h.logger.Error("planned destructive changes are attributed to the owning pull request without matching objects: its stored tasks could not be read",
				append(logAttrs, "error", err)...)
			return nil
		}
		l.tasks[key] = tasks
	}

	var merged ddl.ObjectChanges
	var matched int
	for _, task := range tasks {
		if !taskChangedTable(task, ref) {
			continue
		}
		matched++
		changes, err := l.parser.ObjectChanges(task.DDL)
		if err != nil {
			l.h.logger.Warn("planned destructive changes are attributed to the owning pull request without matching objects: one of its stored statements does not parse",
				append(logAttrs, "task_id", task.TaskIdentifier, "error", err)...)
			return nil
		}
		merged.Added = append(merged.Added, changes.Added...)
		merged.Dropped = append(merged.Dropped, changes.Dropped...)
		merged.Modified = append(merged.Modified, changes.Modified...)
		merged.Referenced = append(merged.Referenced, changes.Referenced...)
	}
	if matched == 0 {
		l.h.logger.Warn("planned destructive changes are attributed to the owning pull request without matching objects: none of its stored tasks changed the table",
			logAttrs...)
		return nil
	}
	return &merged
}

// taskChangedTable reports whether a stored task changed the table in ref, in
// the same database and environment.
func taskChangedTable(task *storage.Task, ref storage.TableRef) bool {
	return task.Database == ref.Database &&
		task.DatabaseType == ref.DatabaseType &&
		task.Environment == ref.Environment &&
		task.TableName == ref.TableName
}
