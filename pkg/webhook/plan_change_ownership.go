package webhook

import (
	"context"
	"sort"

	"github.com/block/schemabot/pkg/apitypes"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/webhook/templates"
)

// annotateAttributedChanges fills in the plan comment's attributed changes: the
// tables the plan would destroy something on that SchemaBot cannot vouch for as
// this pull request's to destroy.
//
// Attribution is table-grained. Stored task history names the table a task
// changed and nothing finer, so the notice can say an open pull request last
// changed this table, never that it created the column or index being dropped.
// Over-attributing is the safe direction: it annotates a destructive change
// rather than leaving one unremarked.
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
func (h *Handler) annotateAttributedChanges(ctx context.Context, client *ghclient.InstallationClient, data *templates.PlanCommentData, planResp *apitypes.PlanResponse, repo string, pr int, environment string) {
	if data == nil {
		return
	}
	tables := plannedDestructiveTables(planResp)
	if len(tables) == 0 {
		return
	}
	gated := unsafeGateTables(planResp)
	for _, table := range tables {
		ref := storage.TableRef{
			Database:     data.Database,
			DatabaseType: data.DatabaseType,
			Environment:  environment,
			TableName:    table,
		}
		change, annotate := h.classifyDestructiveChange(ctx, client, ref, repo, pr, table)
		if annotate {
			_, consentSolicited := gated[table]
			change.OutsideUnsafeGate = !consentSolicited
			data.AttributedChanges = append(data.AttributedChanges, change)
		}
	}
}

// unsafeGateTables returns the tables the --allow-unsafe opt-in gate reads:
// the namespace-level unsafe changes. A destructive change confined to
// individual shards is not among them, so applying never solicits consent
// for it — the attribution disclosure is then the operator's only notice.
func unsafeGateTables(planResp *apitypes.PlanResponse) map[string]struct{} {
	gated := map[string]struct{}{}
	for _, unsafe := range planResp.UnsafeChanges() {
		gated[unsafe.Table] = struct{}{}
	}
	return gated
}

// classifyDestructiveChange decides whether one table's destructive change must
// be annotated, and how it should read. annotate is false only when the lookup
// positively established that no other pull request has an open claim on the
// table.
func (h *Handler) classifyDestructiveChange(ctx context.Context, client *ghclient.InstallationClient, ref storage.TableRef, repo string, pr int, table string) (change templates.AttributedChangeData, annotate bool) {
	owners, err := h.service.Storage().Tasks().FindTableOwners(ctx, ref)
	if err != nil {
		h.logger.Error("planned destructive change's ownership is unresolved: table-ownership lookup failed",
			"repo", repo, "pr", pr, "database", ref.Database, "database_type", ref.DatabaseType,
			"environment", ref.Environment, "table", table, "error", err)
		metrics.RecordPlanChangeOwnership(ctx, repo, ref.Database, ref.Environment, "storage_error")
		return templates.AttributedChangeData{Table: table, Unresolved: true}, true
	}

	var checked int
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
			return templates.AttributedChangeData{Table: table, Unresolved: true}, true
		}
		if info.IsClosed() {
			h.logger.Debug("planned destructive change is not attributed to this owner: its pull request is closed",
				"repo", repo, "pr", pr, "database", ref.Database, "environment", ref.Environment,
				"table", table, "owner_repo", owner.Repository, "owner_pr", owner.PullRequest,
				"owner_merged", info.Merged)
			continue
		}
		h.logger.Warn("planned destructive change is attributed to another open pull request: it last changed this table",
			"repo", repo, "pr", pr, "database", ref.Database, "database_type", ref.DatabaseType,
			"environment", ref.Environment, "table", table,
			"owner_repo", owner.Repository, "owner_pr", owner.PullRequest)
		metrics.RecordPlanChangeOwnership(ctx, repo, ref.Database, ref.Environment, "owned")
		return templates.AttributedChangeData{
			Table:       table,
			Repository:  owner.Repository,
			PullRequest: owner.PullRequest,
		}, true
	}

	if checked == 0 {
		h.logger.Debug("planned destructive change is this pull request's to make: no other pull request is attributed the table",
			"repo", repo, "pr", pr, "database", ref.Database, "environment", ref.Environment, "table", table)
	}
	metrics.RecordPlanChangeOwnership(ctx, repo, ref.Database, ref.Environment, "unowned")
	return templates.AttributedChangeData{}, false
}

// plannedDestructiveTables returns the distinct tables the plan would destroy
// something on — a dropped table, column, or index — in a stable order. Each
// table is judged by the same predicate --allow-unsafe is gated on, but over a
// wider view: both the namespace-level changes and the per-shard ones are read,
// where the unsafe gate reads only the namespace-level ones. A sharded plan can
// carry a destructive change on individual shards that the collapsed view
// omits, and one confined to a single shard is still destructive — so the
// annotation is a superset of what the opt-in gates, which is the safe
// direction for a disclosure.
func plannedDestructiveTables(planResp *apitypes.PlanResponse) []string {
	if planResp == nil {
		return nil
	}
	seen := map[string]struct{}{}
	add := func(t *apitypes.TableChangeResponse) {
		if t.TableName == "" {
			return
		}
		if _, unsafe := t.UnsafeChange(); !unsafe {
			return
		}
		seen[t.TableName] = struct{}{}
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
	tables := make([]string, 0, len(seen))
	for table := range seen {
		tables = append(tables, table)
	}
	sort.Strings(tables)
	return tables
}
