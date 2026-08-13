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

// annotateOwnedDrops fills in the plan comment's held-back drops: the objects
// the plan would drop that SchemaBot cannot vouch for as this pull request's to
// drop.
//
// SchemaBot's workflow is schema-first — a pull request applies its schema
// change and merges afterwards — so between those two moments the live database
// carries an object no merged tree describes. A plan is a full declarative diff
// of the planning pull request's schema files against that live database, so
// the object reads as one to drop. Stored task history knows better: it records
// which pull request last changed each object.
//
// The lookup fails toward ownership. A storage failure, or a pull-request state
// lookup that fails, annotates the drop as unresolved rather than letting it
// fall through to a bare drop with an apply prompt beside it.
func (h *Handler) annotateOwnedDrops(ctx context.Context, client *ghclient.InstallationClient, data *templates.PlanCommentData, planResp *apitypes.PlanResponse, repo string, pr int, environment string) {
	if data == nil {
		return
	}
	tables := plannedDropTables(planResp)
	if len(tables) == 0 {
		return
	}
	for _, table := range tables {
		ref := storage.ObjectRef{
			Database:     data.Database,
			DatabaseType: data.DatabaseType,
			Environment:  environment,
			TableName:    table,
		}
		drop, annotate := h.classifyPlannedDrop(ctx, client, ref, repo, pr, table)
		if annotate {
			data.OwnedDrops = append(data.OwnedDrops, drop)
		}
	}
}

// classifyPlannedDrop decides whether one planned drop must be held back, and
// how it should read. annotate is false only when the lookup positively
// established that no other pull request has an open claim on the object.
func (h *Handler) classifyPlannedDrop(ctx context.Context, client *ghclient.InstallationClient, ref storage.ObjectRef, repo string, pr int, table string) (drop templates.OwnedDropData, annotate bool) {
	owners, err := h.service.Storage().Tasks().FindObjectOwners(ctx, ref)
	if err != nil {
		h.logger.Error("planned drop will be held back: object-ownership lookup failed",
			"repo", repo, "pr", pr, "database", ref.Database, "database_type", ref.DatabaseType,
			"environment", ref.Environment, "table", table, "error", err)
		metrics.RecordPlanDropOwnership(ctx, repo, ref.Database, ref.Environment, "storage_error")
		return templates.OwnedDropData{Table: table, Unresolved: true}, true
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
			h.logger.Error("planned drop will be held back: owning pull request's state could not be read",
				"repo", repo, "pr", pr, "database", ref.Database, "database_type", ref.DatabaseType,
				"environment", ref.Environment, "table", table,
				"owner_repo", owner.Repository, "owner_pr", owner.PullRequest, "error", err)
			metrics.RecordPlanDropOwnership(ctx, repo, ref.Database, ref.Environment, "pr_state_error")
			return templates.OwnedDropData{Table: table, Unresolved: true}, true
		}
		if info.IsClosed() {
			h.logger.Debug("planned drop is not held back by this owner: its pull request is closed",
				"repo", repo, "pr", pr, "database", ref.Database, "environment", ref.Environment,
				"table", table, "owner_repo", owner.Repository, "owner_pr", owner.PullRequest,
				"owner_merged", info.Merged)
			continue
		}
		h.logger.Warn("planned drop will be held back: an open pull request last changed this object",
			"repo", repo, "pr", pr, "database", ref.Database, "database_type", ref.DatabaseType,
			"environment", ref.Environment, "table", table,
			"owner_repo", owner.Repository, "owner_pr", owner.PullRequest)
		metrics.RecordPlanDropOwnership(ctx, repo, ref.Database, ref.Environment, "owned")
		return templates.OwnedDropData{
			Table:       table,
			Repository:  owner.Repository,
			PullRequest: owner.PullRequest,
		}, true
	}

	if checked == 0 {
		h.logger.Debug("planned drop is this pull request's to make: no other pull request is attributed the object",
			"repo", repo, "pr", pr, "database", ref.Database, "environment", ref.Environment, "table", table)
	}
	metrics.RecordPlanDropOwnership(ctx, repo, ref.Database, ref.Environment, "unowned")
	return templates.OwnedDropData{}, false
}

// plannedDropTables returns the distinct tables the plan would drop, in a
// stable order. Both the namespace-level view and the per-shard view are read:
// a sharded plan can carry a drop on individual shards that the collapsed view
// omits, and a drop confined to one shard is still a drop.
func plannedDropTables(planResp *apitypes.PlanResponse) []string {
	if planResp == nil {
		return nil
	}
	seen := map[string]struct{}{}
	add := func(t *apitypes.TableChangeResponse) {
		if !t.DropsTable() || t.TableName == "" {
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
