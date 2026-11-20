package webhook

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/storage"
)

type prSchemaRevision struct {
	Head, Base, BaseRef, DefaultBranch, StackBaseRef string
	NativeStack                                      bool
}

func (r prSchemaRevision) valid() bool {
	return r.Head != "" && r.Base != "" && r.BaseRef != ""
}

func schemaRevision(pr *ghclient.PullRequestInfo) prSchemaRevision {
	return prSchemaRevision{pr.HeadSHA, pr.BaseSHA, pr.BaseRef, pr.DefaultBranch, pr.StackBaseRef, pr.NativeStack}
}

type prPlanDiscoveryKey struct {
	repo         string
	pr           int
	revision     prSchemaRevision
	registration uint64
}

type prPlanDiscovery struct {
	defaultTip          string
	changedFiles, files []ghclient.PRFile
	discovered, managed []ghclient.DiscoveredConfig
	unmanagedFiles      []string
	localDatabases      []string
}

type cachedPRPlanDiscovery struct {
	inputs    *prPlanDiscovery
	expiresAt time.Time
}

// Cache complete, revision-verified discovery only. The TTL bounds the time a
// result can outlive a moving default branch used to narrow a stacked diff.
type prPlanDiscoveryCache struct {
	mu      sync.Mutex
	entries map[prPlanDiscoveryKey]cachedPRPlanDiscovery
}

func (c *prPlanDiscoveryCache) get(key prPlanDiscoveryKey) *prPlanDiscovery {
	c.mu.Lock()
	defer c.mu.Unlock()
	if entry := c.entries[key]; time.Now().Before(entry.expiresAt) {
		return entry.inputs
	}
	delete(c.entries, key)
	return nil
}

func (c *prPlanDiscoveryCache) put(key prPlanDiscoveryKey, inputs *prPlanDiscovery) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.entries == nil {
		c.entries = make(map[prPlanDiscoveryKey]cachedPRPlanDiscovery)
	}
	if len(c.entries) >= 128 {
		var oldest prPlanDiscoveryKey
		var expiry time.Time
		for k, entry := range c.entries {
			if expiry.IsZero() || entry.expiresAt.Before(expiry) {
				oldest, expiry = k, entry.expiresAt
			}
		}
		delete(c.entries, oldest)
	}
	c.entries[key] = cachedPRPlanDiscovery{inputs, time.Now().Add(5 * time.Minute)}
}

func (h *Handler) prPlanDiscoveryKey(repo string, pr int, info *ghclient.PullRequestInfo) prPlanDiscoveryKey {
	return prPlanDiscoveryKey{storage.CanonicalKey(repo), pr, schemaRevision(info), h.config().DatabaseRegistrationRevision()}
}

func (h *Handler) cachedPRPlanDiscovery(ctx context.Context, client *ghclient.InstallationClient, key prPlanDiscoveryKey) (*prPlanDiscovery, error) {
	cached := h.prPlanDiscovery.get(key)
	if cached == nil || cached.defaultTip == "" {
		return cached, nil
	}
	tip, err := client.ResolveDefaultBranchTip(ctx, key.repo)
	if err != nil {
		return nil, fmt.Errorf("verify discovery compare base: %w", err)
	}
	if tip != cached.defaultTip {
		return nil, nil
	}
	return cached, nil
}

func (h *Handler) readPRPlanDiscovery(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, info *ghclient.PullRequestInfo, source, deliveryID string) (*prPlanDiscovery, error) {
	head := info.HeadSHA
	changed, err := client.FetchPRFiles(ctx, repo, pr)
	if err != nil {
		if errors.Is(err, ghclient.ErrPRFilesIncomplete) {
			metrics.RecordPRFileCapExceeded(ctx, repo, ghclient.HasDiscoveryInputFiles(changed))
		}
		return nil, fmt.Errorf("fetch PR files for %s#%d: %w", repo, pr, err)
	}
	files, defaultTip, err := client.PRFilesProposedAgainstDefaultBranch(ctx, repo, head, changed)
	if err != nil {
		return nil, fmt.Errorf("scope PR files for %s#%d to what it proposes: %w", repo, pr, err)
	}
	if len(files) != len(changed) {
		h.logger.Info("auto-plan scoped to the files the PR proposes against the default branch",
			"repo", repo, "pr", pr, "head_sha", head, "default_tip_sha", defaultTip,
			"changed_files", len(changed), "proposed_files", len(files), "source", source, "delivery_id", deliveryID)
	}
	discovered, err := client.FindConfigsForPRFiles(ctx, repo, head, files)
	if err != nil {
		return nil, fmt.Errorf("discover configs for %s#%d: %w", repo, pr, err)
	}
	unmanaged := h.unmanagedServerManagedSchemaChanges(repo, files, discovered)
	managed := h.filterManagedDiscoveredConfigs(ctx, repo, pr, head, source, slices.Clone(discovered))
	discovery := &prPlanDiscovery{defaultTip: defaultTip, changedFiles: changed, files: files, discovered: discovered, managed: managed, unmanagedFiles: unmanaged}
	if h.config().PRSchemaCapForRepo(repo).Limit > 0 {
		localConfigs := h.localPRPlanConfigs(repo, pr, managed)
		discovery.localDatabases = managedDatabaseNames(localConfigs)
	}
	return discovery, nil
}

// Separate locally routable configurations from repository-wide candidates.
func (h *Handler) localPRPlanConfigs(repo string, pr int, managed []ghclient.DiscoveredConfig) []ghclient.DiscoveredConfig {
	cfg := h.config()
	if cfg.TargetResolver.Enabled() {
		return managed
	}
	databases := cfg.DatabaseConfigs()
	local := make([]ghclient.DiscoveredConfig, 0, len(managed))
	for _, entry := range managed {
		db, ok := databases[entry.Config.Database]
		if !ok || cfg.AuthorizePlanSource(api.PlanSourcePolicyRequest{
			Database: entry.Config.Database, Repository: repo, PullRequest: pr, SchemaPath: entry.SchemaDir,
		}) != nil {
			continue
		}
		for env := range db.Environments {
			if cfg.IsEnvironmentAllowed(env) {
				local = append(local, entry)
				break
			}
		}
	}
	return local
}
