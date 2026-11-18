package webhook

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"slices"
	"strings"
	"sync"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/schema"
	"golang.org/x/sync/singleflight"
)

type prSchemaRevision struct {
	Head, BaseRef, Base string
}

func schemaRevision(pr *ghclient.PullRequestInfo) prSchemaRevision {
	return prSchemaRevision{Head: pr.HeadSHA, BaseRef: pr.BaseRef, Base: pr.BaseSHA}
}

func (r prSchemaRevision) valid() bool {
	return r.Head != "" && r.BaseRef != "" && r.Base != ""
}

type prSchemaDiscoveryKey struct {
	Repo          string
	PR            int
	Revision      prSchemaRevision
	DefaultTip    string
	BaseTip       string
	Registrations [32]byte
}

type prSchemaDiscovery struct {
	changed, proposed []ghclient.PRFile
	configs           []ghclient.DiscoveredConfig
	schemas           []ghclient.PRSchema
}

type prSchemaDiscoveryCache struct {
	mu      sync.Mutex
	entries map[prSchemaDiscoveryKey]*prSchemaDiscovery
	group   singleflight.Group
}

const maxCachedPRSchemaDiscoveries = 128

func (c *prSchemaDiscoveryCache) get(key prSchemaDiscoveryKey) *prSchemaDiscovery {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.entries[key]
}

func (c *prSchemaDiscoveryCache) put(key prSchemaDiscoveryKey, discovery *prSchemaDiscovery) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.entries == nil {
		c.entries = make(map[prSchemaDiscoveryKey]*prSchemaDiscovery)
	}
	if len(c.entries) >= maxCachedPRSchemaDiscoveries {
		clear(c.entries)
	}
	c.entries[key] = discovery
}

func (h *Handler) discoverPRSchemaCap(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, revision prSchemaRevision) (*prSchemaDiscovery, error) {
	if !revision.valid() {
		return nil, fmt.Errorf("GitHub returned no head commit or base revision for %s#%d", repo, pr)
	}
	defaultBranch, defaultTip, err := client.DefaultBranchTipForSchemaDiscovery(ctx, repo)
	if err != nil {
		return nil, err
	}
	baseTip := defaultTip
	if revision.BaseRef != defaultBranch {
		baseTip, err = client.BranchTipSHA(ctx, repo, revision.BaseRef)
		if err != nil {
			return nil, err
		}
	}
	registrations, err := h.config().PRSchemaCapRegistrations(repo)
	if err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(registrations)
	if err != nil {
		return nil, fmt.Errorf("encode repo-wide schema registrations: %w", err)
	}
	key := prSchemaDiscoveryKey{repo, pr, revision, defaultTip, baseTip, sha256.Sum256(encoded)}
	if cached := h.prSchemaDiscovery.get(key); cached != nil {
		return cached, nil
	}

	result := h.prSchemaDiscovery.group.DoChan(fmt.Sprintf("%#v", key), func() (any, error) {
		if cached := h.prSchemaDiscovery.get(key); cached != nil {
			return cached, nil
		}
		discovery, err := discoverPRSchemaInputs(ctx, client, key, registrations)
		if err == nil {
			h.prSchemaDiscovery.put(key, discovery)
		}
		return discovery, err
	})
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case result := <-result:
		if result.Err != nil {
			return nil, result.Err
		}
		return result.Val.(*prSchemaDiscovery), nil
	}
}

func discoverPRSchemaInputs(ctx context.Context, client *ghclient.InstallationClient, key prSchemaDiscoveryKey, registrations map[string]api.PRSchemaCapRegistration) (*prSchemaDiscovery, error) {
	dirs := prSchemaConfigDirs(registrations)
	files, mergeBase, err := client.FetchPRFilesForSchemaDirs(ctx, key.Repo, key.PR, key.Revision.Head, key.BaseTip, dirs)
	if err != nil {
		if errors.Is(err, ghclient.ErrPRFilesIncomplete) {
			metrics.RecordPRFileCapExceeded(ctx, key.Repo, ghclient.HasDiscoveryInputFiles(files))
		}
		return nil, err
	}
	proposed, err := client.PRFilesProposedAgainstTip(ctx, key.Repo, key.Revision.Head, key.DefaultTip, files)
	if err != nil {
		return nil, err
	}
	configs, err := client.FindConfigsForPRFiles(ctx, key.Repo, key.Revision.Head, proposed)
	if err != nil {
		return nil, err
	}
	discovery := &prSchemaDiscovery{changed: files, proposed: proposed, configs: configs}
	if len(registrations) > 0 {
		refs := []string{key.Revision.Head}
		for _, ref := range []string{mergeBase, key.DefaultTip} {
			if ref != "" && !slices.Contains(refs, ref) {
				refs = append(refs, ref)
			}
		}
		var affected []ghclient.DiscoveredConfig
		discovery.schemas, affected, err = client.DiscoverPRSchemas(ctx, key.Repo, refs, dirs, proposed,
			func(cfg ghclient.DiscoveredConfig) []string {
				r, ok := registrations[cfg.Config.Database]
				if !ok || r.Type != string(cfg.Config.Type) || !r.AllowsPath(cfg.SchemaDir) {
					return nil
				}
				return r.Environments
			})
		if err != nil {
			return nil, err
		}
		paths := make(map[string]bool, len(configs))
		for _, config := range configs {
			paths[config.Path] = true
		}
		for _, config := range affected {
			if !paths[config.Path] {
				discovery.configs = append(discovery.configs, config)
			}
		}
	}
	// FetchPRFiles is a live read. Before caching under immutable commits,
	// confirm that the inputs were still the PR's actual revision as it ran.
	fresh, err := client.FetchPullRequestNoCache(ctx, key.Repo, key.PR)
	if err != nil {
		return nil, err
	}
	currentBase, err := client.BranchTipSHA(ctx, key.Repo, key.Revision.BaseRef)
	if err != nil {
		return nil, err
	}
	if schemaRevision(fresh) != key.Revision {
		return nil, errPRSchemaRevisionMoved
	}
	if currentBase != key.BaseTip {
		return nil, fmt.Errorf("base branch %s moved during PR schema discovery", key.Revision.BaseRef)
	}
	return discovery, nil
}

func prSchemaConfigDirs(registrations map[string]api.PRSchemaCapRegistration) []string {
	var dirs []string
	for _, registration := range registrations {
		if len(registration.Paths) == 0 {
			return nil
		}
		for _, dir := range registration.Paths {
			dir = path.Clean(strings.TrimSpace(dir))
			if dir == "." || dir == "*" {
				return nil
			}
			dirs = append(dirs, dir)
		}
	}
	slices.Sort(dirs)
	var minimal []string
	for _, dir := range slices.Compact(dirs) {
		if !slices.ContainsFunc(minimal, func(parent string) bool { return schema.PathWithinDirectory(parent, dir) }) {
			minimal = append(minimal, dir)
		}
	}
	return minimal
}
