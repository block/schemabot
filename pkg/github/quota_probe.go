package github

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"

	gh "github.com/google/go-github/v86/github"

	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/panicsafe"
)

// QuotaProbeInterval is how often the quota probe reads each installation's
// budget. It is half the snapshot export age, so one failed read still leaves
// the previous reading exported until the next read replaces it.
const QuotaProbeInterval = metrics.GitHubRateLimitSnapshotMaxAge / 2

// quotaListTimeout bounds listing the App's installations at the start of a
// pass.
const quotaListTimeout = QuotaProbeInterval / 2

// quotaReadTimeout bounds one installation's budget read. Each read has its own
// bound, so an installation that hangs costs only its own read and every other
// installation is still read in the same pass.
const quotaReadTimeout = QuotaProbeInterval / 4

// quotaReadConcurrency is how many installations a pass reads at once, so a
// few slow installations do not hold up the rest while the request rate to
// GitHub stays bounded.
const quotaReadConcurrency = 8

// QuotaProbe keeps every installation's GitHub budget observed while the
// server is idle. Webhook traffic records quota from the headers on each
// response, but an installation with no traffic stops reporting, and its
// budget becomes unknown once the last reading ages out. The probe reads
// GET /rate_limit for each installation, which GitHub does not count against
// the budget it reports, and the response's quota headers are recorded through
// the same transport as every other request. A missing budget therefore means
// the probe cannot reach GitHub, not that the server has been quiet.
type QuotaProbe struct {
	logger            *slog.Logger
	interval          time.Duration
	listTimeout       time.Duration
	readTimeout       time.Duration
	readConcurrency   int
	listInstallations func(ctx context.Context) (quotaTargets, error)
	readQuota         func(ctx context.Context, installationID int64) error
	// reportTargets publishes each pass's targets, so an installation the
	// probe is responsible for counts as unobserved while it has no fresh
	// budget reading.
	reportTargets func(quotaTargets)
	// lastTargets is the last set of targets the probe listed. Only the
	// probe's own pass touches it, and passes run one at a time.
	lastTargets quotaTargets

	mu     sync.Mutex
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// quotaTargets is what one probe pass reads: the installations whose budget
// the probe keeps observed, and how many configured repository owners had no
// resolvable installation on this pass. An unresolved owner counts as an
// unobserved installation, since a budget that serves configured repositories
// is unknown rather than known to be fine.
type quotaTargets struct {
	installations []int64
	unresolved    int
}

// NewQuotaProbe returns a probe over every active installation of the App that
// c authenticates as. It is for a server whose config does not enumerate the
// repositories it serves, so any installation may serve one. Only a server
// configured with a GitHub App builds one, and each replica runs its own, so
// the probe's reads grow with the replica count while none of them spends the
// core budget.
func (c *Client) NewQuotaProbe() *QuotaProbe {
	return c.newQuotaProbe(c.listActiveInstallations)
}

// NewServingQuotaProbe returns a probe over the installations that serve the
// configured repos, resolved each pass with one installation lookup per
// repository owner. An installation of the App that serves none of them,
// such as one on an account the server is not configured for, is not read, so
// its untouched budget neither stands in for a serving installation running
// low nor holds back a rollout gate.
func (c *Client) NewServingQuotaProbe(repos []string) *QuotaProbe {
	repos = slices.Clone(repos)
	return c.newQuotaProbe(func(ctx context.Context) (quotaTargets, error) {
		return c.servingInstallations(ctx, repos), nil
	})
}

func (c *Client) newQuotaProbe(listInstallations func(ctx context.Context) (quotaTargets, error)) *QuotaProbe {
	return &QuotaProbe{
		logger:            c.logger,
		interval:          QuotaProbeInterval,
		listTimeout:       quotaListTimeout,
		readTimeout:       quotaReadTimeout,
		readConcurrency:   quotaReadConcurrency,
		listInstallations: listInstallations,
		reportTargets: func(targets quotaTargets) {
			metrics.SetGitHubQuotaProbeTargets(c.appID, c.loadAppSlug(), targets.installations, targets.unresolved)
		},
		readQuota: func(ctx context.Context, installationID int64) error {
			ic, err := c.forInstallation(ctx, installationID)
			if err != nil {
				return fmt.Errorf("create installation client: %w", err)
			}
			if _, _, _, err := ic.CoreRateLimit(ctx); err != nil {
				return err
			}
			return nil
		},
	}
}

// MarkUnresolved records the App as having one unobserved installation until
// its first pass resolves real targets, so the App exports a failing
// unobserved count from the moment it is marked rather than no series at all.
// Without it, a gate that aggregates across Apps could pass on the Apps that
// have finished a pass while another's budget is still unknown. A first pass
// that cannot list installations leaves the mark in place.
func (p *QuotaProbe) MarkUnresolved() {
	p.reportTargets(quotaTargets{unresolved: 1})
}

// Start reads every installation's budget now and then once per interval,
// until ctx is canceled or Stop is called. Calling Start on a running probe
// does nothing.
func (p *QuotaProbe) Start(ctx context.Context) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cancel != nil {
		p.logger.Warn("GitHub quota probe already running; ignoring second start")
		return
	}
	ctx, p.cancel = context.WithCancel(ctx)
	p.logger.Info("GitHub quota probe started", "interval", p.interval)
	p.wg.Go(func() {
		ticker := time.NewTicker(p.interval)
		defer ticker.Stop()
		for {
			p.probeOnce(ctx)
			select {
			case <-ctx.Done():
				p.logger.Info("GitHub quota probe stopped")
				return
			case <-ticker.C:
			}
		}
	})
}

// Stop cancels the probe and waits for an in-flight pass to return. The wait is
// bounded by the per-read timeout, since every read observes the cancellation.
// A pass only reads GitHub and writes nothing, so canceling it loses nothing.
func (p *QuotaProbe) Stop() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cancel == nil {
		return
	}
	p.cancel()
	p.wg.Wait()
	p.cancel = nil
}

// probeOnce reads the budget of every installation once. Installations are
// independent: one that fails or hangs is logged once its own read times out,
// the rest are still read, and the failed one's budget ages out to unknown on
// its own. Nothing the probe hits, GitHub being down included, ends the
// process: errors are logged, every request is bounded by a timeout, and a
// panic is contained to the list or the one read that raised it (AV-5).
func (p *QuotaProbe) probeOnce(ctx context.Context) {
	targets, err := p.listContained(ctx)
	if err != nil {
		p.logger.Warn("GitHub quota probe could not list App installations; counting the App's installation set as unobserved until a later pass lists it",
			"error", err)
		p.reportTargets(listFailedTargets(p.lastTargets))
		return
	}
	p.lastTargets = targets
	// Report the targets as soon as they are listed, so the gauge counts this
	// pass's installations and unresolved owners while the reads run, and
	// again once the reads are done: the App slug that labels the targets can
	// be learned during the first pass's reads, and the targets must carry the
	// same label as the snapshots those reads record.
	p.reportTargets(targets)
	defer p.reportTargets(targets)
	ids := targets.installations
	if len(ids) == 0 {
		p.logger.Debug("GitHub quota probe has no installations to read", "unresolved_owners", targets.unresolved)
		return
	}
	slots := make(chan struct{}, p.readConcurrency)
	var wg sync.WaitGroup
	defer wg.Wait()
	for _, id := range ids {
		select {
		case slots <- struct{}{}:
		case <-ctx.Done():
			p.logger.Debug("GitHub quota probe stopped before reading every installation", "error", ctx.Err())
			return
		}
		wg.Go(func() {
			defer func() { <-slots }()
			p.readInstallation(ctx, id)
		})
	}
}

// listFailedTargets is what a pass reports when it cannot list the App's
// installations: the last listed installations, still observed while their
// snapshots stay fresh, plus one unresolved. Webhook traffic keeps refreshing
// the snapshots of installations already known, so without the unresolved
// count an installation added since the last successful list would go
// unseen and a quota gate could pass on a partial set.
func listFailedTargets(last quotaTargets) quotaTargets {
	return quotaTargets{
		installations: last.installations,
		unresolved:    last.unresolved + 1,
	}
}

// listContained lists the App's installations under the list timeout,
// converting a panic into an error so the pass ends instead of the process.
func (p *QuotaProbe) listContained(ctx context.Context) (quotaTargets, error) {
	ctx, cancel := context.WithTimeout(ctx, p.listTimeout)
	defer cancel()
	var targets quotaTargets
	recovered, err := panicsafe.Catch(func() error {
		var listErr error
		targets, listErr = p.listInstallations(ctx)
		return listErr
	})
	if recovered != nil {
		p.recordPanic(ctx, recovered, "list App installations")
		return quotaTargets{}, recovered
	}
	return targets, err
}

// readInstallation reads one installation's budget under its own timeout. A
// panic is contained to this read; the other installations are still read.
func (p *QuotaProbe) readInstallation(ctx context.Context, installationID int64) {
	ctx, cancel := context.WithTimeout(ctx, p.readTimeout)
	defer cancel()
	recovered, err := panicsafe.Catch(func() error {
		return p.readQuota(ctx, installationID)
	})
	if recovered != nil {
		p.recordPanic(ctx, recovered, "read installation budget", "installation_id", installationID)
		return
	}
	if err != nil {
		p.logger.Warn("GitHub quota probe could not read installation budget; its budget will age out to unknown until a later pass succeeds",
			"installation_id", installationID, "error", err)
	}
}

// recordPanic logs and counts a panic the probe contained. The affected budget
// ages out to unknown like any failed read, and the next pass tries again.
func (p *QuotaProbe) recordPanic(ctx context.Context, recovered *panicsafe.Error, step string, attrs ...any) {
	p.logger.Error("GitHub quota probe panicked; the panic was contained and the affected budget will age out to unknown",
		append([]any{"step", step, "panic", fmt.Sprint(recovered.Value), "stack", string(recovered.Stack)}, attrs...)...)
	metrics.RecordRecoveredPanic(context.WithoutCancel(ctx), "quota_probe")
}

// listActiveInstallations lists the App's installations through the App JWT,
// skipping suspended ones, which cannot mint the token a budget read needs.
func (c *Client) listActiveInstallations(ctx context.Context) (quotaTargets, error) {
	appClient, err := c.jwtClient()
	if err != nil {
		return quotaTargets{}, fmt.Errorf("list installations: %w", err)
	}
	ids, err := activeInstallationIDs(ctx, appClient, c.logger)
	if err != nil {
		return quotaTargets{}, err
	}
	return quotaTargets{installations: ids}, nil
}

// servingInstallations resolves the installations serving repos, one owner at
// a time, with a fresh lookup on every pass rather than the cached answer, so
// an App that was uninstalled and reinstalled is followed to its new
// installation within a pass instead of being probed under its retired id
// until the server restarts. An owner-wide answer also refreshes the cache
// webhook dispatch reads. The lookup uses the App JWT, which does not spend an
// installation's budget, and costs one request per owner per pass, not one per
// repository: the first repository's answer stands for the whole owner when
// the installation belongs to the owner's account.
//
// A suspended installation is skipped like a repository the App is not
// installed on, matching the all-installations listing: it cannot mint the
// token a budget read needs, and it serves no SchemaBot work until the owner
// unsuspends it, so its budget is not one a gate should wait on.
//
// A lookup that fails for any reason other than the App not being installed
// counts the owner as unresolved and moves on, so GitHub being down fails
// closed at one lookup per owner. Once the pass's deadline has passed, the
// owners not yet resolved count as unresolved without a lookup. A repository
// the App is not installed on has no budget to observe; the owner's other
// repositories are still tried, as they are after an answer that stands only
// for the repository asked about, since an installation limited to selected
// repositories, or on another account, can cover some of them.
func (c *Client) servingInstallations(ctx context.Context, repos []string) quotaTargets {
	byOwner := make(map[string][]string)
	for _, repo := range repos {
		owner, _ := splitRepo(repo)
		ownerKey := strings.ToLower(owner)
		byOwner[ownerKey] = append(byOwner[ownerKey], repo)
	}
	owners := slices.Sorted(maps.Keys(byOwner))
	var targets quotaTargets
	seen := make(map[int64]bool)
	for i, ownerKey := range owners {
		if err := ctx.Err(); err != nil {
			skipped := len(owners) - i
			c.logger.Warn("GitHub quota probe ran out of time resolving installations; counting the remaining owners' budgets as unobserved",
				"unresolved_owners", skipped, "error", err)
			targets.unresolved += skipped
			break
		}
		for _, repo := range byOwner[ownerKey] {
			owner, name := splitRepo(repo)
			lookup, err := c.lookUpRepoInstallation(ctx, repo, owner, name)
			if errors.Is(err, ErrAppNotInstalled) {
				c.logger.Warn("GitHub quota probe found a configured repository the App is not installed on; no installation serves it, so it has no budget to observe",
					"repo", repo, "error", err)
				continue
			}
			if err != nil {
				c.logger.Warn("GitHub quota probe could not resolve the installation serving a configured repository; counting the owner's budget as unobserved",
					"repo", repo, "owner", ownerKey, "error", err)
				targets.unresolved++
				break
			}
			if lookup.suspended {
				c.logger.Warn("GitHub quota probe found a configured repository served by a suspended installation; it cannot mint the token a budget read needs and serves no requests until it is unsuspended, so it has no budget to observe",
					"repo", repo, "installation_id", lookup.id)
			} else if !seen[lookup.id] {
				seen[lookup.id] = true
				targets.installations = append(targets.installations, lookup.id)
			}
			if lookup.ownerWide {
				break
			}
			c.logger.Debug("GitHub quota probe installation answers only the repository asked about; resolving the owner's other repositories",
				"repo", repo, "installation_id", lookup.id)
		}
	}
	return targets
}

func activeInstallationIDs(ctx context.Context, appClient *gh.Client, logger *slog.Logger) ([]int64, error) {
	var ids []int64
	opts := &gh.ListOptions{PerPage: 100}
	for {
		installations, resp, err := appClient.Apps.ListInstallations(ctx, opts)
		if err != nil {
			return nil, fmt.Errorf("list App installations page %d: %w", opts.Page, err)
		}
		for _, installation := range installations {
			if installation.SuspendedAt != nil {
				logger.Debug("GitHub quota probe skipping suspended installation",
					"installation_id", installation.GetID(), "account", installation.GetAccount().GetLogin())
				continue
			}
			ids = append(ids, installation.GetID())
		}
		if resp == nil || resp.NextPage == 0 {
			return ids, nil
		}
		opts.Page = resp.NextPage
	}
}
