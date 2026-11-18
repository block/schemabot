package github

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/bradleyfalzon/ghinstallation/v2"
	gh "github.com/google/go-github/v86/github"

	"github.com/block/schemabot/pkg/metrics"
)

// QuotaProbeInterval is how often the quota probe reads each installation's
// budget. It is half the snapshot export age, so one failed read still leaves
// the previous reading exported until the next read replaces it.
const QuotaProbeInterval = metrics.GitHubRateLimitSnapshotMaxAge / 2

// quotaProbeTimeout bounds one probe pass, so a slow GitHub cannot stack
// passes behind each other.
const quotaProbeTimeout = QuotaProbeInterval

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
	listInstallations func(ctx context.Context) ([]int64, error)
	readQuota         func(ctx context.Context, installationID int64) error

	mu     sync.Mutex
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewQuotaProbe returns a probe over every installation of the App that c
// authenticates as.
func (c *Client) NewQuotaProbe() *QuotaProbe {
	return &QuotaProbe{
		logger:            c.logger,
		interval:          QuotaProbeInterval,
		listInstallations: c.listActiveInstallations,
		readQuota: func(ctx context.Context, installationID int64) error {
			ic, err := c.ForInstallation(installationID)
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
	ctx, p.cancel = context.WithCancel(context.WithoutCancel(ctx))
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

// Stop cancels the probe and waits for an in-flight pass to return. A pass
// only reads GitHub and writes nothing, so canceling it loses nothing.
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
// independent: one that fails to answer is logged and the rest are still read,
// and the failed one's budget ages out to unknown on its own.
func (p *QuotaProbe) probeOnce(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, quotaProbeTimeout)
	defer cancel()
	ids, err := p.listInstallations(ctx)
	if err != nil {
		p.logger.Warn("GitHub quota probe could not list App installations; installation budgets will age out to unknown until a later pass succeeds",
			"error", err)
		return
	}
	if len(ids) == 0 {
		p.logger.Debug("GitHub quota probe found no active App installations")
		return
	}
	for _, id := range ids {
		if err := p.readQuota(ctx, id); err != nil {
			p.logger.Warn("GitHub quota probe could not read installation budget; its budget will age out to unknown until a later pass succeeds",
				"installation_id", id, "error", err)
		}
	}
}

// listActiveInstallations lists the App's installations through the App JWT,
// skipping suspended ones, which cannot mint the token a budget read needs.
func (c *Client) listActiveInstallations(ctx context.Context) ([]int64, error) {
	appBaseTransport := newGitHubRateLimitTransport(newGitHubMetricsTransport(http.DefaultTransport, 0, c.loadAppSlug))
	appTransport, err := ghinstallation.NewAppsTransport(appBaseTransport, c.appID, c.privateKey)
	if err != nil {
		return nil, fmt.Errorf("create app transport to list installations: %w", err)
	}
	appClient := gh.NewClient(&http.Client{Transport: appTransport, Timeout: 10 * time.Second})
	appClient.DisableRateLimitCheck = true
	return activeInstallationIDs(ctx, appClient, c.logger)
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
