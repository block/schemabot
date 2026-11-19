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
	listInstallations func(ctx context.Context) ([]int64, error)
	readQuota         func(ctx context.Context, installationID int64) error

	mu     sync.Mutex
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewQuotaProbe returns a probe over every installation of the App that c
// authenticates as. Only a server configured with a GitHub App builds one, and
// each replica runs its own, so the probe's reads grow with the replica count
// while none of them spends the core budget.
func (c *Client) NewQuotaProbe() *QuotaProbe {
	return &QuotaProbe{
		logger:            c.logger,
		interval:          QuotaProbeInterval,
		listTimeout:       quotaListTimeout,
		readTimeout:       quotaReadTimeout,
		readConcurrency:   quotaReadConcurrency,
		listInstallations: c.listActiveInstallations,
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
	ids, err := p.listContained(ctx)
	if err != nil {
		p.logger.Warn("GitHub quota probe could not list App installations; installation budgets will age out to unknown until a later pass succeeds",
			"error", err)
		return
	}
	if len(ids) == 0 {
		p.logger.Debug("GitHub quota probe found no active App installations")
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

// listContained lists the App's installations under the list timeout,
// converting a panic into an error so the pass ends instead of the process.
func (p *QuotaProbe) listContained(ctx context.Context) ([]int64, error) {
	ctx, cancel := context.WithTimeout(ctx, p.listTimeout)
	defer cancel()
	var ids []int64
	recovered, err := panicsafe.Catch(func() error {
		var listErr error
		ids, listErr = p.listInstallations(ctx)
		return listErr
	})
	if recovered != nil {
		p.recordPanic(ctx, recovered, "list App installations")
		return nil, recovered
	}
	return ids, err
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
func (c *Client) listActiveInstallations(ctx context.Context) ([]int64, error) {
	appTransport, err := ghinstallation.NewAppsTransport(c.appBaseTransport(), c.appID, c.privateKey)
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
