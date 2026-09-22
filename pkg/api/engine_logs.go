package api

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/block/schemabot/pkg/apitypes"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/tern"
)

// EngineLogSource is one data plane's engine-authored log lines for an apply.
// An apply that fanned out across several data planes, or across several
// targets on one data plane, produces one source per read: the log ids and
// clocks belong to the data plane that wrote them, so the sources are kept
// apart rather than merged into one stream.
type EngineLogSource struct {
	// Deployment is the data plane the lines came from.
	Deployment string
	// Target is the execution target the lines describe. Empty where the
	// deployment drives a single unnamed target.
	Target string
	// Entries are the engine's lines, oldest first.
	Entries []*apitypes.LogEntry
	// HasOlder reports that this source has engine lines older than Entries.
	// A reader that shows a window without this signal reads as the engine's
	// complete account, which is the wrong conclusion when the interesting
	// line scrolled past the window.
	HasOlder bool
}

// EngineApplyLogs reads the engine's own account of an apply from the data
// planes that ran it. The control plane stores only its own orchestration
// lines for a remotely driven apply — the engine's lines are written into the
// data plane's storage, under the data plane's own apply id — so reaching them
// means resolving each operation's remote apply id and reading it back over
// the deployment's own endpoint.
//
// limit bounds how many engine lines each source returns, newest kept. A
// deployment that runs in this process is skipped: its engine lines already
// land in control-plane storage, so returning them here would duplicate them
// on every surface that shows both streams.
//
// Only the read of the operation rows is fatal, because without them there is
// nothing to resolve. A data plane that cannot be resolved, reached, or
// decoded is logged with the identifiers needed to triage it and left out of
// the result: an unreadable data plane must not cost the caller the sources
// that did answer.
func (s *Service) EngineApplyLogs(ctx context.Context, apply *storage.Apply, limit int) ([]EngineLogSource, error) {
	if apply == nil {
		return nil, fmt.Errorf("apply is required to read engine logs")
	}
	if limit <= 0 {
		return nil, fmt.Errorf("engine log limit must be positive, got %d for apply %s", limit, apply.ApplyIdentifier)
	}
	ops, err := s.storage.ApplyOperations().ListByApply(ctx, apply.ID)
	if err != nil {
		return nil, fmt.Errorf("list apply operations for engine logs on apply %s: %w", apply.ApplyIdentifier, err)
	}
	var reads []engineLogRead
	for _, deployment := range operationDeployments(ops) {
		reads = append(reads, s.engineLogReadsForDeployment(apply, ops, deployment)...)
	}
	return s.runEngineLogReads(ctx, apply, reads, limit), nil
}

// engineLogRead is one data plane read: a resolved client and the single
// (target, data-plane apply) pair to ask it about.
type engineLogRead struct {
	deployment string
	client     tern.Client
	fetch      *deploymentLogFetch
}

// maxConcurrentEngineLogReads bounds how many data planes are read at once. A
// fan-out across a large fleet must not open a connection per shard at the
// moment an apply fails, which is exactly when the deployments involved are
// least likely to be healthy.
const maxConcurrentEngineLogReads = 8

// runEngineLogReads performs the reads concurrently and returns their sources
// in the order the reads were planned, so a fan-out renders the same way every
// time. Concurrency is what keeps the caller's single deadline fair: read
// serially, one slow data plane spends the whole budget and every deployment
// behind it is dropped for a timeout it did not cause, which would turn one
// unreachable deployment into a fold missing every other deployment's lines.
func (s *Service) runEngineLogReads(ctx context.Context, apply *storage.Apply, reads []engineLogRead, limit int) []EngineLogSource {
	results := make([]*EngineLogSource, len(reads))
	slots := make(chan struct{}, maxConcurrentEngineLogReads)
	var wg sync.WaitGroup
	for i, read := range reads {
		slots <- struct{}{}
		wg.Go(func() {
			defer func() { <-slots }()
			source, err := s.engineLogSource(ctx, apply, read.client, read.deployment, read.fetch, limit)
			switch {
			case err != nil:
				s.recordDeploymentLogFailure(apply, engineLogsOperation, read.deployment, read.fetch, err)
			case source == nil:
				s.logger.Debug("data plane reported no engine lines for this apply",
					append(apply.LogAttrs(), "operation", engineLogsOperation, "operation_deployment", read.deployment,
						"target", read.fetch.target, "external_id", read.fetch.externalID)...)
			default:
				results[i] = source
			}
		})
	}
	wg.Wait()
	var sources []EngineLogSource
	for _, source := range results {
		if source != nil {
			sources = append(sources, *source)
		}
	}
	return sources
}

// engineLogReadsForDeployment plans one deployment's reads, returning none
// when the deployment has no data plane to read from or nothing dispatched to
// read about. Every way of returning nothing is logged by this function, so a
// caller that receives no read knows to look for the reason in the server log.
// Planning is separated from reading so the reads can run together: resolving
// a client is local and cheap, while the read behind it crosses a network to
// another deployment.
func (s *Service) engineLogReadsForDeployment(apply *storage.Apply, ops []*storage.ApplyOperation, deployment string) []engineLogRead {
	attrs := func(extra ...any) []any {
		return append(append(apply.LogAttrs(), "operation", engineLogsOperation, "operation_deployment", deployment), extra...)
	}
	client, err := s.TernClient(deployment, apply.Environment)
	if err != nil {
		s.logger.Error("failed to resolve deployment for engine logs; the engine's own lines for this deployment will not be shown",
			attrs("error", err)...)
		return nil
	}
	if !client.IsRemote() {
		s.logger.Debug("deployment runs in this process; its engine lines are already in the control-plane apply log",
			attrs()...)
		return nil
	}
	fetches, undispatched := deploymentLogFetches(apply, ops, deployment)
	for _, op := range undispatched {
		s.logger.Debug("skipping operation without a remote apply id for engine logs",
			attrs("operation_key", op.OperationKey, "target", op.Target)...)
	}
	if len(fetches) == 0 {
		s.logger.Debug("deployment has no dispatched operation to read engine logs from", attrs()...)
		return nil
	}
	reads := make([]engineLogRead, 0, len(fetches))
	for _, key := range sortedFetchKeys(fetches) {
		reads = append(reads, engineLogRead{deployment: deployment, client: client, fetch: fetches[key]})
	}
	return reads
}

// engineLogSource reads one (target, data-plane apply) pair and keeps the
// engine's lines from it. It returns a nil source, and no error, when the data
// plane answered but the engine said nothing — a change that failed before the
// engine ran leaves exactly that, and it is not a failure to report.
func (s *Service) engineLogSource(ctx context.Context, apply *storage.Apply, client tern.Client, deployment string, fetch *deploymentLogFetch, limit int) (*EngineLogSource, error) {
	// Read the data plane's whole window rather than the caller's limit: the
	// window is served newest-first over every line the apply wrote, and the
	// data plane's own orchestration lines are filtered out here rather than
	// there. A window sized to the engine lines wanted would come back filled
	// with orchestration lines on a chatty apply and hand back none of them.
	resp, err := client.Logs(ctx, &ternv1.LogsRequest{
		ApplyId:     fetch.externalID,
		Target:      fetch.target,
		Database:    apply.Database,
		Type:        apply.DatabaseType,
		Environment: apply.Environment,
		Limit:       tern.MaxLogsLimit,
	})
	if err != nil {
		return nil, fmt.Errorf("read logs for data-plane apply %s on target %q of deployment %q: %w", fetch.externalID, fetch.target, deployment, err)
	}
	// A response filled to the cap means the data plane had more to say than
	// the read could carry, so lines older than this window exist whether or
	// not any of them was the engine's.
	hasOlder := len(resp.Logs) >= tern.MaxLogsLimit
	var entries []*apitypes.LogEntry
	for _, log := range resp.Logs {
		if !engineAuthoredLog(log.Source) {
			continue
		}
		entry, convertErr := deploymentLogEntry(fetch.externalID, log)
		if convertErr != nil {
			return nil, fmt.Errorf("decode engine log record from data-plane apply %s on target %q of deployment %q: %w", fetch.externalID, fetch.target, deployment, convertErr)
		}
		entries = append(entries, entry)
	}
	if len(entries) == 0 {
		return nil, nil
	}
	if len(entries) > limit {
		entries = entries[len(entries)-limit:]
		hasOlder = true
	}
	return &EngineLogSource{Deployment: deployment, Target: fetch.target, Entries: entries, HasOlder: hasOlder}, nil
}

// engineAuthoredLog reports whether a data-plane log line is the engine
// speaking for itself rather than the data plane narrating its own state
// machine. The source is the engine-agnostic marker: every line SchemaBot
// writes about its own orchestration carries the SchemaBot source, and an
// unset source stores as that source too, so anything else came from the
// engine driving the change.
func engineAuthoredLog(source string) bool {
	return source != "" && source != storage.LogSourceSchemaBot
}

// operationDeployments returns the distinct deployments an apply's operations
// ran on, in a stable order so a fan-out reads the same way every time.
func operationDeployments(ops []*storage.ApplyOperation) []string {
	seen := make(map[string]bool, len(ops))
	var deployments []string
	for _, op := range ops {
		if op == nil || seen[op.Deployment] {
			continue
		}
		seen[op.Deployment] = true
		deployments = append(deployments, op.Deployment)
	}
	sort.Strings(deployments)
	return deployments
}
