package tern

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
)

const (
	// remoteProgressBatchUnsupportedFor is how long a remote apply whose data
	// plane answered BatchProgress Unimplemented is polled one operation at a
	// time before batching is tried again, so a data plane upgraded in place
	// starts answering batches without a control plane restart.
	remoteProgressBatchUnsupportedFor = 10 * time.Minute

	// maxProgressBatchOperations bounds the operations one BatchProgress call
	// names, so one answer stays well under the receive limit however many
	// targets share a remote apply. A round with more operations sends one
	// call per chunk.
	maxProgressBatchOperations = 32

	// progressBatchSweepInterval is how often idle remote applies are dropped
	// from the batcher.
	progressBatchSweepInterval = time.Minute
)

// errProgressBatchFallback tells the caller to ask Progress for its operation
// directly, because this round's batch could not answer it.
var errProgressBatchFallback = errors.New("batch progress could not answer this operation; asking progress for it directly")

// remoteProgressBatcher collects the progress polls this client's drives make
// for operations that share one remote apply, and answers each round of them
// with one BatchProgress call. Rounds for a remote apply start at most once
// per poll interval, and a poll joins the next round that has not been sent,
// so its answer is never read before the poll was made.
//
// The batcher only ever reads: a round that cannot answer an operation
// hands it back to its caller, which asks Progress directly as it would
// without the batcher. The zero value is ready to use.
type remoteProgressBatcher struct {
	// interval is the shortest time between two rounds for one remote
	// apply. Zero selects grpcProgressPollInterval.
	interval time.Duration
	// afterFunc schedules a round. Nil selects time.AfterFunc; tests replace
	// it to send rounds when they choose.
	afterFunc func(d time.Duration, f func())

	mu        sync.Mutex
	groups    map[progressBatchKey]*progressBatchGroup
	lastSweep time.Time
	ctx       context.Context
	cancel    context.CancelFunc
	closed    bool
}

// progressBatchKey names the remote apply a round polls.
type progressBatchKey struct {
	remoteApplyID string
	environment   string
}

// progressBatchGroup is the batching state of one remote apply.
type progressBatchGroup struct {
	// pending is the round that has been scheduled but not sent; polls join it.
	pending *progressBatchRound
	// lastSent is when the latest round for this remote apply was sent.
	lastSent time.Time
	// unsupportedUntil is when batching is next tried for a remote apply
	// whose data plane answered Unimplemented.
	unsupportedUntil time.Time
}

type progressBatchRound struct {
	waiters []*progressBatchWaiter
}

type progressBatchWaiter struct {
	operationID string
	answer      chan progressBatchAnswer
}

type progressBatchAnswer struct {
	resp *ternv1.ProgressResponse
	err  error
}

// progress answers the Progress poll for one operation of a remote apply
// from the next round. It returns errProgressBatchFallback when the round
// could not answer it, and the caller's context error if the caller gives up
// first.
func (b *remoteProgressBatcher) progress(ctx context.Context, client ternv1.TernClient, logger *slog.Logger, key progressBatchKey, operationID string) (*ternv1.ProgressResponse, error) {
	waiter := &progressBatchWaiter{operationID: operationID, answer: make(chan progressBatchAnswer, 1)}
	if !b.join(client, logger, key, waiter) {
		return nil, errProgressBatchFallback
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case answer := <-waiter.answer:
		return answer.resp, answer.err
	}
}

// join adds the waiter to the remote apply's next round, scheduling the round
// if none is pending. It reports false when the remote apply is not batched
// right now, so the caller asks directly.
func (b *remoteProgressBatcher) join(client ternv1.TernClient, logger *slog.Logger, key progressBatchKey, waiter *progressBatchWaiter) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		logger.Debug("progress batcher is closed; asking progress directly",
			"remote_apply_id", key.remoteApplyID, "environment", key.environment)
		return false
	}
	if b.groups == nil {
		b.groups = make(map[progressBatchKey]*progressBatchGroup)
		b.ctx, b.cancel = context.WithCancel(context.Background())
	}
	now := time.Now()
	b.sweepIdleGroupsLocked(now)
	group := b.groups[key]
	if group == nil {
		group = &progressBatchGroup{}
		b.groups[key] = group
	}
	if now.Before(group.unsupportedUntil) {
		logger.Debug("data plane does not answer batch progress for this remote apply; asking progress directly",
			"remote_apply_id", key.remoteApplyID, "environment", key.environment,
			"batching_retried_at", group.unsupportedUntil)
		return false
	}
	if group.pending != nil {
		group.pending.waiters = append(group.pending.waiters, waiter)
		return true
	}
	round := &progressBatchRound{waiters: []*progressBatchWaiter{waiter}}
	group.pending = round
	delay := max(group.lastSent.Add(b.pollInterval()).Sub(now), 0)
	ctx := b.ctx
	b.schedule(delay, func() { b.send(ctx, client, logger, key, round) })
	return true
}

func (b *remoteProgressBatcher) pollInterval() time.Duration {
	if b.interval > 0 {
		return b.interval
	}
	return grpcProgressPollInterval
}

func (b *remoteProgressBatcher) schedule(d time.Duration, f func()) {
	if b.afterFunc != nil {
		b.afterFunc(d, f)
		return
	}
	time.AfterFunc(d, f)
}

// sweepIdleGroupsLocked drops remote applies that have no pending round and
// have not been polled for a while, so the batcher does not keep every
// remote apply this client ever drove.
func (b *remoteProgressBatcher) sweepIdleGroupsLocked(now time.Time) {
	if now.Sub(b.lastSweep) < progressBatchSweepInterval {
		return
	}
	b.lastSweep = now
	for key, group := range b.groups {
		idle := group.pending == nil && now.Sub(group.lastSent) > progressBatchSweepInterval
		if idle && !now.Before(group.unsupportedUntil) {
			delete(b.groups, key)
		}
	}
}

// send closes the round to new polls and answers it, one BatchProgress call
// per chunk of operations.
func (b *remoteProgressBatcher) send(ctx context.Context, client ternv1.TernClient, logger *slog.Logger, key progressBatchKey, round *progressBatchRound) {
	b.mu.Lock()
	if group := b.groups[key]; group != nil && group.pending == round {
		group.pending = nil
		group.lastSent = time.Now()
	}
	b.mu.Unlock()

	waitersByID := make(map[string][]*progressBatchWaiter, len(round.waiters))
	operationIDs := make([]string, 0, len(round.waiters))
	for _, waiter := range round.waiters {
		if _, seen := waitersByID[waiter.operationID]; !seen {
			operationIDs = append(operationIDs, waiter.operationID)
		}
		waitersByID[waiter.operationID] = append(waitersByID[waiter.operationID], waiter)
	}

	var wg sync.WaitGroup
	for start := 0; start < len(operationIDs); start += maxProgressBatchOperations {
		chunk := operationIDs[start:min(start+maxProgressBatchOperations, len(operationIDs))]
		wg.Go(func() {
			b.sendChunk(ctx, client, logger, key, chunk, waitersByID)
		})
	}
	wg.Wait()
}

func (b *remoteProgressBatcher) sendChunk(ctx context.Context, client ternv1.TernClient, logger *slog.Logger, key progressBatchKey, operationIDs []string, waitersByID map[string][]*progressBatchWaiter) {
	attrs := []any{"remote_apply_id", key.remoteApplyID, "environment", key.environment, "operations", len(operationIDs)}
	resp, err := client.BatchProgress(ctx, &ternv1.BatchProgressRequest{
		ApplyId:           key.remoteApplyID,
		Environment:       key.environment,
		ApplyOperationIds: operationIDs,
	})
	if err != nil {
		switch status.Code(err) {
		case codes.Unimplemented:
			b.markUnsupported(logger, key, err)
			answerAll(operationIDs, waitersByID, progressBatchAnswer{err: errProgressBatchFallback})
		case codes.ResourceExhausted:
			logger.Warn("batch progress answer was refused as too large; asking progress for each operation of this round directly",
				append(attrs, "error", err)...)
			answerAll(operationIDs, waitersByID, progressBatchAnswer{err: errProgressBatchFallback})
		default:
			// Any other failure is what a direct call would have met on the
			// same connection, so each drive handles it as its own poll error.
			answerAll(operationIDs, waitersByID, progressBatchAnswer{err: err})
		}
		return
	}
	if resp.GetApplyId() != key.remoteApplyID {
		logger.Error("batch progress answered for a different remote apply than it was asked about; asking progress for each operation of this round directly",
			append(attrs, "answered_apply_id", resp.GetApplyId())...)
		answerAll(operationIDs, waitersByID, progressBatchAnswer{err: errProgressBatchFallback})
		return
	}
	entries := make(map[string]*ternv1.OperationProgress, len(resp.GetOperations()))
	for _, entry := range resp.GetOperations() {
		if _, seen := entries[entry.GetApplyOperationId()]; !seen {
			entries[entry.GetApplyOperationId()] = entry
		}
	}
	for _, operationID := range operationIDs {
		answer := batchEntryAnswer(logger, key, operationID, entries[operationID])
		for i, waiter := range waitersByID[operationID] {
			if i > 0 && answer.resp != nil {
				// Each drive owns the answer it is handed.
				answer.resp = proto.Clone(answer.resp).(*ternv1.ProgressResponse)
			}
			waiter.answer <- answer
		}
	}
}

// batchEntryAnswer turns one entry of a batch answer into what a direct
// Progress call for the operation would have returned, or into a fallback
// when the entry cannot stand in for one.
func batchEntryAnswer(logger *slog.Logger, key progressBatchKey, operationID string, entry *ternv1.OperationProgress) progressBatchAnswer {
	attrs := []any{"remote_apply_id", key.remoteApplyID, "environment", key.environment, "remote_operation_id", operationID}
	if entry == nil {
		logger.Warn("batch progress answer has no entry for an operation it was asked about; asking progress for it directly", attrs...)
		return progressBatchAnswer{err: errProgressBatchFallback}
	}
	code := codes.Code(entry.GetErrorCode())
	switch {
	case code == codes.Unimplemented:
		// A direct call answers for itself; Unimplemented from a poll ends
		// the drive, so it is never handed back from a batch.
		logger.Warn("batch progress answered an operation Unimplemented; asking progress for it directly", attrs...)
		return progressBatchAnswer{err: errProgressBatchFallback}
	case code != codes.OK:
		return progressBatchAnswer{err: status.Error(code, entry.GetErrorMessage())}
	case entry.GetProgress() == nil:
		logger.Warn("batch progress answer has an empty entry for an operation it was asked about; asking progress for it directly", attrs...)
		return progressBatchAnswer{err: errProgressBatchFallback}
	default:
		return progressBatchAnswer{resp: entry.GetProgress()}
	}
}

func answerAll(operationIDs []string, waitersByID map[string][]*progressBatchWaiter, answer progressBatchAnswer) {
	for _, operationID := range operationIDs {
		for _, waiter := range waitersByID[operationID] {
			waiter.answer <- answer
		}
	}
}

// markUnsupported stops batching the remote apply's polls until
// remoteProgressBatchUnsupportedFor has passed.
func (b *remoteProgressBatcher) markUnsupported(logger *slog.Logger, key progressBatchKey, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	group := b.groups[key]
	if group == nil {
		group = &progressBatchGroup{}
		b.groups[key] = group
	}
	now := time.Now()
	if now.Before(group.unsupportedUntil) {
		return
	}
	group.unsupportedUntil = now.Add(remoteProgressBatchUnsupportedFor)
	logger.Info("data plane does not answer batch progress; polling each operation of this remote apply directly",
		"remote_apply_id", key.remoteApplyID, "environment", key.environment,
		"batching_retried_at", group.unsupportedUntil, "error", err)
}

// close stops the batcher. A round in flight is cancelled, and later polls
// are handed back to ask directly.
func (b *remoteProgressBatcher) close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.closed = true
	if b.cancel != nil {
		b.cancel()
	}
}
