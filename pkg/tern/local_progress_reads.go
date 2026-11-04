package tern

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"google.golang.org/grpc/codes"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/storage"
)

// ErrBatchProgressUnsupported is what a client answers when it cannot serve
// BatchProgress. The gRPC server reports it as Unimplemented, which tells the
// caller to ask Progress once per operation instead.
var ErrBatchProgressUnsupported = errors.New("batch progress is not supported by this client")

// BatchProgressClient is a client that answers Progress for several operations
// of one apply in a single call. See the BatchProgress RPC.
type BatchProgressClient interface {
	BatchProgress(ctx context.Context, req *ternv1.BatchProgressRequest) (*ternv1.BatchProgressResponse, error)
}

var _ BatchProgressClient = (*LocalClient)(nil)

// progressReads holds the storage rows a Progress answer is built from. The
// apply and its tasks are read up front; the apply's operations, its re-read
// record, and its settled control requests are read on first use and kept,
// so every answer built from the same reads shares one read of each.
type progressReads struct {
	apply *storage.Apply
	tasks []*storage.Task

	// operations holds the apply's operations read so far, by id.
	operations map[int64]*storage.ApplyOperation
	// listed is the apply's full operation list once listOperations has read it.
	listed []*storage.ApplyOperation
	// operationsListed reports that operations holds every operation of the apply.
	operationsListed bool

	recorded       *storage.Apply
	recordedErr    error
	recordedLoaded bool

	settled       []*ternv1.SettledControlRequest
	settledErr    error
	settledLoaded bool
}

// readProgress reads the apply a progress request names and all of its tasks.
func (c *LocalClient) readProgress(ctx context.Context, applyIdentifier string) (*progressReads, error) {
	apply, err := c.storage.Applies().GetByApplyIdentifier(ctx, applyIdentifier)
	if err != nil {
		return nil, fmt.Errorf("get apply %s: %w", applyIdentifier, err)
	}
	if apply == nil {
		return nil, fmt.Errorf("get apply %s: %w", applyIdentifier, storage.ErrApplyNotFound)
	}
	tasks, err := c.storage.Tasks().GetByApplyID(ctx, apply.ID)
	if err != nil {
		return nil, fmt.Errorf("get tasks for apply %s: %w", applyIdentifier, err)
	}
	return &progressReads{apply: apply, tasks: tasks}, nil
}

// listOperations returns every operation of the apply, read once.
func (r *progressReads) listOperations(ctx context.Context, c *LocalClient) ([]*storage.ApplyOperation, error) {
	if r.operationsListed {
		return r.listed, nil
	}
	ops, err := c.storage.ApplyOperations().ListByApply(ctx, r.apply.ID)
	if err != nil {
		return nil, fmt.Errorf("list apply_operations for apply %s: %w", r.apply.ApplyIdentifier, err)
	}
	r.operations = make(map[int64]*storage.ApplyOperation, len(ops))
	for _, op := range ops {
		r.operations[op.ID] = op
	}
	r.listed = ops
	r.operationsListed = true
	return ops, nil
}

// operation returns the operation row with the given id, read once. It reads
// the row from storage unless the apply's operations were already listed, in
// which case an id outside the list returns nil, the same answer a read of a
// row that does not exist gives.
func (r *progressReads) operation(ctx context.Context, c *LocalClient, id int64) (*storage.ApplyOperation, error) {
	if op, ok := r.operations[id]; ok {
		return op, nil
	}
	if r.operationsListed {
		return nil, nil
	}
	op, err := c.storage.ApplyOperations().Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if op != nil {
		r.remember(op)
	}
	return op, nil
}

func (r *progressReads) remember(op *storage.ApplyOperation) {
	if r.operations == nil {
		r.operations = make(map[int64]*storage.ApplyOperation)
	}
	r.operations[op.ID] = op
}

// scopeOperation resolves the operation a progress request is scoped to, or
// nil when the request asks about the whole apply. An id that does not parse,
// names no operation, or names an operation of another apply is refused:
// answering for the whole apply instead would report the rollout member's
// siblings' work as its own.
func (r *progressReads) scopeOperation(ctx context.Context, c *LocalClient, applyOperationID string) (*storage.ApplyOperation, error) {
	if applyOperationID == "" {
		return nil, nil
	}
	if id, err := strconv.ParseInt(applyOperationID, 10, 64); err == nil {
		if op, ok := r.operations[id]; ok && op.ApplyID == r.apply.ID {
			return op, nil
		}
	}
	op, err := c.applyOperationOfApply(ctx, r.apply, applyOperationID)
	if err != nil {
		return nil, fmt.Errorf("scope progress of apply %s to its operation: %w", r.apply.ApplyIdentifier, err)
	}
	r.remember(op)
	return op, nil
}

// recordedApply re-reads the apply row after its tasks were read, so the
// recorded state an answer overlays on the tasks is no older than they are.
// It is read once and the same result, error included, is returned after.
func (r *progressReads) recordedApply(ctx context.Context, c *LocalClient) (*storage.Apply, error) {
	if !r.recordedLoaded {
		r.recorded, r.recordedErr = c.storage.Applies().Get(ctx, r.apply.ID)
		r.recordedLoaded = true
	}
	return r.recorded, r.recordedErr
}

// settledControlRequests returns the apply's settled control requests, read
// once. See LocalClient.settledControlRequests for why callers degrade on an
// error rather than fail the answer.
func (r *progressReads) settledControlRequests(ctx context.Context, c *LocalClient) ([]*ternv1.SettledControlRequest, error) {
	if !r.settledLoaded {
		r.settled, r.settledErr = c.settledControlRequests(ctx, r.apply)
		r.settledLoaded = true
	}
	return r.settled, r.settledErr
}

// BatchProgress answers Progress for each operation the request names, from
// one read of the apply, its tasks, and its operations. Each entry carries
// what Progress scoped to that operation returns, or the error it fails with;
// an error reading the apply itself fails the call. An empty id is refused
// for its entry alone: Progress without an operation answers for the whole
// apply, so honoring one here would report every member's work as the
// caller's own member's.
func (c *LocalClient) BatchProgress(ctx context.Context, req *ternv1.BatchProgressRequest) (*ternv1.BatchProgressResponse, error) {
	if req.GetApplyId() == "" {
		return nil, fmt.Errorf("apply_id is required")
	}
	reads, err := c.readProgress(ctx, req.GetApplyId())
	if err != nil {
		return nil, err
	}
	if _, err := reads.listOperations(ctx, c); err != nil {
		return nil, err
	}
	resp := &ternv1.BatchProgressResponse{
		ApplyId:    req.GetApplyId(),
		Operations: make([]*ternv1.OperationProgress, 0, len(req.GetApplyOperationIds())),
	}
	for _, applyOperationID := range req.GetApplyOperationIds() {
		entry := &ternv1.OperationProgress{ApplyOperationId: applyOperationID}
		if applyOperationID == "" {
			c.logger.Warn("batch progress: answering an entry with no apply_operation_id with INVALID_ARGUMENT instead of the whole apply's progress",
				reads.apply.LogAttrs()...)
			entry.ErrorCode = int32(codes.InvalidArgument)
			entry.ErrorMessage = fmt.Sprintf("batch progress for apply %s: apply_operation_id is required for each entry; ask Progress without one for the whole apply", reads.apply.ApplyIdentifier)
			resp.Operations = append(resp.Operations, entry)
			continue
		}
		progress, err := c.progressFromReads(ctx, reads, applyOperationID)
		if err != nil {
			c.logger.Warn("batch progress: answering one operation with the error its own progress request would fail with",
				append(reads.apply.LogAttrs(), "apply_operation_id", applyOperationID, "error", err)...)
			entry.ErrorCode = int32(progressErrorCode(err))
			entry.ErrorMessage = err.Error()
		} else {
			entry.Progress = progress
		}
		resp.Operations = append(resp.Operations, entry)
	}
	return resp, nil
}

// progressErrorCode is the gRPC status code a failed progress read is
// reported with: NotFound when the apply or its tasks do not exist, Internal
// otherwise.
func progressErrorCode(err error) codes.Code {
	if errors.Is(err, storage.ErrApplyNotFound) || errors.Is(err, storage.ErrTaskNotFound) {
		return codes.NotFound
	}
	return codes.Internal
}
