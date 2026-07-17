// Package tern provides the client interface for schema change orchestration.
package tern

import (
	"context"
	"errors"
	"fmt"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/storage"
)

// ErrNoTasksForApplyOperation is returned by ResumeApplyOperation when no tasks
// scope to the requested operation. It is the fail-closed signal for an invalid
// or stale operation claim: the operation cannot make progress, so the operator
// terminalizes it rather than retrying it forever. Both the local and remote
// clients detect this from storage before any dispatch or state mutation, so
// callers can match it with errors.Is regardless of the transport.
var ErrNoTasksForApplyOperation = errors.New("no tasks found for apply operation")

// ErrApplyOperationRowMissing is returned by ResumeApplyOperation when tasks
// scope to the operation but the apply_operation row itself is absent. It is a
// distinct, more accurate cause than the no-tasks case, but wraps
// ErrNoTasksForApplyOperation so the same fail-closed handling (errors.Is)
// terminalizes the stale claim rather than retrying it forever.
var ErrApplyOperationRowMissing = fmt.Errorf("apply_operation row missing: %w", ErrNoTasksForApplyOperation)

var (
	// ErrPullSchemaUnsupportedType marks a pull request for a database type that
	// the data plane does not yet support.
	ErrPullSchemaUnsupportedType = errors.New("pull schema unsupported database type")
	// ErrPullSchemaInvalidRequest marks a malformed pull request.
	ErrPullSchemaInvalidRequest = errors.New("pull schema invalid request")
)

// Client defines the interface for schema change operations.
// Uses proto-generated types for type safety.
type Client interface {
	// PullSchema fetches the live schema and returns it as declarative schema files.
	PullSchema(ctx context.Context, req *ternv1.PullSchemaRequest) (*ternv1.PullSchemaResponse, error)

	// Plan generates a schema change plan from declarative schema files.
	// Returns a plan_id that can be used with Apply.
	Plan(ctx context.Context, req *ternv1.PlanRequest) (*ternv1.PlanResponse, error)

	// PlanDiff computes a deployment's desired-vs-live diff without persisting a
	// plan. Its result is not applyable (no plan_id); it is the read-only
	// producer the control plane runs per deployment to detect review-time drift.
	PlanDiff(ctx context.Context, req *ternv1.PlanRequest) (*ternv1.PlanDiffResponse, error)

	// Apply executes a previously generated plan.
	// Validates that the schema hasn't changed since Plan was called.
	Apply(ctx context.Context, req *ternv1.ApplyRequest) (*ternv1.ApplyResponse, error)

	// Progress returns detailed progress for an active schema change.
	Progress(ctx context.Context, req *ternv1.ProgressRequest) (*ternv1.ProgressResponse, error)

	// Logs returns a bounded recent window of durable apply logs.
	Logs(ctx context.Context, req *ternv1.LogsRequest) (*ternv1.LogsResponse, error)

	// Cutover triggers the cutover phase when defer_cutover was used.
	Cutover(ctx context.Context, req *ternv1.CutoverRequest) (*ternv1.CutoverResponse, error)

	// Stop pauses an in-progress schema change.
	// For MySQL: user has limited time (based on binlog retention) to resume.
	// For Vitess/PlanetScale: fully stops and cannot be restarted.
	Stop(ctx context.Context, req *ternv1.StopRequest) (*ternv1.StopResponse, error)

	// Cancel terminates an in-progress schema change permanently.
	Cancel(ctx context.Context, req *ternv1.CancelRequest) (*ternv1.CancelResponse, error)

	// Start resumes a stopped schema change.
	Start(ctx context.Context, req *ternv1.StartRequest) (*ternv1.StartResponse, error)

	// Volume modifies the schema change speed/concurrency in-flight.
	Volume(ctx context.Context, req *ternv1.VolumeRequest) (*ternv1.VolumeResponse, error)

	// Revert reverts a completed schema change during the revert window.
	// Only supported for Vitess (PlanetScale).
	Revert(ctx context.Context, req *ternv1.RevertRequest) (*ternv1.RevertResponse, error)

	// SkipRevert skips the revert window and finalizes the schema change.
	// Only supported for Vitess (PlanetScale).
	SkipRevert(ctx context.Context, req *ternv1.SkipRevertRequest) (*ternv1.SkipRevertResponse, error)

	// Health checks the service health.
	Health(ctx context.Context) error

	// ResumeApply starts or resumes work claimed by an operator driver.
	// Fresh pending applies are dispatched for the first time; stale applies
	// use checkpoint/resume capabilities of the underlying engine.
	ResumeApply(ctx context.Context, apply *storage.Apply) error

	// ResumeApplyOperation starts or resumes a single apply_operation (one
	// deployment of a multi-deployment apply), driving only that operation's
	// tasks. The drive logic is identical to ResumeApply; the operation scope
	// only narrows which tasks are loaded/re-queried so a driver can advance one
	// deployment independently of its siblings. Fails closed when no tasks match
	// the operation rather than touching the rest of the apply.
	ResumeApplyOperation(ctx context.Context, apply *storage.Apply, applyOperationID int64) error

	// ResumeApplyOperationCutover drives a single apply_operation that is parked
	// at the cutover barrier (waiting_for_cutover) through its cutover phase:
	// waiting_for_cutover → cutting_over → revert_window → completed. It is the
	// deployment-ordered counterpart to ResumeApplyOperation's copy drive — the
	// operator claims a parked operation whose turn it is (via the cutover-claim
	// predicate) and calls this to perform the high-risk swap, while siblings
	// stay parked. Unlike ResumeApplyOperation it never re-parks: it resumes the
	// operation's engine checkpoint and forces the cutover. Fails closed when no
	// tasks match the operation, like ResumeApplyOperation.
	ResumeApplyOperationCutover(ctx context.Context, apply *storage.Apply, applyOperationID int64) error

	// Endpoint returns the address this client connects to.
	// For GRPCClient, this is the dial address (e.g., "tern-staging:9090").
	// For LocalClient, this is the database name.
	Endpoint() string

	// IsRemote reports whether this client delegates to a separate Tern
	// service with its own storage.
	IsRemote() bool

	// SetPendingObserver sets an observer that will be consumed by the next
	// Apply() call. The observer is registered before the engine starts,
	// preventing the race where the apply completes before the observer is set.
	SetPendingObserver(observer ProgressObserver)

	// SetObserver registers a progress observer for an active apply.
	// Used by the operator to attach an observer before resuming.
	SetObserver(applyID int64, observer ProgressObserver)

	// Close releases any resources held by the client.
	Close() error
}
