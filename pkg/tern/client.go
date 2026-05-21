// Package tern provides the client interface for schema change orchestration.
package tern

import (
	"context"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/storage"
)

// Client defines the interface for schema change operations.
// Uses proto-generated types for type safety.
type Client interface {
	// Plan generates a schema change plan from declarative schema files.
	// Returns a plan_id that can be used with Apply.
	Plan(ctx context.Context, req *ternv1.PlanRequest) (*ternv1.PlanResponse, error)

	// Apply executes a previously generated plan.
	// Validates that the schema hasn't changed since Plan was called.
	Apply(ctx context.Context, req *ternv1.ApplyRequest) (*ternv1.ApplyResponse, error)

	// Progress returns detailed progress for an active schema change.
	Progress(ctx context.Context, req *ternv1.ProgressRequest) (*ternv1.ProgressResponse, error)

	// Cutover triggers the cutover phase when defer_cutover was used.
	Cutover(ctx context.Context, req *ternv1.CutoverRequest) (*ternv1.CutoverResponse, error)

	// Stop pauses an in-progress schema change.
	// For MySQL: user has limited time (based on binlog retention) to resume.
	// For Vitess/PlanetScale: fully stops and cannot be restarted.
	Stop(ctx context.Context, req *ternv1.StopRequest) (*ternv1.StopResponse, error)

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

	// RollbackPlan generates a plan to revert to the schema state before the most recent apply.
	// Uses the OriginalSchema captured at plan time.
	RollbackPlan(ctx context.Context, database string) (*ternv1.PlanResponse, error)

	// Health checks the service health.
	Health(ctx context.Context) error

	// ResumeApply starts or resumes work claimed by a scheduler worker.
	// Fresh pending applies are dispatched for the first time; stale applies
	// use checkpoint/resume capabilities of the underlying engine.
	ResumeApply(ctx context.Context, apply *storage.Apply) error

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
	// Used by the scheduler to attach an observer before resuming.
	SetObserver(applyID int64, observer ProgressObserver)

	// Close releases any resources held by the client.
	Close() error
}
