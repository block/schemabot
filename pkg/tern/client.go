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

	// ResumeApply resumes an in-progress apply whose heartbeat has expired.
	// Uses checkpoint/resume capabilities of the underlying engine.
	// This is called by Service.RecoverInProgress on startup for applies
	// with stale heartbeats (crashed workers).
	ResumeApply(ctx context.Context, apply *storage.Apply) error

	// IsRemote reports whether this client delegates to a separate Tern
	// service with its own storage.
	//
	// When true (GRPCClient): ExecuteApply creates apply/task records in
	// SchemaBot's storage and stores Tern's apply_id as external_id.
	//
	// When false (LocalClient): Apply() already created the records in the
	// same database — ExecuteApply reuses them.
	IsRemote() bool

	// Close releases any resources held by the client.
	Close() error
}
