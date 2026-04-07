// Package postgres implements the Engine interface for PostgreSQL databases.
//
// TODO: This is a stub implementation. The full implementation will use
// pg-osc or a similar tool for online schema changes on PostgreSQL.
package postgres

import (
	"context"
	"fmt"

	"github.com/block/schemabot/pkg/engine"
)

// Engine implements engine.Engine for PostgreSQL databases.
type Engine struct{}

// New creates a new PostgreSQL engine.
func New() *Engine {
	return &Engine{}
}

// Name returns the engine identifier.
func (e *Engine) Name() string {
	return "postgres"
}

// Plan computes the changes needed to reach the desired schema.
func (e *Engine) Plan(ctx context.Context, req *engine.PlanRequest) (*engine.PlanResult, error) {
	return nil, fmt.Errorf("postgres engine not implemented")
}

// Apply starts executing a schema change plan.
func (e *Engine) Apply(ctx context.Context, req *engine.ApplyRequest) (*engine.ApplyResult, error) {
	return nil, fmt.Errorf("postgres engine not implemented")
}

// Progress returns the current status of a schema change.
func (e *Engine) Progress(ctx context.Context, req *engine.ProgressRequest) (*engine.ProgressResult, error) {
	return nil, fmt.Errorf("postgres engine not implemented")
}

// Stop pauses a running schema change.
func (e *Engine) Stop(ctx context.Context, req *engine.ControlRequest) (*engine.ControlResult, error) {
	return nil, fmt.Errorf("postgres engine not implemented")
}

// Start resumes a stopped schema change.
func (e *Engine) Start(ctx context.Context, req *engine.ControlRequest) (*engine.ControlResult, error) {
	return nil, fmt.Errorf("postgres engine not implemented")
}

// Cutover triggers the final table swap.
func (e *Engine) Cutover(ctx context.Context, req *engine.ControlRequest) (*engine.ControlResult, error) {
	return nil, fmt.Errorf("postgres engine not implemented")
}

// Revert rolls back a completed schema change during the revert window.
func (e *Engine) Revert(ctx context.Context, req *engine.ControlRequest) (*engine.ControlResult, error) {
	return nil, fmt.Errorf("postgres engine not implemented")
}

// SkipRevert ends the revert window early, making changes permanent.
func (e *Engine) SkipRevert(ctx context.Context, req *engine.ControlRequest) (*engine.ControlResult, error) {
	return nil, fmt.Errorf("postgres engine not implemented")
}

// Volume adjusts the schema change speed.
func (e *Engine) Volume(ctx context.Context, req *engine.VolumeRequest) (*engine.VolumeResult, error) {
	return nil, fmt.Errorf("postgres engine not implemented")
}

// Compile-time check that Engine implements engine.Engine.
var _ engine.Engine = (*Engine)(nil)
