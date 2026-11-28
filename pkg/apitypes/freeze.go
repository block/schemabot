package apitypes

import "time"

// CreateFreezeRequest sets an environment freeze (POST /api/freezes).
type CreateFreezeRequest struct {
	// Environment is the environment to freeze. Required.
	Environment string `json:"environment"`

	// Deployments and Databases narrow the freeze. Empty covers the whole
	// environment.
	Deployments []string `json:"deployments,omitempty"`
	Databases   []string `json:"databases,omitempty"`

	// Kind is "on_demand" (the default) or "planned". A planned freeze needs
	// EndsAt.
	Kind string `json:"kind,omitempty"`

	// Running is "continue" (the default) or "stop": what happens to applies
	// already running in scope when the freeze starts.
	Running string `json:"running,omitempty"`

	// StartsAt defaults to now. EndsAt may be empty for an on-demand freeze,
	// which then lasts until someone lifts it.
	StartsAt *time.Time `json:"starts_at,omitempty"`
	EndsAt   *time.Time `json:"ends_at,omitempty"`

	// Reason says why the freeze is set. Required.
	Reason string `json:"reason"`
}

// LiftFreezeRequest lifts a freeze early (POST /api/freezes/{id}/lift).
type LiftFreezeRequest struct {
	// Reason optionally says why the freeze is lifted.
	Reason string `json:"reason,omitempty"`
}

// Freeze is one environment freeze.
type Freeze struct {
	ID          string     `json:"id"`
	Environment string     `json:"environment"`
	Deployments []string   `json:"deployments,omitempty"`
	Databases   []string   `json:"databases,omitempty"`
	Kind        string     `json:"kind"`
	Running     string     `json:"running"`
	StartsAt    time.Time  `json:"starts_at"`
	EndsAt      *time.Time `json:"ends_at,omitempty"`
	Reason      string     `json:"reason"`
	CreatedBy   string     `json:"created_by"`
	Source      string     `json:"source"`
	ExternalID  string     `json:"external_id,omitempty"`
	LiftedBy    string     `json:"lifted_by,omitempty"`
	LiftReason  string     `json:"lift_reason,omitempty"`
	LiftedAt    *time.Time `json:"lifted_at,omitempty"`
	CreatedAt   time.Time  `json:"created_at"`

	// InEffect reports whether the freeze blocks changes now.
	InEffect bool `json:"in_effect"`

	// StopSweep reports the stops that setting the freeze issued. Only the
	// response to creating a freeze with running "stop" that is in effect at
	// once has it.
	StopSweep *FreezeStopSweep `json:"stop_sweep,omitempty"`
}

// FreezeStopSweep reports the durable stop a freeze with running "stop"
// issued, when it was set, on every apply in its scope that was pending or
// running. Each stop is recorded with the freeze as its requester.
type FreezeStopSweep struct {
	// Stopped are the applies SchemaBot recorded a stop for.
	Stopped []string `json:"stopped"`

	// NotStopped are the applies in scope that SchemaBot did not stop, with
	// why.
	NotStopped []FreezeUnstoppedApply `json:"not_stopped,omitempty"`

	// Error is set when SchemaBot could not list the applies in scope, so it
	// stopped none of them.
	Error string `json:"error,omitempty"`
}

// FreezeUnstoppedApply is an apply in a freeze's scope that the freeze's stop
// sweep did not stop.
type FreezeUnstoppedApply struct {
	ApplyID string `json:"apply_id"`
	Reason  string `json:"reason"`
}

// FreezeListResponse lists freezes (GET /api/freezes).
type FreezeListResponse struct {
	// InEffect are the freezes blocking changes now, oldest start first.
	InEffect []Freeze `json:"in_effect"`

	// Scheduled are the freezes that start later, soonest first.
	Scheduled []Freeze `json:"scheduled"`

	// Recent are the latest freezes that have ended, lifted or past their
	// end, newest start first. It never repeats a freeze listed above.
	Recent []Freeze `json:"recent"`
}

// FreezeOverrideRequest asks to let one change through the freezes that hold
// it (GET /api/freezes/overrides/{id}). A second person approves it, and the
// change goes ahead only while every blocker it meets is one the request names.
type FreezeOverrideRequest struct {
	ID          string `json:"id"`
	Environment string `json:"environment"`
	// Action is "apply", "start" or "cutover". TargetID is the database,
	// member and schema an apply asks for, or the apply a start or cutover
	// would act on.
	Action   string `json:"action"`
	TargetID string `json:"target_id"`
	Database string `json:"database"`
	// Blockers are the blockers the request covers, each "gate id".
	Blockers    []string   `json:"blockers"`
	Reason      string     `json:"reason"`
	RequestedBy string     `json:"requested_by"`
	ApprovedBy  string     `json:"approved_by,omitempty"`
	ApprovedAt  *time.Time `json:"approved_at,omitempty"`
	ExpiresAt   time.Time  `json:"expires_at"`
	CreatedAt   time.Time  `json:"created_at"`
}
