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
}

// FreezeListResponse lists freezes (GET /api/freezes).
type FreezeListResponse struct {
	// InEffect are the freezes blocking changes now, oldest start first.
	InEffect []Freeze `json:"in_effect"`

	// Scheduled are the freezes that start later, soonest first.
	Scheduled []Freeze `json:"scheduled"`

	// Recent are the latest freezes of any state, newest start first,
	// including the ones above.
	Recent []Freeze `json:"recent"`
}
