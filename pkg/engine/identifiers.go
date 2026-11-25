package engine

import (
	"strings"

	"github.com/google/uuid"
)

// Plan, apply, task, and freeze identifiers are opaque strings: nothing may parse
// structure out of them, so their only guarantees are a recognizable prefix
// and enough randomness that concurrent mints on any pod are vanishingly
// unlikely to collide. Creation time lives in the stored row's created_at
// column, never in the identifier. Identifiers are minted once — at plan,
// apply, task, or freeze creation — and copied verbatim everywhere else, so stored
// rows minted under earlier formats stay resolvable by exact match forever.

// NewPlanID mints a stored plan's identifier.
func NewPlanID() string {
	return "plan-" + randomIdentifierSuffix()
}

// NewApplyID mints a stored apply's identifier.
func NewApplyID() string {
	return "apply-" + randomIdentifierSuffix()
}

// NewTaskID mints a stored task's identifier.
func NewTaskID() string {
	return "task-" + randomIdentifierSuffix()
}

// NewFreezeID mints a stored environment freeze's identifier.
func NewFreezeID() string {
	return "freeze-" + randomIdentifierSuffix()
}

// NewFreezeOverrideID mints a freeze override request's identifier.
func NewFreezeOverrideID() string {
	return "ovr-" + randomIdentifierSuffix()
}

func randomIdentifierSuffix() string {
	return strings.ReplaceAll(uuid.New().String(), "-", "")[:16]
}
