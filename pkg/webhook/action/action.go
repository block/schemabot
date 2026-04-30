// Package action defines constants for webhook command actions.
package action

// Command action constants used in CommandResult.Action and handler routing.
const (
	Plan            = "plan"
	Apply           = "apply"
	ApplyConfirm    = "apply-confirm"
	Unlock          = "unlock"
	Stop            = "stop"
	Cutover         = "cutover"
	Revert          = "revert"
	SkipRevert      = "skip-revert"
	Rollback        = "rollback"
	RollbackConfirm = "rollback-confirm"
	FixLint         = "fix-lint"
	Help            = "help"
)
