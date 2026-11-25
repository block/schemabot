package commands

import (
	"fmt"
	"log/slog"

	"github.com/block/schemabot/pkg/cmd/client"
	"github.com/block/schemabot/pkg/cmd/cliname"
	"github.com/block/schemabot/pkg/cmd/internal/templates"
	"github.com/block/schemabot/pkg/state"
)

// refuseActiveSchemaChange runs the active schema change preflight shared by
// apply and rollback, and refuses the command, with guidance for the schema
// change already in progress, when check reports one. Every active state is
// refused, as the server refuses a new apply while any non-terminal apply holds
// the target, so the operator learns it before the plan, prompt and lock rather
// than after. showLoading controls the spinner, which JSON output suppresses.
//
// A preflight that fails is not a refusal: the server guards the same conflict
// when the apply is created, so the command goes on, and the failure is logged
// so the operator can see the check did not run.
func refuseActiveSchemaChange(database, environment string, showLoading bool, check func() (*client.ActiveSchemaChange, error)) error {
	var active *client.ActiveSchemaChange
	err := withLoading("Checking active schema changes...", showLoading, func() error {
		var checkErr error
		active, checkErr = check()
		return checkErr
	})
	if err != nil {
		slog.Warn("could not check for an active schema change; proceeding, and the server will refuse the apply if a schema change holds the database",
			"database", database, "environment", environment, "error", err)
		return nil
	}
	if active == nil || active.State == "" {
		return nil
	}
	var stateMsg string
	switch {
	case state.IsState(active.State, state.Apply.Pending):
		stateMsg = "A schema change is queued and has not started yet."
	case state.IsState(active.State, state.Apply.WaitingForDeploy):
		stateMsg = "A schema change is waiting for deploy."
	case state.IsState(active.State, state.Apply.WaitingForCutover):
		stateMsg = "A schema change is waiting for cutover."
	case state.IsRunningApplyState(active.State):
		stateMsg = "A schema change is already running."
	case state.IsState(active.State, state.Apply.CuttingOver):
		stateMsg = "A schema change is currently cutting over."
	default:
		stateMsg = heldByStateMessage(active.State)
	}
	fmt.Println()
	fmt.Println("⏳ Schema Change In Progress")
	fmt.Println()
	fmt.Printf("Database: %s\n", database)
	fmt.Printf("Environment: %s\n", environment)
	fmt.Println()
	fmt.Println(stateMsg)
	fmt.Println()
	if state.IsState(active.State, state.Apply.WaitingForDeploy, state.Apply.WaitingForCutover) {
		fmt.Printf("To trigger cutover:  %s\n", templates.ControlCommand("cutover", active.ApplyID, environment))
	}
	fmt.Printf("To watch and manage: %s\n", cliname.Command("status", active.ApplyID))
	return fmt.Errorf("schema change already in progress")
}

// heldByStateMessage names the state of an active schema change the preflight
// has no specific wording for, and, when the state registry says, what it does
// to the database it holds.
func heldByStateMessage(activeState string) string {
	msg := "A schema change is active (" + state.Label(state.NormalizeState(activeState)) + ")"
	if hold := state.Hold(activeState); hold != "" {
		return msg + "; " + hold + "."
	}
	return msg + "."
}
