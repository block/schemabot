package commands

import (
	"context"
	"errors"
	"fmt"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/cmd/client"
	"github.com/block/schemabot/pkg/cmd/internal/templates"
	"github.com/block/schemabot/pkg/state"
)

// RollbackCmd rollbacks a database to its schema state before a specific apply.
type RollbackCmd struct {
	ApplyID      string `arg:"" required:"" help:"Apply ID to rollback"`
	Environment  string `short:"e" required:"" help:"Target environment"`
	AutoApprove  bool   `short:"y" help:"Skip confirmation prompt" name:"auto-approve"`
	Watch        bool   `short:"w" help:"Watch progress until completion" default:"true" negatable:""`
	DeferCutover bool   `help:"Defer cutover until manual trigger" name:"defer-cutover"`
	AllowUnsafe  bool   `help:"Allow destructive changes (DROP TABLE, DROP COLUMN, etc.)" name:"allow-unsafe"`
}

// Run executes the rollback command.
func (cmd *RollbackCmd) Run(ctx context.Context, g *Globals) error {
	ep, err := resolveControlFlags(g.Endpoint, g.Profile, cmd.ApplyID, cmd.Environment)
	if err != nil {
		return err
	}

	// Step 1: Generate rollback plan from the specified apply
	var planResult *apitypes.PlanResponse
	err = withLoading("Generating rollback plan...", true, func() error {
		var planErr error
		planResult, planErr = client.CallRollbackPlanAPI(ep, cmd.ApplyID, cmd.Environment)
		return planErr
	})
	if err != nil {
		return err
	}

	database := planResult.Database
	environment := planResult.Environment
	dbType := planResult.DatabaseType

	// Check for existing active schema change
	var active *client.ActiveSchemaChange
	err = withLoading("Checking active schema changes...", true, func() error {
		var checkErr error
		active, checkErr = client.CheckActiveSchemaChange(ep, database, environment)
		return checkErr
	})
	if err != nil {
		// Ignore status preflight errors; apply is still guarded server-side.
	} else if active != nil && active.State != "" {
		switch {
		case state.IsState(active.State, state.Apply.WaitingForDeploy):
			return fmt.Errorf("cannot rollback: a schema change is waiting for deploy")
		case state.IsState(active.State, state.Apply.WaitingForCutover):
			return fmt.Errorf("cannot rollback: a schema change is waiting for cutover")
		case state.IsRunningApplyState(active.State):
			return fmt.Errorf("cannot rollback: a schema change is already running")
		case state.IsState(active.State, state.Apply.CuttingOver):
			return fmt.Errorf("cannot rollback: a schema change is currently cutting over")
		}
	}

	// Check for errors
	if len(planResult.Errors) > 0 {
		fmt.Println("Errors:")
		for _, e := range planResult.Errors {
			fmt.Printf("  - %s\n", e)
		}
		return fmt.Errorf("rollback plan has errors")
	}

	// Check if there are any changes (DDL or VSchema)
	if !planResult.HasChanges() {
		fmt.Println("No changes. Schema is already at the original state.")
		return nil
	}
	templates.WriteRollbackPlan(planResult, cmd.ApplyID)

	// A rollback plan gets the same execution verdicts as an apply plan, and
	// the server refuses to submit a blocked one, so it is refused here,
	// below the plan that shows the refused statement, before any prompt or
	// lock.
	if err := blockedPlanError("rollback", planResult); err != nil {
		return err
	}

	// Unsafe changes need --allow-unsafe, exactly as for apply. Neither the
	// confirmation prompt nor -y stands in for it: a rollback that drops a table
	// is as destructive as an apply that does.
	unsafeChanges := planResult.UnsafeChanges()
	if len(unsafeChanges) > 0 && !cmd.AllowUnsafe {
		templates.WriteUnsafeChangesBlocked(unsafeChanges, fmt.Sprintf("rollback %s -e %s --allow-unsafe", cmd.ApplyID, cmd.Environment))
		return ErrSilent
	}
	if cmd.AllowUnsafe {
		templates.WriteUnsafeWarningAllowed(unsafeChanges, templates.UnsafeConsentAllowFlag)
	}

	// Show options if any flags are set
	templates.WriteOptions(cmd.DeferCutover, false)

	// Step 3: Prompt for confirmation (unless auto-approve)
	if !cmd.AutoApprove {
		confirmed, err := confirmAction(
			"\nDo you want to apply this rollback? Only 'yes' will be accepted: ",
			"\nRollback cancelled.",
		)
		if err != nil {
			return err
		}
		if !confirmed {
			return nil
		}
	}

	// Step 4: Acquire lock and apply the rollback

	// A Ctrl-C during planning or the prompt ends the command here, before
	// any lock is taken on the operator's behalf.
	if ctx.Err() != nil {
		return stoppedBeforeSubmit(ctx, "rollback")
	}

	owner := client.GenerateCLIOwner()

	var existingLock *client.LockInfo
	err = withLoading("Checking database lock...", true, func() error {
		var lockErr error
		existingLock, lockErr = client.GetLock(ep, database, dbType)
		return lockErr
	})
	if err != nil {
		return fmt.Errorf("check lock: %w", err)
	}
	if existingLock != nil && existingLock.Owner != owner {
		templates.WriteLockConflict(templates.LockConflictData{
			Database:     database,
			DatabaseType: dbType,
			Owner:        existingLock.Owner,
			Repository:   existingLock.Repository,
			PullRequest:  existingLock.PullRequest,
			CreatedAt:    existingLock.CreatedAt,
		})
		return fmt.Errorf("database is locked")
	}

	err = withLoading("Acquiring database lock...", true, func() error {
		_, lockErr := client.AcquireLock(ep, database, dbType, owner, "", 0)
		return lockErr
	})
	if errors.Is(err, client.ErrLockHeld) {
		return fmt.Errorf("database is locked by another user")
	}
	if err != nil {
		return fmt.Errorf("acquire lock: %w", err)
	}
	templates.WriteLockAcquired(templates.LockData{
		Database:     database,
		DatabaseType: dbType,
		Owner:        owner,
	})

	fmt.Println("\nApplying rollback...")

	// A rollback has one plan, and it is the plan every member it runs on
	// runs: the server refuses to roll back an apply narrowed to one member,
	// pairs each member of a mirrored environment with this plan, and refuses
	// a rollout-wide rollback where members are planned on their own, since
	// none of them has a rollback plan. So showing this plan shows what runs
	// on every member, though not which members those are.
	_, err = applyAndWatch(ctx, ep, planResult, true, database, environment, owner, "rollback", cmd.DeferCutover, false, false, cmd.AllowUnsafe, "", cmd.Watch, OutputFormatInteractive, 0)
	return err
}
