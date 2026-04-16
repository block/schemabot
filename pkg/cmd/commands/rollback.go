package commands

import (
	"errors"
	"fmt"

	"github.com/block/schemabot/pkg/cmd/client"
	"github.com/block/schemabot/pkg/cmd/templates"
	"github.com/block/schemabot/pkg/state"
)

// RollbackCmd rollbacks a database to its schema state before a specific apply.
type RollbackCmd struct {
	ApplyID      string `arg:"" required:"" help:"Apply ID to rollback"`
	AutoApprove  bool   `short:"y" help:"Skip confirmation prompt" name:"auto-approve"`
	Watch        bool   `short:"w" help:"Watch progress until completion" default:"true" negatable:""`
	DeferCutover bool   `help:"Defer cutover until manual trigger" name:"defer-cutover"`
}

// Run executes the rollback command.
func (cmd *RollbackCmd) Run(g *Globals) error {
	ep, err := resolveEndpoint(g.Endpoint, g.Profile)
	if err != nil {
		return err
	}

	// Step 1: Generate rollback plan from the specified apply
	fmt.Println("Generating rollback plan...")
	planResult, err := client.CallRollbackPlanAPI(ep, cmd.ApplyID)
	if err != nil {
		return err
	}

	database := planResult.Database
	environment := planResult.Environment
	dbType := planResult.DatabaseType

	// Check for existing active schema change
	active, err := client.CheckActiveSchemaChange(ep, database, environment)
	if err != nil {
		// Ignore errors - progress API may fail if no schema change exists
	} else if active != nil && active.State != "" && !state.IsState(active.State, state.NoActiveChange) {
		switch {
		case state.IsState(active.State, state.Apply.WaitingForCutover):
			return fmt.Errorf("cannot rollback: a schema change is waiting for cutover")
		case state.IsState(active.State, state.Apply.Running):
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

	// Check if there are any changes
	tables := planResult.FlatTables()
	if len(tables) == 0 {
		fmt.Println("No changes. Schema is already at the original state.")
		return nil
	}

	// Step 2: Show the rollback plan
	fmt.Println()
	fmt.Println("Rollback Plan")
	fmt.Println("=============")
	fmt.Printf("Database: %s\n", database)
	fmt.Printf("Environment: %s\n", environment)
	fmt.Println()
	fmt.Println("The following changes will be applied to rollback:")
	fmt.Println()
	for _, tbl := range tables {
		fmt.Printf("  %s (%s):\n", tbl.TableName, tbl.ChangeType)
		fmt.Printf("    %s\n", tbl.DDL)
	}

	// Show unsafe warning if any
	if planResult.HasErrors() {
		unsafeChanges := planResult.UnsafeChanges()
		templates.WriteUnsafeWarningAllowed(unsafeChanges)
	}

	// Show options if any flags are set
	templates.WriteOptions(cmd.DeferCutover)

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
	owner := client.GenerateCLIOwner()

	existingLock, err := client.GetLock(ep, database, dbType)
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

	_, err = client.AcquireLock(ep, database, dbType, owner, "", 0)
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

	_, err = applyAndWatch(ep, planResult, database, environment, owner, "rollback", cmd.DeferCutover, cmd.Watch, OutputFormatInteractive, 0)
	return err
}
