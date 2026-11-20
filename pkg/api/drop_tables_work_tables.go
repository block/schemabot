package api

import (
	"context"
	"fmt"

	"github.com/block/spirit/pkg/utils"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// spiritSharedCheckpointTable is the checkpoint table Spirit shares across the
// tables of one run that changes several tables at once. Spirit keeps the name
// unexported in its migration package; exporting it there retires this copy.
const spiritSharedCheckpointTable = "_spirit_checkpoint"

// DropTablesInUseError refuses a plan or an apply whose drop_tables lists a
// working table of an apply that has not settled on the same database and
// environment. A stopped apply keeps its copy on the target so a start can
// resume it, and dropping that copy would discard the apply's work without the
// disclosure a change discarding an existing copy gets.
type DropTablesInUseError struct {
	// Entry is the drop_tables entry that names the working table.
	Entry string
	// ApplyID is the identifier of the apply the working table belongs to.
	ApplyID string
	// ApplyState is that apply's state when the refusal was decided.
	ApplyState string
	// Table is the table the apply changes that the working table copies.
	// Empty for the checkpoint a run changing several tables shares.
	Table string
}

func (e *DropTablesInUseError) Error() string {
	owner := fmt.Sprintf("which is %s and changes table %q", e.ApplyState, e.Table)
	if e.Table == "" {
		owner = fmt.Sprintf("which is %s and changes several tables in one run", e.ApplyState)
	}
	remedy := "Wait for it to finish or cancel it, then plan again."
	if state.IsState(e.ApplyState, state.Apply.Stopped) {
		remedy = "Start or cancel it, then plan again."
	}
	return fmt.Sprintf("drop_tables entry %q is a working table of apply %s, %s. Dropping it would discard that apply's copy. %s",
		e.Entry, e.ApplyID, owner, remedy)
}

// DropTablesCheckError is the working-table check failing to read what the
// applies that have not settled change. The drop is refused, since it cannot
// be shown safe. The cause is storage's, so a PR comment shows a fixed line
// and the server log carries the cause.
type DropTablesCheckError struct {
	Database    string
	Environment string
	Err         error
}

func (e *DropTablesCheckError) Error() string {
	return fmt.Sprintf("check drop_tables for %s/%s against applies that may still resume: %v", e.Database, e.Environment, e.Err)
}

func (e *DropTablesCheckError) Unwrap() error { return e.Err }

// refuseDropTablesAlsoIgnored refuses a request whose drop_tables names a
// table its ignore_tables withholds. The engine refuses the same overlap, but
// a data plane that predates drop_tables reads only ignore_tables and would
// withhold the table without a word, so the control plane refuses it first.
func refuseDropTablesAlsoIgnored(dropTables, ignoreTables []string) error {
	drops, err := engine.NewDroppedTables(dropTables)
	if err != nil {
		return err
	}
	ignored, err := engine.NewIgnoredTables(ignoreTables)
	if err != nil {
		return err
	}
	return drops.RefuseIgnored(ignored)
}

// refuseDropsOfUnsettledApplyWorkTables refuses drop_tables entries that name a
// working table of an apply on the same database and environment that has not
// settled, stopped ones included, since a stopped apply resumes from its copy.
// The engine's quiet-period check cannot see this: a stopped copy is not
// written to. The names are computed from each apply's tables with Spirit's
// own helpers, never parsed back from an entry, so a long name Spirit
// shortened still matches. A collision refuses the entry for both tables,
// which errs on the safe side. A store that cannot be read refuses the drop
// rather than treating it as unused.
func (s *Service) refuseDropsOfUnsettledApplyWorkTables(ctx context.Context, database, databaseType, environment string, dropTables []string) error {
	if len(dropTables) == 0 {
		return nil
	}
	unsettled, err := s.storage.Applies().GetUnsettled(ctx)
	if err != nil {
		s.logger.Error("drop_tables refused: the applies that may still resume could not be read",
			"database", database, "database_type", databaseType, "environment", environment, "drop_tables", dropTables, "error", err)
		return &DropTablesCheckError{Database: database, Environment: environment, Err: err}
	}
	for _, apply := range unsettled {
		if !appliesToSameDatabase(apply, database, databaseType, environment) {
			s.logger.Debug("drop_tables check skips an unsettled apply on another database or environment",
				append(apply.LogAttrs(), "checked_database", database, "checked_environment", environment)...)
			continue
		}
		owners, err := s.workTableOwners(ctx, apply)
		if err != nil {
			s.logger.Error("drop_tables refused: the tables an apply that may still resume changes could not be read",
				append(apply.LogAttrs(), "drop_tables", dropTables, "error", err)...)
			return &DropTablesCheckError{Database: database, Environment: environment, Err: fmt.Errorf("apply %s: %w", apply.ApplyIdentifier, err)}
		}
		for _, entry := range dropTables {
			if table, ok := owners[entry]; ok {
				s.logger.Warn("drop_tables refused: the entry is a working table of an apply that has not settled",
					append(apply.LogAttrs(), "entry", entry, "table", table)...)
				return &DropTablesInUseError{Entry: entry, ApplyID: apply.ApplyIdentifier, ApplyState: apply.State, Table: table}
			}
		}
	}
	return nil
}

// appliesToSameDatabase reports whether an apply runs on the database and
// environment a plan or apply is being checked for.
func appliesToSameDatabase(apply *storage.Apply, database, databaseType, environment string) bool {
	return storage.CanonicalKey(apply.Database) == storage.CanonicalKey(database) &&
		storage.CanonicalKey(apply.DatabaseType) == storage.CanonicalKey(databaseType) &&
		storage.CanonicalKey(apply.Environment) == storage.CanonicalKey(environment)
}

// workTableOwners maps each working table an apply's runs create to the table
// it belongs to, read from the plans the apply and its operations run. The
// stored plans are read rather than task rows, because a remote data plane
// keeps the task rows of the applies it runs.
func (s *Service) workTableOwners(ctx context.Context, apply *storage.Apply) (map[string]string, error) {
	operations, err := s.storage.ApplyOperations().ListByApply(ctx, apply.ID)
	if err != nil {
		return nil, fmt.Errorf("list operations: %w", err)
	}
	planIDs := make(map[int64]bool)
	if apply.PlanID != 0 {
		planIDs[apply.PlanID] = true
	}
	for _, op := range operations {
		if op.PlanID != 0 {
			planIDs[op.PlanID] = true
		}
	}
	if len(planIDs) == 0 {
		return nil, fmt.Errorf("neither the apply nor its operations name a plan, so the tables the apply changes are unknown")
	}
	owners := make(map[string]string)
	for planID := range planIDs {
		plan, err := s.storage.Plans().GetByID(ctx, planID)
		if err != nil {
			return nil, fmt.Errorf("load plan %d: %w", planID, err)
		}
		if plan == nil {
			return nil, fmt.Errorf("plan %d is not stored, so the tables the apply changes are unknown", planID)
		}
		changes := plan.FlatDDLChanges()
		for _, change := range changes {
			owners[utils.NewTableName(change.Table)] = change.Table
			owners[utils.CheckpointTableName(change.Table)] = change.Table
			owners[utils.OldTableName(change.Table)] = change.Table
		}
		if len(changes) > 1 {
			owners[spiritSharedCheckpointTable] = ""
		}
	}
	return owners, nil
}
