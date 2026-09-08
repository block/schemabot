// cancelled_artifacts.go reclaims what a cancelled schema change left on the
// target: the copied rows are preserved in the pending drops quarantine, and
// the metadata describing where the copy had got to is dropped.
//
// Copied rows are worth more than the schema files that produced them. A table
// an operator deleted from a schema file is quarantined for a retention period
// before it is really gone, so a copy SchemaBot spent days building must be at
// least as recoverable — hence the split: data is preserved, metadata is not.
// A checkpoint without its shadow table describes a copy that no longer exists,
// so keeping one would preserve nothing.
//
// A deployment that runs no quarantine gets the same policy it chose for the
// tables an operator asked to delete: everything is dropped outright.
package spirit

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/block/spirit/pkg/utils"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/mysqlconn"
	"github.com/block/schemabot/pkg/pendingdrops"
)

// deferredCutoverSentinelTable is the schema-level marker a deferred cutover
// waits on. It is one row, and it is scoped to the schema rather than to a
// table, so it belongs to whichever schema change is deferring its cutover.
//
// Dropping it is not the loss of a record: it is the signal that releases a
// deferred cutover. Dropping the one a live schema change is waiting on cuts
// that change over without an operator asking for it, which is why a release
// requires that no schema change in the schema is live — not merely none on the
// tables it names.
const deferredCutoverSentinelTable = "_spirit_sentinel"

// ReleaseCancelledArtifacts reclaims a cancelled schema change's artifacts for
// tables named by the caller, connecting from the request's credentials rather
// than from any in-process state. A schema change is most often cancelled long
// after the instance that ran it has gone, so the release has to work with
// nothing but the target and the table list.
func (e *Engine) ReleaseCancelledArtifacts(ctx context.Context, req *engine.ReleaseArtifactsRequest) (*engine.ReleaseArtifactsResult, error) {
	if req == nil {
		return nil, fmt.Errorf("release artifacts request is required")
	}
	if req.Credentials == nil || req.Credentials.DSN == "" {
		return nil, fmt.Errorf("DSN credentials required to release cancelled schema change artifacts")
	}

	database, err := statelessControlDatabase(req.Credentials.DSN, req.Database)
	if err != nil {
		return nil, fmt.Errorf("parse DSN for cancelled schema change artifact release: %w", err)
	}
	if database == "" {
		return nil, fmt.Errorf("database is required to release cancelled schema change artifacts")
	}
	// Every schema change names at least one table, so an empty list is a lost
	// one rather than a change that touched none — and it is not the no-op it
	// looks like. The schema-level artifacts are not derived from the table
	// list, so a release would still discard the schema's shared checkpoint and
	// release its deferred cutover on behalf of no table at all.
	if len(req.Tables) == 0 {
		return nil, fmt.Errorf("tables are required to release cancelled schema change artifacts in database %s", database)
	}

	db, err := mysqlconn.Open(req.Credentials.DSN)
	if err != nil {
		return nil, fmt.Errorf("open database %s to release cancelled schema change artifacts: %w", database, err)
	}
	defer utils.CloseAndLog(db)
	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("connect to database %s to release cancelled schema change artifacts: %w", database, err)
	}

	return e.releaseArtifacts(ctx, db, database, req.Tables)
}

// releaseArtifacts is the disposal both cancel paths share, so a cancelled
// copy is disposed of the same way whether or not the runner that made it is
// still alive.
func (e *Engine) releaseArtifacts(ctx context.Context, db *sql.DB, database string, tables []string) (*engine.ReleaseArtifactsResult, error) {
	result := &engine.ReleaseArtifactsResult{}

	data, err := existingTables(ctx, db, database, dataBearingArtifacts(tables))
	if err != nil {
		return nil, fmt.Errorf("find cancelled schema change copy in database %s: %w", database, err)
	}
	metadata, err := existingTables(ctx, db, database, perTableMetadataArtifacts(tables))
	if err != nil {
		return nil, fmt.Errorf("find cancelled schema change metadata in database %s: %w", database, err)
	}

	shared, retained, err := e.disposeOfSchemaScopedArtifacts(ctx, db, database, tables)
	if err != nil {
		return nil, err
	}
	metadata = append(metadata, shared...)
	if len(retained) > 0 {
		result.Retained = retained
		result.RetainedReason = schemaBusyRetentionReason
	}

	if e.disablePendingDrops {
		// The quarantine and the cleaner that empties it are enabled by the
		// same deployment setting, so preserving a copy where the quarantine is
		// off would move it into a database nothing ever sweeps rather than
		// keep it recoverable. Discard it instead, matching what the deployment
		// already chose for the tables an operator asked to delete.
		discarded, err := discardArtifacts(ctx, db, database, data)
		if err != nil {
			return nil, err
		}
		result.Discarded = append(result.Discarded, discarded...)
	} else {
		preserved, err := preserveArtifacts(ctx, db, database, data)
		if err != nil {
			return nil, err
		}
		result.Preserved = append(result.Preserved, preserved...)
	}

	discarded, err := discardArtifacts(ctx, db, database, metadata)
	if err != nil {
		return nil, err
	}
	result.Discarded = append(result.Discarded, discarded...)

	e.logger.Info("released cancelled schema change artifacts",
		"database", database,
		"tables", tables,
		"preserved", len(result.Preserved),
		"discarded", len(result.Discarded),
		"retained", len(result.Retained),
		"quarantine_enabled", !e.disablePendingDrops)
	return result, nil
}

// schemaBusyRetentionReason is what an operator is told when the shared tables
// were left alone. It names the condition rather than the tables, because the
// tables are already listed beside it and the condition is what has to change
// before reclaiming them by hand is safe.
const schemaBusyRetentionReason = "another schema change has work in this schema, and the shared checkpoint and deferred cutover sentinel belong to whichever change owns them"

// disposeOfSchemaScopedArtifacts decides the fate of the two tables no single
// schema change owns, returning the ones to reclaim and the ones to leave
// alone. They are reclaimed only where the schema holds nothing this release
// cannot account for, because reclaiming the sentinel releases the deferred
// cutover of whichever change is waiting on it.
func (e *Engine) disposeOfSchemaScopedArtifacts(ctx context.Context, db *sql.DB, database string, tables []string) (reclaim, retain []string, err error) {
	present, err := existingTables(ctx, db, database, schemaScopedArtifacts())
	if err != nil {
		return nil, nil, fmt.Errorf("find shared cancelled schema change metadata in database %s: %w", database, err)
	}
	if len(present) == 0 {
		return nil, nil, nil
	}

	unaccounted, err := unaccountedArtifacts(ctx, db, database, tables)
	if err != nil {
		return nil, nil, fmt.Errorf("find other schema changes' artifacts in database %s: %w", database, err)
	}
	if len(unaccounted) == 0 {
		return present, nil, nil
	}

	e.logger.Warn("retaining a cancelled schema change's shared metadata: another schema change has work in this schema",
		"database", database,
		"tables", tables,
		"retained", present,
		"unaccounted_artifacts", unaccounted)

	retain = make([]string, 0, len(present))
	for _, name := range present {
		retain = append(retain, database+"."+name)
	}
	return nil, retain, nil
}

// Artifact name shapes. Spirit derives an artifact name by wrapping a table
// name in a leading underscore and one of these suffixes, reserving room for
// the suffix before it truncates — so both survive for a table name of any
// length, and a name carrying neither was not derived from a table name. A
// test pins them against Spirit's naming helpers, since a shape restated here
// that Spirit later stopped producing would refuse every release carrying it.
const (
	artifactPrefix           = "_"
	shadowTableSuffix        = "_new"
	cutoverOriginalSuffix    = "_old"
	perTableCheckpointSuffix = "_chkpnt"
)

// verifyReleasableArtifacts refuses to release any name that is not a schema
// change artifact's. Every name a release touches is derived from a table name
// by the helpers above, or is one of the two schema-level tables, so a name
// shaped like neither cannot have come from either — a table name that reached
// the DDL unwrapped, say.
//
// It guards the two functions that execute the DDL rather than the entry point
// that validates a request, because that is the last position from which a
// name cannot yet have been dropped or renamed. A release that would touch
// anything else does not run at all: the names are checked together, before the
// first statement, so a bad name cannot be found half way through.
func verifyReleasableArtifacts(database string, names []string) error {
	for _, name := range names {
		if isReleasableArtifactName(name) {
			continue
		}
		return fmt.Errorf("refusing to release %s.%s: not a schema change artifact name", database, name)
	}
	return nil
}

func isReleasableArtifactName(name string) bool {
	if name == sharedCheckpointTable || name == deferredCutoverSentinelTable {
		return true
	}
	if !strings.HasPrefix(name, artifactPrefix) {
		return false
	}
	return strings.HasSuffix(name, shadowTableSuffix) ||
		strings.HasSuffix(name, cutoverOriginalSuffix) ||
		strings.HasSuffix(name, perTableCheckpointSuffix)
}

// preserveArtifacts renames the given tables into the pending drops quarantine
// so the rows they hold stay recoverable until retention expires.
func preserveArtifacts(ctx context.Context, db *sql.DB, database string, names []string) ([]engine.PreservedArtifact, error) {
	if len(names) == 0 {
		return nil, nil
	}
	if err := verifyReleasableArtifacts(database, names); err != nil {
		return nil, err
	}

	moves := make([]pendingdrops.TableMove, 0, len(names))
	for _, name := range names {
		moves = append(moves, pendingdrops.TableMove{SchemaName: database, TableName: name})
	}
	// One atomic RENAME covers every table, so a name that no longer exists
	// would fail the whole move — which is why only tables just confirmed
	// present are passed.
	moved, err := pendingdrops.MoveTables(ctx, db, moves, time.Now())
	if err != nil {
		return nil, fmt.Errorf("preserve cancelled schema change data from database %s: %w", database, err)
	}

	preserved := make([]engine.PreservedArtifact, 0, len(moved))
	for _, table := range moved {
		preserved = append(preserved, engine.PreservedArtifact{
			Source:      database + "." + table.TableName,
			Destination: table.QuarantineSchema + "." + table.QuarantineTable,
		})
	}
	return preserved, nil
}

// discardArtifacts drops the given tables outright, reporting each by its full
// schema.table name so a release reads the same way whichever half disposed of
// a given artifact.
func discardArtifacts(ctx context.Context, db *sql.DB, database string, names []string) ([]string, error) {
	if err := verifyReleasableArtifacts(database, names); err != nil {
		return nil, err
	}

	discarded := make([]string, 0, len(names))
	for _, name := range names {
		if _, err := db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s.%s",
			quoteIdentifier(database), quoteIdentifier(name))); err != nil {
			return nil, fmt.Errorf("discard cancelled schema change artifact %s.%s: %w", database, name, err)
		}
		discarded = append(discarded, database+"."+name)
	}
	return discarded, nil
}

// dataBearingArtifacts names the tables that hold rows: the shadow table the
// copy was written into, and the original table a completed cutover swapped
// out. Both are preserved rather than dropped.
func dataBearingArtifacts(tables []string) []string {
	names := make([]string, 0, len(tables)*2)
	for _, table := range tables {
		names = append(names, utils.NewTableName(table), utils.OldTableName(table))
	}
	return names
}

// perTableMetadataArtifacts names the per-table checkpoints, which describe a
// copy rather than hold one. Each is derived from a table the caller named, so
// no schema change other than the cancelled one can own it.
func perTableMetadataArtifacts(tables []string) []string {
	names := make([]string, 0, len(tables))
	for _, table := range tables {
		names = append(names, utils.CheckpointTableName(table))
	}
	return names
}

// schemaScopedArtifacts names the two tables that belong to the schema change
// rather than to any one of its tables, and so are named whatever tables it
// touched. They are the only artifacts a release can reach that a schema change
// other than the cancelled one can own, which is why they are the ones the
// release gives up on the moment the schema shows another change's work.
func schemaScopedArtifacts() []string {
	return []string{sharedCheckpointTable, deferredCutoverSentinelTable}
}

func isSchemaScopedArtifact(name string) bool {
	return name == sharedCheckpointTable || name == deferredCutoverSentinelTable
}

// unaccountedArtifacts names the artifacts in database that the caller's tables
// do not derive, so their presence means a schema change other than the
// cancelled one has work in this schema. The schema-scoped tables are excluded:
// they are what the answer decides the fate of, and they are owned by no
// particular change.
//
// This is what stands between a release and a cutover it was never asked to
// perform. The deferred-cutover sentinel carries no owner and no timestamp —
// it is one empty table, shared, and created with IF NOT EXISTS — so it cannot
// be attributed by reading it. What can be attributed is the rest of the
// schema: a change reaches a deferred cutover only after copying, and it writes
// its shadow table and its checkpoint before it creates the sentinel, so a
// change that could be waiting on that sentinel has always left one of those
// behind by the time a release looks.
//
// It errs toward leaving things alone. An artifact abandoned by some earlier
// change reads the same as a live one and retains the shared tables too, which
// costs an operator two tables to remove by hand and costs a live schema change
// nothing.
func unaccountedArtifacts(ctx context.Context, db *sql.DB, database string, tables []string) ([]string, error) {
	accountedFor := make(map[string]bool)
	for _, name := range dataBearingArtifacts(tables) {
		accountedFor[name] = true
	}
	for _, name := range perTableMetadataArtifacts(tables) {
		accountedFor[name] = true
	}

	// Every artifact name starts with the prefix, and `_` is a single-character
	// wildcard in LIKE, so it is escaped rather than passed through as one.
	rows, err := db.QueryContext(ctx, `
		SELECT table_name FROM information_schema.tables
		WHERE table_schema = ? AND table_name LIKE ? ESCAPE '!'`,
		database, "!"+artifactPrefix+"%")
	if err != nil {
		return nil, fmt.Errorf("query information_schema for artifacts in %s: %w", database, err)
	}
	defer utils.CloseAndLog(rows)

	var unaccounted []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("scan artifact name in %s: %w", database, err)
		}
		if accountedFor[name] || isSchemaScopedArtifact(name) || !isReleasableArtifactName(name) {
			continue
		}
		unaccounted = append(unaccounted, name)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read artifact names in %s: %w", database, err)
	}

	sort.Strings(unaccounted)
	return unaccounted, nil
}

// existingTables returns the subset of candidates that exist in database,
// preserving the order they were given in so the result is stable.
func existingTables(ctx context.Context, db *sql.DB, database string, candidates []string) ([]string, error) {
	if len(candidates) == 0 {
		return nil, nil
	}

	args := make([]any, 0, len(candidates)+1)
	args = append(args, database)
	placeholders := make([]string, len(candidates))
	for i, name := range candidates {
		placeholders[i] = "?"
		args = append(args, name)
	}

	query := fmt.Sprintf(`
		SELECT table_name FROM information_schema.tables
		WHERE table_schema = ? AND table_name IN (%s)`, strings.Join(placeholders, ", "))
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query information_schema for %v: %w", candidates, err)
	}
	defer utils.CloseAndLog(rows)

	present := make(map[string]bool, len(candidates))
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("scan table name for %v: %w", candidates, err)
		}
		present[name] = true
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read table names for %v: %w", candidates, err)
	}

	// A caller may name the same table more than once. The quarantine move is a
	// single atomic RENAME, so a source repeated within it fails the whole
	// release; each present table is therefore taken exactly once.
	found := make([]string, 0, len(present))
	for _, name := range candidates {
		if present[name] {
			present[name] = false
			found = append(found, name)
		}
	}
	return found, nil
}
