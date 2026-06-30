package planetscale

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	ps "github.com/planetscale/planetscale-go/planetscale"

	"github.com/block/spirit/pkg/utils"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/psclient"
	"github.com/block/schemabot/pkg/state"
)

// Progress polls deploy request status from PlanetScale's API and optionally queries
// SHOW VITESS_MIGRATIONS for per-table, per-shard row counts and ETA.
func (e *Engine) Progress(ctx context.Context, req *engine.ProgressRequest) (*engine.ProgressResult, error) {
	if req.ResumeState == nil || req.ResumeState.Metadata == "" {
		return &engine.ProgressResult{
			State:   engine.StatePending,
			Message: "No active schema change",
		}, nil
	}

	meta, err := decodePSMetadata(req.ResumeState.Metadata)
	if err != nil {
		return nil, fmt.Errorf("decode resume state: %w", err)
	}

	if meta.DeployRequestID == 0 {
		return &engine.ProgressResult{
			State:    engine.StatePending,
			Message:  fmt.Sprintf("Setting up branch %s", meta.BranchName),
			Metadata: psDisplayMetadata(meta),
		}, nil
	}

	client, err := e.getClient(req.Credentials)
	if err != nil {
		return nil, fmt.Errorf("get planetscale client: %w", err)
	}

	dr, err := e.getDeployRequest(ctx, client, credOrg(req.Credentials), req.Database, meta.DeployRequestID)
	if err != nil {
		// Deploy request not found is permanent (e.g., server restarted
		// with fresh LocalScale state but stale apply record).
		var psErr *ps.Error
		if errors.As(err, &psErr) && psErr.Code == ps.ErrNotFound {
			return nil, engine.NewPermanentError("deploy request #%d not found: %w", meta.DeployRequestID, err)
		}
		return nil, fmt.Errorf("get deploy request: %w", err)
	}

	engineState := deployStateToEngineState(dr.DeploymentState)

	// Deferred deploy: the deploy request is ready but hasn't been triggered yet.
	if meta.DeferredDeploy && dr.DeploymentState == deployState.Ready {
		engineState = engine.StateWaitingForDeploy
	}

	// Update instant DDL flag from deploy request if not already set.
	if !meta.IsInstant && dr.Deployment != nil && dr.Deployment.InstantDDLEligible {
		meta.IsInstant = true
	}

	// Update metadata with DeployedAt when available (used by tern layer for
	// revert window timeout calculation).
	metaChanged := false
	if dr.DeployedAt != nil && meta.DeployedAt == nil {
		meta.DeployedAt = dr.DeployedAt
		metaChanged = true
	}

	// Track the VSchema-application phase so it can be surfaced from stored state
	// without a synthetic task row. Persisting it through the resume state lets a
	// progress read served from storage project it via PSDisplayMetadata.
	if vs := nextVSchemaStatus(meta.VSchemaStatus, dr.DeploymentState); vs != meta.VSchemaStatus {
		meta.VSchemaStatus = vs
		metaChanged = true
	}

	// Re-encode once if anything changed. A failed encode would desync the live
	// progress projection from what persists to storage, so surface it rather
	// than continuing with stale resume state.
	if metaChanged {
		encoded, err := encodePSMetadata(meta)
		if err != nil {
			return nil, fmt.Errorf("encode planetscale resume metadata for deploy request #%d: %w", meta.DeployRequestID, err)
		}
		req.ResumeState = &engine.ResumeState{
			MigrationContext: req.ResumeState.MigrationContext,
			Metadata:         encoded,
		}
	}

	// Late schema-change-context recovery. A progress poll can run in a process
	// that never captured the pre-deploy baseline — a different replica, or an
	// apply whose deploy was created before Vitess exposed its context — so the
	// stored context is empty even though the deploy is live and producing
	// per-shard migrations. Rediscover it, diffing against whatever baseline is
	// persisted in engine resume metadata (possibly empty on a fresh database, in
	// which case every in-flight context is a candidate) and anchored to the
	// deploy's creation time; selection stays ambiguous and keeps the empty value
	// when it can't safely choose. Keep the rest of the resume state intact so the
	// per-shard query below can attach progress.
	if req.ResumeState.MigrationContext == "" {
		recovered := e.discoverMigrationContext(ctx, client, req.Database, req.Credentials, meta.ExistingMigrationCtxs, dr.CreatedAt)
		if recovered != "" {
			e.logger.Info("recovered migration context on progress poll",
				"database", req.Database, "deploy_request", meta.DeployRequestID, "context", recovered)
			req.ResumeState = &engine.ResumeState{
				MigrationContext: recovered,
				Metadata:         req.ResumeState.Metadata,
			}
		}
	}

	e.logger.Debug("progress poll",
		"database", req.Database,
		"deploy_request", meta.DeployRequestID,
		"deploy_state", dr.DeploymentState,
		"engine_state", engineState,
		"is_instant", meta.IsInstant,
		"has_migration_context", req.ResumeState != nil && req.ResumeState.MigrationContext != "",
		"has_vtgate_dsn", req.Credentials.DSN != "",
	)

	result := &engine.ProgressResult{
		State:       engineState,
		Message:     deployStateToMessage(dr.DeploymentState),
		ResumeState: req.ResumeState,
		Metadata:    psDisplayMetadata(meta),
	}

	// Enrich with per-shard progress from SHOW VITESS_MIGRATIONS.
	// Requires a vtgate DSN (Credentials.DSN) and a migration context
	// (from the engine resume state) to query per-shard state.
	hasVtgateDSN := req.Credentials.DSN != ""
	hasMigrationContext := req.ResumeState != nil && req.ResumeState.MigrationContext != ""
	if hasVtgateDSN && hasMigrationContext {
		tables, overallProgress := e.queryVitessMigrations(ctx, client, req.Database, req.Credentials, req.ResumeState.MigrationContext)
		e.logger.Debug("vitess migrations queried",
			"database", req.Database,
			"table_count", len(tables),
			"overall_progress", overallProgress,
		)
		if len(tables) > 0 {
			result.Tables = tables
			if overallProgress > 0 {
				result.Progress = overallProgress
			}
		}
	} else {
		// No per-shard/row-copy progress this poll. A missing vtgate DSN is a target
		// resolution gap that persists for the whole apply (warned once at apply
		// start); an unset MigrationContext is transient during setup/recovery.
		// Either way the comment and CLI fall back to deploy-request state.
		e.logger.Debug("skipping per-shard progress",
			"database", req.Database,
			"has_vtgate_dsn", hasVtgateDSN,
			"has_migration_context", hasMigrationContext,
		)
	}

	// Propagate instant DDL flag to all tables. Instant DDL may complete
	// before migration context discovery, so we use the flag from deploy
	// metadata as the authoritative source.
	if meta.IsInstant {
		e.logger.Debug("marking tables as instant DDL",
			"database", req.Database,
			"table_count", len(result.Tables),
		)
		for i := range result.Tables {
			result.Tables[i].IsInstant = true
		}
	}

	return result, nil
}

// captureExistingContexts returns the set of migration_context values currently
// in SHOW VITESS_MIGRATIONS. Used as a baseline before deploying so that new
// contexts can be identified after deploy.
func (e *Engine) captureExistingContexts(ctx context.Context, client psclient.PSClient, database string, creds *engine.Credentials) map[string]MigrationContextTimestamps {
	existing := make(map[string]MigrationContextTimestamps)
	if creds.DSN == "" {
		return existing
	}

	branch := mainBranch(creds)
	keyspaces, err := client.ListKeyspaces(ctx, &ps.ListKeyspacesRequest{
		Organization: credOrg(creds),
		Database:     database,
		Branch:       branch,
	})
	if err != nil {
		e.logger.Warn("captureExistingContexts: failed to list keyspaces", "error", err)
		return existing
	}

	for _, ks := range keyspaces {
		rows, err := e.showVitessMigrationsForKeyspace(ctx, creds.DSN, ks.Name, "")
		if err != nil {
			e.logger.Debug("capture existing contexts: query failed", "keyspace", ks.Name, "error", err)
			continue
		}
		for _, r := range rows {
			if r.MigrationContext != "" {
				existing[r.MigrationContext] = migrationRowTimestamps(r)
			}
		}
	}

	e.logger.Info("captured schema change context baseline", "count", len(existing))
	return existing
}

// migrationRowTimestamps snapshots a baseline row's Vitess timestamp fields for
// durable storage. The values are diagnostic; baseline membership is keyed by
// migration_context.
func migrationRowTimestamps(row vitessMigrationRow) MigrationContextTimestamps {
	return MigrationContextTimestamps{
		RequestedTimestamp: formatMigrationTimestamp(row.RequestedAt),
		StartedTimestamp:   formatMigrationTimestamp(row.StartedAt),
		CompletedTimestamp: formatMigrationTimestamp(row.CompletedAt),
	}
}

func formatMigrationTimestamp(ts *time.Time) string {
	if ts == nil {
		return ""
	}
	return ts.UTC().Format(time.RFC3339)
}

// discoverMigrationContext finds the schema change context that this apply's
// deploy created. It collects current SHOW VITESS_MIGRATIONS rows across all
// keyspaces and selects the single non-baseline, non-terminal context via
// selectSchemaChangeContext. Returns "" when no unambiguous candidate exists
// (zero or multiple), so the caller keeps the stored identifier rather than
// attaching progress to the wrong context.
func (e *Engine) discoverMigrationContext(ctx context.Context, client psclient.PSClient, database string, creds *engine.Credentials, existingContexts map[string]MigrationContextTimestamps, deployCreatedAt time.Time) string {
	if creds.DSN == "" {
		e.logger.Debug("skipping schema change context discovery, no DSN configured")
		return ""
	}

	e.logger.Info("discovering schema change context", "database", database, "baseline_count", len(existingContexts), "deploy_created_at", deployCreatedAt)

	branch := mainBranch(creds)
	keyspaces, err := client.ListKeyspaces(ctx, &ps.ListKeyspacesRequest{
		Organization: credOrg(creds),
		Database:     database,
		Branch:       branch,
	})
	if err != nil {
		e.logger.Warn("failed to list keyspaces for schema change context discovery", "error", err)
		return ""
	}

	var rows []vitessMigrationRow
	for _, ks := range keyspaces {
		ksRows, err := e.showVitessMigrationsForKeyspace(ctx, creds.DSN, ks.Name, "")
		if err != nil {
			e.logger.Debug("failed to query schema changes for keyspace", "keyspace", ks.Name, "error", err)
			continue
		}
		rows = append(rows, ksRows...)
	}

	discovered, candidates := selectSchemaChangeContext(rows, existingContexts, deployCreatedAt)
	switch {
	case discovered != "":
		e.logger.Info("discovered schema change context", "database", database, "context", discovered)
		return discovered
	case len(candidates) > 1:
		// Multiple in-flight contexts not in the baseline, and the
		// requested-at/after-deploy tie-break could not isolate one. Attaching to
		// any one could render the wrong change's progress, so keep the stored
		// identifier.
		e.logger.Warn("multiple in-flight schema change contexts; keeping stored identifier to avoid attaching to the wrong change",
			"database", database, "candidate_count", len(candidates), "candidates", candidates)
		return ""
	default:
		e.logger.Warn("schema change context not discovered yet", "database", database)
		return ""
	}
}

// selectSchemaChangeContext picks the schema change context created by this
// apply's deploy from a set of SHOW VITESS_MIGRATIONS rows. Candidates are the
// distinct contexts that are NOT in the pre-deploy baseline AND are still
// non-terminal — a freshly started change is queued or running, whereas the
// completed history that SHOW VITESS_MIGRATIONS retains is terminal and must be
// ignored so an empty-baseline rediscovery never attaches to an old, unrelated
// change. It returns the single context when exactly one candidate remains and
// the full candidate list so the caller can distinguish the zero and multiple
// cases (both ambiguous, both must keep the stored identifier).
func selectSchemaChangeContext(rows []vitessMigrationRow, existingContexts map[string]MigrationContextTimestamps, deployCreatedAt time.Time) (string, []string) {
	// A context is a candidate if it is absent from the pre-deploy baseline and
	// any of its shards is still non-terminal: the change is in flight even when
	// some shards have already finished. earliestRequested tracks each candidate's
	// earliest requested_timestamp for the multi-candidate tie-break below.
	nonTerminal := make(map[string]bool)
	earliestRequested := make(map[string]*time.Time)
	for _, r := range rows {
		if r.MigrationContext == "" {
			continue
		}
		if _, inBaseline := existingContexts[r.MigrationContext]; inBaseline {
			continue
		}
		if !state.IsTerminalVitessState(r.Status) {
			nonTerminal[r.MigrationContext] = true
		}
		if earlierTime(r.RequestedAt, earliestRequested[r.MigrationContext]) {
			earliestRequested[r.MigrationContext] = r.RequestedAt
		}
	}

	candidates := make([]string, 0, len(nonTerminal))
	for c := range nonTerminal {
		candidates = append(candidates, c)
	}
	sort.Strings(candidates)

	if len(candidates) <= 1 {
		if len(candidates) == 1 {
			return candidates[0], candidates
		}
		return "", candidates
	}

	// More than one in-flight, non-baseline candidate. This deploy's context was
	// requested at or after the deploy was created, so prefer the earliest such
	// candidate; ties break deterministically on the context string. If no
	// candidate has a requested timestamp at/after the deploy — including when
	// timestamps are unavailable — stay ambiguous so the caller keeps the stored
	// identifier rather than attaching to the wrong change.
	best := ""
	var bestRequested *time.Time
	for _, c := range candidates {
		requested := earliestRequested[c]
		if requested == nil || requested.Before(deployCreatedAt) {
			continue
		}
		if best == "" || requested.Before(*bestRequested) || (requested.Equal(*bestRequested) && c < best) {
			best = c
			bestRequested = requested
		}
	}
	if best != "" {
		return best, candidates
	}
	return "", candidates
}

// earlierTime reports whether candidate is strictly earlier than current,
// treating a nil current as "unset" (any real candidate is earlier) and a nil
// candidate as never earlier. Used to track the earliest requested_timestamp
// seen for a context.
func earlierTime(candidate, current *time.Time) bool {
	if candidate == nil {
		return false
	}
	if current == nil {
		return true
	}
	return candidate.Before(*current)
}

// migrationContextDiscoveryAttempts and migrationContextDiscoveryInterval bound
// how long discoverSchemaChangeContextWithRetry waits for Vitess to create
// migrations after a deploy request is submitted. Vitess does not always create
// them immediately, so the first poll can legitimately return nothing.
const (
	migrationContextDiscoveryAttempts = 10
	migrationContextDiscoveryInterval = 500 * time.Millisecond
)

// discoverSchemaChangeContextWithRetry polls discoverMigrationContext until a new
// Vitess context appears or the bounded attempts are exhausted. Vitess may not
// have created migrations immediately after the deploy request is submitted, so
// a single poll can miss the context. Returns "" if no new context was found
// within the window; respects ctx cancellation between attempts.
func (e *Engine) discoverSchemaChangeContextWithRetry(ctx context.Context, client psclient.PSClient, database string, creds *engine.Credentials, existingContexts map[string]MigrationContextTimestamps, deployCreatedAt time.Time) string {
	// Without a vtgate DSN there is nothing to poll; retrying would only burn the
	// discovery window on a query that can never succeed.
	if creds.DSN == "" {
		e.logger.Debug("skipping schema change context discovery, no DSN configured", "database", database)
		return ""
	}

	for attempt := range migrationContextDiscoveryAttempts {
		migrationContext := e.discoverMigrationContext(ctx, client, database, creds, existingContexts, deployCreatedAt)
		if migrationContext != "" {
			return migrationContext
		}
		if attempt < migrationContextDiscoveryAttempts-1 {
			select {
			case <-ctx.Done():
				e.logger.Debug("schema change context discovery cancelled", "database", database, "error", ctx.Err())
				return ""
			case <-time.After(migrationContextDiscoveryInterval):
			}
		}
	}
	return ""
}

// vitessMigrationRow holds a single row from SHOW VITESS_MIGRATIONS.
type vitessMigrationRow struct {
	MigrationUUID    string
	MigrationContext string
	Keyspace         string
	Shard            string
	Table            string
	Status           string // queued, running, ready_to_complete, complete, failed, cancelled
	ReadyToComplete  bool
	DDLAction        string
	Progress         int
	ETASeconds       int64
	RowsCopied       int64
	TableRows        int64
	IsImmediate      bool
	CutoverAttempts  int
	RequestedAt      *time.Time
	StartedAt        *time.Time
	CompletedAt      *time.Time
}

// queryVitessMigrations queries SHOW VITESS_MIGRATIONS across all keyspaces via vtgate
// and aggregates per-shard results into per-table TableProgress entries.
func (e *Engine) queryVitessMigrations(ctx context.Context, client psclient.PSClient, database string, creds *engine.Credentials, migrationContext string) ([]engine.TableProgress, int) {
	branch := mainBranch(creds)
	keyspaces, err := client.ListKeyspaces(ctx, &ps.ListKeyspacesRequest{
		Organization: credOrg(creds),
		Database:     database,
		Branch:       branch,
	})
	if err != nil {
		e.logger.Warn("queryVitessMigrations: failed to list keyspaces", "error", err)
		return nil, 0
	}

	var allRows []vitessMigrationRow
	for _, ks := range keyspaces {
		rows, err := e.showVitessMigrationsForKeyspace(ctx, creds.DSN, ks.Name, migrationContext)
		if err != nil {
			e.logger.Error("per-shard progress query failed", "keyspace", ks.Name, "database", database, "error", err)
			continue
		}
		allRows = append(allRows, rows...)
	}

	if len(allRows) == 0 {
		return nil, 0
	}

	return aggregateShardProgress(allRows)
}

// showVitessMigrationsForKeyspace connects to vtgate and runs
// SHOW VITESS_MIGRATIONS LIKE '<context>' for a single keyspace.
// If migrationContext is empty, returns all migrations.
func (e *Engine) showVitessMigrationsForKeyspace(ctx context.Context, dsn, keyspace, migrationContext string) ([]vitessMigrationRow, error) {
	if migrationContext != "" {
		if err := validateMigrationContext(migrationContext); err != nil {
			return nil, fmt.Errorf("validate context for keyspace %s: %w", keyspace, err)
		}
	}

	db, err := e.getVtgateDB(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("get vtgate connection for keyspace %s: %w", keyspace, err)
	}

	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("get connection: %w", err)
	}
	defer utils.CloseAndLog(conn)

	if _, err := conn.ExecContext(ctx, "USE `"+keyspace+"`"); err != nil {
		return nil, fmt.Errorf("use keyspace %s: %w", keyspace, err)
	}

	query := "SHOW VITESS_MIGRATIONS"
	if migrationContext != "" {
		query += " LIKE '" + migrationContext + "'"
	}
	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("show vitess_migrations: %w", err)
	}
	defer utils.CloseAndLog(rows)

	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("get columns: %w", err)
	}

	var result []vitessMigrationRow
	for rows.Next() {
		colValues := make([]sql.NullString, len(columns))
		colPtrs := make([]any, len(columns))
		for i := range colValues {
			colPtrs[i] = &colValues[i]
		}
		if err := rows.Scan(colPtrs...); err != nil {
			e.logger.Debug("scan vitess_migrations row", "keyspace", keyspace, "error", err)
			continue
		}
		colMap := make(map[string]string)
		for i, col := range columns {
			if colValues[i].Valid {
				colMap[col] = colValues[i].String
			}
		}

		row := vitessMigrationRow{
			MigrationUUID:    colMap["migration_uuid"],
			MigrationContext: colMap["migration_context"],
			Keyspace:         colMap["keyspace"],
			Shard:            colMap["shard"],
			Table:            colMap["mysql_table"],
			Status:           colMap["migration_status"],
			ReadyToComplete:  colMap["ready_to_complete"] == "1",
			DDLAction:        colMap["ddl_action"],
			IsImmediate:      colMap["is_immediate_operation"] == "1",
		}
		if v, err := parseProgressPercent(colMap["progress"]); err != nil {
			e.logger.Debug("parse vitess_migrations field", "field", "progress", "value", colMap["progress"], "error", err)
		} else {
			row.Progress = v
		}
		if v, err := parseInt64(colMap["eta_seconds"]); err != nil {
			e.logger.Debug("parse vitess_migrations field", "field", "eta_seconds", "value", colMap["eta_seconds"], "error", err)
		} else {
			row.ETASeconds = v
		}
		if v, err := parseInt64(colMap["rows_copied"]); err != nil {
			e.logger.Debug("parse vitess_migrations field", "field", "rows_copied", "value", colMap["rows_copied"], "error", err)
		} else {
			row.RowsCopied = v
		}
		if v, err := parseInt64(colMap["table_rows"]); err != nil {
			e.logger.Debug("parse vitess_migrations field", "field", "table_rows", "value", colMap["table_rows"], "error", err)
		} else {
			row.TableRows = v
		}
		if v, err := parseInt64(colMap["cutover_attempts"]); err == nil {
			row.CutoverAttempts = int(v)
		}

		if ts, parseErr := time.Parse("2006-01-02 15:04:05", colMap["requested_timestamp"]); parseErr == nil {
			row.RequestedAt = &ts
		}
		if ts, parseErr := time.Parse("2006-01-02 15:04:05", colMap["started_timestamp"]); parseErr == nil {
			row.StartedAt = &ts
		}
		if ts, parseErr := time.Parse("2006-01-02 15:04:05", colMap["completed_timestamp"]); parseErr == nil {
			row.CompletedAt = &ts
		}

		result = append(result, row)
	}
	return result, rows.Err()
}

// validateMigrationContext rejects migration context strings containing unsafe characters.
func validateMigrationContext(s string) error {
	if strings.ContainsAny(s, "'\"\\`") {
		return fmt.Errorf("invalid context: contains unsafe characters")
	}
	return nil
}

// psDisplayMetadata projects the engine's deploy metadata into the progress
// result's display fields, so the renderer gets branch / deploy-request URL /
// instant / deferred status straight from the progress result — no core decoding
// of the opaque resume state and no engine-specific side table.
func psDisplayMetadata(meta *psMetadata) map[string]string {
	if meta == nil {
		return nil
	}
	// Allocate lazily: most polls set at least one field, but a bare metadata
	// blob should return nil without allocating on the hot progress path.
	var m map[string]string
	set := func(k, v string) {
		if m == nil {
			m = make(map[string]string, 4)
		}
		m[k] = v
	}
	if meta.BranchName != "" {
		set("branch_name", meta.BranchName)
	}
	if meta.DeployRequestURL != "" {
		set("deploy_request_url", meta.DeployRequestURL)
	}
	if meta.IsInstant {
		set("is_instant", "true")
	}
	if meta.DeferredDeploy {
		set("deferred_deploy", "true")
	}
	if meta.RevertExpiresAt != nil {
		set("revert_expires_at", meta.RevertExpiresAt.UTC().Format(time.RFC3339))
	}
	// Project per-keyspace VSchema state. A PlanetScale deploy has a single
	// VSchema phase, so every changed keyspace carries the same deploy-level
	// status; the per-keyspace shape lets engines that apply VSchema per keyspace
	// (and the renderers) track them independently.
	if changes := vschemaChangesForDisplay(meta); len(changes) > 0 {
		if encoded, err := apitypes.EncodeVSchemaChanges(changes); err != nil {
			slog.Warn("failed to encode VSchema changes for display metadata", "error", err)
		} else if encoded != "" {
			set(apitypes.VSchemaChangesMetadataKey, encoded)
		}
	}
	return m
}

// vschemaChangesForDisplay builds the per-keyspace VSchema display entries from
// stored metadata, stamping each keyspace with the deploy-level VSchema status.
func vschemaChangesForDisplay(meta *psMetadata) []apitypes.VSchemaChange {
	if len(meta.VSchemaDiffs) == 0 {
		return nil
	}
	changes := make([]apitypes.VSchemaChange, 0, len(meta.VSchemaDiffs))
	for _, d := range meta.VSchemaDiffs {
		changes = append(changes, apitypes.VSchemaChange{
			Namespace: d.Namespace,
			Status:    meta.VSchemaStatus,
			Diff:      d.Diff,
		})
	}
	return changes
}

// parseProgressPercent parses the Vitess schema_migrations.progress column,
// which is a float percentage (e.g. "54.35"), rounds it to the nearest int,
// and clamps the result to [0, 100].
func parseProgressPercent(s string) (int, error) {
	if s == "" {
		return 0, nil
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, fmt.Errorf("parse progress percent %q: %w", s, err)
	}
	pct := int(math.Round(f))
	if pct < 0 {
		return 0, nil
	}
	return min(pct, 100), nil
}

func parseInt64(s string) (int64, error) {
	if s == "" {
		return 0, nil
	}
	return strconv.ParseInt(s, 10, 64)
}

// aggregateShardProgress groups SHOW VITESS_MIGRATIONS rows by migration_uuid
// and produces per-table progress with per-shard breakdown.
func aggregateShardProgress(rows []vitessMigrationRow) ([]engine.TableProgress, int) {
	type tableKey struct {
		keyspace string
		table    string
		uuid     string
	}
	type shardData struct {
		shard           string
		status          string
		readyToComplete bool
		progress        int
		rowsCopied      int64
		tableRows       int64
		etaSeconds      int64
		isImmediate     bool
		cutoverAttempts int
		startedAt       *time.Time
		completedAt     *time.Time
	}

	tableShards := make(map[tableKey][]shardData)
	tableOrder := make([]tableKey, 0)

	for _, r := range rows {
		key := tableKey{keyspace: r.Keyspace, table: r.Table, uuid: r.MigrationUUID}
		if _, exists := tableShards[key]; !exists {
			tableOrder = append(tableOrder, key)
		}
		tableShards[key] = append(tableShards[key], shardData{
			shard:           r.Shard,
			status:          r.Status,
			readyToComplete: r.ReadyToComplete,
			progress:        r.Progress,
			rowsCopied:      r.RowsCopied,
			tableRows:       r.TableRows,
			etaSeconds:      r.ETASeconds,
			isImmediate:     r.IsImmediate,
			cutoverAttempts: r.CutoverAttempts,
			startedAt:       r.StartedAt,
			completedAt:     r.CompletedAt,
		})
	}

	var totalRowsCopied, totalTableRows int64
	var tables []engine.TableProgress

	for _, key := range tableOrder {
		shards := tableShards[key]

		// Sort shards by Vitess key range for consistent ordering
		sort.Slice(shards, func(i, j int) bool {
			return shardLess(shards[i].shard, shards[j].shard)
		})

		var tblRowsCopied, tblTableRows, maxETA int64
		var tblProgress int
		var tblStartedAt *time.Time
		var latestCompletedAt *time.Time
		allShardsCompleted := true
		shardProgress := make([]engine.ShardProgress, len(shards))
		isInstant := true

		// Determine aggregate table state from shard states
		tableState := state.Vitess.Complete
		for i, sh := range shards {
			tblTableRows += sh.tableRows
			if sh.etaSeconds > maxETA {
				maxETA = sh.etaSeconds
			}
			if !sh.isImmediate {
				isInstant = false
			}
			// Table started_at = earliest shard started_at
			if sh.startedAt != nil && (tblStartedAt == nil || sh.startedAt.Before(*tblStartedAt)) {
				tblStartedAt = sh.startedAt
			}
			// Track latest completed_at across shards
			if sh.completedAt == nil {
				allShardsCompleted = false
			} else if latestCompletedAt == nil || sh.completedAt.After(*latestCompletedAt) {
				latestCompletedAt = sh.completedAt
			}

			// Resolve effective shard state: running + ready_to_complete = ready_to_complete
			shardState := sh.status
			if sh.status == state.Vitess.Running && sh.readyToComplete {
				shardState = state.Vitess.ReadyToComplete
			}

			shardPct := min(sh.progress, 100)
			shardCopied := sh.rowsCopied
			// When a shard is ready for cutover, the copy phase is complete.
			// Clamp to 100% since Vitess row counts can lag behind slightly.
			if shardState == state.Vitess.ReadyToComplete || shardState == state.Vitess.Complete {
				shardPct = 100
				if sh.tableRows > 0 && shardCopied < sh.tableRows {
					shardCopied = sh.tableRows
				}
			}

			tblRowsCopied += shardCopied

			shardProgress[i] = engine.ShardProgress{
				Shard:           sh.shard,
				State:           shardState,
				Progress:        shardPct,
				RowsCopied:      shardCopied,
				RowsTotal:       sh.tableRows,
				ETASeconds:      sh.etaSeconds,
				CutoverAttempts: sh.cutoverAttempts,
			}

			tableState = resolveTableState(tableState, shardState)
		}

		if tblTableRows > 0 {
			// rows_copied can momentarily exceed table_rows (concurrent inserts
			// during copy), so clamp to 100.
			tblProgress = min(int(tblRowsCopied*100/tblTableRows), 100)
		} else if tableState == state.Vitess.Complete || tableState == state.Vitess.ReadyToComplete {
			tblProgress = 100
		}

		totalRowsCopied += tblRowsCopied
		totalTableRows += tblTableRows

		// Table completed_at is only set when all shards have completed.
		var tblCompletedAt *time.Time
		if allShardsCompleted {
			tblCompletedAt = latestCompletedAt
		}

		tables = append(tables, engine.TableProgress{
			Namespace:   key.keyspace,
			Table:       key.table,
			State:       tableState,
			Progress:    tblProgress,
			RowsCopied:  tblRowsCopied,
			RowsTotal:   tblTableRows,
			ETASeconds:  maxETA,
			Shards:      shardProgress,
			IsInstant:   isInstant,
			StartedAt:   tblStartedAt,
			CompletedAt: tblCompletedAt,
		})
	}

	overallProgress := 0
	if totalTableRows > 0 {
		overallProgress = min(int(totalRowsCopied*100/totalTableRows), 100)
	} else if len(tables) > 0 {
		allDone := true
		for _, t := range tables {
			if t.State != state.Vitess.Complete && t.State != state.Vitess.ReadyToComplete {
				allDone = false
				break
			}
		}
		if allDone {
			overallProgress = 100
		}
	}

	return tables, overallProgress
}

// resolveTableState merges a shard's state into the current table state.
// A table has one Vitess migration per shard, each in a different state.
// This picks the "worst" state so the table reflects the least-progressed shard:
//
//	failed            — any shard failed, table is failed
//	running           — at least one shard still copying rows
//	queued            — at least one shard not started, none running or failed
//	ready_to_complete — all shards done copying, waiting for cutover
//	complete          — all shards finished (initial value)
func resolveTableState(tableState, shardState string) string {
	switch shardState {
	case state.Vitess.Failed, state.Vitess.Cancelled:
		return state.Vitess.Failed
	case state.Vitess.Running:
		if tableState != state.Vitess.Failed {
			return state.Vitess.Running
		}
	case state.Vitess.Queued, state.Vitess.Ready, state.Vitess.Requested:
		if tableState != state.Vitess.Failed && tableState != state.Vitess.Running {
			return state.Vitess.Queued
		}
	case state.Vitess.ReadyToComplete:
		if tableState == state.Vitess.Complete {
			return state.Vitess.ReadyToComplete
		}
	}
	return tableState
}

// shardLess compares two Vitess shard key ranges for sorting.
func shardLess(a, b string) bool {
	aStart := ""
	bStart := ""
	if idx := strings.Index(a, "-"); idx > 0 {
		aStart = a[:idx]
	}
	if idx := strings.Index(b, "-"); idx > 0 {
		bStart = b[:idx]
	}
	if aStart == "" && bStart != "" {
		return true
	}
	if aStart != "" && bStart == "" {
		return false
	}
	return aStart < bStart
}
