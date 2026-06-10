package planetscale

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	mysql "github.com/go-sql-driver/mysql"
	ps "github.com/planetscale/planetscale-go/planetscale"

	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/utils"
	"golang.org/x/sync/errgroup"

	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/lint"
	"github.com/block/schemabot/pkg/psclient"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/state"
)

// Apply starts executing a schema change plan.
// Creates a PlanetScale branch, applies DDL via MySQL connection to the branch,
// then creates and starts a deploy request.
func (e *Engine) Apply(ctx context.Context, req *engine.ApplyRequest) (*engine.ApplyResult, error) {
	e.logger.Info("applying plan",
		"plan_id", req.PlanID,
		"database", req.Database,
	)

	client, err := e.getClient(req.Credentials)
	if err != nil {
		return nil, fmt.Errorf("get planetscale client: %w", err)
	}

	org := credOrg(req.Credentials)
	main := mainBranch(req.Credentials)

	// Check if resuming
	if req.ResumeState != nil && req.ResumeState.Metadata != "" {
		return e.resumeApply(ctx, client, org, req)
	}

	// emitEvent logs a lifecycle event and sends it to the caller for apply_logs recording.
	emitEvent := func(event engine.ApplyEvent) {
		attrs := []any{"database", req.Database}
		for k, v := range event.Metadata {
			attrs = append(attrs, k, v)
		}
		e.logger.Info(event.Message, attrs...)
		if req.OnEvent != nil {
			req.OnEvent(event)
		}
	}

	// Track in-flight apply metadata for progress queries during setup.
	migCtx := ""
	if req.ResumeState != nil {
		migCtx = req.ResumeState.MigrationContext
	}
	// persistState persists apply metadata to storage via OnStateChange for crash recovery.
	// On first apply, migCtx is empty until the tern layer assigns one via ResumeState.
	// persistState is a no-op in this window — if the worker crashes before Apply returns,
	// there's no ResumeState to recover from. The tern layer handles this by retrying
	// the full Apply on the next heartbeat recovery cycle.
	persistState := func(meta *psMetadata) {
		if migCtx == "" || req.OnStateChange == nil {
			return
		}
		encoded, err := encodePSMetadata(meta)
		if err != nil {
			e.logger.Warn("failed to encode apply metadata for persistence", "error", err)
			return
		}
		req.OnStateChange(&engine.ResumeState{
			MigrationContext: migCtx,
			Metadata:         encoded,
		})
	}

	// Create or reuse a branch
	existingBranch := req.Options["branch"]
	var branchName string
	branchStart := time.Now()

	if existingBranch != "" {
		// Reuse existing branch: wait for ready, refresh schema from main, wait again
		branchName = existingBranch
		if branchName == main {
			return nil, engine.NewPermanentError("cannot reuse the %s branch: use a development branch", main)
		}
		persistState(&psMetadata{BranchName: branchName})
		emitEvent(engine.ApplyEvent{
			Message:  fmt.Sprintf("Reusing branch %s", branchName),
			Metadata: map[string]string{"branch": branchName},
			NewState: state.Apply.PreparingBranch,
		})

		// Verify branch exists
		if _, err := client.GetBranch(ctx, &ps.GetDatabaseBranchRequest{
			Organization: org, Database: req.Database, Branch: branchName,
		}); err != nil {
			return nil, fmt.Errorf("branch %s not found: %w", branchName, err)
		}

		// Wait for branch to be ready (may be initializing from a prior create)
		if err := e.waitForBranchReady(ctx, client, org, req.Database, branchName); err != nil {
			return nil, fmt.Errorf("wait for branch %s: %w", branchName, err)
		}

		// Sync with main to pick up latest schema
		emitEvent(engine.ApplyEvent{
			Message:  fmt.Sprintf("Refreshing schema for branch %s from %s", branchName, main),
			Metadata: map[string]string{"branch": branchName},
		})
		if err := client.RefreshSchema(ctx, org, req.Database, branchName); err != nil {
			return nil, fmt.Errorf("refresh schema for branch %s: %w", branchName, err)
		}

		// Wait for sync to complete
		if err := e.waitForBranchReady(ctx, client, org, req.Database, branchName); err != nil {
			return nil, fmt.Errorf("wait for schema refresh %s: %w", branchName, err)
		}
		elapsed := time.Since(branchStart).Round(time.Second)
		emitEvent(engine.ApplyEvent{
			Message:  fmt.Sprintf("Branch %s schema refreshed (%s)", branchName, elapsed),
			Metadata: map[string]string{"branch": branchName},
			NewState: state.Apply.ApplyingBranchChanges,
		})

	} else {
		// Create a new branch
		branchName = generateBranchName(req.Database, req.PlanID)
		persistState(&psMetadata{BranchName: branchName})
		emitEvent(engine.ApplyEvent{
			Message:  fmt.Sprintf("Creating branch %s", branchName),
			Metadata: map[string]string{"branch": branchName},
			NewState: state.Apply.PreparingBranch,
		})

		_, err = e.createBranch(ctx, client, org, req.Database, branchName, main)
		if err != nil {
			return nil, fmt.Errorf("create branch: %w", err)
		}

		// Wait for branch to be ready
		if err := e.waitForBranchReady(ctx, client, org, req.Database, branchName); err != nil {
			return nil, fmt.Errorf("wait for branch: %w", err)
		}
		elapsed := time.Since(branchStart).Round(time.Second)
		emitEvent(engine.ApplyEvent{
			Message:  fmt.Sprintf("Branch %s ready (%s)", branchName, elapsed),
			Metadata: map[string]string{"branch": branchName},
			NewState: state.Apply.ApplyingBranchChanges,
		})
	}

	// Get branch credentials for MySQL access (used for DDL apply and validation).
	pwCtx, pwCancel := context.WithTimeout(ctx, 10*time.Second)
	defer pwCancel()

	password, err := client.CreateBranchPassword(pwCtx, &ps.DatabaseBranchPasswordRequest{
		Organization: org,
		Database:     req.Database,
		Branch:       branchName,
		Role:         "admin",
		TTL:          3600,
	})
	if err != nil {
		return nil, fmt.Errorf("create branch password: %w", err)
	}

	// For reused branches, verify the branch schema matches main. If the
	// branch has stale DDL from a previous failed apply, RefreshSchema won't
	// remove it — the branch will be ahead of main, producing inverted
	// diffs (e.g., DROP COLUMN instead of ADD COLUMN).
	// Uses MySQL to fetch the branch schema (real-time, no API staleness).
	if existingBranch != "" {
		keyspaces := sortedKeyspaces(req.SchemaFiles)
		if err := e.verifyBranchMatchesMain(ctx, client, org, req.Database, branchName, main, keyspaces, req.SchemaFiles, password); err != nil {
			return nil, fmt.Errorf("branch %s has stale changes from a previous apply — delete the branch and retry without --branch: %w", branchName, err)
		}
	}

	// Apply DDL and VSchema changes to all keyspaces
	emitEvent(engine.ApplyEvent{
		Message:  "Applying changes to branch",
		Metadata: map[string]string{"branch": branchName},
		NewState: state.Apply.ApplyingBranchChanges,
	})
	if err := e.applyChangesToBranch(ctx, req.Changes, req.SchemaFiles, password, client, org, req.Database, branchName, emitEvent); err != nil {
		return nil, fmt.Errorf("apply changes to branch: %w", err)
	}
	ddlCount := 0
	for _, sc := range req.Changes {
		ddlCount += len(sc.TableChanges)
	}
	emitEvent(engine.ApplyEvent{
		Message:  fmt.Sprintf("Applied %d DDL changes to branch %s", ddlCount, branchName),
		Metadata: map[string]string{"branch": branchName},
		NewState: state.Apply.ValidatingBranch,
	})

	// Verify the branch now matches the desired schema. If DDL application
	// was partial or the branch had stale state, some tables may still differ.
	// Catch this before creating the deploy request to prevent deploying
	// unexpected changes (e.g., DROP COLUMN when ADD COLUMN was intended).
	//
	// DDL is fetched via MySQL (real-time), but VSchema is fetched via the
	// PlanetScale API which may return stale data after UpdateKeyspaceVSchema.
	// Retry up to 30s to allow the API to converge.
	keyspaces := sortedKeyspaces(req.SchemaFiles)
	if err := e.verifyBranchMatchesDesiredWithRetry(ctx, client, org, req.Database, branchName, keyspaces, req.SchemaFiles, password); err != nil {
		return nil, fmt.Errorf("branch validation failed after DDL apply: %w", err)
	}
	emitEvent(engine.ApplyEvent{
		Message:  "Branch schema validated — matches desired state",
		Metadata: map[string]string{"branch": branchName},
		NewState: state.Apply.CreatingDeployRequest,
	})

	// Capture existing migration_contexts before deploy so we can discover the new one
	existingContexts := e.captureExistingContexts(ctx, client, req.Database, req.Credentials)

	// Check defer options
	deferCutover := req.Options["defer_cutover"] == "true"
	deferDeploy := req.Options["defer_deploy"] == "true"

	// Create deploy request and wait for it to be ready.
	// The server computes the schema diff asynchronously — poll until the deploy
	// request transitions from "pending" to "ready" (or "no_changes"/"error").
	drStart := time.Now()
	autoDeleteBranch := existingBranch == "" // don't delete reused branches
	dr, err := e.createDeployRequest(ctx, client, org, req.Database, branchName, main, !deferCutover, autoDeleteBranch)
	if err != nil {
		return nil, fmt.Errorf("create deploy request: %w", err)
	}
	emitEvent(engine.ApplyEvent{
		Message: fmt.Sprintf("Deploy request #%d created, validating...", dr.Number),
		Metadata: map[string]string{
			"deploy_request_id":  fmt.Sprintf("%d", dr.Number),
			"deploy_request_url": dr.HtmlURL,
			"branch":             branchName,
		},
		NewState: state.Apply.ValidatingDeployRequest,
	})
	persistState(&psMetadata{
		BranchName:       branchName,
		DeployRequestID:  dr.Number,
		DeployRequestURL: dr.HtmlURL,
	})
	dr, err = e.waitForDeployRequestPending(ctx, client, org, req.Database, dr)
	if err != nil {
		return nil, err
	}
	if dr.DeploymentState == deployState.Error {
		errMsg := formatDeployRequestError(dr)
		emitEvent(engine.ApplyEvent{
			Message:  errMsg,
			Metadata: map[string]string{"deploy_request_id": fmt.Sprintf("%d", dr.Number)},
		})
		return nil, fmt.Errorf("%s", errMsg)
	}
	if dr.DeploymentState == deployState.NoChanges {
		emitEvent(engine.ApplyEvent{
			Message:  fmt.Sprintf("Deploy request #%d: no changes detected", dr.Number),
			Metadata: map[string]string{"deploy_request_id": fmt.Sprintf("%d", dr.Number)},
		})
		return &engine.ApplyResult{Message: "no changes detected"}, nil
	}

	// Determine instant DDL eligibility. Prefer instant when PlanetScale reports
	// it as eligible — instant DDL modifies metadata only (no row copy), so it
	// completes immediately and has no revert window regardless of skip_revert.
	// Instant DDL is orthogonal to defer flags — the mechanism (instant vs row copy)
	// is independent of when the deploy executes.
	instantEligible := dr.Deployment != nil && dr.Deployment.InstantDDLEligible
	useInstant := instantEligible

	// Log the raw deploy request fields for debugging instant DDL detection.
	if dr.Deployment != nil {
		e.logger.Info("deploy request deployment info",
			"database", req.Database,
			"deploy_request", dr.Number,
			"instant_ddl_eligible", dr.Deployment.InstantDDLEligible,
			"deployment_state", dr.Deployment.State,
		)
	} else {
		e.logger.Warn("deploy request has nil deployment",
			"database", req.Database,
			"deploy_request", dr.Number,
			"deploy_state", dr.DeploymentState,
		)
	}
	e.logger.Info("instant DDL decision",
		"database", req.Database,
		"deploy_request", dr.Number,
		"has_deployment", dr.Deployment != nil,
		"instant_eligible", instantEligible,
		"use_instant", useInstant,
		"defer_cutover", deferCutover,
		"defer_deploy", deferDeploy,
		"deploy_state", dr.DeploymentState,
	)

	// Log when --defer-cutover has no effect for instant DDL
	if deferCutover && useInstant {
		e.logger.Info("--defer-cutover has no effect for instant DDL",
			"database", req.Database,
			"deploy_request", dr.Number,
		)
		emitEvent(engine.ApplyEvent{
			Message: "Note: --defer-cutover has no effect for instant DDL",
		})
	}

	drElapsed := time.Since(drStart).Round(time.Second)
	readyMsg := fmt.Sprintf("Deploy request #%d ready (%s)", dr.Number, drElapsed)
	if useInstant {
		readyMsg += " — instant DDL eligible"
	}
	emitEvent(engine.ApplyEvent{
		Message: readyMsg,
		Metadata: map[string]string{
			"deploy_request_id":  fmt.Sprintf("%d", dr.Number),
			"deploy_request_url": dr.HtmlURL,
			"instant_ddl":        fmt.Sprintf("%t", useInstant),
		},
	})

	// Deferred deploy: don't call DeployDeployRequest yet. The user will review
	// the deploy request diff on PlanetScale and trigger via `schemabot cutover`.
	if deferDeploy {
		e.logger.Info("deferring deploy — user must trigger via cutover",
			"database", req.Database,
			"deploy_request", dr.Number,
			"instant_eligible", useInstant,
		)
		meta, encErr := encodePSMetadata(&psMetadata{
			BranchName:       branchName,
			DeployRequestID:  dr.Number,
			DeployRequestURL: dr.HtmlURL,
			IsInstant:        useInstant,
			DeferredDeploy:   true,
		})
		if encErr != nil {
			return nil, fmt.Errorf("encode metadata for deferred deploy request #%d: %w", dr.Number, encErr)
		}
		suffix := ""
		if useInstant {
			suffix = " (instant DDL)"
		}
		return &engine.ApplyResult{
			Accepted: true,
			Message:  fmt.Sprintf("Deploy request #%d ready%s — waiting for deploy", dr.Number, suffix),
			ResumeState: &engine.ResumeState{
				MigrationContext: migCtx,
				Metadata:         meta,
			},
		}, nil
	}

	// Deploy (starts the schema change). PlanetScale may still be validating
	// the deploy request even after reporting "ready", so retry on transient
	// validation errors.
	drNumber := dr.Number
	var deployErr error
	for attempt := range maxRetries {
		if attempt > 0 {
			delay := retryDelay(attempt, deployErr)
			e.logger.Warn("retrying deploy request", "deploy_request", drNumber, "attempt", attempt+1, "delay", delay.Round(time.Millisecond), "error", deployErr)
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(delay):
			}
		}
		dr, deployErr = client.DeployDeployRequest(ctx, &ps.PerformDeployRequest{
			Organization: org,
			Database:     req.Database,
			Number:       drNumber,
			InstantDDL:   useInstant,
		})
		if deployErr == nil {
			break
		}
		if strings.Contains(deployErr.Error(), "approved") {
			return nil, fmt.Errorf("deploy request #%d could not be deployed: PlanetScale deploy request approvals are not supported — disable 'Require administrator approval for deploy requests' in the PlanetScale database settings", drNumber)
		}
		if !isRetryablePSError(deployErr) {
			return nil, fmt.Errorf("deploy deploy request #%d: %w", drNumber, deployErr)
		}
	}
	if deployErr != nil {
		return nil, fmt.Errorf("deploy deploy request #%d (after %d attempts): %w", drNumber, maxRetries, deployErr)
	}

	emitEvent(engine.ApplyEvent{
		Message: fmt.Sprintf("Deploy request #%d deployed", dr.Number),
		Metadata: map[string]string{
			"deploy_request_id": fmt.Sprintf("%d", dr.Number),
			"instant_ddl":       fmt.Sprintf("%t", useInstant),
		},
	})

	// Discover migration_context by diffing current SHOW VITESS_MIGRATIONS against
	// the pre-deploy baseline. Retries because Vitess may not have created migrations
	// immediately after the deploy request is submitted.
	var migrationContext string
	for attempt := range 10 {
		migrationContext = e.discoverMigrationContext(ctx, client, req.Database, req.Credentials, existingContexts)
		if migrationContext != "" {
			break
		}
		if attempt < 9 {
			time.Sleep(500 * time.Millisecond)
		}
	}

	meta, err := encodePSMetadata(&psMetadata{
		BranchName:       branchName,
		DeployRequestID:  dr.Number,
		DeployRequestURL: dr.HtmlURL,
		IsInstant:        useInstant,
	})
	if err != nil {
		return nil, fmt.Errorf("encode metadata for deploy request #%d: %w", dr.Number, err)
	}

	return &engine.ApplyResult{
		Accepted: true,
		Message:  fmt.Sprintf("Deploy request #%d created", dr.Number),
		ResumeState: &engine.ResumeState{
			MigrationContext: migrationContext,
			Metadata:         meta,
		},
	}, nil
}

// applyChangesToBranch applies VSchema and DDL changes to all keyspaces.
// VSchema updates are applied sequentially (PlanetScale rejects concurrent
// VSchema writes during schema snapshots). DDL is applied in parallel after
// all VSchema changes are committed.
func (e *Engine) applyChangesToBranch(ctx context.Context, changes []engine.SchemaChange, schemaFiles schema.SchemaFiles, password *ps.DatabaseBranchPassword, client psclient.PSClient, org, database, branchName string, emitEvent func(engine.ApplyEvent)) error {
	if len(changes) == 0 {
		e.logger.Debug("no changes to apply to branch", "branch", branchName)
		return nil
	}

	total := len(changes)
	var applied atomic.Int32

	// Serialize event callbacks — OnEvent mutates shared apply state.
	var eventMu sync.Mutex
	safeEmit := func(event engine.ApplyEvent) {
		eventMu.Lock()
		defer eventMu.Unlock()
		emitEvent(event)
	}

	safeEmit(engine.ApplyEvent{
		Message:  fmt.Sprintf("Applying changes to %d keyspaces on branch %s", total, branchName),
		Metadata: map[string]string{"branch": branchName},
		NewState: state.Apply.ApplyingBranchChanges,
	})

	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(maxConcurrentKeyspaces)
	for _, sc := range changes {
		g.Go(func() error {
			if err := e.applyKeyspaceChanges(gCtx, sc, schemaFiles, password, client, org, database, branchName); err != nil {
				return err
			}
			n := int(applied.Add(1))
			safeEmit(engine.ApplyEvent{
				Message:  fmt.Sprintf("Applied keyspace %s (%d/%d)", sc.Namespace, n, total),
				Metadata: map[string]string{"keyspace": sc.Namespace},
			})
			return nil
		})
	}
	return g.Wait()
}

// applyKeyspaceChanges applies VSchema and DDL for a single keyspace with retries.
// Uses longer backoff when PlanetScale reports a schema snapshot is in progress.
func (e *Engine) applyKeyspaceChanges(ctx context.Context, sc engine.SchemaChange, schemaFiles schema.SchemaFiles, password *ps.DatabaseBranchPassword, client psclient.PSClient, org, database, branchName string) error {
	start := time.Now()
	e.logger.Info(fmt.Sprintf("applying changes to keyspace %s on branch %s", sc.Namespace, branchName),
		"keyspace", sc.Namespace,
		"ddl_count", len(sc.TableChanges),
		"has_vschema", sc.Metadata["vschema_changed"] == "true",
		"branch", branchName,
	)

	maxAttempts := maxRetries
	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if attempt > 0 {
			delay := retryDelay(attempt, lastErr)
			e.logger.Warn("retrying keyspace apply", "keyspace", sc.Namespace, "attempt", attempt+1, "delay", delay.Round(time.Millisecond), "error", lastErr)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}
		}

		if err := e.applyKeyspaceChangesOnce(ctx, sc, schemaFiles, password, client, org, database, branchName); err != nil {
			lastErr = err
			e.logger.Error(fmt.Sprintf("keyspace %s apply attempt %d failed", sc.Namespace, attempt+1), "keyspace", sc.Namespace, "attempt", attempt+1, "error", err)
			if !isRetryableEngineError(err) {
				return engine.NewPermanentError("apply keyspace %s: %w", sc.Namespace, err)
			}
			if isSnapshotInProgress(err) && maxAttempts == maxRetries {
				maxAttempts = maxSnapshotRetries
				e.logger.Info("schema snapshot in progress, extending retries",
					"keyspace", sc.Namespace, "max_attempts", maxAttempts)
			}
			continue
		}
		e.logger.Info(fmt.Sprintf("keyspace %s changes applied (%s)", sc.Namespace, time.Since(start).Round(time.Second)), "keyspace", sc.Namespace, "elapsed", time.Since(start).Round(time.Second))
		return nil
	}
	finalErr := fmt.Errorf("apply keyspace %s (after %d attempts): %w", sc.Namespace, maxAttempts, lastErr)
	return finalErr
}

// applyKeyspaceChangesOnce applies VSchema and DDL for a single keyspace in one attempt.
func (e *Engine) applyKeyspaceChangesOnce(ctx context.Context, sc engine.SchemaChange, schemaFiles schema.SchemaFiles, password *ps.DatabaseBranchPassword, client psclient.PSClient, org, database, branchName string) error {
	// Apply VSchema first — vtgate needs VSchema to route DDL correctly
	if vschemaContent := getVSchemaContent(sc, schemaFiles); vschemaContent != "" {
		if err := e.updateBranchVSchema(ctx, client, org, database, branchName, sc.Namespace, vschemaContent); err != nil {
			return fmt.Errorf("update vschema for %s: %w", sc.Namespace, err)
		}
		e.logger.Info(fmt.Sprintf("applied vschema for %s on branch %s", sc.Namespace, branchName), "keyspace", sc.Namespace, "branch", branchName)
	}

	if len(sc.TableChanges) == 0 {
		e.logger.Debug("no DDL for keyspace, vschema-only", "keyspace", sc.Namespace, "branch", branchName)
		return nil
	}

	// Build DSN targeting this specific keyspace.
	// TLS is configured automatically when RegisterMTLS has been called.
	mysqlCfg := mysql.NewConfig()
	mysqlCfg.User = password.Username
	mysqlCfg.Passwd = password.PlainText
	mysqlCfg.Net = "tcp"
	mysqlCfg.Addr = password.Hostname
	mysqlCfg.DBName = sc.Namespace
	mysqlCfg.InterpolateParams = true
	if mtlsRegistered.Load() {
		mysqlCfg.TLSConfig = mtlsConfigName
	}
	dsn := mysqlCfg.FormatDSN()

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("open branch connection for %s: %w", sc.Namespace, err)
	}
	defer utils.CloseAndLog(db)

	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping branch for %s: %w", sc.Namespace, err)
	}

	for _, tc := range sc.TableChanges {
		e.logger.Info(fmt.Sprintf("applying DDL to %s.%s on branch", sc.Namespace, tc.Table),
			"keyspace", sc.Namespace,
			"table", tc.Table,
			"operation", tc.Operation,
			"ddl", tc.DDL,
		)
		if _, err := db.ExecContext(ctx, tc.DDL); err != nil {
			return fmt.Errorf("execute DDL on %s.%s: %w\nstatement: %s", sc.Namespace, tc.Table, err, tc.DDL)
		}
	}
	return nil
}

// getVSchemaContent extracts the vschema.json content for a keyspace from schema files.
// Returns empty string if no VSchema change is needed.
func getVSchemaContent(sc engine.SchemaChange, schemaFiles schema.SchemaFiles) string {
	if sc.Metadata["vschema_changed"] != "true" {
		return ""
	}
	if ns, ok := schemaFiles[sc.Namespace]; ok && ns != nil {
		if content, ok := ns.Files["vschema.json"]; ok {
			return content
		}
	}
	return ""
}

// updateBranchVSchema updates the VSchema for a keyspace on a branch
// using the PlanetScale SDK's UpdateKeyspaceVSchema endpoint.
func (e *Engine) updateBranchVSchema(ctx context.Context, client psclient.PSClient, org, database, branch, keyspace, vschemaJSON string) error {
	e.logger.Info(fmt.Sprintf("updating VSchema for %s on branch %s", keyspace, branch),
		"branch", branch,
		"keyspace", keyspace,
	)
	_, err := client.UpdateKeyspaceVSchema(ctx, &ps.UpdateKeyspaceVSchemaRequest{
		Organization: org,
		Database:     database,
		Branch:       branch,
		Keyspace:     keyspace,
		VSchema:      vschemaJSON,
	})
	if err != nil {
		return fmt.Errorf("update vschema for keyspace %s on branch %s: %w", keyspace, branch, err)
	}
	return nil
}

// diffBranchForResume fetches the working branch's current schema and diffs it
// against the desired schema to find DDL that wasn't applied before the crash.
func (e *Engine) diffBranchForResume(ctx context.Context, client psclient.PSClient, org, database, branch string, schemaFiles schema.SchemaFiles) ([]engine.SchemaChange, error) {
	currentSchema, err := e.fetchDatabaseSchema(ctx, client, org, database, branch, sortedKeyspaces(schemaFiles))
	if err != nil {
		return nil, fmt.Errorf("fetch branch schema: %w", err)
	}

	var changes []engine.SchemaChange
	for _, keyspace := range sortedKeyspaces(schemaFiles) {
		ns := schemaFiles[keyspace]

		// Build current table schemas from branch
		var currentTableSchemas []table.TableSchema
		if tables, ok := currentSchema[keyspace]; ok {
			currentTableSchemas = append(currentTableSchemas, tables...)
		}

		// Build desired table schemas from files
		desiredTableSchemas, err := parseDesiredSchemas(keyspace, ns)
		if err != nil {
			return nil, err
		}

		// Diff: what DDL is needed to bring branch from current to desired?
		plan, err := lint.PlanChanges(currentTableSchemas, desiredTableSchemas, nil, e.linter.SpiritConfig())
		if err != nil {
			return nil, fmt.Errorf("diff keyspace %s for resume: %w", keyspace, err)
		}
		if !plan.HasChanges() {
			continue
		}

		sc := engine.SchemaChange{
			Namespace: keyspace,
			Metadata:  make(map[string]string),
		}
		for _, pc := range plan.Changes {
			stmtType, _, classifyErr := ddl.ClassifyStatement(pc.Statement)
			if classifyErr != nil {
				return nil, fmt.Errorf("classify statement in keyspace %s: %w", keyspace, classifyErr)
			}
			sc.TableChanges = append(sc.TableChanges, engine.TableChange{
				Table:     pc.TableName,
				Operation: stmtType,
				DDL:       pc.Statement,
			})
		}
		changes = append(changes, sc)
	}
	return changes, nil
}

// resumeApply resumes a schema change after restart.
// Handles two crash scenarios:
//   - Branch exists, no deploy request: diff branch against desired schema, apply remaining DDL, create deploy request
//   - Branch exists, deploy request exists: just return current state for Progress polling
func (e *Engine) resumeApply(ctx context.Context, client psclient.PSClient, org string, req *engine.ApplyRequest) (*engine.ApplyResult, error) {
	meta, err := decodePSMetadata(req.ResumeState.Metadata)
	if err != nil {
		return nil, fmt.Errorf("decode resume state: %w", err)
	}

	emitEvent := func(event engine.ApplyEvent) {
		attrs := []any{"database", req.Database}
		for k, v := range event.Metadata {
			attrs = append(attrs, k, v)
		}
		e.logger.Info(event.Message, attrs...)
		if req.OnEvent != nil {
			req.OnEvent(event)
		}
	}

	e.logger.Info("resuming apply",
		"branch", meta.BranchName,
		"deploy_request", meta.DeployRequestID,
	)

	// If we have a deploy request ID, check its current state.
	if meta.DeployRequestID != 0 {
		dr, err := e.getDeployRequest(ctx, client, org, req.Database, meta.DeployRequestID)
		if err != nil {
			// Deploy request may have been cleaned up — start fresh.
			e.logger.Warn("deploy request not found on resume, starting fresh",
				"deploy_request", meta.DeployRequestID, "error", err)
			req.ResumeState = nil
			return e.Apply(ctx, req)
		}

		// If the deploy request failed, start fresh with a new branch rather
		// than resuming a broken deploy.
		if dr.DeploymentState == deployState.Error || dr.DeploymentState == deployState.CompleteError {
			e.logger.Warn("deploy request in error state on resume, starting fresh",
				"deploy_request", meta.DeployRequestID, "state", dr.DeploymentState)
			req.ResumeState = nil
			return e.Apply(ctx, req)
		}

		meta.DeployRequestURL = dr.HtmlURL
		updatedMeta, err := encodePSMetadata(meta)
		if err != nil {
			return nil, fmt.Errorf("encode metadata for deploy request #%d: %w", meta.DeployRequestID, err)
		}
		return &engine.ApplyResult{
			Accepted: true,
			Message:  fmt.Sprintf("Resumed deploy request #%d (state: %s)", dr.Number, dr.DeploymentState),
			ResumeState: &engine.ResumeState{
				MigrationContext: req.ResumeState.MigrationContext,
				Metadata:         updatedMeta,
			},
		}, nil
	}

	// No deploy request yet — worker crashed after branch creation but before
	// the deploy request was created. Diff the branch against desired schema
	// to find DDL that wasn't applied before the crash, then apply only the
	// missing changes.
	e.logger.Info("resuming from branch (no deploy request yet)", "branch", meta.BranchName)

	// Check if the branch still exists — it may have been deleted by TTL
	// between the crash and recovery. If so, start fresh.
	if err := e.waitForBranchReady(ctx, client, org, req.Database, meta.BranchName); err != nil {
		e.logger.Warn("branch no longer available on resume, starting fresh", "branch", meta.BranchName, "error", err)
		req.ResumeState = nil
		return e.Apply(ctx, req)
	}

	// Diff branch's current state against desired to find un-applied DDL
	remainingChanges, err := e.diffBranchForResume(ctx, client, org, req.Database, meta.BranchName, req.SchemaFiles)
	if err != nil {
		return nil, fmt.Errorf("diff branch for resume: %w", err)
	}

	if len(remainingChanges) > 0 {
		e.logger.Info("applying remaining DDL on resume", "branch", meta.BranchName, "keyspaces", len(remainingChanges))
		resumePwCtx, resumePwCancel := context.WithTimeout(ctx, 10*time.Second)
		defer resumePwCancel()

		password, err := client.CreateBranchPassword(resumePwCtx, &ps.DatabaseBranchPasswordRequest{
			Organization: org, Database: req.Database, Branch: meta.BranchName, Role: "admin", TTL: 3600,
		})
		if err != nil {
			return nil, fmt.Errorf("create branch password on resume: %w", err)
		}
		if err := e.applyChangesToBranch(ctx, remainingChanges, req.SchemaFiles, password, client, org, req.Database, meta.BranchName, emitEvent); err != nil {
			return nil, fmt.Errorf("apply remaining DDL on resume: %w", err)
		}
	} else {
		e.logger.Info("all DDL already applied on branch", "branch", meta.BranchName)
	}

	// VSchema may not have been applied before the crash — re-apply
	// (VSchema updates are idempotent, they overwrite the entire VSchema)
	for _, sc := range req.Changes {
		if vschemaContent := getVSchemaContent(sc, req.SchemaFiles); vschemaContent != "" {
			if err := e.updateBranchVSchema(ctx, client, org, req.Database, meta.BranchName, sc.Namespace, vschemaContent); err != nil {
				return nil, fmt.Errorf("update vschema for %s on resume: %w", sc.Namespace, err)
			}
		}
	}

	// Create deploy request
	main := mainBranch(req.Credentials)
	deferCutover := req.Options["defer_cutover"] == "true"
	deferDeploy := req.Options["defer_deploy"] == "true"

	dr, err := e.createDeployRequest(ctx, client, org, req.Database, meta.BranchName, main, !deferCutover, true)
	if err != nil {
		return nil, fmt.Errorf("create deploy request on resume: %w", err)
	}
	dr, err = e.waitForDeployRequestPending(ctx, client, org, req.Database, dr)
	if err != nil {
		return nil, fmt.Errorf("wait for deploy request on resume: %w", err)
	}
	if dr.DeploymentState == deployState.Error {
		return nil, fmt.Errorf("deploy request #%d failed on resume (state: %s)", dr.Number, dr.DeploymentState)
	}
	if dr.DeploymentState == deployState.NoChanges {
		return &engine.ApplyResult{Message: "no changes detected on resume"}, nil
	}

	// Deploy — prefer instant when eligible (no row copy, no revert window needed).
	instantEligible := dr.Deployment != nil && dr.Deployment.InstantDDLEligible
	useInstant := instantEligible

	meta.DeployRequestID = dr.Number
	meta.DeployRequestURL = dr.HtmlURL
	meta.IsInstant = useInstant

	// Deferred deploy on resume: don't start the deploy yet.
	if deferDeploy {
		meta.DeferredDeploy = true
		persistMeta, encErr := encodePSMetadata(meta)
		if encErr != nil {
			return nil, fmt.Errorf("encode metadata for deferred deploy on resume: %w", encErr)
		}
		if req.OnStateChange != nil {
			req.OnStateChange(&engine.ResumeState{
				MigrationContext: req.ResumeState.MigrationContext,
				Metadata:         persistMeta,
			})
		}
		suffix := ""
		if useInstant {
			suffix = " (instant DDL)"
		}
		return &engine.ApplyResult{
			Accepted: true,
			Message:  fmt.Sprintf("Deploy request #%d ready%s — waiting for deploy", dr.Number, suffix),
			ResumeState: &engine.ResumeState{
				MigrationContext: req.ResumeState.MigrationContext,
				Metadata:         persistMeta,
			},
		}, nil
	}

	persistMeta, err := encodePSMetadata(meta)
	if err != nil {
		return nil, fmt.Errorf("encode metadata on resume: %w", err)
	}
	if req.OnStateChange != nil {
		req.OnStateChange(&engine.ResumeState{
			MigrationContext: req.ResumeState.MigrationContext,
			Metadata:         persistMeta,
		})
	}

	dr, err = client.DeployDeployRequest(ctx, &ps.PerformDeployRequest{
		Organization: org, Database: req.Database, Number: dr.Number, InstantDDL: useInstant,
	})
	if err != nil {
		return nil, fmt.Errorf("deploy on resume: %w", err)
	}

	e.logger.Info("resumed and deployed", "number", dr.Number, "branch", meta.BranchName)
	return &engine.ApplyResult{
		Accepted: true,
		Message:  fmt.Sprintf("Resumed and deployed request #%d", dr.Number),
		ResumeState: &engine.ResumeState{
			MigrationContext: req.ResumeState.MigrationContext,
			Metadata:         persistMeta,
		},
	}, nil
}
