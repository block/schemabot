package planetscale

import (
	"context"
	"database/sql"
	"errors"
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

// noChangesApplyResult builds the result for a converged apply where the deploy
// request reported no schema differences. It must be Accepted so the tern layer
// completes the apply's tasks instead of treating the no-op as a failure.
func noChangesApplyResult(message string) *engine.ApplyResult {
	return &engine.ApplyResult{Accepted: true, Message: message}
}

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
		if err := e.verifyBranchMatchesMain(ctx, client, org, req.Database, branchName, main, keyspaces, password); err != nil {
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
		return noChangesApplyResult("no changes detected"), nil
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
	migrationContext := e.discoverSchemaChangeContextWithRetry(ctx, client, req.Database, req.Credentials, existingContexts)

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
//   - Branch exists, no deploy request: diff branch against desired schema, apply remaining DDL, then create and deploy the deploy request
//   - Branch exists, deploy request exists: reattach, deploy it when it was created but never started, and rediscover the Vitess migration_context for progress
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

	// If we have a deploy request ID, the worker crashed after creating it.
	if meta.DeployRequestID != 0 {
		return e.resumeExistingDeployRequest(ctx, client, org, req, meta)
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
		return noChangesApplyResult("no changes detected on resume"), nil
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

	// Capture the migration_context baseline before deploying so the new Vitess
	// context can be identified once Vitess creates migrations for this deploy.
	existingContexts := e.captureExistingContexts(ctx, client, req.Database, req.Credentials)

	dr, err = client.DeployDeployRequest(ctx, &ps.PerformDeployRequest{
		Organization: org, Database: req.Database, Number: dr.Number, InstantDDL: useInstant,
	})
	if err != nil {
		return nil, fmt.Errorf("deploy on resume: %w", err)
	}

	migrationContext := e.resolveResumeSchemaChangeContext(ctx, client, req, existingContexts)

	// Persist the rediscovered context now so a crash before this function
	// returns does not lose it — the returned ResumeState alone is not durable.
	e.persistResumeSchemaChangeContext(req, migrationContext, persistMeta)

	e.logger.Info("resumed and deployed", "number", dr.Number, "branch", meta.BranchName, "migration_context", migrationContext)
	return &engine.ApplyResult{
		Accepted: true,
		Message:  fmt.Sprintf("Resumed and deployed request #%d", dr.Number),
		ResumeState: &engine.ResumeState{
			MigrationContext: migrationContext,
			Metadata:         persistMeta,
		},
	}, nil
}

// resumeExistingDeployRequest resumes an apply whose deploy request was already
// created before the crash. It reattaches to the recovered deploy request,
// deploys it when the worker crashed after creation but before the deploy was
// started, and rediscovers the Vitess migration_context so per-shard progress
// keeps working for the rest of the apply.
func (e *Engine) resumeExistingDeployRequest(ctx context.Context, client psclient.PSClient, org string, req *engine.ApplyRequest, meta *psMetadata) (*engine.ApplyResult, error) {
	dr, err := e.getDeployRequest(ctx, client, org, req.Database, meta.DeployRequestID)
	if err != nil {
		// Only a genuine not-found means the deploy request was cleaned up and the
		// apply should start fresh. A transient API error must propagate so resume
		// retries against the same deploy request — forking a fresh branch and
		// deploy request here would start a duplicate schema change while the
		// original is still in flight.
		var psErr *ps.Error
		if errors.As(err, &psErr) && psErr.Code == ps.ErrNotFound {
			e.logger.Warn("deploy request not found on resume, starting fresh",
				"database", req.Database, "deploy_request", meta.DeployRequestID, "error", err)
			req.ResumeState = nil
			return e.Apply(ctx, req)
		}
		return nil, fmt.Errorf("get deploy request #%d on resume: %w", meta.DeployRequestID, err)
	}

	// If the deploy request failed, start fresh with a new branch rather
	// than resuming a broken deploy.
	if dr.DeploymentState == deployState.Error || dr.DeploymentState == deployState.CompleteError {
		e.logger.Warn("deploy request in error state on resume, starting fresh",
			"database", req.Database, "deploy_request", meta.DeployRequestID, "state", dr.DeploymentState)
		req.ResumeState = nil
		return e.Apply(ctx, req)
	}

	meta.DeployRequestURL = dr.HtmlURL
	updatedMeta, err := encodePSMetadata(meta)
	if err != nil {
		return nil, fmt.Errorf("encode metadata for deploy request #%d: %w", meta.DeployRequestID, err)
	}

	migrationContext := req.ResumeState.MigrationContext

	// A non-deferred deploy request that crashed after creation but before being
	// deployed sits in "ready" indefinitely: Progress maps "ready" to pending,
	// the deferred-deploy promotion to waiting_for_deploy does not apply, and the
	// tern auto-deploy trigger only fires on waiting_for_deploy. Start the deploy
	// here, mirroring the fresh path, so the schema change actually runs.
	//
	// The gate reads dr just above and then calls DeployDeployRequest, so two
	// resumes racing here could both decide to deploy. That read→call window is
	// self-correcting: the provider accepts at most one deploy and rejects the
	// loser, so a duplicate schema change is never started.
	if deployRequestNeedsResumeDeploy(dr, meta) {
		e.logger.Info("deploying recovered deploy request that was never started",
			"database", req.Database, "deploy_request", dr.Number, "branch", meta.BranchName, "instant_ddl", meta.IsInstant)

		// Capture the migration_context baseline before deploying so the new
		// Vitess context can be identified after Vitess creates migrations.
		existingContexts := e.captureExistingContexts(ctx, client, req.Database, req.Credentials)

		deployed, deployErr := client.DeployDeployRequest(ctx, &ps.PerformDeployRequest{
			Organization: org, Database: req.Database, Number: dr.Number, InstantDDL: meta.IsInstant,
		})
		if deployErr != nil {
			return nil, fmt.Errorf("deploy recovered deploy request #%d on resume: %w", dr.Number, deployErr)
		}
		dr = deployed

		migrationContext = e.resolveResumeSchemaChangeContext(ctx, client, req, existingContexts)

		// Persist the rediscovered context now so a crash before this function
		// returns does not lose it — the returned ResumeState alone is not durable.
		e.persistResumeSchemaChangeContext(req, migrationContext, updatedMeta)

		e.logger.Info("resumed and deployed recovered deploy request",
			"database", req.Database, "deploy_request", dr.Number, "branch", meta.BranchName, "migration_context", migrationContext)
		return &engine.ApplyResult{
			Accepted: true,
			Message:  fmt.Sprintf("Resumed and deployed request #%d", dr.Number),
			ResumeState: &engine.ResumeState{
				MigrationContext: migrationContext,
				Metadata:         updatedMeta,
			},
		}, nil
	}

	// Reattach-only path: no deploy was started here. If the stored context is
	// still the tern apply identifier (not a real Vitess context), an earlier
	// crash lost the discovered context, so per-shard progress would stay empty
	// for the rest of the apply. Rediscover it from the current migrations and
	// persist the result. A stored value that is already a real Vitess context is
	// kept as-is.
	if !isRealVitessContext(migrationContext) {
		e.logger.Info("rediscovering Vitess context on reattach; stored value is not a real context",
			"database", req.Database, "deploy_request", dr.Number, "stored_context", migrationContext)
		// The deploy ran in a prior process, so its context is already in SHOW
		// VITESS_MIGRATIONS. Discover against an empty baseline, which makes every
		// context a candidate; selection keeps only non-terminal candidates so a
		// completed historical context from an unrelated change is never matched.
		// A genuinely finished change yields no non-terminal candidate, so the
		// stored identifier is preserved rather than attached to stale progress.
		rediscovered := e.discoverSchemaChangeContextWithRetry(ctx, client, req.Database, req.Credentials, map[string]bool{})
		if rediscovered != "" {
			migrationContext = rediscovered
			e.persistResumeSchemaChangeContext(req, migrationContext, updatedMeta)
		} else {
			e.logger.Warn("Vitess context not found on reattach; per-shard progress will be empty until it is found",
				"database", req.Database, "deploy_request", dr.Number, "stored_context", migrationContext)
		}
	}

	e.logger.Info("reattached to deploy request on resume",
		"database", req.Database, "deploy_request", dr.Number, "state", dr.DeploymentState,
		"deferred_deploy", meta.DeferredDeploy, "has_migration_context", migrationContext != "")
	return &engine.ApplyResult{
		Accepted: true,
		Message:  fmt.Sprintf("Resumed deploy request #%d (state: %s)", dr.Number, dr.DeploymentState),
		ResumeState: &engine.ResumeState{
			MigrationContext: migrationContext,
			Metadata:         updatedMeta,
		},
	}, nil
}

// deployRequestNeedsResumeDeploy reports whether a recovered deploy request must
// be deployed during resume. This is the case only for a non-deferred deploy
// request that finished PlanetScale's diff ("ready") but was never started —
// the worker crashed between creating the deploy request and deploying it. A
// deferred deploy is left for the operator-triggered deploy, and a request that
// already has a DeployedAt timestamp is in flight and must not be re-deployed.
func deployRequestNeedsResumeDeploy(dr *ps.DeployRequest, meta *psMetadata) bool {
	return dr.DeploymentState == deployState.Ready && !meta.DeferredDeploy && dr.DeployedAt == nil
}

// resolveResumeSchemaChangeContext rediscovers the Vitess migration_context after a
// resume deploy. It prefers a freshly discovered context (the one that appeared
// since the pre-deploy baseline). When discovery turns up nothing — Vitess may
// not have created migrations within the bounded discovery window — it preserves
// the stored value rather than clobbering a real context with an empty string.
// A stored value is only a real Vitess context when it already appears in the
// baseline; otherwise it is the tern-assigned apply identifier, which never
// matches SHOW VITESS_MIGRATIONS, so per-shard progress stays empty until a real
// context is found.
func (e *Engine) resolveResumeSchemaChangeContext(ctx context.Context, client psclient.PSClient, req *engine.ApplyRequest, existingContexts map[string]bool) string {
	stored := req.ResumeState.MigrationContext

	discovered := e.discoverSchemaChangeContextWithRetry(ctx, client, req.Database, req.Credentials, existingContexts)
	if discovered != "" {
		return discovered
	}

	if stored != "" && existingContexts[stored] {
		e.logger.Debug("keeping stored Vitess context on resume", "database", req.Database, "context", stored)
		return stored
	}

	e.logger.Warn("Vitess context not discovered on resume; per-shard progress will be empty until it is found",
		"database", req.Database, "stored_context", stored)
	return stored
}

// isRealVitessContext reports whether a migration_context value is a real Vitess
// context (the "<system>:<uuid>" form that appears in SHOW VITESS_MIGRATIONS,
// e.g. "singularity:17694ee9-...") rather than the tern-assigned apply identifier
// (e.g. "apply-1a2b3c..."). The apply identifier never matches a SHOW
// VITESS_MIGRATIONS row, so per-shard progress stays empty until a real context
// is resolved. The colon separating system and uuid is the distinguishing
// marker — tern identifiers never contain one.
func isRealVitessContext(migrationContext string) bool {
	return strings.Contains(migrationContext, ":")
}

// persistResumeSchemaChangeContext durably records a freshly resolved Vitess context
// via OnStateChange so a crash after deploy (but before the resume function
// returns) does not lose it — without persistence the stored ResumeState would
// still hold the tern apply identifier, leaving the next resume with no per-shard
// Vitess progress. It only persists a real Vitess context; an empty or
// apply-identifier value carries no per-shard progress and is left untouched so a
// previously persisted real context is never clobbered.
func (e *Engine) persistResumeSchemaChangeContext(req *engine.ApplyRequest, migrationContext, metadata string) {
	if req.OnStateChange == nil {
		e.logger.Debug("not persisting resume context: no OnStateChange callback",
			"database", req.Database, "context", migrationContext)
		return
	}
	if !isRealVitessContext(migrationContext) {
		e.logger.Debug("not persisting resume context: not a real Vitess context yet",
			"database", req.Database, "context", migrationContext)
		return
	}
	e.logger.Info("persisting rediscovered Vitess context on resume",
		"database", req.Database, "context", migrationContext)
	req.OnStateChange(&engine.ResumeState{
		MigrationContext: migrationContext,
		Metadata:         metadata,
	})
}
