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

	mysql "github.com/block/mysql"
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

// vschemaDiffsFromChanges extracts the per-keyspace VSchema diffs carried on the
// plan annotations (SchemaChange.Metadata["vschema"]), preserving plan order.
// Returns nil when no namespace changes its VSchema. Each keyspace is kept
// separate so progress views can render and track them independently.
func vschemaDiffsFromChanges(changes []engine.SchemaChange) []vschemaKeyspaceDiff {
	var diffs []vschemaKeyspaceDiff
	for _, sc := range changes {
		if d := sc.Metadata["vschema"]; d != "" {
			diffs = append(diffs, vschemaKeyspaceDiff{Namespace: sc.Namespace, Diff: d})
		}
	}
	return diffs
}

// Apply starts executing a schema change plan.
// Creates a PlanetScale branch, applies DDL via MySQL connection to the branch,
// then creates and starts a deploy request.
func (e *Engine) Apply(ctx context.Context, req *engine.ApplyRequest) (result *engine.ApplyResult, retErr error) {
	r := *req
	r.Database = e.resolveDatabase(req.Credentials, req.Database)
	req = &r

	e.logger.Info("applying plan",
		"plan_id", req.PlanID,
		"database", req.Database,
	)

	// Per-shard and row-copy progress come from SHOW VITESS_MIGRATIONS, which the
	// engine reads over a vtgate MySQL connection. With API credentials but no
	// vtgate DSN the apply still runs (DDL goes through the PlanetScale API), but
	// progress degrades to the deploy-request state only — no rows, no per-shard
	// breakdown. Surface it once per apply so a blank progress bar is explained
	// rather than a silent skip. (A nil Credentials fails hard in getClient below,
	// so it is not the degraded-progress case and is not warned here.)
	if req.Credentials != nil && req.Credentials.DSN == "" {
		e.logger.Warn("vitess apply has no vtgate DSN: per-shard and row-copy progress will be unavailable (deploy-request state only); check the target resolver's vtgate endpoint and credentials",
			"database", req.Database, "plan_id", req.PlanID)
	}

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

	emitEvent := e.eventEmitter(req)

	// Track in-flight apply metadata for progress queries during setup.
	migCtx := ""
	if req.ResumeState != nil {
		migCtx = req.ResumeState.MigrationContext
	}
	// persistState persists apply metadata to storage via OnStateChange for crash recovery.
	// On first apply, migCtx is empty until the tern layer assigns one via ResumeState.
	// persistState is a no-op in this window — if the driver crashes before Apply returns,
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

	// Capture the per-keyspace VSchema diffs carried on the plan annotations so
	// they can be surfaced from stored state alongside the deploy's VSchema
	// status, without a synthetic task row. Empty when no namespace changes its
	// VSchema.
	vschemaDiffs := vschemaDiffsFromChanges(req.Changes)

	// Create or reuse a branch
	existingBranch := req.Options["branch"]
	var branchName string
	branchStart := time.Now()

	// A branch SchemaBot creates exists only to carry this apply's DDL into a
	// deploy request. Once that deploy request exists it owns the teardown
	// (AutoDeleteBranch); until then nothing does, so an apply that fails while
	// preparing the branch would strand it. Branches are quota'd, and the
	// failures in this window are the ordinary ones — DDL the engine refuses —
	// so the strand accumulates until branch creation itself starts failing for
	// unrelated schema changes. ownedBranch names the branch this apply is
	// responsible for; it is cleared once the deploy request takes ownership,
	// and it is never set for an operator-supplied branch, which SchemaBot does
	// not own.
	//
	// A create-deploy-request response lost to a timeout leaves ownedBranch set
	// even though the deploy request may exist server-side. Deleting the branch
	// is still the right cleanup there, not a hazard: deploying is a separate,
	// later call that only runs after the create succeeded client-side, so the
	// orphan carries no running schema change, and the apply records the deploy
	// request identifier only after a successful response, so no retry will
	// ever drive it. Should the API refuse to delete a branch while its deploy
	// request is open, the refusal surfaces on the cleanup's error path with
	// the identifiers for manual reclamation.
	ownedBranch := ""
	defer func() {
		if retErr == nil || ownedBranch == "" {
			return
		}
		e.deleteOwnedBranch(ctx, client, org, req.Database, ownedBranch, retErr)
	}()

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
		ownedBranch = branchName

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
	dr, err := e.createDeployRequest(ctx, client, org, req.Database, branchName, main, autoDeleteBranch)
	if err != nil {
		return nil, fmt.Errorf("create deploy request: %w", err)
	}
	// The deploy request now owns the branch's teardown.
	ownedBranch = ""
	emitEvent(deployRequestCreatedEvent(dr, branchName))
	persistState(&psMetadata{
		BranchName:            branchName,
		DeployRequestID:       dr.Number,
		DeployRequestURL:      dr.HtmlURL,
		VSchemaDiffs:          vschemaDiffs,
		ExistingMigrationCtxs: existingContexts,
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
	// Because there is no revert window, an unsafe change always takes the
	// row-copy path instead.
	instantEligible := dr.Deployment != nil && dr.Deployment.InstantDDLEligible
	unsafe, unsafeReason := e.changesContainUnsafe(req.Changes, req.Database)
	useInstant := useInstantDDL(dr, deferCutover, unsafe)

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
		"unsafe", unsafe,
		"deploy_state", dr.DeploymentState,
	)

	if instantEligible && !useInstant {
		if unsafe {
			e.logger.Info("declining instant DDL because the change is unsafe — the row copy keeps a revert window",
				"database", req.Database,
				"deploy_request", dr.Number,
				"reason", unsafeReason,
			)
		} else {
			e.logger.Info("declining instant DDL so the deferred cutover has a gate to hold",
				"database", req.Database,
				"deploy_request", dr.Number,
			)
		}
		emitEvent(rowCopyDeclineEvent(unsafe, unsafeReason))
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

	if deferCutover {
		if err := e.verifyCutoverHeld(ctx, client, org, req.Database, dr.Number); err != nil {
			return nil, err
		}
	}

	// Deferred deploy: don't call DeployDeployRequest yet. The user will review
	// the deploy request diff on PlanetScale and trigger via `schemabot cutover`.
	if deferDeploy {
		e.logger.Info("deferring deploy — user must trigger via cutover",
			"database", req.Database,
			"deploy_request", dr.Number,
			"use_instant", useInstant,
		)
		meta, encErr := encodePSMetadata(&psMetadata{
			BranchName:            branchName,
			DeployRequestID:       dr.Number,
			DeployRequestURL:      dr.HtmlURL,
			IsInstant:             useInstant,
			DeferredDeploy:        true,
			VSchemaDiffs:          vschemaDiffs,
			ExistingMigrationCtxs: existingContexts,
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
	// the deploy request even after it leaves the pending state, and transient
	// API errors may occur; deployDeployRequest absorbs both.
	dr, err = e.deployDeployRequest(ctx, client, org, req.Database, dr.Number, useInstant)
	if err != nil {
		return nil, err
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
	migrationContext := e.discoverSchemaChangeContextWithRetry(ctx, client, req.Database, req.Credentials, existingContexts, dr.CreatedAt)

	meta, err := encodePSMetadata(&psMetadata{
		BranchName:            branchName,
		DeployRequestID:       dr.Number,
		DeployRequestURL:      dr.HtmlURL,
		IsInstant:             useInstant,
		VSchemaDiffs:          vschemaDiffs,
		ExistingMigrationCtxs: existingContexts,
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
			e.logger.Error(fmt.Sprintf("keyspace %s apply attempt %d failed", sc.Namespace, attempt+1), "keyspace", sc.Namespace, "database", database, "branch", branchName, "attempt", attempt+1, "error", err)
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

	db, err := sql.Open("block-mysql", dsn)
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

// eventEmitter returns a closure that logs a lifecycle event and sends it to
// the caller for apply_logs recording.
func (e *Engine) eventEmitter(req *engine.ApplyRequest) func(engine.ApplyEvent) {
	return func(event engine.ApplyEvent) {
		attrs := []any{"database", req.Database}
		for k, v := range event.Metadata {
			attrs = append(attrs, k, v)
		}
		e.logger.Info(event.Message, attrs...)
		if req.OnEvent != nil {
			req.OnEvent(event)
		}
	}
}

// rowCopyDeclineEvent is the operator-facing event for an instant-eligible
// change that takes the row-copy path instead. It states the trade the decline
// bought, since the operator otherwise sees only that an eligible change took
// the slow path. Unsafety outranks the deferred cutover as the stated reason:
// it declines instant DDL even when nothing was deferred.
func rowCopyDeclineEvent(unsafe bool, unsafeReason string) engine.ApplyEvent {
	if unsafe {
		return engine.ApplyEvent{
			Message: fmt.Sprintf("Deploying with a row copy rather than instant DDL: %s. The row copy keeps a revert window open to undo the change.", unsafeReason),
		}
	}
	return engine.ApplyEvent{
		Message: "Deploying with a row copy rather than instant DDL: instant DDL swaps the schema as soon as the deploy runs, and this cutover was deferred.",
	}
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

	emitEvent := e.eventEmitter(req)

	e.logger.Info("resuming apply",
		"branch", meta.BranchName,
		"deploy_request", meta.DeployRequestID,
	)

	// If we have a deploy request ID, the driver crashed after creating it.
	if meta.DeployRequestID != 0 {
		return e.resumeExistingDeployRequest(ctx, client, org, req, meta)
	}

	// No deploy request yet — driver crashed after branch creation but before
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
	deferDeploy := req.Options["defer_deploy"] == "true"
	deferCutover := req.Options["defer_cutover"] == "true"

	dr, err := e.createDeployRequest(ctx, client, org, req.Database, meta.BranchName, main, true)
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

	if deferCutover {
		if err := e.verifyCutoverHeld(ctx, client, org, req.Database, dr.Number); err != nil {
			return nil, err
		}
	}

	// Deploy — prefer instant when eligible, unless the cutover was deferred or
	// the change is unsafe, both of which need the row-copy path (the gate to
	// hold, and the revert window to undo the change).
	instantEligible := dr.Deployment != nil && dr.Deployment.InstantDDLEligible
	unsafe, unsafeReason := e.changesContainUnsafe(req.Changes, req.Database)
	useInstant := useInstantDDL(dr, deferCutover, unsafe)
	if instantEligible && !useInstant {
		if unsafe {
			e.logger.Info("declining instant DDL on resume because the change is unsafe — the row copy keeps a revert window",
				"database", req.Database,
				"deploy_request", dr.Number,
				"reason", unsafeReason,
			)
		} else {
			e.logger.Info("declining instant DDL on resume so the deferred cutover has a gate to hold",
				"database", req.Database,
				"deploy_request", dr.Number,
			)
		}
		emitEvent(rowCopyDeclineEvent(unsafe, unsafeReason))
	}

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

	dr, err = e.deployDeployRequest(ctx, client, org, req.Database, dr.Number, useInstant)
	if err != nil {
		return nil, fmt.Errorf("deploy on resume: %w", err)
	}

	migrationContext := e.resolveResumeSchemaChangeContext(ctx, client, req, existingContexts, dr.CreatedAt)

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
// deploys it when the driver crashed after creation but before the deploy was
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
	deferDeploy := req.Options["defer_deploy"] == "true"
	deferCutover := req.Options["defer_cutover"] == "true"

	if deployRequestNeedsResumeDeploy(dr, meta, deferDeploy) {
		// The cutover setting is settled when the deploy request is created and no
		// later call can change it, so a recovered request has to be verified here
		// for the same reason the fresh path verifies before deploying: this drive
		// cannot know how the request it inherited was created. Deploying one whose
		// cutover the backend still owns would let the schema swap itself, which is
		// what the operator deferring the cutover asked to prevent.
		if deferCutover {
			if err := e.verifyCutoverHeld(ctx, client, org, req.Database, dr.Number); err != nil {
				return nil, err
			}
		}

		// The instant decision is re-taken against the deferral this drive was
		// given and the safety of the changes, rather than read straight out of
		// recovered metadata. Instant DDL swaps the schema as the deploy runs, so
		// running one while the operator holds the cutover hands them a gate with
		// nothing left behind it, and running an unsafe one leaves no revert
		// window to undo the change — and the metadata was written by a drive
		// whose decision inputs this one cannot confirm. The stored decision is
		// only ever narrowed here: a deploy that was never going to be instant
		// stays that way.
		unsafe, unsafeReason := e.changesContainUnsafe(req.Changes, req.Database)
		useInstant := meta.IsInstant && !deferCutover && !unsafe
		if meta.IsInstant && !useInstant {
			if unsafe {
				e.logger.Info("declining instant DDL on the recovered deploy request because the change is unsafe — the row copy keeps a revert window",
					"database", req.Database, "deploy_request", dr.Number, "reason", unsafeReason)
			} else {
				e.logger.Info("declining instant DDL on the recovered deploy request so the deferred cutover has a gate to hold",
					"database", req.Database, "deploy_request", dr.Number)
			}
			e.eventEmitter(req)(rowCopyDeclineEvent(unsafe, unsafeReason))
			// Re-encode so stored state carries the narrowed decision: metadata
			// persisted with the stale IsInstant would let a later consumer of the
			// stored value (a deferred Start, a subsequent resume) widen back to
			// instant after this drive declined it.
			meta.IsInstant = useInstant
			if updatedMeta, err = encodePSMetadata(meta); err != nil {
				return nil, fmt.Errorf("encode narrowed metadata for deploy request #%d: %w", dr.Number, err)
			}
		}

		e.logger.Info("deploying recovered deploy request that was never started",
			"database", req.Database, "deploy_request", dr.Number, "branch", meta.BranchName, "instant_ddl", useInstant)

		// Capture the migration_context baseline before deploying so the new
		// Vitess context can be identified after Vitess creates migrations.
		existingContexts := e.captureExistingContexts(ctx, client, req.Database, req.Credentials)

		deployed, deployErr := e.deployDeployRequest(ctx, client, org, req.Database, dr.Number, useInstant)
		if deployErr != nil {
			return nil, fmt.Errorf("recovered deploy request on resume: %w", deployErr)
		}
		dr = deployed

		migrationContext = e.resolveResumeSchemaChangeContext(ctx, client, req, existingContexts, dr.CreatedAt)

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
		// VITESS_MIGRATIONS. Discover against the pre-deploy baseline persisted in
		// engine resume metadata so contexts that predate this deploy are excluded,
		// anchored to the deploy's creation time to disambiguate concurrent changes.
		// Selection keeps only non-terminal candidates, so a completed historical
		// change is never matched and a genuinely finished change yields no
		// candidate — preserving the stored identifier rather than attaching stale
		// progress. The baseline may be empty for applies created before this
		// persistence existed; discovery then falls back to baseline-free selection.
		rediscovered := e.discoverSchemaChangeContextWithRetry(ctx, client, req.Database, req.Credentials, meta.ExistingMigrationCtxs, dr.CreatedAt)
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
// the driver crashed between creating the deploy request and deploying it. A
// deferred deploy is left for the operator-triggered deploy, and a request that
// already has a DeployedAt timestamp is in flight and must not be re-deployed.
// The deferral is read from both the stored metadata and the operator's request:
// the metadata flag is persisted only after the deploy request is created, so a
// crash inside that window leaves a deferred apply whose stored metadata does not
// yet say so, and the request the operator made is what settles it.
func deployRequestNeedsResumeDeploy(dr *ps.DeployRequest, meta *psMetadata, deferDeploy bool) bool {
	return dr.DeploymentState == deployState.Ready && !meta.DeferredDeploy && !deferDeploy && dr.DeployedAt == nil
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
func (e *Engine) resolveResumeSchemaChangeContext(ctx context.Context, client psclient.PSClient, req *engine.ApplyRequest, existingContexts map[string]MigrationContextTimestamps, deployCreatedAt time.Time) string {
	stored := req.ResumeState.MigrationContext

	discovered := e.discoverSchemaChangeContextWithRetry(ctx, client, req.Database, req.Credentials, existingContexts, deployCreatedAt)
	if discovered != "" {
		return discovered
	}

	if _, inBaseline := existingContexts[stored]; stored != "" && inBaseline {
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
