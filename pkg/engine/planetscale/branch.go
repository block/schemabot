package planetscale

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	mysql "github.com/block/mysql"
	ps "github.com/planetscale/planetscale-go/planetscale"

	"github.com/block/spirit/pkg/statement"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/utils"
	"golang.org/x/sync/errgroup"

	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/lint"
	"github.com/block/schemabot/pkg/psclient"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/vschema"
)

// foreignKeyRefusalReason is the plan-time disclosure for a statement that
// declares a foreign key. The engine rejects it outright, so the statement is a
// guaranteed failure rather than a risk for the author to weigh, and a plan that
// offered to apply it would be offering a wait that can only end in an error.
// The wording matches the refusal the MySQL engine already reports for the same
// constraint, so one operator-facing vocabulary covers both.
//
// This is the only rejection this engine's plan predicts. Every other statement
// it will not accept is rejected when the DDL runs on the branch, before any
// data is copied: the apply fails, but it fails against a throwaway branch and
// never leaves a table half-changed. Predicting this one is worth the special
// case because the rejection is deterministic and the statement is common;
// predicting the rest would mean reimplementing the engine's own DDL acceptance
// rules, and getting that wrong would refuse changes that work.
const foreignKeyRefusalReason = "foreign key constraints are not supported"

// verifyBranchMatchesDesiredWithRetry retries verifyBranchMatchesDesired up to
// 90s to handle PlanetScale VSchema API staleness. DDL schema is fetched via
// MySQL (real-time) and fails fast on mismatch. Only VSchema errors are
// retried, since GetKeyspaceVSchema may return stale data after
// UpdateKeyspaceVSchema.
func (e *Engine) verifyBranchMatchesDesiredWithRetry(ctx context.Context, client psclient.PSClient, org, database, branch string, keyspaces []string, schemaFiles schema.SchemaFiles, password *ps.DatabaseBranchPassword) error {
	const maxAttempts = 18
	const pollInterval = 5 * time.Second

	var lastErr error
	for attempt := range maxAttempts {
		if attempt > 0 {
			e.logger.Info("retrying branch schema validation, waiting for VSchema API to converge",
				"branch", branch, "attempt", attempt+1, "delay", pollInterval)
			select {
			case <-ctx.Done():
				return fmt.Errorf("context cancelled waiting for schema validation: %w", ctx.Err())
			case <-time.After(pollInterval):
			}
		}

		lastErr = e.verifyBranchMatchesDesired(ctx, client, org, database, branch, keyspaces, schemaFiles, password)
		if lastErr == nil {
			if attempt > 0 {
				e.logger.Info("branch schema validated after retry",
					"branch", branch, "attempts", attempt+1)
			}
			return nil
		}

		// Only retry VSchema staleness errors. DDL validation uses MySQL
		// (real-time) so DDL mismatches are genuine failures.
		if !strings.Contains(lastErr.Error(), "unexpected VSchema difference") {
			return lastErr
		}
		e.logger.Debug("VSchema validation attempt failed (API may be stale)",
			"branch", branch, "attempt", attempt+1, "error", lastErr)
	}
	return fmt.Errorf("VSchema still mismatched after %d attempts (%ds): %w",
		maxAttempts, maxAttempts*int(pollInterval.Seconds()), lastErr)
}

// verifyBranchMatchesDesired validates that the branch schema matches the
// desired schema files for all keyspaces. Fetches DDL schema directly via
// MySQL (LoadSchemaFromDB) to avoid PlanetScale's GetBranchSchema API, which
// returns stale schema until an asynchronous schema snapshot completes after
// DDL execution.
func (e *Engine) verifyBranchMatchesDesired(ctx context.Context, client psclient.PSClient, org, database, branch string, keyspaces []string, schemaFiles schema.SchemaFiles, password *ps.DatabaseBranchPassword) error {
	branchSchema, err := e.fetchBranchSchemaViaMySQL(ctx, password, keyspaces)
	if err != nil {
		return fmt.Errorf("fetch branch schema via MySQL for validation: %w", err)
	}

	for _, ks := range keyspaces {
		ns := schemaFiles[ks]
		if ns == nil {
			continue
		}

		ddlChanges, vschemaChanged, _, err := e.diffKeyspace(ctx, client, org, database, branch, ks, ns, branchSchema)
		if err != nil {
			return fmt.Errorf("validate keyspace %s: %w", ks, err)
		}

		if len(ddlChanges) > 0 {
			var summaries []string
			var statements []string
			for _, ch := range ddlChanges {
				classified, _ := statement.Classify(ch.DDL)
				summary := ch.DDL
				if len(classified) > 0 {
					summary = fmt.Sprintf("%s %s", classified[0].Type, classified[0].Table)
				}
				summaries = append(summaries, summary)
				statements = append(statements, ch.DDL)
			}
			e.logger.Error("branch validation failed: unexpected DDL changes",
				"keyspace", ks,
				"branch", branch,
				"change_count", len(ddlChanges),
				"changes", summaries,
				"statements", statements,
				"branch_table_count", len(branchSchema[ks]),
				"desired_file_count", len(ns.Files),
			)
			return fmt.Errorf("keyspace %s has %d unexpected DDL changes after apply: %v\nstatements:\n%s",
				ks, len(ddlChanges), summaries, strings.Join(statements, "\n"))
		}

		if vschemaChanged {
			return fmt.Errorf("keyspace %s has unexpected VSchema difference after apply — VSchema may not have been applied to the branch", ks)
		}
	}

	e.logger.Info("branch schema validated via MySQL — matches desired state",
		"branch", branch,
		"keyspaces", len(keyspaces),
	)
	return nil
}

// fetchBranchSchemaViaMySQL connects to the branch via MySQL using the branch
// password and loads table schemas with LoadSchemaFromDB. This returns the
// real-time schema, bypassing PlanetScale's cached GetBranchSchema API.
func (e *Engine) fetchBranchSchemaViaMySQL(ctx context.Context, password *ps.DatabaseBranchPassword, keyspaces []string) (map[string][]table.TableSchema, error) {
	mysqlCfg := mysql.NewConfig()
	mysqlCfg.User = password.Username
	mysqlCfg.Passwd = password.PlainText
	mysqlCfg.Net = "tcp"
	mysqlCfg.Addr = password.Hostname
	mysqlCfg.InterpolateParams = true
	if mtlsRegistered.Load() {
		mysqlCfg.TLSConfig = mtlsConfigName
	}

	var mu sync.Mutex
	result := make(map[string][]table.TableSchema, len(keyspaces))
	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(20)

	for _, keyspace := range keyspaces {
		ks := keyspace
		g.Go(func() error {
			ksCfg := mysqlCfg.Clone()
			ksCfg.DBName = ks
			db, err := sql.Open("block-mysql", ksCfg.FormatDSN())
			if err != nil {
				return fmt.Errorf("open branch MySQL for keyspace %s: %w", ks, err)
			}
			defer utils.CloseAndLog(db)

			if err := db.PingContext(gCtx); err != nil {
				return fmt.Errorf("ping branch MySQL for keyspace %s: %w", ks, err)
			}

			tables, err := table.LoadSchemaFromDB(gCtx, db, table.WithoutUnderscoreTables)
			if err != nil {
				return fmt.Errorf("load schema for keyspace %s: %w", ks, err)
			}
			mu.Lock()
			result[ks] = tables
			mu.Unlock()
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return result, nil
}

// diffKeyspace diffs a single keyspace's schema between a branch and the
// desired schema files. Returns DDL changes, whether VSchema differs, and the
// current VSchema content fetched for that diff.
// Shared by Plan() and verifyBranchMatchesDesired().
func (e *Engine) diffKeyspace(ctx context.Context, client psclient.PSClient, org, database, branch, ks string, ns *schema.Namespace, currentSchema map[string][]table.TableSchema) ([]engine.TableChange, bool, string, error) {
	var currentTableSchemas []table.TableSchema
	if tables, ok := currentSchema[ks]; ok {
		currentTableSchemas = append(currentTableSchemas, tables...)
	}

	desiredTableSchemas, parseErr := parseDesiredSchemas(ks, ns)
	if parseErr != nil {
		return nil, false, "", parseErr
	}

	plan, planErr := lint.PlanChanges(currentTableSchemas, desiredTableSchemas, nil, e.linter.SpiritConfig())
	if planErr != nil {
		return nil, false, "", fmt.Errorf("plan changes for keyspace %s: %w", ks, planErr)
	}

	if len(plan.Changes) > 0 {
		e.logger.Info("diffKeyspace: changes detected",
			"keyspace", ks,
			"change_count", len(plan.Changes),
			"current_table_count", len(currentTableSchemas),
			"desired_table_count", len(desiredTableSchemas),
		)
		for _, pc := range plan.Changes {
			e.logger.Info("diffKeyspace: change detail",
				"keyspace", ks,
				"table", pc.TableName,
				"statement", pc.Statement[:min(len(pc.Statement), 200)],
			)
		}
		// Log table names from both sides for debugging
		var currentNames, desiredNames []string
		for _, t := range currentTableSchemas {
			currentNames = append(currentNames, t.Name)
		}
		for _, t := range desiredTableSchemas {
			desiredNames = append(desiredNames, t.Name)
		}
		e.logger.Info("diffKeyspace: table names",
			"keyspace", ks,
			"current", currentNames,
			"desired", desiredNames,
		)
	}

	var tableChanges []engine.TableChange
	for _, pc := range plan.Changes {
		stmtType, _, classifyErr := ddl.ClassifyStatement(pc.Statement)
		if classifyErr != nil {
			return nil, false, "", fmt.Errorf("classify statement in keyspace %s: %w", ks, classifyErr)
		}
		change := engine.TableChange{
			Table:     pc.TableName,
			Operation: stmtType,
			DDL:       pc.Statement,
		}
		if errViolations := pc.Errors(); len(errViolations) > 0 {
			change.IsUnsafe = true
			msgs := make([]string, len(errViolations))
			for i, v := range errViolations {
				msgs[i] = v.Message
			}
			change.UnsafeReason = strings.Join(msgs, "; ")
		}
		// Only a CREATE TABLE or an ALTER TABLE can declare a foreign key.
		// Gating on the type keeps the verdict — an informational field — off
		// the statement types the refusal check's own parser rejects outright.
		// The classifier and the check parse with the same parser, so a
		// statement classified as either type parses for both; what remains is
		// the check's own post-parse analysis, and an error there fails the
		// plan rather than guessing, matching the MySQL engine's refusal gate.
		if stmtType == ddl.StatementCreateTable || stmtType == ddl.StatementAlterTable {
			declaresFK, fkErr := ddl.DeclaresForeignKey(pc.Statement)
			if fkErr != nil {
				return nil, false, "", fmt.Errorf("foreign key check for table %s in keyspace %s: %w", pc.TableName, ks, fkErr)
			}
			if declaresFK {
				change.ExecutionMode = engine.ExecutionModeBlocked
				change.ModeReason = foreignKeyRefusalReason
				e.logger.Info("plan contains a statement Vitess will refuse at apply time",
					"keyspace", ks, "table", pc.TableName, "reason", change.ModeReason)
			}
		}
		tableChanges = append(tableChanges, change)
	}

	// Check VSchema diff
	vschemaChanged := false
	currentVSchemaRaw := ""
	if content, ok := ns.Files["vschema.json"]; ok && content != "" {
		currentVSchema, fetchErr := client.GetKeyspaceVSchema(ctx, &ps.GetKeyspaceVSchemaRequest{
			Organization: org,
			Database:     database,
			Branch:       branch,
			Keyspace:     ks,
		})
		if fetchErr != nil {
			var psErr *ps.Error
			if !errors.As(fetchErr, &psErr) || psErr.Code != ps.ErrNotFound {
				return nil, false, "", fmt.Errorf("fetch VSchema for keyspace %s: %w", ks, fetchErr)
			}
			// Keyspace has no VSchema on this branch yet — either it has
			// never had one, or the API has not converged after a recent
			// write. Treat as empty so the desired VSchema appears as a
			// change; validation callers absorb convergence lag through
			// the VSchema staleness retry.
			e.logger.Warn("VSchema not found for keyspace, treating as empty",
				"keyspace", ks, "branch", branch)
		}
		if currentVSchema != nil {
			currentVSchemaRaw = currentVSchema.Raw
		}
		vschemaChanged = vschema.Changed(currentVSchemaRaw, content)
		if vschemaChanged {
			e.logger.Info("diffKeyspace: VSchema mismatch detected",
				"keyspace", ks,
				"branch", branch,
				"current_normalized", vschema.Normalize(currentVSchemaRaw),
				"desired_normalized", vschema.Normalize(content),
			)
		}
	}

	return tableChanges, vschemaChanged, currentVSchemaRaw, nil
}

// verifyBranchMatchesMain uses Spirit's differ to compare the branch schema
// against main for the given keyspaces. Returns an error if any DDL changes
// exist, indicating the branch has stale DDL from a previous failed apply
// that RefreshSchema did not clean up.
func (e *Engine) verifyBranchMatchesMain(ctx context.Context, client psclient.PSClient, org, database, branchName, mainBranch string, keyspaces []string, password *ps.DatabaseBranchPassword) error {
	// Fetch main schema via API (stable, not recently modified) and branch
	// schema via MySQL (real-time, avoids API staleness after RefreshSchema).
	mainSchema, err := e.fetchDatabaseSchema(ctx, client, org, database, mainBranch, keyspaces)
	if err != nil {
		return fmt.Errorf("fetch main schema: %w", err)
	}
	branchSchema, err := e.fetchBranchSchemaViaMySQL(ctx, password, keyspaces)
	if err != nil {
		return fmt.Errorf("fetch branch schema via MySQL: %w", err)
	}

	// Build a Namespace from main's schema so we can use diffKeyspace.
	// This diffs branch (current) against main (desired) — any changes
	// mean the branch is ahead of main.
	for _, ks := range keyspaces {
		mainTables := mainSchema[ks]
		branchTables := branchSchema[ks]

		// Quick length check — different table counts means mismatch
		if len(branchTables) != len(mainTables) {
			return fmt.Errorf("keyspace %s: branch has %d tables, main has %d — branch has stale state from a previous apply",
				ks, len(branchTables), len(mainTables))
		}

		// Use Spirit to diff branch vs main (normalized comparison)
		mainNS := &schema.Namespace{Files: make(map[string]string)}
		for _, t := range mainTables {
			mainNS.Files[t.Name+".sql"] = t.Schema + ";"
		}

		changes, _, _, diffErr := e.diffKeyspace(ctx, client, org, database, branchName, ks, mainNS, branchSchema)
		if diffErr != nil {
			return fmt.Errorf("diff branch vs main for %s: %w", ks, diffErr)
		}
		if len(changes) > 0 {
			return fmt.Errorf("keyspace %s: branch has %d DDL differences from main after refresh — branch has stale state from a previous apply",
				ks, len(changes))
		}
	}

	e.logger.Info("branch schema matches main", "branch", branchName, "keyspaces", len(keyspaces))
	return nil
}

func (e *Engine) fetchDatabaseSchema(ctx context.Context, client psclient.PSClient, org, database, branch string, keyspaces []string) (map[string][]table.TableSchema, error) {
	var mu sync.Mutex
	result := make(map[string][]table.TableSchema, len(keyspaces))
	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(20)

	for _, keyspace := range keyspaces {
		ks := keyspace
		g.Go(func() error {
			schemaResult, err := client.GetBranchSchema(gCtx, &ps.BranchSchemaRequest{
				Organization: org,
				Database:     database,
				Branch:       branch,
				Keyspace:     ks,
			})
			if err != nil {
				var psErr *ps.Error
				if errors.As(err, &psErr) && psErr.Code == ps.ErrNotFound {
					// Keyspace doesn't exist yet — treat as empty so all
					// tables appear as CREATEs in the diff.
					e.logger.Info("keyspace not found on branch, treating as empty",
						"keyspace", ks, "branch", branch)
					mu.Lock()
					result[ks] = nil
					mu.Unlock()
					return nil
				}
				return fmt.Errorf("fetch schema for keyspace %s: %w", ks, err)
			}

			tables := make([]table.TableSchema, len(schemaResult))
			for i, t := range schemaResult {
				tables[i] = table.TableSchema{Name: t.Name, Schema: t.Raw}
			}
			mu.Lock()
			result[ks] = tables
			mu.Unlock()
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return result, nil
}

func (e *Engine) fetchPlanSchema(ctx context.Context, client psclient.PSClient, org, database, branch string, creds *engine.Credentials, keyspaces []string) (map[string][]table.TableSchema, error) {
	parent, err := client.GetBranch(ctx, &ps.GetDatabaseBranchRequest{
		Organization: org,
		Database:     database,
		Branch:       branch,
	})
	if err != nil {
		return nil, fmt.Errorf("get branch %s: %w", branch, err)
	}

	if parent.SafeMigrations {
		e.logger.Debug("using PlanetScale schema API for plan", "database", database, "branch", branch)
		return e.fetchDatabaseSchema(ctx, client, org, database, branch, keyspaces)
	}

	if creds == nil || creds.DSN == "" {
		return nil, fmt.Errorf("safe schema changes are not enabled on branch %q of database %q and vtgate DSN is not configured", branch, database)
	}

	e.logger.Info("using vtgate schema for plan because PlanetScale safe schema changes are disabled", "database", database, "branch", branch)
	return e.fetchVtgateSchema(ctx, creds.DSN, keyspaces)
}

func (e *Engine) fetchVtgateSchema(ctx context.Context, dsn string, keyspaces []string) (map[string][]table.TableSchema, error) {
	var mu sync.Mutex
	result := make(map[string][]table.TableSchema, len(keyspaces))
	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(20)

	for _, keyspace := range keyspaces {
		ks := keyspace
		g.Go(func() error {
			db, err := e.getVtgateKeyspaceDB(gCtx, dsn, ks)
			if err != nil {
				return fmt.Errorf("get vtgate connection for keyspace %s: %w", ks, err)
			}
			tables, err := table.LoadSchemaFromDB(gCtx, db, table.WithoutUnderscoreTables)
			if err != nil {
				return fmt.Errorf("load schema for keyspace %s: %w", ks, err)
			}
			mu.Lock()
			result[ks] = tables
			mu.Unlock()
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return result, nil
}

func (e *Engine) createBranch(ctx context.Context, client psclient.PSClient, org, database, branchName, parentBranch string) (*ps.DatabaseBranch, error) {
	getCtx, getCancel := context.WithTimeout(ctx, 10*time.Second)
	defer getCancel()

	parent, err := client.GetBranch(getCtx, &ps.GetDatabaseBranchRequest{
		Organization: org,
		Database:     database,
		Branch:       parentBranch,
	})
	if err != nil {
		return nil, fmt.Errorf("get parent branch: %w", err)
	}

	if !parent.SafeMigrations {
		return nil, fmt.Errorf("safe schema changes not enabled on branch %q of database %q — enable it in the PlanetScale console before running schema changes", parentBranch, database)
	}

	createCtx, createCancel := context.WithTimeout(ctx, 30*time.Second)
	defer createCancel()

	branch, err := client.CreateBranch(createCtx, &ps.CreateDatabaseBranchRequest{
		Organization: org,
		Database:     database,
		Name:         branchName,
		ParentBranch: parentBranch,
		Region:       parent.Region.Slug,
	})
	if err != nil {
		// Idempotent: if branch exists, return it
		if strings.Contains(err.Error(), "Name has already been taken") {
			e.logger.Info("branch already exists, reusing", "branch", branchName)
			return client.GetBranch(ctx, &ps.GetDatabaseBranchRequest{
				Organization: org,
				Database:     database,
				Branch:       branchName,
			})
		}
		return nil, fmt.Errorf("create branch %s: %w", branchName, err)
	}
	return branch, nil
}

func (e *Engine) waitForBranchReady(ctx context.Context, client psclient.PSClient, org, database, branchName string) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Minute)
	defer cancel()

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	var consecutiveErrors int
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout waiting for branch %s", branchName)
		case <-ticker.C:
			branch, err := client.GetBranch(ctx, &ps.GetDatabaseBranchRequest{
				Organization: org,
				Database:     database,
				Branch:       branchName,
			})
			if err != nil {
				consecutiveErrors++
				e.logger.Warn("error checking branch status",
					"branch", branchName, "error", err, "consecutive_errors", consecutiveErrors)
				if consecutiveErrors >= 5 {
					return fmt.Errorf("branch %s not reachable after %d attempts: %w", branchName, consecutiveErrors, err)
				}
				continue
			}
			consecutiveErrors = 0
			if branch.Ready {
				return nil
			}
		}
	}
}

func (e *Engine) createDeployRequest(ctx context.Context, client psclient.PSClient, org, database, branchName, intoBranch string, autoDeleteBranch bool) (*ps.DeployRequest, error) {
	// AutoCutover stays off so SchemaBot is the sole cutover actor: the deploy
	// request parks at pending_cutover and the driver completes it via Cutover
	// (or an operator does, when cutover is deferred). Letting PlanetScale cut
	// over on its own would race the driver's cutover call and move the schema
	// without SchemaBot's involvement or caller attribution.
	//
	// The client sends this setting explicitly rather than through the SDK's
	// request struct, whose omitempty tag drops a false — see
	// psclient.CreateDeployRequest. Saying it out loud is the whole point: it is
	// the only chance to say it, since the API offers no way to change
	// auto_cutover once the deploy request exists.
	return client.CreateDeployRequest(ctx, &ps.CreateDeployRequestRequest{
		Organization:     org,
		Database:         database,
		Branch:           branchName,
		IntoBranch:       intoBranch,
		AutoCutover:      false,
		AutoDeleteBranch: autoDeleteBranch,
	})
}

// deployRequestCreatedEvent states, on the operator's timeline, the cutover
// ownership SchemaBot asked this deploy request to be created with.
//
// The apply's log surface carries lifecycle events, not the engine's own log
// lines, so a decision left in an engine logger is invisible to the operator
// reading the schema change. This one is settled at creation and can never be
// changed afterwards, and it is the fact an operator needs first when a schema
// change swaps without them: either SchemaBot held the cutover, and the swap
// was its call to make, or the deploy request was not created holding it.
//
// It reports the request rather than the outcome, because that is all it has:
// the created deploy request echoes back no cutover setting, so what the
// backend recorded is not knowable here. verifyCutoverHeld reads it back and is
// what turns this into a confirmed fact before a deferred cutover is deployed.
func deployRequestCreatedEvent(dr *ps.DeployRequest, branchName string) engine.ApplyEvent {
	return engine.ApplyEvent{
		Message: fmt.Sprintf("Deploy request #%d created, requesting SchemaBot hold the cutover, validating...", dr.Number),
		Metadata: map[string]string{
			"deploy_request_id":      fmt.Sprintf("%d", dr.Number),
			"deploy_request_url":     dr.HtmlURL,
			"branch":                 branchName,
			"requested_auto_cutover": "false",
		},
		NewState: state.Apply.ValidatingDeployRequest,
	}
}

// verifyCutoverHeld checks that the backend is holding the cutover of a deploy
// request whose cutover the operator deferred.
//
// auto_cutover is settled when the deploy request is created and no later call
// can change it, so the create request is the only thing standing between a
// deferred cutover and a schema that swaps seconds after the deploy goes ready.
// A create request that did not arrive as sent leaves no trace on any surface
// the operator reads: the deploy request looks ordinary right up to the moment
// it cuts itself over. Reading the setting back is the only way to know the
// deferral took.
//
// Fails closed, including when the setting cannot be read — the deploy has not
// started, so refusing costs a re-run, while proceeding costs the decision the
// operator kept for themselves.
//
// A read that does not answer is read again before it is treated as a refusal.
// Auto-cutover on is an answer and stops the deploy immediately; anything else —
// a request that did not arrive, a response carrying no deployment, a body that
// did not decode — leaves the question open, and none of those can be told apart
// from a moment's lag from here. Seconds of re-reading is cheap next to failing
// an apply that would have been fine, and a gate that refuses at random is one
// operators learn to re-run past, which is how a gate stops being one.
func (e *Engine) verifyCutoverHeld(ctx context.Context, client psclient.PSClient, org, database string, number uint64) error {
	const maxAttempts = 3
	const pollInterval = 2 * time.Second

	var lastErr error
	for attempt := range maxAttempts {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return fmt.Errorf("deploy request #%d was not deployed because its cutover was deferred and reading the cutover setting was interrupted: %w", number, ctx.Err())
			case <-time.After(pollInterval):
			}
		}

		autoCutover, err := client.DeployRequestAutoCutover(ctx, org, database, number)
		if err != nil {
			lastErr = err
			e.logger.Warn("could not read the deploy request's cutover setting, reading again before refusing the deploy",
				"database", database, "deploy_request", number, "attempt", attempt+1, "error", err)
			continue
		}
		if autoCutover {
			return fmt.Errorf("deploy request #%d was not deployed: its cutover was deferred, but the deploy request holds auto-cutover on and would swap the schema on its own once the deploy finishes. Auto-cutover cannot be changed after a deploy request is created, so re-run the schema change to get a deploy request that holds the cutover", number)
		}
		e.logger.Info("the deploy request holds the cutover for the operator",
			"database", database, "deploy_request", number, "attempts", attempt+1)
		return nil
	}
	return fmt.Errorf("deploy request #%d was not deployed because its cutover was deferred and SchemaBot could not confirm the cutover is held: %w", number, lastErr)
}

// useInstantDDL decides whether to run an eligible schema change as instant DDL.
//
// Instant DDL rewrites metadata only: the deploy executes and the schema is
// swapped in a single step, with nothing in between. That makes it the right
// way to run an eligible change — except in two cases where the in-between is
// the point:
//
//   - A deferred cutover is a decision the operator kept for themselves, and
//     instant DDL takes it: there is no pending_cutover to park at, so the
//     schema swaps the moment the deploy runs and the operator is told
//     afterwards. The row-copy path parks at the gate.
//   - An unsafe change — one that destroys existing data, in Spirit's unsafe
//     vocabulary (DROP TABLE, DROP COLUMN, DROP PARTITION, ...) — must keep a
//     revert window: the row-copy path retains the old data and can revert
//     after cutover, while instant DDL destroys the data the moment the
//     deploy runs with no way back.
//
// Both cases trade the speed of an eligible change for the safety net that
// the change requires.
func useInstantDDL(dr *ps.DeployRequest, deferCutover, unsafe bool) bool {
	if dr.Deployment == nil || !dr.Deployment.InstantDDLEligible {
		return false
	}
	return !deferCutover && !unsafe
}

// changesContainUnsafe reports whether any DDL statement in the requested
// changes is unsafe (destroys existing data, per ddl.UnsafeStatement), and
// names the operation. The verdict is unconditional — it does not honor
// allow-unsafe: the plan gate proves the operator intended the change, while
// the revert window is what recovers an intended change that turns out to be
// wrong, so passing the first must never remove the second. Classification
// failures fail closed: a statement that cannot be classified is treated as
// unsafe, so the deploy takes the row-copy path and keeps its revert window —
// the cost is time, never data.
func (e *Engine) changesContainUnsafe(changes []engine.SchemaChange, database string) (bool, string) {
	for _, sc := range changes {
		for _, tc := range sc.TableChanges {
			unsafe, reason, err := ddl.UnsafeStatement(tc.DDL)
			if err != nil {
				e.logger.Warn("could not classify statement for the instant DDL decision, taking the row-copy path to keep a revert window",
					"database", database,
					"namespace", sc.Namespace,
					"ddl", tc.DDL,
					"error", err,
				)
				return true, "a statement could not be classified"
			}
			if unsafe {
				return true, reason
			}
		}
	}
	return false, ""
}

func (e *Engine) getDeployRequest(ctx context.Context, client psclient.PSClient, org, database string, number uint64) (*ps.DeployRequest, error) {
	return client.GetDeployRequest(ctx, &ps.GetDeployRequestRequest{
		Organization: org,
		Database:     database,
		Number:       number,
	})
}

// deployDeployRequest starts a deploy request's schema change, absorbing the
// rejections that clear on their own. PlanetScale keeps rejecting the deploy
// while the deploy request's pre-deploy safety validation is still running —
// a not-ready condition, not a failure — so that rejection polls on its own
// bounded budget (deployValidationWait) instead of counting toward the
// transient-error retry bound, which validation routinely outlives. Transient
// API errors retry with backoff up to maxRetries. Any other rejection fails
// immediately.
func (e *Engine) deployDeployRequest(ctx context.Context, client psclient.PSClient, org, database string, number uint64, instantDDL bool) (*ps.DeployRequest, error) {
	var validationDeadline time.Time
	validationWaitLogged := false
	transientAttempts := 0
	for {
		dr, err := client.DeployDeployRequest(ctx, &ps.PerformDeployRequest{
			Organization: org,
			Database:     database,
			Number:       number,
			InstantDDL:   instantDDL,
		})
		if err == nil {
			return dr, nil
		}
		if strings.Contains(err.Error(), "approved") {
			return nil, fmt.Errorf("deploy request #%d could not be deployed: PlanetScale deploy request approvals are not supported — disable 'Require administrator approval for deploy requests' in the PlanetScale database settings", number)
		}
		switch {
		case isDeployStillValidatingError(err):
			// The budget starts at the first validating rejection, not at the
			// first deploy attempt, so transient retries never eat into it.
			if validationDeadline.IsZero() {
				validationDeadline = time.Now().Add(deployValidationWait)
			}
			if time.Now().After(validationDeadline) {
				return nil, fmt.Errorf("deploy deploy request #%d: PlanetScale was still validating the deploy request after waiting %s: %w", number, deployValidationWait, err)
			}
			if !validationWaitLogged {
				e.logger.Info("deploy rejected because the deploy request is still validating; deploy will be retried until validation completes",
					"database", database, "deploy_request", number, "poll_interval", deployValidationPollInterval, "wait_bound", deployValidationWait)
				validationWaitLogged = true
			} else {
				e.logger.Debug("deploy request still validating; retrying deploy",
					"database", database, "deploy_request", number)
			}
			select {
			case <-ctx.Done():
				return nil, fmt.Errorf("context cancelled waiting for deploy request #%d to finish validating: %w", number, ctx.Err())
			case <-time.After(deployValidationPollInterval):
			}
		case isRetryablePSError(err):
			transientAttempts++
			if transientAttempts >= maxRetries {
				return nil, fmt.Errorf("deploy deploy request #%d (after %d attempts): %w", number, maxRetries, err)
			}
			delay := retryDelay(transientAttempts, err)
			e.logger.Warn("retrying deploy request",
				"database", database, "deploy_request", number, "attempt", transientAttempts+1, "delay", delay.Round(time.Millisecond), "error", err)
			select {
			case <-ctx.Done():
				return nil, fmt.Errorf("context cancelled retrying deploy request #%d: %w", number, ctx.Err())
			case <-time.After(delay):
			}
		default:
			return nil, fmt.Errorf("deploy deploy request #%d: %w", number, err)
		}
	}
}

// waitForDeployRequestPending polls a deploy request until PlanetScale finishes
// computing its schema diff and transitions it out of the pending state. The
// deploy request number is held in a local so a transient poll error never
// dereferences a nil deploy request, and the poll honors context cancellation so
// a deploy stuck in pending does not block indefinitely.
func (e *Engine) waitForDeployRequestPending(ctx context.Context, client psclient.PSClient, org, database string, dr *ps.DeployRequest) (*ps.DeployRequest, error) {
	// A nil deploy request means an upstream caller never created or fetched it;
	// poll has nothing to track, so surface the invariant violation rather than
	// dereferencing it below.
	if dr == nil {
		return nil, fmt.Errorf("wait for deploy request in database %s: deploy request is nil", database)
	}

	number := dr.Number

	ticker := time.NewTicker(deployRequestPollInterval)
	defer ticker.Stop()

	for dr.DeploymentState == deployState.Pending {
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("context cancelled waiting for deploy request %d: %w", number, ctx.Err())
		case <-ticker.C:
		}
		next, err := e.getDeployRequest(ctx, client, org, database, number)
		if err != nil {
			return nil, fmt.Errorf("poll deploy request %d: %w", number, err)
		}
		dr = next
	}
	return dr, nil
}
