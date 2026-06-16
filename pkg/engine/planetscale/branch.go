package planetscale

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	mysql "github.com/go-sql-driver/mysql"
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
)

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
			db, err := sql.Open("mysql", ksCfg.FormatDSN())
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
			return nil, false, "", fmt.Errorf("fetch VSchema for keyspace %s: %w", ks, fetchErr)
		}
		if currentVSchema != nil {
			currentVSchemaRaw = currentVSchema.Raw
		}
		vschemaChanged = VSchemaChanged(currentVSchemaRaw, content)
		if vschemaChanged {
			e.logger.Info("diffKeyspace: VSchema mismatch detected",
				"keyspace", ks,
				"branch", branch,
				"current_normalized", normalizeVSchemaJSON(currentVSchemaRaw),
				"desired_normalized", normalizeVSchemaJSON(content),
			)
		}
	}

	return tableChanges, vschemaChanged, currentVSchemaRaw, nil
}

// verifyBranchMatchesMain uses Spirit's differ to compare the branch schema
// against main for the given keyspaces. Returns an error if any DDL changes
// exist, indicating the branch has stale DDL from a previous failed apply
// that RefreshSchema did not clean up.
func (e *Engine) verifyBranchMatchesMain(ctx context.Context, client psclient.PSClient, org, database, branchName, mainBranch string, keyspaces []string, schemaFiles schema.SchemaFiles, password *ps.DatabaseBranchPassword) error {
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

func (e *Engine) createDeployRequest(ctx context.Context, client psclient.PSClient, org, database, branchName, intoBranch string, autoCutover, autoDeleteBranch bool) (*ps.DeployRequest, error) {
	return client.CreateDeployRequest(ctx, &ps.CreateDeployRequestRequest{
		Organization:     org,
		Database:         database,
		Branch:           branchName,
		IntoBranch:       intoBranch,
		AutoCutover:      autoCutover,
		AutoDeleteBranch: autoDeleteBranch,
	})
}

func (e *Engine) getDeployRequest(ctx context.Context, client psclient.PSClient, org, database string, number uint64) (*ps.DeployRequest, error) {
	return client.GetDeployRequest(ctx, &ps.GetDeployRequestRequest{
		Organization: org,
		Database:     database,
		Number:       number,
	})
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
