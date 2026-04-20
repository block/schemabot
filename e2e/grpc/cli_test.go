//go:build e2e

// This file contains CLI-based gRPC e2e tests. These verify the full path:
// CLI -> HTTP API -> GRPCClient -> Tern gRPC -> Spirit -> MySQL.
//
// Each test creates its own table for isolation and cleans up via grpcEnsureNoActiveChange.
package grpc

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/block/schemabot/e2e/testutil"
	"github.com/block/schemabot/pkg/e2eutil"
	"github.com/block/schemabot/pkg/state"
	"github.com/stretchr/testify/assert"
)

// TestGRPCCLI_Status_NoActiveChange verifies that status shows the database when idle.
func TestGRPCCLI_Status_NoActiveChange(t *testing.T) {
	bin := grpcCLIBuildOrFind(t)
	endpoint := grpcSchemabotURL(t)

	tableName := uniqueGRPCTableName("cli_idle")
	grpcCreateTestTable(t, "staging", tableName, fmt.Sprintf(
		`CREATE TABLE %s (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255) NOT NULL)`, tableName))

	grpcEnsureNoActiveChange(t, "testapp", "staging")

	schemaDir := grpcCLISchemaDir(t, map[string]string{
		tableName + ".sql": fmt.Sprintf(
			`CREATE TABLE %s (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255) NOT NULL);`, tableName),
	})

	out := e2eutil.RunCLIInDir(t, bin, schemaDir, "status",
		"--database", "testapp",
		"--endpoint", endpoint,
	)
	e2eutil.AssertContains(t, out, "testapp")
}

// TestGRPCCLI_PlanApply_AddColumn tests a full plan -> apply -> progress -> verify workflow via CLI.
func TestGRPCCLI_PlanApply_AddColumn(t *testing.T) {
	bin := grpcCLIBuildOrFind(t)
	endpoint := grpcSchemabotURL(t)

	tableName := uniqueGRPCTableName("cli_addcol")
	grpcCreateTestTable(t, "staging", tableName, fmt.Sprintf(
		`CREATE TABLE %s (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255) NOT NULL)`, tableName))

	grpcSeedRows(t, "staging", tableName, "name", "CONCAT('user_', seq)", 1000)
	grpcEnsureNoActiveChange(t, "testapp", "staging")

	// Schema dir with the new column added
	schemaDir := grpcCLISchemaDir(t, map[string]string{
		tableName + ".sql": fmt.Sprintf(
			`CREATE TABLE %s (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255) NOT NULL, email VARCHAR(255) DEFAULT NULL);`, tableName),
	})

	// Plan
	out := e2eutil.RunCLIInDir(t, bin, schemaDir, "plan",
		"-s", ".",
		"-e", "staging",
		"--endpoint", endpoint,
	)
	e2eutil.AssertContains(t, out, "Schema Change Plan")
	e2eutil.AssertContains(t, out, "ADD COLUMN")

	// Apply
	out = e2eutil.RunCLIInDir(t, bin, schemaDir, "apply",
		"-s", ".",
		"-e", "staging",
		"--endpoint", endpoint,
		"-y",
		"--watch=false",
	)
	e2eutil.AssertContains(t, out, "Apply started")
	applyID := parseApplyID(t, out)

	// Wait for completion via apply ID progress polling
	testutil.WaitForState(t, endpoint, applyID, state.Apply.Completed, 3*time.Minute)

	// Verify column exists via direct MySQL
	assert.True(t, grpcColumnExists(t, "staging", tableName, "email"),
		"expected column 'email' to exist on %s after apply", tableName)

	grpcEnsureNoActiveChange(t, "testapp", "staging")
}

// TestGRPCCLI_Plan_NoChanges verifies that plan shows "No schema changes detected" when schema is up to date.
func TestGRPCCLI_Plan_NoChanges(t *testing.T) {
	bin := grpcCLIBuildOrFind(t)
	endpoint := grpcSchemabotURL(t)

	tableName := uniqueGRPCTableName("cli_nochange")
	grpcCreateTestTable(t, "staging", tableName, fmt.Sprintf(
		`CREATE TABLE %s (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255) NOT NULL)`, tableName))

	grpcSeedRows(t, "staging", tableName, "name", "CONCAT('user_', seq)", 100)
	grpcEnsureNoActiveChange(t, "testapp", "staging")

	// Schema dir matches the existing table exactly
	schemaDir := grpcCLISchemaDir(t, map[string]string{
		tableName + ".sql": fmt.Sprintf(
			`CREATE TABLE %s (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255) NOT NULL);`, tableName),
	})

	// Plan should show no changes since the schema already matches
	out := e2eutil.RunCLIInDir(t, bin, schemaDir, "plan",
		"-s", ".",
		"-e", "staging",
		"--endpoint", endpoint,
	)
	e2eutil.AssertContains(t, out, "No schema changes detected")
}

// TestGRPCCLI_DeferCutover tests the deferred cutover workflow via CLI.
// Uses INT->BIGINT PK change to force Spirit copy-swap (not instant DDL).
func TestGRPCCLI_DeferCutover(t *testing.T) {
	bin := grpcCLIBuildOrFind(t)
	endpoint := grpcSchemabotURL(t)

	tableName := uniqueGRPCTableName("cli_cutover")
	grpcCreateTestTable(t, "staging", tableName, fmt.Sprintf(
		`CREATE TABLE %s (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255) NOT NULL, data TEXT)`, tableName))

	grpcSeedRows(t, "staging", tableName, "name, data",
		"CONCAT('user_', seq), REPEAT('x', 200)", 10000)
	grpcEnsureNoActiveChange(t, "testapp", "staging")

	// Schema with INT->BIGINT PK change
	schemaDir := grpcCLISchemaDir(t, map[string]string{
		tableName + ".sql": fmt.Sprintf(
			`CREATE TABLE %s (id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255) NOT NULL, data TEXT);`, tableName),
	})

	// Apply with --defer-cutover
	out := e2eutil.RunCLIInDir(t, bin, schemaDir, "apply",
		"-s", ".",
		"-e", "staging",
		"--endpoint", endpoint,
		"-y",
		"--watch=false",
		"--defer-cutover",
		"--allow-unsafe",
		"-o", "json",
	)
	applyID := parseApplyID(t, out)

	// Wait for waiting_for_cutover or completed (Spirit may be too fast)
	finalState := testutil.WaitForAnyState(t, endpoint, applyID,
		[]string{state.Apply.WaitingForCutover, state.Apply.Completed}, 3*time.Minute)

	if strings.Contains(finalState, state.Apply.WaitingForCutover) {
		// Verify progress shows waiting
		out = e2eutil.RunCLIInDir(t, bin, schemaDir, "progress",
			applyID,
			"--endpoint", endpoint,
			"--watch=false",
		)
		e2eutil.AssertContains(t, out, "Waiting for cutover")

		// Trigger cutover via CLI
		out = e2eutil.RunCLIInDir(t, bin, schemaDir, "cutover",
			applyID,
			"--endpoint", endpoint,
			"--watch=false",
		)
		e2eutil.AssertContains(t, out, "Cutover requested")

		// Wait for completion
		testutil.WaitForState(t, endpoint, applyID, state.Apply.Completed, 2*time.Minute)
	}

	grpcEnsureNoActiveChange(t, "testapp", "staging")
}

// TestGRPCCLI_StopStart tests stopping and resuming an in-progress schema change via CLI.
// Uses INT->BIGINT PK change to force Spirit copy-swap (not instant DDL).
func TestGRPCCLI_StopStart(t *testing.T) {
	bin := grpcCLIBuildOrFind(t)
	endpoint := grpcSchemabotURL(t)

	tableName := uniqueGRPCTableName("cli_stopstart")
	grpcCreateTestTable(t, "staging", tableName, fmt.Sprintf(
		`CREATE TABLE %s (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255) NOT NULL, data TEXT)`, tableName))

	grpcSeedRows(t, "staging", tableName, "name, data",
		"CONCAT('user_', seq), REPEAT('x', 500)", 100000)
	grpcEnsureNoActiveChange(t, "testapp", "staging")

	// Schema with INT->BIGINT PK change
	schemaDir := grpcCLISchemaDir(t, map[string]string{
		tableName + ".sql": fmt.Sprintf(
			`CREATE TABLE %s (id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255) NOT NULL, data TEXT);`, tableName),
	})

	// Apply
	out := e2eutil.RunCLIInDir(t, bin, schemaDir, "apply",
		"-s", ".",
		"-e", "staging",
		"--endpoint", endpoint,
		"-y",
		"--watch=false",
		"--allow-unsafe",
		"-o", "json",
	)
	applyID := parseApplyID(t, out)

	// Wait for it to be running (not just planned)
	testutil.WaitForAnyState(t, endpoint, applyID,
		[]string{state.Apply.Running, state.Apply.Completed}, 60*time.Second)

	// Check if already completed -- Spirit may be too fast
	progOut, _ := e2eutil.RunCLIWithErrorInDir(t, bin, schemaDir, "progress",
		applyID,
		"--endpoint", endpoint,
		"--watch=false",
	)
	if !strings.Contains(e2eutil.StripANSI(progOut), "Completed") {
		// Stop via CLI
		e2eutil.RunCLIInDir(t, bin, schemaDir, "stop",
			applyID,
			"--endpoint", endpoint,
		)

		// Wait for stopped state
		testutil.WaitForState(t, endpoint, applyID, state.Apply.Stopped, 30*time.Second)

		// Verify progress shows stopped
		out = e2eutil.RunCLIInDir(t, bin, schemaDir, "progress",
			applyID,
			"--endpoint", endpoint,
			"--watch=false",
		)
		e2eutil.AssertContains(t, out, "Stopped")

		// Start via CLI
		e2eutil.RunCLIInDir(t, bin, schemaDir, "start",
			applyID,
			"--endpoint", endpoint,
			"--watch=false",
		)
	}

	// Wait for completion
	testutil.WaitForState(t, endpoint, applyID, state.Apply.Completed, 5*time.Minute)

	grpcEnsureNoActiveChange(t, "testapp", "staging")
}

// TestGRPCCLI_Volume tests adjusting schema change speed during an apply via CLI.
// Uses INT->BIGINT PK change to force Spirit copy-swap (not instant DDL).
func TestGRPCCLI_Volume(t *testing.T) {
	bin := grpcCLIBuildOrFind(t)
	endpoint := grpcSchemabotURL(t)

	tableName := uniqueGRPCTableName("cli_volume")
	grpcCreateTestTable(t, "staging", tableName, fmt.Sprintf(
		`CREATE TABLE %s (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255) NOT NULL, data TEXT)`, tableName))

	grpcSeedRows(t, "staging", tableName, "name, data",
		"CONCAT('user_', seq), REPEAT('x', 500)", 100000)
	grpcEnsureNoActiveChange(t, "testapp", "staging")

	// Schema with INT->BIGINT PK change
	schemaDir := grpcCLISchemaDir(t, map[string]string{
		tableName + ".sql": fmt.Sprintf(
			`CREATE TABLE %s (id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255) NOT NULL, data TEXT);`, tableName),
	})

	// Apply
	out := e2eutil.RunCLIInDir(t, bin, schemaDir, "apply",
		"-s", ".",
		"-e", "staging",
		"--endpoint", endpoint,
		"-y",
		"--watch=false",
		"--allow-unsafe",
		"-o", "json",
	)
	applyID := parseApplyID(t, out)

	// Wait for running state
	testutil.WaitForAnyState(t, endpoint, applyID,
		[]string{state.Apply.Running, state.Apply.Completed}, 60*time.Second)

	// Try to adjust volume (may fail if Spirit completed too fast -- that's OK)
	progOut, _ := e2eutil.RunCLIWithErrorInDir(t, bin, schemaDir, "progress",
		applyID,
		"--endpoint", endpoint,
		"--watch=false",
	)
	if !strings.Contains(e2eutil.StripANSI(progOut), "Completed") {
		volOut, err := e2eutil.RunCLIWithErrorInDir(t, bin, schemaDir, "volume",
			applyID,
			"-v", "5",
			"--endpoint", endpoint,
		)
		if err != nil {
			t.Logf("volume command returned error (may have completed): %v\nOutput: %s", err, volOut)
		} else {
			t.Logf("volume adjustment output: %s", volOut)
		}
	}

	// Wait for completion
	testutil.WaitForState(t, endpoint, applyID, state.Apply.Completed, 5*time.Minute)

	grpcEnsureNoActiveChange(t, "testapp", "staging")
}

// TestGRPCCLI_Progress_ByApplyID tests that progress can be fetched by apply ID.
func TestGRPCCLI_Progress_ByApplyID(t *testing.T) {
	bin := grpcCLIBuildOrFind(t)
	endpoint := grpcSchemabotURL(t)

	tableName := uniqueGRPCTableName("cli_byapplyid")
	grpcCreateTestTable(t, "staging", tableName, fmt.Sprintf(
		`CREATE TABLE %s (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255) NOT NULL)`, tableName))

	grpcSeedRows(t, "staging", tableName, "name", "CONCAT('user_', seq)", 1000)
	grpcEnsureNoActiveChange(t, "testapp", "staging")

	// Schema dir with a new column added
	schemaDir := grpcCLISchemaDir(t, map[string]string{
		tableName + ".sql": fmt.Sprintf(
			`CREATE TABLE %s (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255) NOT NULL, notes VARCHAR(255) DEFAULT NULL);`, tableName),
	})

	// Apply (capture apply_id from output)
	out := e2eutil.RunCLIInDir(t, bin, schemaDir, "apply",
		"-s", ".",
		"-e", "staging",
		"--endpoint", endpoint,
		"-y",
		"--watch=false",
	)
	e2eutil.AssertContains(t, out, "Apply started")
	applyID := parseApplyID(t, out)
	t.Logf("captured apply ID: %s", applyID)

	// Wait for completion via apply ID polling
	testutil.WaitForState(t, endpoint, applyID, state.Apply.Completed, 3*time.Minute)

	// Now fetch progress by apply ID
	out = e2eutil.RunCLIInDir(t, bin, schemaDir, "progress",
		applyID,
		"--endpoint", endpoint,
		"--watch=false",
	)
	e2eutil.AssertContains(t, out, tableName)

	grpcEnsureNoActiveChange(t, "testapp", "staging")
}

// TestGRPCCLI_PlanApply_Production tests plan -> apply -> verify on the production environment via CLI.
// Proves cross-service routing: SchemaBot routes to the production Tern service.
func TestGRPCCLI_PlanApply_Production(t *testing.T) {
	bin := grpcCLIBuildOrFind(t)
	endpoint := grpcSchemabotURL(t)

	tableName := uniqueGRPCTableName("cli_prod")
	grpcCreateTestTable(t, "production", tableName, fmt.Sprintf(
		`CREATE TABLE %s (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255) NOT NULL)`, tableName))

	grpcSeedRows(t, "production", tableName, "name", "CONCAT('user_', seq)", 1000)
	grpcEnsureNoActiveChange(t, "testapp", "production")

	colName := "prod_cli_col"
	schemaDir := grpcCLISchemaDir(t, map[string]string{
		tableName + ".sql": fmt.Sprintf(
			`CREATE TABLE %s (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255) NOT NULL, %s VARCHAR(255) DEFAULT NULL);`, tableName, colName),
	})

	// Plan against production
	out := e2eutil.RunCLIInDir(t, bin, schemaDir, "plan",
		"-s", ".",
		"-e", "production",
		"--endpoint", endpoint,
	)
	e2eutil.AssertContains(t, out, "Schema Change Plan")
	e2eutil.AssertContains(t, out, "ADD COLUMN")

	// Apply against production
	out = e2eutil.RunCLIInDir(t, bin, schemaDir, "apply",
		"-s", ".",
		"-e", "production",
		"--endpoint", endpoint,
		"-y",
		"--watch=false",
	)
	e2eutil.AssertContains(t, out, "Apply started")
	applyID := parseApplyID(t, out)

	// Wait for completion via apply ID polling
	testutil.WaitForState(t, endpoint, applyID, state.Apply.Completed, 3*time.Minute)

	// Verify column exists via direct MySQL
	assert.True(t, grpcColumnExists(t, "production", tableName, colName),
		"expected column %q to exist on %s in production", colName, tableName)

	grpcEnsureNoActiveChange(t, "testapp", "production")
}
