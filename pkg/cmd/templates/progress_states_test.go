package templates

import (
	"strings"
	"testing"

	"github.com/block/schemabot/pkg/state"
	"github.com/stretchr/testify/assert"
)

func TestStateLabel_PlanetScalePhases(t *testing.T) {
	assert.Equal(t, "Preparing branch", StateLabel(state.Apply.PreparingBranch))
	assert.Equal(t, "Applying changes to branch", StateLabel(state.Apply.ApplyingBranchChanges))
	assert.Equal(t, "Validating branch", StateLabel(state.Apply.ValidatingBranch))
	assert.Equal(t, "Creating deploy request", StateLabel(state.Apply.CreatingDeployRequest))
	assert.Equal(t, "Validating deploy request", StateLabel(state.Apply.ValidatingDeployRequest))
	assert.Equal(t, "Cancelled", StateLabel(state.Apply.Cancelled))
}

func TestFormatProgressState_PlanetScalePhases(t *testing.T) {
	assert.Contains(t, FormatProgressState(state.Apply.PreparingBranch), "Preparing branch")
	assert.Contains(t, FormatProgressState(state.Apply.ApplyingBranchChanges), "Applying changes to branch")
	assert.Contains(t, FormatProgressState(state.Apply.ValidatingBranch), "Validating branch")
	assert.Contains(t, FormatProgressState(state.Apply.CreatingDeployRequest), "Creating deploy request")
	assert.Contains(t, FormatProgressState(state.Apply.ValidatingDeployRequest), "Validating deploy request")
	assert.Contains(t, FormatProgressState(state.Apply.Cancelled), "Cancelled")
}

func TestProgressSymbol(t *testing.T) {
	assert.Equal(t, "+ ", progressSymbol("create"))
	assert.Equal(t, "- ", progressSymbol("drop"))
	assert.Equal(t, "~ ", progressSymbol("alter"))
	assert.Equal(t, "~ ", progressSymbol(""))
}

func TestFormatTableProgress_ChangeTypeSymbol(t *testing.T) {
	for _, tt := range []struct {
		changeType string
		symbol     string
	}{
		{"create", "+"},
		{"drop", "-"},
		{"alter", "~"},
	} {
		tp := TableProgress{
			TableName:  "users",
			ChangeType: tt.changeType,
			Status:     state.Apply.Completed,
		}
		output := FormatTableProgress(tp)
		assert.Contains(t, output, tt.symbol+" users:", "expected %q symbol for %s", tt.symbol, tt.changeType)
	}
}

func TestFormatTableProgress_InstantDDL(t *testing.T) {
	tp := TableProgress{
		TableName:  "users",
		ChangeType: "alter",
		Status:     state.Apply.Running,
		IsInstant:  true,
	}
	output := FormatTableProgress(tp)
	assert.Contains(t, output, "Applying instantly...")

	tp.Status = state.Apply.Completed
	output = FormatTableProgress(tp)
	assert.Contains(t, output, "Applied instantly")
}

func TestFormatTableProgress_CreateDropLabels(t *testing.T) {
	for _, changeType := range []string{"create", "drop"} {
		tp := TableProgress{
			TableName:  "users",
			ChangeType: changeType,
			Status:     state.Apply.Running,
		}
		output := FormatTableProgress(tp)
		assert.Contains(t, output, "Applying...", "%s should show 'Applying...'", changeType)
	}

	tp := TableProgress{
		TableName:  "users",
		ChangeType: "alter",
		Status:     state.Apply.CuttingOver,
	}
	output := FormatTableProgress(tp)
	assert.Contains(t, output, "Cutting over...")
}

func TestVSchemaStatusLabel(t *testing.T) {
	assert.Equal(t, "Pending", vschemaStatusLabel(state.Apply.Pending))
	assert.Equal(t, "Pending", vschemaStatusLabel(state.Apply.WaitingForDeploy))
	assert.Contains(t, vschemaStatusLabel(state.Apply.Running), "Applying")
	assert.Contains(t, vschemaStatusLabel(state.Apply.WaitingForCutover), "Applying")
	assert.Contains(t, vschemaStatusLabel(state.Apply.CuttingOver), "Applying")
	assert.Contains(t, vschemaStatusLabel(state.Apply.Completed), "Applied")
	assert.Contains(t, vschemaStatusLabel(state.Apply.Failed), "Failed")
	assert.Contains(t, vschemaStatusLabel(state.Apply.RevertWindow), "pending revert")
}

func TestVSchemaTaskRenderedWithDDLTasks(t *testing.T) {
	tables := []TableProgress{
		{TableName: "users", Namespace: "myapp_sharded", Status: state.Apply.Completed, DDL: "ALTER TABLE `users` ADD COLUMN `phone` varchar(20)"},
		{TableName: "VSchema: myapp_sharded", Namespace: "myapp_sharded", Status: state.Apply.Running},
	}
	result := FormatNamespacedTables(tables)
	assert.Contains(t, result, "VSchema")
	assert.Contains(t, result, "users")
	vsIdx := strings.Index(result, "VSchema")
	usersIdx := strings.Index(result, "users:")
	assert.Less(t, vsIdx, usersIdx, "VSchema should render before DDL tables")
}

func TestIsVSchemaTask_Variants(t *testing.T) {
	assert.True(t, isVSchemaTask(TableProgress{TableName: "VSchema"}))
	assert.True(t, isVSchemaTask(TableProgress{TableName: "VSchema: myapp_sharded"}))
	assert.True(t, isVSchemaTask(TableProgress{TableName: "vschema:myapp"}))
	assert.False(t, isVSchemaTask(TableProgress{TableName: "users"}))
}

func TestStateColorFunc_PlanetScalePhases(t *testing.T) {
	for _, s := range []string{
		state.Apply.PreparingBranch,
		state.Apply.ApplyingBranchChanges,
		state.Apply.ValidatingBranch,
		state.Apply.CreatingDeployRequest,
		state.Apply.ValidatingDeployRequest,
		state.Apply.Cancelled,
	} {
		fn := stateColorFunc(s)
		assert.NotNil(t, fn, "expected color function for state %q", s)
	}
}
