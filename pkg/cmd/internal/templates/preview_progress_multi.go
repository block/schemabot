package templates

import (
	"fmt"
	"time"

	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

func previewCLIMultiDeploymentApplyInProgress() {
	WriteProgress(multiDeploymentProgressData([]ProgressOperation{
		{Deployment: "us-east", Target: "orders-us-east", State: state.ApplyOperation.WaitingForCutover, CutoverPolicy: storage.CutoverPolicyBarrier, OnFailure: storage.OnFailureHalt},
		{Deployment: "eu-west", Target: "orders-eu-west", State: state.ApplyOperation.Running, CutoverPolicy: storage.CutoverPolicyBarrier, OnFailure: storage.OnFailureHalt},
		{Deployment: "ap-south", Target: "orders-ap-south", State: state.ApplyOperation.Pending, CutoverPolicy: storage.CutoverPolicyBarrier, OnFailure: storage.OnFailureHalt},
	}, []TableProgress{
		{Deployment: "us-east", Target: "orders-us-east", TableName: "orders", ChangeType: "alter", Dialect: schema.DialectMySQL, DDL: "ALTER TABLE `orders` ADD COLUMN `source` varchar(32) DEFAULT NULL", Status: state.Task.WaitingForCutover, RowsCopied: 80000, RowsTotal: 80000, PercentComplete: 100},
		{Deployment: "eu-west", Target: "orders-eu-west", TableName: "orders", ChangeType: "alter", Dialect: schema.DialectMySQL, DDL: "ALTER TABLE `orders` ADD COLUMN `source` varchar(32) DEFAULT NULL", Status: state.Task.Running, RowsCopied: 42000, RowsTotal: 120000, PercentComplete: 35, ETASeconds: 240},
		{Deployment: "ap-south", Target: "orders-ap-south", TableName: "orders", ChangeType: "alter", Dialect: schema.DialectMySQL, DDL: "ALTER TABLE `orders` ADD COLUMN `source` varchar(32) DEFAULT NULL", Status: state.Task.Pending},
	}))
}

func previewCLIMultiDeploymentApplyFailed() {
	WriteProgress(multiDeploymentProgressData([]ProgressOperation{
		{Deployment: "us-east", Target: "orders-us-east", State: state.ApplyOperation.Completed, StartedAt: previewTime.Add(-8 * time.Minute).Format(time.RFC3339), CutoverPolicy: storage.CutoverPolicyRolling, OnFailure: storage.OnFailureHalt},
		{Deployment: "eu-west", Target: "orders-eu-west", State: state.ApplyOperation.Failed, CutoverPolicy: storage.CutoverPolicyRolling, OnFailure: storage.OnFailureHalt, ErrorMessage: "duplicate key name 'idx_orders_source'"},
		{Deployment: "ap-south", Target: "orders-ap-south", State: state.ApplyOperation.Pending, CutoverPolicy: storage.CutoverPolicyRolling, OnFailure: storage.OnFailureHalt},
	}, []TableProgress{
		{Deployment: "us-east", Target: "orders-us-east", TableName: "orders", ChangeType: "alter", Dialect: schema.DialectMySQL, DDL: "ALTER TABLE `orders` ADD COLUMN `source` varchar(32) DEFAULT NULL", Status: state.Task.Completed, RowsCopied: 80000, RowsTotal: 80000, PercentComplete: 100},
		{Deployment: "eu-west", Target: "orders-eu-west", TableName: "orders", ChangeType: "alter", Dialect: schema.DialectMySQL, DDL: "ALTER TABLE `orders` ADD INDEX `idx_orders_source` (`source`)", Status: state.Task.Failed, RowsCopied: 0, RowsTotal: 120000, PercentComplete: 0},
		{Deployment: "ap-south", Target: "orders-ap-south", TableName: "orders", ChangeType: "alter", Dialect: schema.DialectMySQL, DDL: "ALTER TABLE `orders` ADD COLUMN `source` varchar(32) DEFAULT NULL", Status: TaskCancelled},
	}))
}

func previewCLIMultiDeploymentApplyHaltedWithLiveSibling() {
	WriteProgress(multiDeploymentProgressData([]ProgressOperation{
		{Deployment: "us-east", Target: "orders-us-east", State: state.ApplyOperation.Failed, CutoverPolicy: storage.CutoverPolicyBarrier, OnFailure: storage.OnFailureHalt, ErrorMessage: "duplicate key name 'idx_orders_source'"},
		{Deployment: "eu-west", Target: "orders-eu-west", State: state.ApplyOperation.Running, CutoverPolicy: storage.CutoverPolicyBarrier, OnFailure: storage.OnFailureHalt},
		{Deployment: "ap-south", Target: "orders-ap-south", State: state.ApplyOperation.Pending, CutoverPolicy: storage.CutoverPolicyBarrier, OnFailure: storage.OnFailureHalt},
	}, []TableProgress{
		{Deployment: "us-east", Target: "orders-us-east", TableName: "orders", ChangeType: "alter", Dialect: schema.DialectMySQL, DDL: "ALTER TABLE `orders` ADD INDEX `idx_orders_source` (`source`)", Status: state.Task.Failed, RowsCopied: 0, RowsTotal: 80000, PercentComplete: 0},
		{Deployment: "eu-west", Target: "orders-eu-west", TableName: "orders", ChangeType: "alter", Dialect: schema.DialectMySQL, DDL: "ALTER TABLE `orders` ADD COLUMN `source` varchar(32) DEFAULT NULL", Status: state.Task.Running, RowsCopied: 42000, RowsTotal: 120000, PercentComplete: 35, ETASeconds: 240},
		{Deployment: "ap-south", Target: "orders-ap-south", TableName: "orders", ChangeType: "alter", Dialect: schema.DialectMySQL, DDL: "ALTER TABLE `orders` ADD COLUMN `source` varchar(32) DEFAULT NULL", Status: state.Task.Pending},
	}))
}

func previewCLIMultiDeploymentApplyCompleted() {
	data := multiDeploymentProgressData([]ProgressOperation{
		{Deployment: "us-east", Target: "orders-us-east", State: state.ApplyOperation.Completed, StartedAt: previewTime.Add(-8 * time.Minute).Format(time.RFC3339), CutoverPolicy: storage.CutoverPolicyRolling, OnFailure: storage.OnFailureHalt},
		{Deployment: "eu-west", Target: "orders-eu-west", State: state.ApplyOperation.Completed, StartedAt: previewTime.Add(-8 * time.Minute).Format(time.RFC3339), CutoverPolicy: storage.CutoverPolicyRolling, OnFailure: storage.OnFailureHalt},
		{Deployment: "ap-south", Target: "orders-ap-south", State: state.ApplyOperation.Completed, StartedAt: previewTime.Add(-8 * time.Minute).Format(time.RFC3339), CutoverPolicy: storage.CutoverPolicyRolling, OnFailure: storage.OnFailureHalt},
	}, []TableProgress{
		{Deployment: "us-east", Target: "orders-us-east", TableName: "orders", ChangeType: "alter", Dialect: schema.DialectMySQL, DDL: "ALTER TABLE `orders` ADD COLUMN `source` varchar(32) DEFAULT NULL", Status: state.Task.Completed, RowsCopied: 80000, RowsTotal: 80000, PercentComplete: 100},
		{Deployment: "eu-west", Target: "orders-eu-west", TableName: "orders", ChangeType: "alter", Dialect: schema.DialectMySQL, DDL: "ALTER TABLE `orders` ADD COLUMN `source` varchar(32) DEFAULT NULL", Status: state.Task.Completed, RowsCopied: 120000, RowsTotal: 120000, PercentComplete: 100},
		{Deployment: "ap-south", Target: "orders-ap-south", TableName: "orders", ChangeType: "alter", Dialect: schema.DialectMySQL, DDL: "ALTER TABLE `orders` ADD COLUMN `source` varchar(32) DEFAULT NULL", Status: state.Task.Completed, RowsCopied: 60000, RowsTotal: 60000, PercentComplete: 100},
	})
	data.CompletedAt = previewTime.Add(-1 * time.Minute).Format(time.RFC3339)
	WriteProgress(data)
}

func previewCLIMultiDeployAllOutput() {
	sections := []struct {
		name string
		fn   func()
	}{
		{"BARRIER ROLLOUT IN PROGRESS", previewCLIMultiDeploymentApplyInProgress},
		{"HALT ON FAILURE (ONE DEPLOYMENT FAILED)", previewCLIMultiDeploymentApplyFailed},
		{"HALT ON FAILURE (A SIBLING IS STILL RUNNING)", previewCLIMultiDeploymentApplyHaltedWithLiveSibling},
		{"ALL DEPLOYMENTS COMPLETED", previewCLIMultiDeploymentApplyCompleted},
		{"MULTI-TARGET ROLLOUT PAST A FAILED TARGET", previewCLIMultiTargetRolloutInProgress},
		{"MULTI-TARGET ROLLOUT WAITING FOR CUTOVER", previewCLIMultiTargetRolloutWaitingForCutover},
		{"MULTI-TARGET ROLLOUT STOPPED", previewCLIMultiTargetRolloutStopped},
	}
	for i, section := range sections {
		if i > 0 {
			fmt.Println()
		}
		fmt.Printf("--- %s ---\n\n", section.name)
		section.fn()
	}
}

// previewCLIMultiTargetRolloutInProgress is one deployment of 64 targets
// continuing past a failed one: 40 done, 19 copying, 4 not started.
func previewCLIMultiTargetRolloutInProgress() {
	var ops []ProgressOperation
	var tables []TableProgress
	for i := range 64 {
		target := fmt.Sprintf("payments-%03d", i+1)
		op := ProgressOperation{Deployment: "prod", Target: target, CutoverPolicy: storage.CutoverPolicyParallel, OnFailure: storage.OnFailureContinue}
		table := TableProgress{Deployment: "prod", Target: target, TableName: "orders", ChangeType: "alter", Dialect: schema.DialectMySQL, DDL: "ALTER TABLE `orders` ADD COLUMN `source` varchar(32) DEFAULT NULL", RowsTotal: 80000}
		switch {
		case i < 40:
			op.State, table.Status, table.RowsCopied, table.PercentComplete = state.ApplyOperation.Completed, state.Task.Completed, 80000, 100
			op.StartedAt = previewTime.Add(-8 * time.Minute).Format(time.RFC3339)
		case i == 40:
			op.State, op.ErrorMessage, op.ExternalID = state.ApplyOperation.Failed, "duplicate key name 'idx_orders_source'", "spirit-apply-041"
			table.Status, table.RowsCopied, table.PercentComplete = state.Task.Failed, 12000, 15
		case i < 60:
			copied := int64(20000 + (i-41)*3000)
			op.State, table.Status, table.RowsCopied, table.PercentComplete, table.ETASeconds = state.ApplyOperation.Running, state.Task.Running, copied, int(copied*100/80000), int64(600-(i-41)*25)
		default:
			op.State = state.ApplyOperation.Pending
			ops = append(ops, op)
			continue
		}
		ops = append(ops, op)
		tables = append(tables, table)
	}
	data := multiDeploymentProgressData(ops, tables)
	data.State = state.Apply.RunningDegraded
	WriteProgress(data)
}

// previewCLIMultiTargetRolloutWaitingForCutover is three targets of an apply
// that defers cutover holding at the cutover gate, one of which holds a schema
// of its own and so runs a different change.
func previewCLIMultiTargetRolloutWaitingForCutover() {
	var ops []ProgressOperation
	var tables []TableProgress
	for i, ddl := range []string{
		"ALTER TABLE `orders` ADD COLUMN `source` varchar(32) DEFAULT NULL",
		"ALTER TABLE `orders` ADD COLUMN `source` varchar(32) DEFAULT NULL",
		"ALTER TABLE `orders` MODIFY COLUMN `source` varchar(32) DEFAULT NULL",
	} {
		target := fmt.Sprintf("payments-%03d", i+1)
		ops = append(ops, ProgressOperation{Deployment: "prod", Target: target, State: state.ApplyOperation.WaitingForCutover, CutoverPolicy: storage.CutoverPolicyBarrier, OnFailure: storage.OnFailureHalt})
		tables = append(tables, TableProgress{Deployment: "prod", Target: target, TableName: "orders", ChangeType: "alter", Dialect: schema.DialectMySQL, DDL: ddl, Status: state.Task.WaitingForCutover, RowsCopied: 80000, RowsTotal: 80000, PercentComplete: 100})
	}
	data := multiDeploymentProgressData(ops, tables)
	data.State = state.Apply.WaitingForCutover
	data.Options = map[string]string{"defer_cutover": "true"}
	WriteProgress(data)
}

// previewCLIMultiTargetRolloutStopped is three targets stopped part-way: one
// had already finished, the other two stopped mid-copy.
func previewCLIMultiTargetRolloutStopped() {
	var ops []ProgressOperation
	var tables []TableProgress
	for i, copied := range []int64{80000, 32000, 20000} {
		target := fmt.Sprintf("payments-%03d", i+1)
		op := ProgressOperation{Deployment: "prod", Target: target, State: state.ApplyOperation.Stopped, StartedAt: previewTime.Add(-8 * time.Minute).Format(time.RFC3339), CutoverPolicy: storage.CutoverPolicyParallel, OnFailure: storage.OnFailureHalt}
		table := TableProgress{Deployment: "prod", Target: target, TableName: "orders", ChangeType: "alter", Dialect: schema.DialectMySQL, DDL: "ALTER TABLE `orders` ADD COLUMN `source` varchar(32) DEFAULT NULL", Status: state.Task.Stopped, RowsCopied: copied, RowsTotal: 80000, PercentComplete: int(copied * 100 / 80000)}
		if copied == 80000 {
			op.State, table.Status = state.ApplyOperation.Completed, state.Task.Completed
		}
		ops = append(ops, op)
		tables = append(tables, table)
	}
	data := multiDeploymentProgressData(ops, tables)
	data.State = state.Apply.Stopped
	WriteProgress(data)
}

func multiDeploymentProgressData(ops []ProgressOperation, tables []TableProgress) ProgressData {
	return ProgressData{
		ApplyID:     "apply-multi-a1b2c3d4",
		Environment: "production",
		Caller:      "github:octocat@acme/shop#412",
		State:       state.Apply.Running,
		StartedAt:   previewTime.Add(-8 * time.Minute).Format(time.RFC3339),
		Operations:  ops,
		Tables:      tables,
	}
}
