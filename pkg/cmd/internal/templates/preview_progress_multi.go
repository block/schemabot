package templates

import (
	"fmt"
	"time"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

func previewCLIMultiDeploymentApplyInProgress() {
	WriteProgress(multiDeploymentProgressData([]ProgressOperation{
		{Deployment: "us-east", Target: "orders-us-east", State: state.ApplyOperation.WaitingForCutover, CutoverPolicy: storage.CutoverPolicyBarrier, OnFailure: storage.OnFailureHalt},
		{Deployment: "eu-west", Target: "orders-eu-west", State: state.ApplyOperation.Running, CutoverPolicy: storage.CutoverPolicyBarrier, OnFailure: storage.OnFailureHalt},
		{Deployment: "ap-south", Target: "orders-ap-south", State: state.ApplyOperation.Pending, CutoverPolicy: storage.CutoverPolicyBarrier, OnFailure: storage.OnFailureHalt},
	}, []TableProgress{
		{Deployment: "us-east", TableName: "orders", ChangeType: "alter", DDL: "ALTER TABLE `orders` ADD COLUMN `source` varchar(32) DEFAULT NULL", Status: state.Task.WaitingForCutover, RowsCopied: 80000, RowsTotal: 80000, PercentComplete: 100},
		{Deployment: "eu-west", TableName: "orders", ChangeType: "alter", DDL: "ALTER TABLE `orders` ADD COLUMN `source` varchar(32) DEFAULT NULL", Status: state.Task.Running, RowsCopied: 42000, RowsTotal: 120000, PercentComplete: 35, ETASeconds: 240},
		{Deployment: "ap-south", TableName: "orders", ChangeType: "alter", DDL: "ALTER TABLE `orders` ADD COLUMN `source` varchar(32) DEFAULT NULL", Status: state.Task.Pending},
	}))
}

func previewCLIMultiDeploymentApplyFailed() {
	WriteProgress(multiDeploymentProgressData([]ProgressOperation{
		{Deployment: "us-east", Target: "orders-us-east", State: state.ApplyOperation.Completed, CutoverPolicy: storage.CutoverPolicyRolling, OnFailure: storage.OnFailureHalt},
		{Deployment: "eu-west", Target: "orders-eu-west", State: state.ApplyOperation.Failed, CutoverPolicy: storage.CutoverPolicyRolling, OnFailure: storage.OnFailureHalt, ErrorMessage: "duplicate key name 'idx_orders_source'"},
		{Deployment: "ap-south", Target: "orders-ap-south", State: state.ApplyOperation.Pending, CutoverPolicy: storage.CutoverPolicyRolling, OnFailure: storage.OnFailureHalt},
	}, []TableProgress{
		{Deployment: "us-east", TableName: "orders", ChangeType: "alter", DDL: "ALTER TABLE `orders` ADD COLUMN `source` varchar(32) DEFAULT NULL", Status: state.Task.Completed, RowsCopied: 80000, RowsTotal: 80000, PercentComplete: 100},
		{Deployment: "eu-west", TableName: "orders", ChangeType: "alter", DDL: "ALTER TABLE `orders` ADD INDEX `idx_orders_source` (`source`)", Status: state.Task.Failed, RowsCopied: 0, RowsTotal: 120000, PercentComplete: 0},
		{Deployment: "ap-south", TableName: "orders", ChangeType: "alter", DDL: "ALTER TABLE `orders` ADD COLUMN `source` varchar(32) DEFAULT NULL", Status: TaskCancelled},
	}))
}

func previewCLIMultiDeploymentApplyCompleted() {
	data := multiDeploymentProgressData([]ProgressOperation{
		{Deployment: "us-east", Target: "orders-us-east", State: state.ApplyOperation.Completed, CutoverPolicy: storage.CutoverPolicyRolling, OnFailure: storage.OnFailureHalt},
		{Deployment: "eu-west", Target: "orders-eu-west", State: state.ApplyOperation.Completed, CutoverPolicy: storage.CutoverPolicyRolling, OnFailure: storage.OnFailureHalt},
		{Deployment: "ap-south", Target: "orders-ap-south", State: state.ApplyOperation.Completed, CutoverPolicy: storage.CutoverPolicyRolling, OnFailure: storage.OnFailureHalt},
	}, []TableProgress{
		{Deployment: "us-east", TableName: "orders", ChangeType: "alter", DDL: "ALTER TABLE `orders` ADD COLUMN `source` varchar(32) DEFAULT NULL", Status: state.Task.Completed, RowsCopied: 80000, RowsTotal: 80000, PercentComplete: 100},
		{Deployment: "eu-west", TableName: "orders", ChangeType: "alter", DDL: "ALTER TABLE `orders` ADD COLUMN `source` varchar(32) DEFAULT NULL", Status: state.Task.Completed, RowsCopied: 120000, RowsTotal: 120000, PercentComplete: 100},
		{Deployment: "ap-south", TableName: "orders", ChangeType: "alter", DDL: "ALTER TABLE `orders` ADD COLUMN `source` varchar(32) DEFAULT NULL", Status: state.Task.Completed, RowsCopied: 60000, RowsTotal: 60000, PercentComplete: 100},
	})
	data.CompletedAt = previewTime.Add(-1 * time.Minute).Format(time.RFC3339)
	WriteProgress(data)
}

// previewUnsettledTaskOutput renders a rollout whose aggregate verdict has
// settled while one deployment is still copying. Under a halt-on-failure
// policy, eu-west failing settles the apply immediately, so us-east keeps
// driving with no parent left to say so: the apply row is not a row its driver
// touches, and a driver holding only an operation lease may not bump it. The
// table is reported at the state its driver last persisted, which is also what
// a row whose driver went away looks like until a reaper settles it.
func previewUnsettledTaskOutput() {
	data := multiDeploymentProgressData([]ProgressOperation{
		{Deployment: "us-east", Target: "orders-us-east", State: state.ApplyOperation.Running, CutoverPolicy: storage.CutoverPolicyRolling, OnFailure: storage.OnFailureHalt},
		{Deployment: "eu-west", Target: "orders-eu-west", State: state.ApplyOperation.Failed, CutoverPolicy: storage.CutoverPolicyRolling, OnFailure: storage.OnFailureHalt, ErrorMessage: "duplicate key name 'idx_orders_source'"},
		{Deployment: "ap-south", Target: "orders-ap-south", State: state.ApplyOperation.Pending, CutoverPolicy: storage.CutoverPolicyRolling, OnFailure: storage.OnFailureHalt},
	}, []TableProgress{
		{Deployment: "us-east", TableName: "orders", ChangeType: "alter", DDL: "ALTER TABLE `orders` ADD INDEX `idx_orders_source` (`source`)", Status: state.Task.Running, RowsCopied: 156342, RowsTotal: 397453, PercentComplete: 39},
		{Deployment: "eu-west", TableName: "orders", ChangeType: "alter", DDL: "ALTER TABLE `orders` ADD INDEX `idx_orders_source` (`source`)", Status: state.Task.Failed, RowsCopied: 0, RowsTotal: 120000, PercentComplete: 0},
		{Deployment: "ap-south", TableName: "orders", ChangeType: "alter", DDL: "ALTER TABLE `orders` ADD INDEX `idx_orders_source` (`source`)", Status: state.Task.Pending},
	})
	data.State = state.Apply.Failed
	WriteProgress(data)
}

func previewCLIMultiDeployAllOutput() {
	sections := []struct {
		name string
		fn   func()
	}{
		{"BARRIER ROLLOUT IN PROGRESS", previewCLIMultiDeploymentApplyInProgress},
		{"HALT ON FAILURE (ONE DEPLOYMENT FAILED)", previewCLIMultiDeploymentApplyFailed},
		{"HALT ON FAILURE WHILE A DEPLOYMENT IS STILL COPYING", previewUnsettledTaskOutput},
		{"ALL DEPLOYMENTS COMPLETED", previewCLIMultiDeploymentApplyCompleted},
	}
	for i, section := range sections {
		if i > 0 {
			fmt.Println()
		}
		fmt.Printf("--- %s ---\n\n", section.name)
		section.fn()
	}
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
