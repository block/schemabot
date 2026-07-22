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

func previewCLIMultiDeployAllOutput() {
	sections := []struct {
		name string
		fn   func()
	}{
		{"BARRIER ROLLOUT IN PROGRESS", previewCLIMultiDeploymentApplyInProgress},
		{"HALT ON FAILURE (ONE DEPLOYMENT FAILED)", previewCLIMultiDeploymentApplyFailed},
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
		Caller:      "octocat",
		State:       state.Apply.Running,
		StartedAt:   previewTime.Add(-8 * time.Minute).Format(time.RFC3339),
		Operations:  ops,
		Tables:      tables,
	}
}
