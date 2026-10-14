package templates

import (
	"fmt"
	"time"

	"github.com/block/schemabot/pkg/apitypes"
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
		{Deployment: "us-east", Target: "orders-us-east", State: state.ApplyOperation.Completed, CutoverPolicy: storage.CutoverPolicyRolling, OnFailure: storage.OnFailureHalt},
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
		{Deployment: "us-east", Target: "orders-us-east", State: state.ApplyOperation.Completed, CutoverPolicy: storage.CutoverPolicyRolling, OnFailure: storage.OnFailureHalt},
		{Deployment: "eu-west", Target: "orders-eu-west", State: state.ApplyOperation.Completed, CutoverPolicy: storage.CutoverPolicyRolling, OnFailure: storage.OnFailureHalt},
		{Deployment: "ap-south", Target: "orders-ap-south", State: state.ApplyOperation.Completed, CutoverPolicy: storage.CutoverPolicyRolling, OnFailure: storage.OnFailureHalt},
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
		{"SHARDED APPLY ON A KEYSPACE'S ONLY SHARD", previewCLISingleShardApplyCompleted},
		{"SHARDED APPLY COPYING ACROSS FOUR SHARDS", previewCLIShardedApplyRunning},
		{"SHARDED APPLY WITH A FAILED SHARD", previewCLIShardedApplyFailed},
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

// previewCLISingleShardApplyCompleted is a completed apply on a keyspace's
// only shard, finalized with no VSchema change to show: one change on one
// database, rendered without a section for the shard or the finalizer.
func previewCLISingleShardApplyCompleted() {
	ddl := "ALTER TABLE `orders` ADD COLUMN `source` varchar(32) DEFAULT NULL"
	WriteProgress(ProgressData{
		ApplyID:     "apply-shard-a1b2c3d4",
		Database:    "shop",
		Environment: "production",
		Caller:      "github:octocat@acme/shop#412",
		State:       state.Apply.Completed,
		StartedAt:   previewTime.Add(-1 * time.Minute).Format(time.RFC3339),
		CompletedAt: previewTime.Add(-54 * time.Second).Format(time.RFC3339),
		Sharded:     true,
		Operations: []ProgressOperation{
			{Deployment: "prod", OperationKey: "shop_001/-/orders", OperationKind: storage.ApplyOperationKindWork, ExternalOperationID: "1157", State: state.ApplyOperation.Completed},
			{Deployment: "prod", OperationKey: "shop_001/group_finalizer", OperationKind: storage.ApplyOperationKindGroupFinalizer, ExternalOperationID: "1158", State: state.ApplyOperation.Completed},
		},
		Tables: []TableProgress{
			{Deployment: "prod", Namespace: "shop_001", TableName: "orders", ChangeType: "alter", Dialect: schema.DialectMySQL, DDL: ddl, Status: state.Task.Completed, RowsCopied: 80000, RowsTotal: 80000, PercentComplete: 100},
		},
	})
}

// previewShardedVSchemaDiff is the VSchema diff of the sharded previews'
// keyspace: a vindex for the table the apply indexes.
const previewShardedVSchemaDiff = `   "tables": {
     "orders": {
+      "column_vindexes": [{"column": "customer_id", "name": "hash"}]
     }
   }`

// shardedPreviewProgress is a sharded apply changing `orders` across the four
// shards of shop_001, as the server reports it: the table rolled up across
// its shards, the operation rows for each shard and the keyspace's finalizer,
// and the finalizer's VSchema change, with its diff, in the display metadata.
func shardedPreviewProgress(applyState, finalizerState, vschemaStatus string, shards []ShardProgress, table TableProgress) ProgressData {
	keys := []string{"shop_001/-40/orders", "shop_001/40-80/orders", "shop_001/80-c0/orders", "shop_001/c0-/orders"}
	var ops []ProgressOperation
	for i, key := range keys {
		ops = append(ops, ProgressOperation{Deployment: "prod", OperationKey: key, OperationKind: storage.ApplyOperationKindWork, State: shards[i].Status})
	}
	ops = append(ops, ProgressOperation{Deployment: "prod", OperationKey: "shop_001/group_finalizer", OperationKind: storage.ApplyOperationKindGroupFinalizer, State: finalizerState})
	vschema, err := apitypes.EncodeVSchemaChanges([]apitypes.VSchemaChange{{Namespace: "shop_001", Status: vschemaStatus, Diff: previewShardedVSchemaDiff}})
	if err != nil {
		panic(err)
	}
	table.Deployment, table.Namespace, table.TableName, table.ChangeType, table.Dialect = "prod", "shop_001", "orders", "alter", schema.DialectMySQL
	table.DDL = "ALTER TABLE `orders` ADD INDEX `idx_created_at`(`created_at`)"
	table.Shards = shards
	return ProgressData{
		ApplyID:     "apply-shard-e5f6a7b8",
		Database:    "shop",
		Environment: "production",
		Caller:      "github:octocat@acme/shop#412",
		State:       applyState,
		StartedAt:   previewTime.Add(-4 * time.Minute).Format(time.RFC3339),
		Sharded:     true,
		Operations:  ops,
		Tables:      []TableProgress{table},
		Metadata:    map[string]string{apitypes.VSchemaChangesMetadataKey: vschema},
	}
}

// previewCLIShardedApplyRunning is a sharded apply whose first shard is
// copying while the other three wait for their wave: one table with its
// shards under it, its rows covering the one shard that has started.
func previewCLIShardedApplyRunning() {
	WriteProgress(shardedPreviewProgress(state.Apply.Running, state.ApplyOperation.Pending, "", []ShardProgress{
		{Shard: "-40", Status: state.Task.Running, RowsCopied: 914707, RowsTotal: 1466232, PercentComplete: 62, ETASeconds: 195},
		{Shard: "40-80", Status: state.Task.Pending},
		{Shard: "80-c0", Status: state.Task.Pending},
		{Shard: "c0-", Status: state.Task.Pending},
	}, TableProgress{Status: state.Task.Running, RowsCopied: 914707, RowsTotal: 1466232, PercentComplete: 62, ETASeconds: 195, EstimatedBytes: new(int64(23_400_000_000)), PlannedShards: 4}))
}

// previewCLIShardedApplyFailed is a sharded apply whose first shard failed,
// halting the rest: the error names the shard, and the table lists where it
// failed.
func previewCLIShardedApplyFailed() {
	data := shardedPreviewProgress(state.Apply.Failed, state.ApplyOperation.Cancelled, "cancelled", []ShardProgress{
		{Shard: "-40", Status: state.Task.Failed},
		{Shard: "40-80", Status: state.Task.Cancelled},
		{Shard: "80-c0", Status: state.Task.Cancelled},
		{Shard: "c0-", Status: state.Task.Cancelled},
	}, TableProgress{Status: state.Task.Failed})
	data.CompletedAt = previewTime.Add(-3 * time.Minute).Format(time.RFC3339)
	data.Operations[0].ErrorMessage = "resolve shard primary for -40: context deadline exceeded"
	data.ErrorMessage = firstOperationError(data.Operations)
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
