package templates

import (
	"fmt"
	"strings"
	"time"

	"github.com/block/schemabot/pkg/state"
)

func previewSeqPendingOutput() {
	fmt.Println("Sequential mode: Just started (all tables pending)")
	fmt.Println()

	data := ProgressData{
		State:     state.Apply.Pending,
		Engine:    "Spirit",
		ApplyID:   "apply-a1b2c3d4e5f6",
		StartedAt: previewTime.Add(-10 * time.Second).Format(time.RFC3339),
		Tables: []TableProgress{
			{TableName: "users", DDL: seqDDLs[0].ddl, Status: state.Apply.Pending},
			{TableName: "orders", DDL: seqDDLs[1].ddl, Status: state.Apply.Pending},
			{TableName: "products", DDL: seqDDLs[2].ddl, Status: state.Apply.Pending},
		},
	}
	WriteProgress(data)
}

func previewSeqFirstRunOutput() {
	fmt.Println("Sequential mode: First table running, others queued")
	fmt.Println()

	data := ProgressData{
		State:     state.Apply.Running,
		Engine:    "Spirit",
		ApplyID:   "apply-a1b2c3d4e5f6",
		StartedAt: previewTime.Add(-5 * time.Minute).Format(time.RFC3339),
		Tables: []TableProgress{
			{
				TableName:       "users",
				DDL:             seqDDLs[0].ddl,
				Status:          state.Apply.Running,
				RowsCopied:      875000,
				RowsTotal:       2500000,
				PercentComplete: 35,
				ETASeconds:      510, // 8m 30s
			},
			{TableName: "orders", DDL: seqDDLs[1].ddl, Status: state.Apply.Pending},
			{TableName: "products", DDL: seqDDLs[2].ddl, Status: state.Apply.Pending},
		},
	}
	WriteProgress(data)
}

func previewSeqSecondRunOutput() {
	fmt.Println("Sequential mode: First complete, second running")
	fmt.Println()

	data := ProgressData{
		State:     state.Apply.Running,
		Engine:    "Spirit",
		ApplyID:   "apply-a1b2c3d4e5f6",
		StartedAt: previewTime.Add(-12 * time.Minute).Format(time.RFC3339),
		Tables: []TableProgress{
			{TableName: "users", DDL: seqDDLs[0].ddl, Status: state.Apply.Completed},
			{
				TableName:       "orders",
				DDL:             seqDDLs[1].ddl,
				Status:          state.Apply.Running,
				RowsCopied:      3000000,
				RowsTotal:       5000000,
				PercentComplete: 60,
				ETASeconds:      735, // 12m 15s
			},
			{TableName: "products", DDL: seqDDLs[2].ddl, Status: state.Apply.Pending},
		},
	}
	WriteProgress(data)
}

func previewSeqThirdRunOutput() {
	fmt.Println("Sequential mode: First two complete, third running")
	fmt.Println()

	data := ProgressData{
		State:     state.Apply.Running,
		Engine:    "Spirit",
		ApplyID:   "apply-a1b2c3d4e5f6",
		StartedAt: previewTime.Add(-20 * time.Minute).Format(time.RFC3339),
		Tables: []TableProgress{
			{TableName: "users", DDL: seqDDLs[0].ddl, Status: state.Apply.Completed},
			{TableName: "orders", DDL: seqDDLs[1].ddl, Status: state.Apply.Completed},
			{
				TableName:       "products",
				DDL:             seqDDLs[2].ddl,
				Status:          state.Apply.Running,
				RowsCopied:      160000,
				RowsTotal:       200000,
				PercentComplete: 80,
				ETASeconds:      165, // 2m 45s
			},
		},
	}
	WriteProgress(data)
}

func previewSeqAllDoneOutput() {
	fmt.Println("Sequential mode: All tables completed successfully")
	fmt.Println()

	data := ProgressData{
		State:       state.Apply.Completed,
		Engine:      "Spirit",
		ApplyID:     "apply-a1b2c3d4e5f6",
		StartedAt:   previewTime.Add(-25 * time.Minute).Format(time.RFC3339),
		CompletedAt: previewTime.Add(-30 * time.Second).Format(time.RFC3339),
		Tables: []TableProgress{
			{TableName: "users", DDL: seqDDLs[0].ddl, Status: state.Apply.Completed},
			{TableName: "orders", DDL: seqDDLs[1].ddl, Status: state.Apply.Completed},
			{TableName: "products", DDL: seqDDLs[2].ddl, Status: state.Apply.Completed},
		},
	}
	WriteProgress(data)
	fmt.Println("✓ Apply complete!")
}

func previewSeqFirstFailOutput() {
	fmt.Println("Sequential mode: First table failed (others cancelled)")
	fmt.Println()

	data := ProgressData{
		State:        state.Apply.Failed,
		Engine:       "Spirit",
		ApplyID:      "apply-a1b2c3d4e5f6",
		StartedAt:    previewTime.Add(-8 * time.Minute).Format(time.RFC3339),
		CompletedAt:  previewTime.Add(-30 * time.Second).Format(time.RFC3339),
		ErrorMessage: "lock wait timeout exceeded; try restarting transaction",
		Tables: []TableProgress{
			{TableName: "users", DDL: seqDDLs[0].ddl, Status: state.Apply.Failed, PercentComplete: 65},
			{TableName: "orders", DDL: seqDDLs[1].ddl, Status: TaskCancelled},
			{TableName: "products", DDL: seqDDLs[2].ddl, Status: TaskCancelled},
		},
	}
	WriteProgress(data)
}

func previewSeqMidFailOutput() {
	fmt.Println("Sequential mode: Middle table failed")
	fmt.Println()

	data := ProgressData{
		State:        state.Apply.Failed,
		Engine:       "Spirit",
		ApplyID:      "apply-a1b2c3d4e5f6",
		StartedAt:    previewTime.Add(-18 * time.Minute).Format(time.RFC3339),
		CompletedAt:  previewTime.Add(-30 * time.Second).Format(time.RFC3339),
		ErrorMessage: "Lost connection to MySQL server during query (MySQL error 2013)",
		Tables: []TableProgress{
			{TableName: "users", DDL: seqDDLs[0].ddl, Status: state.Apply.Completed},
			{TableName: "orders", DDL: seqDDLs[1].ddl, Status: state.Apply.Failed, PercentComplete: 45},
			{TableName: "products", DDL: seqDDLs[2].ddl, Status: TaskCancelled},
		},
	}
	WriteProgress(data)
}

func previewSeqStoppedOutput() {
	fmt.Println("Sequential mode: User stopped mid-apply")
	fmt.Println()

	data := ProgressData{
		State:     state.Apply.Stopped,
		Engine:    "Spirit",
		ApplyID:   "apply-a1b2c3d4e5f6",
		StartedAt: previewTime.Add(-5 * time.Minute).Format(time.RFC3339),
		Tables: []TableProgress{
			{TableName: "users", DDL: seqDDLs[0].ddl, Status: state.Apply.Completed},
			{
				TableName:       "orders",
				DDL:             seqDDLs[1].ddl,
				Status:          state.Apply.Stopped,
				RowsCopied:      112045,
				RowsTotal:       266383,
				PercentComplete: 42,
			},
			{TableName: "products", DDL: seqDDLs[2].ddl, Status: state.Apply.Stopped, PercentComplete: 0},
		},
	}
	WriteProgress(data)

	fmt.Println("Use 'schemabot start' to resume from checkpoint.")
}

func previewSequentialAllOutput() {
	sections := []struct {
		name string
		fn   func()
	}{
		{"PENDING (all queued)", previewSeqPendingOutput},
		{"FIRST TABLE RUNNING", previewSeqFirstRunOutput},
		{"SECOND TABLE RUNNING", previewSeqSecondRunOutput},
		{"THIRD TABLE RUNNING", previewSeqThirdRunOutput},
		{"ALL COMPLETED", previewSeqAllDoneOutput},
		{"FIRST TABLE FAILED", previewSeqFirstFailOutput},
		{"MIDDLE TABLE FAILED", previewSeqMidFailOutput},
		{"STOPPED BY USER", previewSeqStoppedOutput},
	}

	for i, s := range sections {
		if i > 0 {
			fmt.Println()
		}
		fmt.Println("---", s.name, strings.Repeat("-", 50-len(s.name)))
		fmt.Println()
		s.fn()
	}
}

// =============================================================================
// Defer Cutover Previews (--defer-cutover / atomic mode)
// =============================================================================
