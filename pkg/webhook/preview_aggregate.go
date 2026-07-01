package webhook

import "github.com/block/schemabot/pkg/storage"

// PreviewAggregateSummary renders a representative aggregate-check Details
// summary for TEMPLATES.md: the leader's own per-database checks with their
// change summaries and statuses, plus a folded Tenant deployments section. The
// mix of running, applied, and pending rows produces a blocking conclusion so
// both tables and a non-passing title are exercised.
func PreviewAggregateSummary() string {
	checks := []*storage.Check{
		{
			DatabaseType:  "vitess",
			DatabaseName:  "commerce",
			HasChanges:    true,
			Status:        checkStatusInProgress,
			ChangeSummary: "2 creates, 1 alter · 2 vschema updates",
		},
		{
			DatabaseType:  "mysql",
			DatabaseName:  "orders",
			HasChanges:    true,
			Status:        checkStatusCompleted,
			Conclusion:    checkConclusionSuccess,
			ChangeSummary: "1 alter",
		},
		{
			DatabaseType: aggregateSentinel,
			DatabaseName: "tenant-b",
			HasChanges:   true,
			Status:       checkStatusCompleted,
			Conclusion:   checkConclusionSuccess,
		},
		{
			DatabaseType: aggregateSentinel,
			DatabaseName: "tenant-c",
			HasChanges:   true,
			Status:       checkStatusInProgress,
		},
	}

	conclusion, _ := computeAggregate(checks)
	title, summary := aggregateSummary(checks, conclusion)
	return title + "\n\n" + summary
}
