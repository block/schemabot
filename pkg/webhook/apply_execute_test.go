package webhook

import (
	"errors"
	"fmt"
	"testing"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/webhook/action"
	"github.com/stretchr/testify/assert"
)

// An apply the database type refused for a requested feature is told which
// feature was refused and the command to re-issue without the option that asked
// for it, in the environment the refused command named, so the operator's next
// action is in the comment rather than left to be inferred from the refusal.
func TestApplyExecutionErrorMessage(t *testing.T) {
	t.Run("unsupported feature names the command to re-issue without the option", func(t *testing.T) {
		err := &api.UnsupportedFeatureError{Database: "orders", DatabaseType: storage.DatabaseTypePostgres, Feature: schema.FeatureDeferredCutover}
		assert.Equal(t,
			"database \"orders\": deferred cutover is not supported for database_type: postgres. "+
				"Run `schemabot apply -e production` again without `--defer-cutover`.",
			applyExecutionErrorMessage(action.Apply, "production", err))
	})

	t.Run("unsupported feature no command option requests has no remedy", func(t *testing.T) {
		err := &api.UnsupportedFeatureError{Database: "orders", DatabaseType: storage.DatabaseTypePostgres, Feature: schema.FeatureMultiTarget}
		msg := applyExecutionErrorMessage(action.Apply, "staging", err)
		assert.Equal(t, err.Error()+".", msg)
		assert.NotContains(t, msg, "again without")
	})

	t.Run("lock intent change remains actionable", func(t *testing.T) {
		assert.Contains(t, applyExecutionErrorMessage(action.Apply, "staging", fmt.Errorf("verify lock: %w", storage.ErrLockIntentChanged)), "review the latest plan")
	})

	// A refusal of one target's own plan names that target and the table, from
	// fields SchemaBot controls, so the engine's reason and the plan identifier
	// in the underlying error never reach the comment.
	t.Run("member plan refusal names the target", func(t *testing.T) {
		cause := errors.New("stored plan plan-7f3a contains a blocked change for table \"orders\": dial tcp 10.0.0.7:3306")
		blocked := fmt.Errorf("queue apply: %w", &api.MemberPlanRefusedError{
			MemberID: "eu/payments-002", Target: "payments-002", Refusal: api.MemberPlanBlocked, Table: "orders", Err: cause,
		})
		msg := applyExecutionErrorMessage(action.Apply, "production", blocked)
		assert.Equal(t, "Target `payments-002` has a change on table `orders` that its engine refuses to execute, so nothing was applied. Fix what that target's plan names as the reason, then run the command again.", msg)
		assert.NotContains(t, msg, "10.0.0.7")
		assert.NotContains(t, msg, "plan-7f3a")

		unsafe := fmt.Errorf("queue apply: %w", &api.MemberPlanRefusedError{
			MemberID: "eu/payments-002", Target: "payments-002", Refusal: api.MemberPlanUndisclosedUnsafe, Table: "legacy_orders", Err: cause,
		})
		msg = applyExecutionErrorMessage(action.Apply, "production", unsafe)
		assert.Equal(t, "Target `payments-002` has an unsafe change on table `legacy_orders` that the plan comment never showed, so nothing was applied. Run apply again for this environment: its comment shows each target's own plan, and `--allow-unsafe` can then consent to this change.", msg)
		assert.NotContains(t, msg, "10.0.0.7")
		assert.NotContains(t, msg, "plan-7f3a")

		vschema := &api.MemberPlanRefusedError{
			MemberID: "eu/payments-002", Target: "payments-002", Refusal: api.MemberPlanUndisclosedUnsafe, Namespace: "ns_0", Err: cause,
		}
		assert.Contains(t, applyExecutionErrorMessage(action.Apply, "production", vschema), "an unsafe change on the VSchema of namespace `ns_0` that the plan comment never showed")

		unknown := &api.MemberPlanRefusedError{
			MemberID: "eu/payments-002", Target: "payments-002", Refusal: api.MemberPlanRefusal(99), Table: "orders", Err: cause,
		}
		assert.Equal(t, "Failed to execute apply. See SchemaBot server logs for details.", applyExecutionErrorMessage(action.Apply, "production", unknown),
			"a refusal kind with no line of its own never renders the error text")
	})

	t.Run("internal error remains sanitized", func(t *testing.T) {
		assert.Equal(t, "Failed to execute apply. See SchemaBot server logs for details.", applyExecutionErrorMessage(action.Apply, "staging", errors.New("secret DSN")))
	})
}

// A rollback-confirm whose lock stopped pinning the confirmed plan is told that
// nothing ran and to plan a fresh rollback, in SchemaBot's words rather than
// the storage error's. A rollback the database type cannot run is told which
// feature was refused and the rollback-confirm to re-issue without the option
// that asked for it, because retrying the same command would be refused the
// same way and the refusal left the pinned rollback in place for the re-issue.
// Every other dispatch failure stays in server logs behind fixed guidance that
// does not promise a retry will succeed.
func TestRollbackExecutionErrorMessage(t *testing.T) {
	t.Run("lock intent change coaches a fresh rollback", func(t *testing.T) {
		msg := rollbackExecutionErrorMessage("staging", fmt.Errorf("store apply and tasks: %w", storage.ErrLockIntentChanged))
		assert.Equal(t, msgRollbackLockIntentChanged, msg)
		assert.Contains(t, msg, "nothing was applied")
		assert.Contains(t, msg, "run the rollback command again")
		assert.NotContains(t, msg, storage.ErrLockIntentChanged.Error())
	})

	t.Run("unsupported feature names the refused feature and the rollback-confirm to re-issue", func(t *testing.T) {
		err := fmt.Errorf("execute apply: %w", &api.UnsupportedFeatureError{Database: "orders", DatabaseType: storage.DatabaseTypePostgres, Feature: schema.FeatureDeferredCutover})
		assert.Equal(t,
			"database \"orders\": deferred cutover is not supported for database_type: postgres. "+
				"Run `schemabot rollback-confirm -e staging` again without `--defer-cutover`. "+
				"The pending rollback stays pinned for it.",
			rollbackExecutionErrorMessage("staging", err))
	})

	t.Run("internal errors use fixed guidance without a retry promise", func(t *testing.T) {
		err := errors.New("dial tcp storage.internal:3306: connection refused")
		msg := rollbackExecutionErrorMessage("staging", err)
		assert.Equal(t, "Failed to execute rollback. See SchemaBot server logs for details.", msg)
		assert.NotContains(t, msg, err.Error())
		assert.NotContains(t, msg, "retry")
	})
}

func TestPendingRollbackApplyRefusal(t *testing.T) {
	t.Run("loaded plan offers confirmation in its environment", func(t *testing.T) {
		msg := pendingRollbackApplyRefusal("orders", &storage.Plan{Environment: "staging"})
		assert.Contains(t, msg, "schemabot rollback-confirm -e staging")
		assert.Contains(t, msg, "schemabot unlock")
	})

	t.Run("unavailable plan only offers unlock", func(t *testing.T) {
		msg := pendingRollbackApplyRefusal("orders", nil)
		assert.Contains(t, msg, "rollback plan that is unavailable")
		assert.Contains(t, msg, "schemabot unlock")
		assert.NotContains(t, msg, "rollback-confirm")
	})
}

func TestDDLMatchesStoredPlan(t *testing.T) {
	tests := []struct {
		name       string
		planResp   *apitypes.PlanResponse
		storedPlan *storage.Plan
		wantMatch  bool
	}{
		{
			name: "identical change matches",
			planResp: &apitypes.PlanResponse{
				Changes: []*apitypes.SchemaChangeResponse{
					{Namespace: "mydb", TableChanges: []*apitypes.TableChangeResponse{
						{TableName: "users", ChangeType: "alter", DDL: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)"},
					}},
				},
			},
			storedPlan: &storage.Plan{
				Namespaces: map[string]*storage.NamespacePlanData{
					"mydb": {Tables: []storage.TableChange{
						{Table: "users", Operation: "alter", DDL: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)"},
					}},
				},
			},
			wantMatch: true,
		},
		{
			name: "extra change does not match",
			planResp: &apitypes.PlanResponse{
				Changes: []*apitypes.SchemaChangeResponse{
					{Namespace: "mydb", TableChanges: []*apitypes.TableChangeResponse{
						{TableName: "users", ChangeType: "alter", DDL: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)"},
						{TableName: "users", ChangeType: "alter", DDL: "ALTER TABLE `users` DROP COLUMN `old_field`"},
					}},
				},
			},
			storedPlan: &storage.Plan{
				Namespaces: map[string]*storage.NamespacePlanData{
					"mydb": {Tables: []storage.TableChange{
						{Table: "users", Operation: "alter", DDL: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)"},
					}},
				},
			},
			wantMatch: false,
		},
		{
			name: "different DDL content does not match",
			planResp: &apitypes.PlanResponse{
				Changes: []*apitypes.SchemaChangeResponse{
					{Namespace: "mydb", TableChanges: []*apitypes.TableChangeResponse{
						{TableName: "users", ChangeType: "alter", DDL: "ALTER TABLE `users` ADD COLUMN `email` varchar(500)"},
					}},
				},
			},
			storedPlan: &storage.Plan{
				Namespaces: map[string]*storage.NamespacePlanData{
					"mydb": {Tables: []storage.TableChange{
						{Table: "users", Operation: "alter", DDL: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)"},
					}},
				},
			},
			wantMatch: false,
		},
		{
			name: "same changes in different order match",
			planResp: &apitypes.PlanResponse{
				Changes: []*apitypes.SchemaChangeResponse{
					{Namespace: "mydb", TableChanges: []*apitypes.TableChangeResponse{
						{TableName: "orders", ChangeType: "alter", DDL: "ALTER TABLE `orders` ADD INDEX `idx_status` (`status`)"},
						{TableName: "users", ChangeType: "alter", DDL: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)"},
					}},
				},
			},
			storedPlan: &storage.Plan{
				Namespaces: map[string]*storage.NamespacePlanData{
					"mydb": {Tables: []storage.TableChange{
						{Table: "users", Operation: "alter", DDL: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)"},
						{Table: "orders", Operation: "alter", DDL: "ALTER TABLE `orders` ADD INDEX `idx_status` (`status`)"},
					}},
				},
			},
			wantMatch: true,
		},
		{
			name: "same DDL under a different namespace does not match",
			planResp: &apitypes.PlanResponse{
				Changes: []*apitypes.SchemaChangeResponse{
					{Namespace: "keyspace_b", TableChanges: []*apitypes.TableChangeResponse{
						{TableName: "accounts", ChangeType: "create", DDL: "CREATE TABLE `accounts` (`id` bigint)"},
					}},
				},
			},
			storedPlan: &storage.Plan{
				Namespaces: map[string]*storage.NamespacePlanData{
					"keyspace_a": {Tables: []storage.TableChange{
						{Table: "accounts", Operation: "create", DDL: "CREATE TABLE `accounts` (`id` bigint)"},
					}},
				},
			},
			wantMatch: false,
		},
		{
			name: "same DDL against a different table does not match",
			planResp: &apitypes.PlanResponse{
				Changes: []*apitypes.SchemaChangeResponse{
					{Namespace: "mydb", TableChanges: []*apitypes.TableChangeResponse{
						{TableName: "customers", ChangeType: "alter", DDL: "ALTER TABLE `t` ADD COLUMN `email` varchar(255)"},
					}},
				},
			},
			storedPlan: &storage.Plan{
				Namespaces: map[string]*storage.NamespacePlanData{
					"mydb": {Tables: []storage.TableChange{
						{Table: "users", Operation: "alter", DDL: "ALTER TABLE `t` ADD COLUMN `email` varchar(255)"},
					}},
				},
			},
			wantMatch: false,
		},
		{
			name: "same DDL with a different operation does not match",
			planResp: &apitypes.PlanResponse{
				Changes: []*apitypes.SchemaChangeResponse{
					{Namespace: "mydb", TableChanges: []*apitypes.TableChangeResponse{
						{TableName: "events", ChangeType: "drop", DDL: "-- events"},
					}},
				},
			},
			storedPlan: &storage.Plan{
				Namespaces: map[string]*storage.NamespacePlanData{
					"mydb": {Tables: []storage.TableChange{
						{Table: "events", Operation: "create", DDL: "-- events"},
					}},
				},
			},
			wantMatch: false,
		},
		{
			name: "operation comparison is case-insensitive",
			planResp: &apitypes.PlanResponse{
				Changes: []*apitypes.SchemaChangeResponse{
					{Namespace: "mydb", TableChanges: []*apitypes.TableChangeResponse{
						{TableName: "users", ChangeType: "ALTER", DDL: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)"},
					}},
				},
			},
			storedPlan: &storage.Plan{
				Namespaces: map[string]*storage.NamespacePlanData{
					"mydb": {Tables: []storage.TableChange{
						{Table: "users", Operation: "alter", DDL: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)"},
					}},
				},
			},
			wantMatch: true,
		},
		{
			name: "empty response namespace matches default-normalized stored namespace",
			planResp: &apitypes.PlanResponse{
				Changes: []*apitypes.SchemaChangeResponse{
					{Namespace: "", TableChanges: []*apitypes.TableChangeResponse{
						{TableName: "users", ChangeType: "alter", DDL: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)"},
					}},
				},
			},
			storedPlan: &storage.Plan{
				Namespaces: map[string]*storage.NamespacePlanData{
					"default": {Tables: []storage.TableChange{
						{Table: "users", Operation: "alter", DDL: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)"},
					}},
				},
			},
			wantMatch: true,
		},
		{
			name: "empty plans match",
			planResp: &apitypes.PlanResponse{
				Changes: []*apitypes.SchemaChangeResponse{},
			},
			storedPlan: &storage.Plan{
				Namespaces: map[string]*storage.NamespacePlanData{},
			},
			wantMatch: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.wantMatch, ddlMatchesStoredPlan(tt.planResp, tt.storedPlan))
		})
	}
}

// The consent recorded on a pending confirmation only speaks for the apply the
// operator was actually shown. The lock carries no environment, so a disclosure
// earned confirming one environment must not disarm the copy gate in another,
// and a confirmation whose plan cannot be loaded must count as no disclosure at
// all rather than as consent.
func TestDisclosureDescribesThisApply(t *testing.T) {
	tests := []struct {
		name        string
		lock        *storage.Lock
		plan        *storage.Plan
		environment string
		want        bool
	}{
		{
			name:        "disclosure shown for this environment is consent",
			lock:        &storage.Lock{DisclosedCopyDiscard: true, PendingPlanID: "plan-1"},
			plan:        &storage.Plan{PlanIdentifier: "plan-1", Environment: "staging"},
			environment: "staging",
			want:        true,
		},
		{
			name:        "disclosure shown for another environment is not consent",
			lock:        &storage.Lock{DisclosedCopyDiscard: true, PendingPlanID: "plan-1"},
			plan:        &storage.Plan{PlanIdentifier: "plan-1", Environment: "staging"},
			environment: "production",
			want:        false,
		},
		{
			name:        "no disclosure is not consent",
			lock:        &storage.Lock{DisclosedCopyDiscard: false, PendingPlanID: "plan-1"},
			plan:        &storage.Plan{PlanIdentifier: "plan-1", Environment: "staging"},
			environment: "staging",
			want:        false,
		},
		{
			name:        "a disclosure whose plan did not load is not consent",
			lock:        &storage.Lock{DisclosedCopyDiscard: true, PendingPlanID: "plan-1"},
			plan:        nil,
			environment: "staging",
			want:        false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, disclosureDescribesThisApply(tt.lock, tt.plan, tt.environment))
		})
	}
}

// The drift disclosure names what moved, per table, so the operator can see it
// without diffing two comments themselves. It reports both directions — a
// change this plan added, and one the plan behind the apply had that this one
// does not — since either is a reason to look again before confirming.
func TestPlanDriftCause(t *testing.T) {
	planResp := &apitypes.PlanResponse{
		Changes: []*apitypes.SchemaChangeResponse{
			{Namespace: "mydb", TableChanges: []*apitypes.TableChangeResponse{
				{TableName: "users", ChangeType: "alter", DDL: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)"},
				{TableName: "products", ChangeType: "alter", DDL: "ALTER TABLE `products` ADD INDEX `idx_sku` (`sku`)"},
			}},
		},
	}
	storedPlan := &storage.Plan{
		Namespaces: map[string]*storage.NamespacePlanData{
			"mydb": {Tables: []storage.TableChange{
				{Table: "users", Operation: "alter", DDL: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)"},
				{Table: "shipments", Operation: "create", DDL: "CREATE TABLE `shipments` (`id` bigint)"},
			}},
		},
	}

	cause := planDriftCause(planResp, storedPlan)

	assert.Equal(t, "Schema changes differ from the plan this apply was started from", cause.Heading)
	assert.Equal(t, []string{
		"`products` (alter) is new",
		"`shipments` (create) is no longer planned",
	}, cause.Entries, "the unchanged `users` alter is not drift and is not listed")
}

// A rollout's cause says, per target, what moved in its plan. A table change is
// named as on a single target; one changed target is named in the heading, so
// its entries leave the name off, while entries spanning several targets each
// start with theirs. A plan whose statements are the same but which differs in
// how they run, or a target that gained or lost its only work, still gets one
// entry, so no changed target goes unnamed.
func TestTargetPlanDriftEntries(t *testing.T) {
	alter := func(ddl string) *storage.Plan {
		return &storage.Plan{Namespaces: map[string]*storage.NamespacePlanData{
			"mydb": {Tables: []storage.TableChange{{Table: "users", Operation: "alter", DDL: ddl}}},
		}}
	}
	addEmail := "ALTER TABLE `users` ADD COLUMN `email` varchar(255)"
	modifyEmail := "ALTER TABLE `users` MODIFY COLUMN `email` varchar(255)"
	direct := alter(addEmail)
	direct.Namespaces["mydb"].Tables[0].ExecutionMode = "direct"

	assert.Equal(t, []string{"`users` (alter) now runs a different statement"},
		targetPlanDriftEntries(alter(addEmail), alter(modifyEmail), ""))
	assert.Equal(t, []string{"Target `us`: `users` (alter) now runs a different statement"},
		targetPlanDriftEntries(alter(addEmail), alter(modifyEmail), "us"))
	assert.Equal(t, []string{"Target `us`: `users` (alter) is new"},
		targetPlanDriftEntries(nil, alter(addEmail), "us"))
	assert.Equal(t, []string{"How its statements run changed"},
		targetPlanDriftEntries(alter(addEmail), direct, ""))
	assert.Equal(t, []string{"Target `us`: how its statements run changed"},
		targetPlanDriftEntries(alter(addEmail), direct, "us"))
	assert.Empty(t, targetPlanDriftEntries(alter(addEmail), alter(addEmail), "us"), "an unchanged plan has no entry")
	assert.Empty(t, targetPlanDriftEntries(nil, &storage.Plan{}, "us"), "a target with nothing to do then or now has no entry")
}

// A re-plan that differs in dozens of changes has already made its point, and
// listing every one buries the statements the operator came to read.
func TestPlanDriftCauseCapsTheEntryList(t *testing.T) {
	var tableChanges []*apitypes.TableChangeResponse
	for i := range planDriftEntryCap + 3 {
		tableChanges = append(tableChanges, &apitypes.TableChangeResponse{
			TableName:  fmt.Sprintf("t%02d", i),
			ChangeType: "create",
			DDL:        fmt.Sprintf("CREATE TABLE `t%02d` (`id` bigint)", i),
		})
	}

	cause := planDriftCause(
		&apitypes.PlanResponse{Changes: []*apitypes.SchemaChangeResponse{{Namespace: "mydb", TableChanges: tableChanges}}},
		&storage.Plan{Namespaces: map[string]*storage.NamespacePlanData{"mydb": {}}},
	)

	assert.Len(t, cause.Entries, planDriftEntryCap+1)
	assert.Equal(t, "and 3 more changes", cause.Entries[planDriftEntryCap])
}

// The likeliest drift is an amended statement on a table both plans carry: a
// commit lands between the plan and the apply that changes the same ALTER. The
// entry names the table and the operation, so reporting that as an addition and
// a removal would assert the same change is both present and absent, with the
// statement that tells them apart left out by design.
func TestPlanDriftCauseReportsAnAmendedStatementOnce(t *testing.T) {
	cause := planDriftCause(
		&apitypes.PlanResponse{Changes: []*apitypes.SchemaChangeResponse{
			{Namespace: "mydb", TableChanges: []*apitypes.TableChangeResponse{
				{TableName: "orders", ChangeType: "alter", DDL: "ALTER TABLE `orders` ADD COLUMN `notes` varchar(500)"},
			}},
		}},
		&storage.Plan{Namespaces: map[string]*storage.NamespacePlanData{
			"mydb": {Tables: []storage.TableChange{
				{Table: "orders", Operation: "alter", DDL: "ALTER TABLE `orders` ADD COLUMN `notes` varchar(255)"},
			}},
		}},
	)

	assert.Equal(t, []string{
		"`orders` (alter) now runs a different statement",
	}, cause.Entries)
}

// Two keyspaces can carry the same table under the same operation, so an entry
// that named the table alone would report one keyspace's change as another's
// removal. The namespace appears only when the drift spans more than one.
func TestPlanDriftCauseNamesTheNamespaceOnlyWhenItDisambiguates(t *testing.T) {
	sameTableTwoKeyspaces := planDriftCause(
		&apitypes.PlanResponse{Changes: []*apitypes.SchemaChangeResponse{
			{Namespace: "shard_a", TableChanges: []*apitypes.TableChangeResponse{
				{TableName: "orders", ChangeType: "create", DDL: "CREATE TABLE `orders` (`id` bigint)"},
			}},
		}},
		&storage.Plan{Namespaces: map[string]*storage.NamespacePlanData{
			"shard_b": {Tables: []storage.TableChange{
				{Table: "orders", Operation: "create", DDL: "CREATE TABLE `orders` (`id` bigint)"},
			}},
		}},
	)
	assert.Equal(t, []string{
		"`orders` in `shard_a` (create) is new",
		"`orders` in `shard_b` (create) is no longer planned",
	}, sameTableTwoKeyspaces.Entries)

	oneKeyspace := planDriftCause(
		&apitypes.PlanResponse{Changes: []*apitypes.SchemaChangeResponse{
			{Namespace: "mydb", TableChanges: []*apitypes.TableChangeResponse{
				{TableName: "orders", ChangeType: "create", DDL: "CREATE TABLE `orders` (`id` bigint)"},
			}},
		}},
		&storage.Plan{Namespaces: map[string]*storage.NamespacePlanData{"mydb": {}}},
	)
	assert.Equal(t, []string{
		"`orders` (create) is new",
	}, oneKeyspace.Entries)
}

// A re-plan can route a statement to direct execution that the plan behind the
// apply's comment ran through the engine, with the same DDL. Only those
// statements are newly direct: one already disclosed as direct, or one that
// now runs through the engine instead, was not hidden from the operator.
func TestNewlyDirectChanges(t *testing.T) {
	const swap = "ALTER TABLE `users` DROP PRIMARY KEY, ADD PRIMARY KEY (`id`, `tenant_id`)"
	const addColumn = "ALTER TABLE `orders` ADD COLUMN `notes` text"
	replan := func(usersMode, ordersMode string) *apitypes.PlanResponse {
		return &apitypes.PlanResponse{Changes: []*apitypes.SchemaChangeResponse{
			{Namespace: "mydb", TableChanges: []*apitypes.TableChangeResponse{
				{TableName: "users", ChangeType: "alter", DDL: swap, ExecutionMode: usersMode},
				{TableName: "orders", ChangeType: "alter", DDL: addColumn, ExecutionMode: ordersMode},
			}},
		}}
	}
	stored := func(usersMode string) *storage.Plan {
		return &storage.Plan{Namespaces: map[string]*storage.NamespacePlanData{
			"mydb": {Tables: []storage.TableChange{
				{Table: "users", Operation: "alter", DDL: swap, ExecutionMode: usersMode},
				{Table: "orders", Operation: "alter", DDL: addColumn},
			}},
		}}
	}

	t.Run("statement the plan ran through the engine is newly direct", func(t *testing.T) {
		assert.Equal(t, []directChangeIdentity{{namespace: "mydb", table: "users", ddl: swap}},
			newlyDirectChanges(replan("direct", ""), stored("")))
	})
	t.Run("statement the plan already disclosed as direct is not", func(t *testing.T) {
		assert.Empty(t, newlyDirectChanges(replan("direct", ""), stored("direct")))
	})
	t.Run("statement that stops being direct is not", func(t *testing.T) {
		assert.Empty(t, newlyDirectChanges(replan("", ""), stored("direct")))
	})
	t.Run("every newly direct statement is returned in table order", func(t *testing.T) {
		assert.Equal(t, []directChangeIdentity{
			{namespace: "mydb", table: "orders", ddl: addColumn},
			{namespace: "mydb", table: "users", ddl: swap},
		}, newlyDirectChanges(replan("direct", "direct"), stored("")))
	})
	t.Run("a shard newly direct is newly direct even when another shard was disclosed", func(t *testing.T) {
		planResp := &apitypes.PlanResponse{Shards: []*apitypes.ShardPlanResponse{
			{Namespace: "ks", Shard: "-80", Changes: []*apitypes.TableChangeResponse{{TableName: "users", DDL: swap, ExecutionMode: "direct"}}},
			{Namespace: "ks", Shard: "80-", Changes: []*apitypes.TableChangeResponse{{TableName: "users", DDL: swap, ExecutionMode: "direct"}}},
		}}
		storedPlan := &storage.Plan{Shards: []storage.ShardPlan{
			{Namespace: "ks", Shard: "-80", Changes: []storage.TableChange{{Table: "users", DDL: swap, ExecutionMode: "direct"}}},
			{Namespace: "ks", Shard: "80-", Changes: []storage.TableChange{{Table: "users", DDL: swap}}},
		}}
		assert.Equal(t, []directChangeIdentity{{namespace: "ks", shard: "80-", table: "users", ddl: swap}},
			newlyDirectChanges(planResp, storedPlan))
	})
	t.Run("a sharded namespace is judged by its shard rows, not its namespace summary", func(t *testing.T) {
		planResp := &apitypes.PlanResponse{
			Changes: []*apitypes.SchemaChangeResponse{
				{Namespace: "ks", TableChanges: []*apitypes.TableChangeResponse{{TableName: "users", DDL: swap, ExecutionMode: "direct"}}},
			},
			Shards: []*apitypes.ShardPlanResponse{
				{Namespace: "ks", Shard: "-80", Changes: []*apitypes.TableChangeResponse{{TableName: "users", DDL: swap, ExecutionMode: "direct"}}},
			},
		}
		storedPlan := &storage.Plan{
			Namespaces: map[string]*storage.NamespacePlanData{
				"ks": {Tables: []storage.TableChange{{Table: "users", DDL: swap, ExecutionMode: "direct"}}},
			},
			Shards: []storage.ShardPlan{
				{Namespace: "ks", Shard: "-80", Changes: []storage.TableChange{{Table: "users", DDL: swap}}},
			},
		}
		assert.Equal(t, []directChangeIdentity{{namespace: "ks", shard: "-80", table: "users", ddl: swap}},
			newlyDirectChanges(planResp, storedPlan),
			"one entry for the shard, and the namespace summary neither adds one nor counts as disclosure")
	})
	t.Run("a namespace summary the disclosed plan rendered per shard does not disclose an unsharded re-plan", func(t *testing.T) {
		planResp := &apitypes.PlanResponse{Changes: []*apitypes.SchemaChangeResponse{
			{Namespace: "ks", TableChanges: []*apitypes.TableChangeResponse{{TableName: "users", DDL: swap, ExecutionMode: "direct"}}},
		}}
		storedPlan := &storage.Plan{
			Namespaces: map[string]*storage.NamespacePlanData{
				"ks": {Tables: []storage.TableChange{{Table: "users", DDL: swap, ExecutionMode: "direct"}}},
			},
			Shards: []storage.ShardPlan{
				{Namespace: "ks", Shard: "-80", Changes: []storage.TableChange{{Table: "users", DDL: swap}}},
			},
		}
		assert.Equal(t, []directChangeIdentity{{namespace: "ks", table: "users", ddl: swap}},
			newlyDirectChanges(planResp, storedPlan))
	})
	t.Run("statement differing only in surrounding whitespace is the same statement", func(t *testing.T) {
		storedPlan := &storage.Plan{Namespaces: map[string]*storage.NamespacePlanData{
			"mydb": {Tables: []storage.TableChange{
				{Table: "users", DDL: "\n" + swap + "\n", ExecutionMode: "direct"},
				{Table: "orders", DDL: addColumn},
			}},
		}}
		assert.Empty(t, newlyDirectChanges(replan("direct", ""), storedPlan))
	})
}

func TestNewlyDirectCauseNamesEachTable(t *testing.T) {
	cause := newlyDirectCause([]directChangeIdentity{
		{namespace: "mydb", table: "users", ddl: "ALTER TABLE `users` DROP PRIMARY KEY"},
		{namespace: "ks", shard: "-80", table: "events", ddl: "ALTER TABLE `events` DROP PRIMARY KEY"},
		{namespace: "ks", shard: "80-", table: "events", ddl: "ALTER TABLE `events` DROP PRIMARY KEY"},
		{namespace: "ks", shard: "80-", table: "orders", ddl: "ALTER TABLE `orders` DROP PRIMARY KEY"},
	})
	assert.Equal(t, "Changes run differently from the plan this apply was started from", cause.Heading)
	assert.Equal(t, []string{
		"`users` now runs as direct execution",
		"`events` (shards `-80`, `80-`) now runs as direct execution",
		"`orders` (shard `80-`) now runs as direct execution",
	}, cause.Entries)
}
