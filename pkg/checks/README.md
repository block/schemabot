# Check Runs

GitHub Check Runs are status indicators attached to a PR's head commit. They appear in the "Checks" tab and can block merge via branch protection rules.

SchemaBot uses check runs to enforce the schema change workflow: changes must be applied to all environments before the PR can merge.

## What Is a Check Run

A GitHub Check Run has:
- **Name**: identifies what the check is for (e.g., `SchemaBot: staging/mysql/payments`)
- **Status**: `queued`, `in_progress`, or `completed`
- **Conclusion** (when completed): `success`, `failure`, `action_required`, `neutral`
- **Output**: title, summary, and optional detailed text

`action_required` is special — GitHub renders it as a blocking check that requires user action. This is how SchemaBot blocks merge until schema changes are applied.

## Check Run Naming

**Aggregate check** (one per PR):

```
SchemaBot
```

This is the only check that needs to be required in branch protection. It rolls up all per-database checks into a single conclusion.

**Per-database checks** (one per environment × database, informational):

```
SchemaBot: staging/mysql/payments
SchemaBot: production/mysql/payments
SchemaBot: staging/vitess/commerce
```

A PR touching one database gets 2 per-database checks (staging + production) plus 1 aggregate. A PR touching 3 databases gets 6 per-database checks plus 1 aggregate.

## Lifecycle

```
PR opened / commits pushed
  → auto-plan runs
  → check created: action_required (changes detected) or success (no changes)

schemabot apply -e staging
  → check updated: action_required (plan posted with lock)

schemabot apply-confirm -e staging
  → check updated: in_progress (apply running)

Apply completes
  → check updated: success (applied) or failure (error)

schemabot unlock (or merge PR)
  → lock released, check updated: neutral (cancelled)

PR closed (merged or unmerged)
  → lock released, check records deleted from storage
  ⚠️  Does not cancel in-flight applies — the apply will run to completion
```

## State Transitions

```
                    ┌─────────────┐
    auto-plan ───→  │   success   │ (no changes)
                    └─────────────┘

                    ┌─────────────────┐
    auto-plan ───→  │ action_required │ (changes detected)
                    └────────┬────────┘
                             │
                    apply-confirm
                             │
                    ┌────────▼────────┐
                    │   in_progress   │
                    └────────┬────────┘
                             │
                     ┌───────┴───────┐
                     │               │
              ┌──────▼──────┐ ┌──────▼──────┐
              │   success   │ │   failure   │
              └─────────────┘ └─────────────┘

                    ┌─────────────┐
    unlock ──────→  │   neutral   │ (cancelled)
                    └─────────────┘
```

## Storage

Check records are persisted in the `checks` MySQL table with a unique key on `(repository, pull_request, environment, database_type, database_name)`. This allows:
- **Per-PR queries**: find all checks for a PR (cleanup, aggregate computation)
- **Per-database queries**: find all PRs touching a database (cross-PR blocking)
- **Check run lookup**: find a check by its GitHub check run ID (webhook handling)

## Staging-First Enforcement

Environments are applied in order. Each environment requires all prior environments to have a `success` check before allowing apply. The order comes from the `environments` list in `schemabot.yaml`:

```yaml
environments:
  - staging       # first (always allowed)
  - production    # second (requires staging to be success)
```

This generalizes to any number of environments:

```yaml
environments:
  - sandbox       # first
  - staging       # requires sandbox success
  - production    # requires both sandbox and staging success
```

When a user runs `schemabot apply -e <env>`, SchemaBot checks all prior environments' check run conclusions:

| Prior Environment Check | Apply Allowed? | Reason |
|------------------------|---------------|--------|
| `success` (no changes or applied) | ✅ Yes | Prior environment is clean |
| `action_required` (unapplied changes) | ❌ No | Apply prior environment first |
| `in_progress` (apply running) | ❌ No | Wait for prior environment to complete |
| `failure` (apply failed) | ❌ No | Fix and re-apply prior environment |
| No check exists | ✅ Yes | Prior environment has no changes |

The check is based on the **check run conclusion**, not the apply history. This means staging is satisfied when the check is `success` — whether that's from a completed apply or because there were never any changes to apply.

### Normal flow: staging then production

```
1. PR opened with schema changes
   → staging: action_required
   → production: action_required

2. schemabot apply -e staging → schemabot apply-confirm -e staging
   → staging: success
   → production: action_required

3. schemabot apply -e production
   → Allowed (staging check is success)
   → production: in_progress → success

4. PR can merge
```

### Production remediation (no staging changes)

A PR that only affects production — staging is already up to date.

```
1. PR opened
   → staging: success (no changes detected)
   → production: action_required

2. schemabot apply -e production
   → Allowed (staging check is already success)
```

### Staging apply failed

```
1. staging: action_required
2. schemabot apply -e staging → fails
   → staging: failure

3. schemabot apply -e production
   → Blocked: "Staging failed. Fix the issue and re-apply staging."

4. User fixes the issue, re-applies staging
   → staging: success

5. schemabot apply -e production
   → Allowed
```

### Multiple databases, mixed staging status

PR touches `payments` (staging applied) and `orders` (staging not applied). Each database is checked independently.

```
schemabot apply -e production -d payments
  → Allowed (payments staging check is success)

schemabot apply -e production -d orders
  → Blocked (orders staging check is action_required)
```

### New commits pushed after staging apply

```
1. staging applied → staging: success
2. User pushes new commits (synchronize)
3. Auto-plan re-runs:
   - If no new changes: staging check stays success → production allowed
   - If new changes detected: staging check goes to action_required
     → production blocked until re-applied
```

### Database with no staging environment

Some databases only have production in `schemabot.yaml`:

```yaml
database: legacy_db
type: mysql
environments:
  - production
```

No staging check is created. Production apply is allowed directly.

### Rollback (emergency)

`schemabot rollback <apply-id>` is NOT gated by staging-first. Rollbacks are emergency operations that need to execute immediately regardless of staging state.

### Staging in progress

```
1. schemabot apply-confirm -e staging (running)
   → staging: in_progress

2. schemabot apply -e production
   → Blocked: "Staging is currently in progress. Wait for it to complete."

3. Staging completes → staging: success

4. schemabot apply -e production
   → Allowed
```

### Implementation

The check runs in `handleApplyCommand` before generating the plan:

```go
if environment == "production" {
    stagingCheck := lookupStagingCheck(repo, pr, database)

    if stagingCheck == nil {
        // No staging environment configured — allow
    } else if stagingCheck.Conclusion == "success" {
        // Staging clean — allow
    } else if stagingCheck.Status == "in_progress" {
        // Staging running — block with "wait" message
    } else {
        // Staging has pending/failed changes — block
    }
}
```

Uses the stored check records (`Checks().Get()`) rather than querying the GitHub API, so it's fast and doesn't count against rate limits.

**CLI is break-glass by design.** Environment ordering is only enforced via PR comment commands. The `schemabot` CLI allows applying to any environment directly — this is intentional for emergency remediation, debugging, and operators who need to bypass the safety gate.

Atlantis does NOT enforce environment ordering — each workspace is independent. Staging-first is a SchemaBot-specific safety feature because schema changes are irreversible operations on live data where staging validation catches lock contention, slow copies, and compatibility issues before production.

## Stale Check Cleanup

When new commits are pushed (`synchronize`), SchemaBot re-discovers which databases are affected. Checks for databases no longer in the PR are updated to `success` so they don't block merge.

## PR Close Cleanup

When a PR is closed (merged or not), all locks held by the PR are released and all check records are deleted from storage.

## Aggregate Check

The `SchemaBot` aggregate check rolls up all per-database checks into a single conclusion. This solves the monorepo problem: you can't require dynamic per-database check names in branch protection because different PRs produce different names, and new databases would require branch protection updates.

### Aggregate Logic

Priority order (first match wins):

1. ANY per-database check `in_progress` → aggregate status `in_progress`
2. ANY per-database check `failure` → aggregate `failure`
3. ANY per-database check `action_required` → aggregate `action_required`
4. ALL per-database checks `success` → aggregate `success`

PRs that don't touch schema files get no aggregate check — they aren't blocked.

### Branch Protection Setup

Require the `SchemaBot` status check in branch protection. This is a one-time setup that never changes.

**Via the GitHub UI** (after SchemaBot has run on at least one PR):

1. Go to **Settings → Branches → Branch protection rules**
2. Edit (or add) a rule for your default branch
3. Enable **Require status checks to pass before merging**
4. Search for `SchemaBot` in the status check dropdown and select it

**Via the GitHub API** (works before SchemaBot has ever run):

```bash
gh api repos/{owner}/{repo}/branches/{branch}/protection \
  --method PUT \
  --input - <<'EOF'
{
  "required_status_checks": {
    "strict": false,
    "contexts": ["SchemaBot"]
  },
  "enforce_admins": null,
  "required_pull_request_reviews": null,
  "restrictions": null
}
EOF
```

### SHA Tracking

Check runs are tied to a specific commit SHA. When new commits are pushed:

- Per-database checks for removed databases are **re-created** on the new SHA as `success`
- The aggregate check is **re-created** on the new SHA (not updated on the old one)
- Per-database checks for still-affected databases are re-created by the auto-plan

This ensures all checks are visible on the current HEAD commit in the PR.
