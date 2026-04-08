# Review Gate

SchemaBot can require PR approval before `schemabot apply` proceeds. This prevents unapproved schema changes from being applied to any environment.

## Enabling

The review gate is off by default. Enable it with the CLI:

```bash
# Enable globally
schemabot settings require_review true

# Enable for a specific repo only
schemabot settings require_review:octocat/my-repo true

# Disable
schemabot settings require_review false
```

Repo-specific settings take precedence over the global setting.

## How It Works

When enabled, `schemabot apply` and `schemabot apply-confirm` check for PR approval before proceeding:

1. **CODEOWNERS check**: If the repo has a CODEOWNERS file (checked from the base branch), SchemaBot requires approval from a listed code owner. Team slugs (`@org/team`) are expanded to individual members via the GitHub Teams API.

2. **Fallback**: If no CODEOWNERS file exists, any non-self approval satisfies the gate.

3. **Self-approval blocked**: The PR author's own approval never counts, even if they're a code owner.

4. **Fail closed**: If the GitHub API fails during the review check, SchemaBot blocks the apply and posts an error comment. Admins can run schema changes directly via the CLI to bypass the review gate.

If the gate blocks, SchemaBot posts a comment listing the required reviewers and instructions.

**With CODEOWNERS** (template: `pkg/webhook/templates/review.go`):

```markdown
## Review Required

**Database**: `payments` | **Environment**: `staging`

*Requested by @alice at 2026-03-26 12:00:00 UTC*

Schema changes require approval from a code owner before applying.

**Code owners** (from CODEOWNERS):
- @bob
- @org/dba-team

### Next steps
1. Request a review from a code owner above
2. Once approved, run `schemabot apply -e staging` again
```

**Without CODEOWNERS**:

```markdown
## Review Required

**Database**: `payments` | **Environment**: `staging`

*Requested by @alice at 2026-03-26 12:00:00 UTC*

Schema changes require at least one approval before applying.

### Next steps
1. Request a review from a teammate
2. Once approved, run `schemabot apply -e staging` again
```

## CODEOWNERS

SchemaBot looks for CODEOWNERS in the standard GitHub locations on the base branch:

1. `.github/CODEOWNERS`
2. `CODEOWNERS`
3. `docs/CODEOWNERS`

The base branch is used (not the PR's head branch) to prevent a PR from relaxing its own approval requirements by modifying CODEOWNERS.

## Edge Cases

**Approval withdrawn during an active apply**: The review gate checks approval at the time of `schemabot apply` and `schemabot apply-confirm`. Once an apply is executing, there is no ongoing approval check. Withdrawing approval mid-apply does not stop it — interrupting a running schema change (e.g. Spirit copying rows) is more dangerous than letting it complete.

**PR force-pushed after approval**: GitHub automatically dismisses approvals when a PR is force-pushed. If `schemabot apply` was already run and a lock is held, `schemabot apply-confirm` will re-check the gate and block if the approval was dismissed.

**Reviewer leaves the team**: Approval is checked against team membership at the time of the gate check, not at the time the review was submitted. If a reviewer approved and was later removed from the CODEOWNERS team, their approval no longer counts.

**CODEOWNERS changes between apply and apply-confirm**: Both commands fetch CODEOWNERS fresh from the base branch. If CODEOWNERS is updated between the two commands (e.g. a team is removed), the apply-confirm check uses the new CODEOWNERS.

**Multiple databases in one PR**: The review gate runs per `schemabot apply` invocation. Each database's schema path is matched against CODEOWNERS patterns independently, so different databases can require approval from different owners (e.g. `schema/payments/` owned by `@payments-team`, `schema/orders/` owned by `@orders-team`). This follows standard CODEOWNERS path-matching semantics.

## Future Work

- **Server-side `default_reviewers`**: Configure required reviewers in the server config, independent of CODEOWNERS.
- **Unsafe change escalation**: Require additional approval for destructive operations (DROP TABLE, DROP COLUMN, etc.).
