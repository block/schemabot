package webhook

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/block/schemabot/pkg/tern"
)

// targetComparisonBudget bounds the time one review gate evaluation spends
// comparing schema files on the database's target, across every file and
// every approval it checks. Each comparison has its own timeout in the API
// service; the budget keeps a slow or unreachable target from multiplying
// that timeout by the number of files and approvals. Once it is spent, each
// remaining file counts as changed.
const targetComparisonBudget = 90 * time.Second

// errTargetComparisonBudgetSpent marks a schema file left uncompared because
// the evaluation spent its target comparison budget.
var errTargetComparisonBudgetSpent = errors.New("target comparison budget spent")

// targetStatementComparer compares schema files on the environment's primary
// target for one review gate evaluation. It runs the comparisons one at a
// time against a single deadline, and remembers each answer by the four
// files' content, so a file two approvals share is compared once.
type targetStatementComparer struct {
	h           *Handler
	repo        string
	pr          int
	database    string
	environment string
	budget      time.Duration

	mu       sync.Mutex
	deadline time.Time // set by the first comparison
	answers  map[[4]string]targetComparison
}

// targetComparison is the target's answer for one set of four files.
type targetComparison struct {
	same   bool
	reason string
}

func (h *Handler) newTargetStatementComparer(repo string, pr int, database, environment string) *targetStatementComparer {
	return &targetStatementComparer{
		h: h, repo: repo, pr: pr, database: database, environment: environment,
		budget:  targetComparisonBudget,
		answers: make(map[[4]string]targetComparison),
	}
}

// compare is a ghclient.SchemaFileChangeComparer. An error proves nothing,
// so the file counts as changed; when ctx itself was cancelled, the caller
// reports the evaluation as cancelled instead.
func (c *targetStatementComparer) compare(ctx context.Context, approvedBase, approved, headBase, head string) (bool, string, error) {
	key := [4]string{approvedBase, approved, headBase, head}
	logAttrs := []any{"repo", c.repo, "pr", c.pr, "database", c.database, "environment", c.environment}

	// The gate checks approvals one at a time, so holding the lock across
	// the call costs nothing and keeps the deadline and answers consistent.
	c.mu.Lock()
	defer c.mu.Unlock()
	if answer, ok := c.answers[key]; ok {
		c.h.logger.Debug("review gate: reusing the target's answer for a schema file another approval already compared", logAttrs...)
		return answer.same, answer.reason, nil
	}
	if c.deadline.IsZero() {
		c.deadline = time.Now().Add(c.budget)
	}
	if !time.Now().Before(c.deadline) {
		c.h.logger.Warn("review gate: the target comparison budget is spent, so this schema file counts as changed",
			append(logAttrs, "budget", c.budget)...)
		return false, "", fmt.Errorf("compare schema file for database %q environment %q: %w", c.database, c.environment, errTargetComparisonBudgetSpent)
	}

	compareCtx, cancel := context.WithDeadline(ctx, c.deadline)
	defer cancel()
	same, reason, err := c.h.service.CompareSchemaFileChange(compareCtx, c.database, c.environment, approvedBase, approved, headBase, head)
	if err != nil {
		switch {
		case ctx.Err() != nil:
			c.h.logger.Info("review gate: target schema file comparison stopped because the evaluation was cancelled",
				append(logAttrs, "error", err)...)
		case errors.Is(err, tern.ErrCompareSchemaFileChangeUnsupported):
			c.h.logger.Info("review gate: the data plane cannot compare schema files, so this schema file counts as changed",
				append(logAttrs, "error", err)...)
		default:
			c.h.logger.Warn("review gate: the target could not compare a schema file, so it counts as changed",
				append(logAttrs, "error", err)...)
		}
		return false, "", err
	}
	c.answers[key] = targetComparison{same: same, reason: reason}
	return same, reason, nil
}
