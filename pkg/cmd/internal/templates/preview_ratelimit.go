package templates

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/block/schemabot/pkg/apitypes"
)

// previewRateLimitRetryAfter is the wait a refused pull advertises at the
// default budgets, where a bucket refills faster than the second Retry-After
// can express and the advertised wait rounds up to it.
const previewRateLimitRetryAfter = time.Second

// previewPullRateLimitedCallerOutput shows a pull refused because the caller
// spent its own request budget: one client looping, told to slow down.
func previewPullRateLimitedCallerOutput() {
	WriteSchemaPullFailure(rateLimitedPullFailure(apitypes.PullRateLimitCallerReason))
}

// previewPullRateLimitedSharedOutput shows the same caller-lane refusal on a
// server that does not authenticate callers, where the budget is shared by
// every client. The message says so rather than blaming the operator in front
// of it, who may have made only one of the requests being counted.
func previewPullRateLimitedSharedOutput() {
	WriteSchemaPullFailure(rateLimitedPullFailure(apitypes.PullRateLimitSharedReason))
}

// previewPullRateLimitedTargetOutput shows a pull refused because the database
// it names is already absorbing every client's reads. The caller may be well
// inside its own budget, so the message names the other budget rather than
// blaming the caller's rate.
func previewPullRateLimitedTargetOutput() {
	WriteSchemaPullFailure(rateLimitedPullFailure(apitypes.PullRateLimitTargetReason))
}

// previewPullRateLimitedResponseOutput shows what a service caller reads off
// the wire, for clients that consume the API directly rather than through the
// CLI: the status, the Retry-After header, and the body that repeats the wait
// for clients that only read bodies.
func previewPullRateLimitedResponseOutput() {
	body := apitypes.NewRateLimitedResponse(apitypes.PullRateLimitTargetReason, previewRateLimitRetryAfter)
	encoded, err := json.MarshalIndent(body, "", "  ")
	if err != nil {
		// Marshaling a struct of strings and an int cannot fail, but a preview
		// that silently printed nothing would leave TEMPLATES.md quietly wrong.
		fmt.Printf("error: render rate-limited response body: %v\n", err)
		return
	}

	fmt.Printf("HTTP/1.1 %d %s\n", http.StatusTooManyRequests, http.StatusText(http.StatusTooManyRequests))
	fmt.Printf("Content-Type: application/json\n")
	fmt.Printf("Retry-After: %d\n", body.RetryAfterSeconds)
	fmt.Println()
	fmt.Println(string(encoded))
}

// rateLimitedPullFailure builds the client-side view of a refused pull from the
// server's own reason, so the previewed CLI output carries the message the
// server actually sends.
func rateLimitedPullFailure(reason string) SchemaPullFailure {
	body := apitypes.NewRateLimitedResponse(reason, previewRateLimitRetryAfter)
	return SchemaPullFailure{
		Operation:   "Pull",
		Database:    "orders",
		Environment: "production",
		Status:      http.StatusTooManyRequests,
		ErrorCode:   body.ErrorCode,
		Message:     body.Error,
	}
}

// previewRateLimitAllOutput shows every rate-limit surface: what an operator
// sees at the terminal for each budget, and what a service caller reads off
// the wire.
func previewRateLimitAllOutput() {
	sections := []struct {
		name string
		fn   func()
	}{
		{"PULL REFUSED: CALLER BUDGET SPENT", previewPullRateLimitedCallerOutput},
		{"PULL REFUSED: SHARED BUDGET SPENT (AUTH DISABLED)", previewPullRateLimitedSharedOutput},
		{"PULL REFUSED: TARGET BUDGET SPENT", previewPullRateLimitedTargetOutput},
		{"API RESPONSE (SERVICE CALLERS)", previewPullRateLimitedResponseOutput},
	}
	printSections(sections)
}
