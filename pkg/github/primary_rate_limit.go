package github

import (
	"errors"
	"time"

	"github.com/gofri/go-github-ratelimit/v2/github_ratelimit/github_primary_ratelimit"
	gh "github.com/google/go-github/v86/github"
)

// PrimaryRateLimitResetAt reports whether err is GitHub's primary rate limit
// and, when it is, when the limit resets. A primary rate limit means the
// installation's hourly budget for a resource is spent, so every further
// request of that resource is refused until the reset.
//
// The limit reaches a caller in one of two shapes. The client's rate limit
// transport returns *github_primary_ratelimit.RateLimitReachedError, both for
// the response that exhausted the budget and for later requests it refuses
// without sending. A response for a resource the transport does not track
// reaches go-github's own error handling and comes back as *gh.RateLimitError.
// Both carry the reset time GitHub reported.
//
// It reports false for a reset time that was not reported or could not be
// read, because a caller that waits for the reset has nothing to wait for.
func PrimaryRateLimitResetAt(err error) (time.Time, bool) {
	if reached, ok := errors.AsType[*github_primary_ratelimit.RateLimitReachedError](err); ok && reached.ResetTime != nil {
		return reportedResetTime(*reached.ResetTime)
	}
	if limited, ok := errors.AsType[*gh.RateLimitError](err); ok {
		return reportedResetTime(limited.Rate.Reset.Time)
	}
	return time.Time{}, false
}

// reportedResetTime reports whether resetAt is a reset time GitHub reported.
// A missing or unreadable X-RateLimit-Reset header comes through as the zero
// time or the Unix epoch.
func reportedResetTime(resetAt time.Time) (time.Time, bool) {
	if resetAt.IsZero() || resetAt.Unix() <= 0 {
		return time.Time{}, false
	}
	return resetAt, true
}
