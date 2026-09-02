package api

import (
	"net/http"
	"strconv"
	"time"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/auth"
	"github.com/block/schemabot/pkg/metrics"
)

// Rate-limit scopes and decisions, as reported to metrics. Both are fixed sets
// so the counter's cardinality stays bounded.
const (
	rateLimitScopeCaller = "caller"
	rateLimitScopeTarget = "target"

	rateLimitDecisionAllow = "allow"
	rateLimitDecisionLimit = "limit"
)

// pullRateLimitEndpoint labels the pull endpoint's rate-limit metrics.
const pullRateLimitEndpoint = "/api/pull"

// targetRateLimitKey builds the per-target bucket key. The NUL separator keeps
// two different targets from ever colliding on one bucket, which a printable
// separator could not guarantee: database and environment names are
// operator-supplied.
func targetRateLimitKey(database, environment string) string {
	return database + "\x00" + environment
}

// callerRateLimitKey returns the identity a request's per-caller budget is
// charged to: the authenticated subject (an operator's identity from the
// identity-header lane, or a service caller's SPIFFE ID from the gateway
// lane).
//
// When API auth is disabled every request arrives as the synthetic anonymous
// user, so all traffic shares one bucket and the per-caller budget acts as a
// process-wide budget. That is the honest reading of the configuration: with
// no identities to tell callers apart, the only budget that can be enforced is
// the aggregate one. The per-target budget is unaffected either way.
func callerRateLimitKey(r *http.Request) string {
	if subject, ok := auth.AuthenticatedSubject(r.Context()); ok {
		return subject
	}
	return auth.AnonymousSubject
}

// checkPullRateLimit spends the request's pull budget in both lanes and reports
// whether it may proceed. When a lane is exhausted it writes the 429 itself and
// returns false, so the caller only has to return.
//
// The two lanes are checked separately rather than folded together because they
// protect different things and a limited request should say which budget it
// ran out of: the per-caller budget protects the control plane from one runaway
// client, and the per-target budget protects a single database from the
// aggregate of every client reading it. An operator triaging a 429 needs to
// know which one to raise.
//
// This runs inside the handler rather than in middleware because the target is
// only known once the request body has been decoded, the same reason the
// forward-auth middleware cannot make per-database decisions.
//
// The caller lane is spent first, which also bounds the target lane's bucket
// map: a target the request names but this server does not route still gets a
// bucket, so a client cycling through invented database names can only mint as
// many as its own budget admits before the limiter's idle sweep reclaims them.
//
// The environment recorded on the metric is clamped to a configured one. The
// budget itself is keyed on the environment the request named, whatever that
// is — an unroutable request still spends budget — but an arbitrary caller
// string must never reach a metric attribute and mint a series per value.
// Logs carry the unclamped name, which is what an operator needs to see.
func (s *Service) checkPullRateLimit(w http.ResponseWriter, r *http.Request, database, environment string) bool {
	if !s.pullRateLimitEnforced() {
		return true
	}

	ctx := r.Context()
	metricEnvironment := s.config.metricEnvironmentAttribute(environment)

	caller := callerRateLimitKey(r)
	if allowed, retryAfter := s.pullPerCallerLimiter.Allow(caller); !allowed {
		metrics.RecordRateLimitDecision(ctx, pullRateLimitEndpoint, rateLimitScopeCaller, rateLimitDecisionLimit, metricEnvironment)
		s.logger.Warn("pull schema rejected because the caller exceeded its request budget",
			"caller", caller,
			"database", database,
			"environment", environment,
			"retry_after", retryAfter,
		)
		s.writeRateLimited(w, retryAfter, apitypes.PullRateLimitCallerReason)
		return false
	}
	metrics.RecordRateLimitDecision(ctx, pullRateLimitEndpoint, rateLimitScopeCaller, rateLimitDecisionAllow, metricEnvironment)

	if allowed, retryAfter := s.pullPerTargetLimiter.Allow(targetRateLimitKey(database, environment)); !allowed {
		metrics.RecordRateLimitDecision(ctx, pullRateLimitEndpoint, rateLimitScopeTarget, rateLimitDecisionLimit, metricEnvironment)
		s.logger.Warn("pull schema rejected because the target exceeded its request budget",
			"caller", caller,
			"database", database,
			"environment", environment,
			"retry_after", retryAfter,
		)
		s.writeRateLimited(w, retryAfter, apitypes.PullRateLimitTargetReason)
		return false
	}
	metrics.RecordRateLimitDecision(ctx, pullRateLimitEndpoint, rateLimitScopeTarget, rateLimitDecisionAllow, metricEnvironment)

	return true
}

// pullRateLimitEnforced reports whether either lane can refuse a request.
// Rate limiting being off is a configuration decision, logged once at startup,
// so a disabled server returns here without recording a decision it never
// made: an "allow" per pull would otherwise imply a budget was consulted.
func (s *Service) pullRateLimitEnforced() bool {
	return s.pullPerCallerLimiter != nil || s.pullPerTargetLimiter != nil
}

// writeRateLimited writes a 429 carrying how long the caller must wait, both as
// the standard Retry-After header and in the response body. The body repeats it
// because the CLI's HTTP client reads error bodies and not response headers, so
// a header-only hint would be invisible to the client most likely to be
// limited. Both come from the same rounded value the response body carries, so
// the header and the message can never disagree.
func (s *Service) writeRateLimited(w http.ResponseWriter, retryAfter time.Duration, reason string) {
	body := apitypes.NewRateLimitedResponse(reason, retryAfter)
	w.Header().Set("Retry-After", strconv.Itoa(body.RetryAfterSeconds))
	s.writeJSON(w, http.StatusTooManyRequests, body)
}
