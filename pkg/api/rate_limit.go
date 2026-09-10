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

// The pull budget is spent in two lanes, each with its own check so a limited
// request can say which budget it ran out of: the per-caller budget protects
// the control plane from one runaway client, and the per-target budget
// protects a single database from the aggregate of every client reading it.
// An operator triaging a 429 needs to know which one to raise.
//
// The lanes are two functions rather than one because they bracket the app
// selector's resolution: the caller lane is spent before resolving, so
// resolution — server-side work either selector shape triggers — is always
// paid for; the target lane is spent after, so it keys on the resolved
// database and a by-app pull drains the same bucket as a by-name pull. The
// caller-lane-first order also bounds the target lane's bucket map: a target
// the request names but this server does not route still gets a bucket, so a
// client cycling through invented database names can only mint as many as its
// own budget admits before the limiter's idle sweep reclaims them.
//
// These run inside the handler rather than in middleware because the target is
// only known once the request body has been decoded, the same reason the
// forward-auth middleware cannot make per-database decisions.
//
// The environment recorded on the metric is clamped to a configured one. The
// budget itself is keyed on the environment the request named, whatever that
// is — an unroutable request still spends budget — but an arbitrary caller
// string must never reach a metric attribute and mint a series per value.
// Logs carry the unclamped names, which are what an operator needs to see.

// checkPullCallerBudget spends the request's per-caller pull budget and
// reports whether it may proceed. When the budget is exhausted it writes the
// 429 itself and returns false, so the caller only has to return. Exactly one
// of database or app is set, per the handler's selector validation; both are
// logged so a refusal names whichever selector the request used.
func (s *Service) checkPullCallerBudget(w http.ResponseWriter, r *http.Request, database, app, environment string) bool {
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
			"app", app,
			"environment", environment,
			"retry_after", retryAfter,
		)
		s.writeRateLimited(w, retryAfter, s.pullCallerRateLimitReason())
		return false
	}
	metrics.RecordRateLimitDecision(ctx, pullRateLimitEndpoint, rateLimitScopeCaller, rateLimitDecisionAllow, metricEnvironment)
	return true
}

// checkPullTargetBudget spends the pull budget of the resolved target database
// and reports whether the request may proceed. When the budget is exhausted it
// writes the 429 itself and returns false, so the caller only has to return.
func (s *Service) checkPullTargetBudget(w http.ResponseWriter, r *http.Request, database, environment string) bool {
	if !s.pullRateLimitEnforced() {
		return true
	}

	ctx := r.Context()
	metricEnvironment := s.config.metricEnvironmentAttribute(environment)

	caller := callerRateLimitKey(r)
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

// pullCallerRateLimitReason names the budget a caller-lane refusal ran out of.
// It reads the auth configuration rather than the request's own identity
// because the two cases differ in what the operator has to fix: on an
// authenticated server the caller really did spend its own budget, while on an
// unauthenticated one the budget is shared by every client and the request
// being refused may have contributed almost nothing to it.
func (s *Service) pullCallerRateLimitReason() string {
	if s.config.Auth.Enabled() {
		return apitypes.PullRateLimitCallerReason
	}
	return apitypes.PullRateLimitSharedReason
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
