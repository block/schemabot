// Package ratelimit provides a keyed token-bucket limiter for the API's
// expensive read endpoints. Reading a database's live schema costs a round of
// catalog introspection per namespace, so an unbounded caller — a CLI loop, an
// onboarding agent working a fleet-sized queue, or a service polling for schema
// inventory — can saturate the control plane and the target database alike.
// Callers spend from a budget keyed by whatever they should be isolated on
// (the caller's identity, the database being read), and a key that runs dry is
// told how long to wait instead of being served.
//
// A limiter's buckets live in the process, so a multi-replica deployment
// enforces the configured budget per replica: the fleet-wide ceiling is
// replicas × the configured rate. That is deliberate for now — the alternative
// is a storage round trip on every request, which costs more than the endpoint
// it protects — but it means the configured number is a per-replica floor on
// what a caller can spend, not an exact global cap.
package ratelimit

import (
	"sync"
	"time"

	"golang.org/x/time/rate"

	"github.com/block/schemabot/pkg/clock"
)

// Config is one limiter's budget.
type Config struct {
	// RequestsPerMinute is the sustained rate a single key may spend.
	RequestsPerMinute int

	// Burst is how many requests a key may spend at once before the sustained
	// rate takes over. It must be at least 1 for the limiter to admit anything.
	Burst int
}

// idleTTL is how long a key's bucket is retained after its last request. The
// fleet has hundreds of databases and every caller/database pair mints a
// bucket, so idle ones are dropped to bound the map. Eviction is safe at any
// TTL longer than the refill time of a full burst: a bucket that has been idle
// that long has already refilled, so a fresh one behaves identically.
const idleTTL = 10 * time.Minute

// sweepInterval bounds how often Allow scans for idle buckets, so a hot
// endpoint does not walk the whole map on every request.
const sweepInterval = time.Minute

type bucket struct {
	limiter  *rate.Limiter
	lastSeen time.Time
}

// Limiter admits requests against a per-key token bucket. It is safe for
// concurrent use.
type Limiter struct {
	limit rate.Limit
	burst int
	clock clock.Clock

	mu        sync.Mutex
	buckets   map[string]*bucket
	lastSweep time.Time
}

// New builds a limiter for the given budget. It returns nil when the config
// does not express a usable budget (a non-positive rate or burst), which is the
// disabled state: a nil *Limiter admits everything. Callers that must
// distinguish "disabled" from "configured" should check the config themselves
// rather than the returned pointer.
func New(cfg Config, clk clock.Clock) *Limiter {
	if cfg.RequestsPerMinute <= 0 || cfg.Burst <= 0 {
		return nil
	}
	return &Limiter{
		limit:   rate.Limit(float64(cfg.RequestsPerMinute) / 60.0),
		burst:   cfg.Burst,
		clock:   clock.Default(clk),
		buckets: make(map[string]*bucket),
	}
}

// Allow spends one request from key's budget. It reports whether the request
// may proceed and, when it may not, how long the caller must wait before the
// next request would be admitted. A nil limiter admits every request (see New).
//
// A rejected request costs the key nothing: the reservation is canceled, so a
// caller that keeps hammering a dry bucket does not push its own recovery
// further away.
func (l *Limiter) Allow(key string) (allowed bool, retryAfter time.Duration) {
	if l == nil {
		return true, 0
	}
	now := l.clock.Now()

	l.mu.Lock()
	defer l.mu.Unlock()

	l.sweepLocked(now)

	b := l.buckets[key]
	if b == nil {
		b = &bucket{limiter: rate.NewLimiter(l.limit, l.burst)}
		l.buckets[key] = b
	}
	b.lastSeen = now

	reservation := b.limiter.ReserveN(now, 1)
	if !reservation.OK() {
		// Unreachable with burst >= 1, which New guarantees: a single request
		// never exceeds the bucket's capacity. Handled rather than ignored so a
		// future budget change cannot turn this into a silent admit.
		return false, time.Duration(float64(time.Minute) / float64(l.limit))
	}
	if wait := reservation.DelayFrom(now); wait > 0 {
		reservation.CancelAt(now)
		return false, wait
	}
	return true, 0
}

// sweepLocked drops buckets idle for longer than idleTTL, at most once per
// sweepInterval. The caller must hold l.mu.
func (l *Limiter) sweepLocked(now time.Time) {
	if !l.lastSweep.IsZero() && now.Sub(l.lastSweep) < sweepInterval {
		return
	}
	l.lastSweep = now
	for key, b := range l.buckets {
		if now.Sub(b.lastSeen) > idleTTL {
			delete(l.buckets, key)
		}
	}
}
