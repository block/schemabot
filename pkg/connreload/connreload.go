// Package connreload provides a database/sql connector whose credentials
// survive rotation of the underlying secret.
//
// When a new physical connection is refused because the server rejected its
// credentials — the signature of a password rotated out from under a running
// pod — the connector re-resolves its DSN and retries once, so rotation is
// transparent and does not require a restart. Established connections
// authenticated before the rotation keep working; only new dials take the
// reload path.
//
//	secret rotated ──► new conn ──► credentials refused
//	                                      │
//	                                      ▼
//	                    reload: re-resolve DSN (re-read secret)
//	                                      │ (on error: keep current credentials)
//	                                      ▼
//	                    retry with fresh credentials ──► success
//
// Nothing here is engine-specific. A caller supplies two small functions —
// Resolve, which turns a raw DSN into the driver.Connector that dials it, and
// Refused, which recognizes its driver's credentials-refused error — and this
// package owns everything about *when* to reload: at most one reload in flight,
// one reload per generation of credentials however many dials failed against
// it, a cooldown so a secrets-backend outage cannot be amplified into one
// resolve per rejected dial, and a reload that runs detached so a hung secret
// resolution cannot pin a pool connection slot.
//
// It exists as one implementation because that scheduling is the subtle part
// and the part worth testing once. SchemaBot's MySQL and PostgreSQL storage
// pools resolve their DSN through the same secrets machinery, so a difference
// in how aggressively they re-resolve it would say nothing about either engine.
package connreload

import (
	"context"
	"database/sql/driver"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// DefaultCooldown bounds how often a failing reload is retried. After a reload
// fails, further refused dials within this window surface their dial error
// without invoking Reload again, so a secrets-backend outage costs at most one
// resolve attempt per window instead of one per refused dial. The window is
// also armed when a reload succeeds but the dial retrying with the reloaded
// credentials is refused too — a backend that keeps answering with a credential
// the server rejects (stale secret sync, dropped grant, a user the rotation
// renamed) likewise costs one resolve per window, not one per connection.
const DefaultCooldown = 30 * time.Second

// Config describes one reloadable pool. Resolve, Refused and Reload are
// required.
type Config struct {
	// Resolve turns a raw DSN into the connector that dials it. It is called
	// once for the DSN passed to New and once per successful Reload, never per
	// dial, so it is the right place to put DSN normalization and connector
	// construction however expensive they are. Returning an error rejects the
	// DSN: at New time that fails the open, and on the reload path it keeps the
	// pool on its current credentials.
	Resolve func(dsn string) (driver.Connector, error)

	// Refused reports whether a dial error is the server rejecting the
	// credentials the connection presented, as opposed to any other failure. It
	// must be specific: every error it accepts costs a secret resolution, and
	// every error it accepts that no rotation can fix costs one per cooldown
	// window forever. It must also unwrap, because database/sql wraps.
	Refused func(error) bool

	// Reload re-resolves the raw DSN, typically by re-reading a mounted secret.
	// It is passed to Resolve, so it returns the DSN in the same form New was
	// given. An error keeps the current credentials.
	//
	// It runs on its own goroutine, detached from the dial that elected it, and
	// may block: a hung Reload delays the pool's credential refresh but cannot
	// block healthy dials or hold a pool connection slot. A panic is recovered
	// and treated as a failed reload.
	Reload func() (string, error)

	// Driver is what the pool's Driver method reports. database/sql uses it
	// only for driver-level feature detection, so it may be the driver's
	// zero-value instance.
	Driver driver.Driver

	// Name identifies the pool in log records. Optional.
	Name string

	// Cooldown overrides DefaultCooldown. Non-positive means DefaultCooldown.
	Cooldown time.Duration

	// now is the clock, for tests. nil means time.Now.
	now func() time.Time
}

// Connector dials with the most recently resolved credentials and refreshes
// them, at most once per failed attempt, when a dial is refused. gen counts
// credential swaps so concurrent failed dials trigger a single reload: a dial
// that failed against an already-superseded generation retries with the current
// credentials instead of reloading again.
type Connector struct {
	cfg Config

	mu             sync.Mutex
	current        driver.Connector
	gen            uint64
	lastReloadFail time.Time
	reloading      chan struct{} // non-nil while a reload for the current generation is in flight; closed when it finishes
}

var _ driver.Connector = (*Connector)(nil)

// New resolves dsn and returns a connector that dials it, reloading
// credentials as described on the package. A dsn Resolve rejects is returned as
// an error, so a bad DSN fails when the pool is opened rather than on first
// use.
func New(dsn string, cfg Config) (*Connector, error) {
	if cfg.Resolve == nil || cfg.Refused == nil || cfg.Reload == nil {
		return nil, fmt.Errorf("connreload: Resolve, Refused and Reload are required")
	}
	initial, err := cfg.Resolve(dsn)
	if err != nil {
		return nil, err
	}
	return &Connector{cfg: cfg, current: initial}, nil
}

func (c *Connector) Connect(ctx context.Context) (driver.Conn, error) {
	current, gen := c.snapshot()
	conn, err := current.Connect(ctx)
	if err == nil || !c.cfg.Refused(err) {
		return conn, err
	}
	fresh, freshGen, ok := c.refresh(ctx, gen)
	if !ok {
		// Reload failed; surface the refusal that triggered it.
		return nil, err
	}
	conn, err = fresh.Connect(ctx)
	if err != nil && c.cfg.Refused(err) {
		// The freshly resolved credentials are no better: the secret store
		// keeps answering with a credential the server rejects. Arm the
		// cooldown so subsequent refused dials back off instead of resolving
		// once per connection.
		c.armCooldown(freshGen)
		c.log().Warn("dial with reloaded credentials was also refused; backing off further reloads", "error", err)
	}
	return conn, err
}

func (c *Connector) Driver() driver.Driver { return c.cfg.Driver }

// log returns the logger for this pool, tagged with its name when it has one.
func (c *Connector) log() *slog.Logger {
	if c.cfg.Name == "" {
		return slog.Default()
	}
	return slog.With("pool", c.cfg.Name)
}

func (c *Connector) cooldown() time.Duration {
	if c.cfg.Cooldown > 0 {
		return c.cfg.Cooldown
	}
	return DefaultCooldown
}

// clock returns the current time, honoring the test seam.
func (c *Connector) clock() time.Time {
	if c.cfg.now != nil {
		return c.cfg.now()
	}
	return time.Now()
}

func (c *Connector) snapshot() (driver.Connector, uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.current, c.gen
}

// armCooldown starts a cooldown window as if a reload had failed, bounding
// resolve traffic when reloads succeed but the credentials they return keep
// being refused. refusedGen is the generation whose credentials were refused:
// when the connector has already advanced past it, the arm is a stale verdict
// on superseded credentials and must not suppress the newer generation's
// reloads.
func (c *Connector) armCooldown(refusedGen uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.gen != refusedGen {
		c.log().Debug("skipping cooldown arm: credentials advanced past the refused generation",
			"refused_gen", refusedGen, "current_gen", c.gen)
		return
	}
	c.lastReloadFail = c.clock()
}

// refresh resolves fresh credentials after a dial using generation failedGen
// was refused. When another dial already swapped the credentials, the current
// ones are returned without reloading again. A reload or resolve error keeps
// the current credentials and reports false, so a transient resolve failure
// cannot wedge the pool; it also arms the cooldown so a secrets-backend outage
// is retried once per window, not once per refused dial. On success it also
// returns the generation the returned connector belongs to, so a later refusal
// of it can be attributed to the right generation.
//
// The Reload callback runs detached from every dial — outside the connector
// mutex and on its own goroutine — so a hung secret resolution can neither
// block healthy dials from snapshotting the current credentials nor pin the
// dial that elected it: database/sql counts a dial against the pool's
// connection budget before Connect runs, so a pinned dial would hold a pool
// slot for as long as the reload hangs. The reloading guard keeps it to one
// reload in flight at a time: every same-generation failure, the electing dial
// included, waits for the reload's outcome — or gives up when its own dial
// context ends, surfacing the dial error.
func (c *Connector) refresh(ctx context.Context, failedGen uint64) (driver.Connector, uint64, bool) {
	for {
		c.mu.Lock()
		if c.gen != failedGen {
			current, gen := c.current, c.gen
			c.mu.Unlock()
			return current, gen, true
		}
		if !c.lastReloadFail.IsZero() && c.clock().Sub(c.lastReloadFail) < c.cooldown() {
			c.mu.Unlock()
			c.log().Debug("skipping DSN reload during cooldown after a failed reload; surfacing the dial error")
			return nil, 0, false
		}
		done := c.reloading
		if done == nil {
			done = make(chan struct{})
			c.reloading = done
			go c.runReload(done)
		}
		c.mu.Unlock()
		select {
		case <-done:
			// The reload finished; re-check the connector state to pick up the
			// swapped credentials or the armed cooldown.
		case <-ctx.Done():
			c.log().Debug("dial context ended while waiting for an in-flight DSN reload; surfacing the dial error")
			return nil, 0, false
		}
	}
}

// runReload invokes Reload and resolves its DSN outside the connector mutex,
// then publishes the outcome under it: success swaps the credentials, advances
// the generation, and clears the cooldown; failure arms the cooldown. It runs
// on its own goroutine, detached from the dial that elected it, so waiters
// observe the outcome through the connector state rather than a return value.
// The publish runs in a defer so the reloading guard is released and waiters
// are unblocked even if Reload panics; the panic is recovered and treated as a
// failed reload — a detached goroutine has no caller to propagate it to, and a
// panicking secret resolver must leave the pool on its current credentials,
// not crash the process.
func (c *Connector) runReload(done chan struct{}) {
	var fresh driver.Connector
	defer func() {
		if r := recover(); r != nil {
			c.log().Error("DSN reload after a refused dial panicked; keeping current credentials", "panic", r)
		}
		c.mu.Lock()
		defer c.mu.Unlock()
		c.reloading = nil
		close(done)
		if fresh == nil {
			c.lastReloadFail = c.clock()
			return
		}
		c.current = fresh
		c.gen++
		c.lastReloadFail = time.Time{}
		c.log().Info("reloaded credentials after a refused dial")
	}()

	dsn, err := c.cfg.Reload()
	if err != nil {
		c.log().Error("DSN reload after a refused dial failed; keeping current credentials", "error", err)
		return
	}
	resolved, err := c.cfg.Resolve(dsn)
	if err != nil {
		c.log().Error("resolving the reloaded DSN failed; keeping current credentials", "error", err)
		return
	}
	fresh = resolved
}
