// Package clock provides a minimal time source abstraction so orchestration
// code (operator, comment observer) can be exercised deterministically in
// tests without sleeping or racing against the wall clock.
package clock

import (
	"reflect"
	"sync"
	"time"
)

// Clock is the minimal time source orchestration code depends on. Production
// callers use Real; tests use Fake to advance time explicitly.
type Clock interface {
	Now() time.Time
}

// Real is a Clock backed by the operating system wall clock.
type Real struct{}

// Now returns the current wall-clock time.
func (Real) Now() time.Time { return time.Now() }

// Fake is a deterministic Clock for tests. The zero value is unsafe; use
// NewFake. Now is safe to call from multiple goroutines; Advance and Set
// serialize updates with a mutex.
type Fake struct {
	mu  sync.Mutex
	now time.Time
}

// NewFake returns a Fake clock pinned to start.
func NewFake(start time.Time) *Fake {
	return &Fake{now: start}
}

// Now returns the current fake time.
func (f *Fake) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

// Advance moves the fake clock forward by d.
func (f *Fake) Advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = f.now.Add(d)
}

// Set pins the fake clock to t.
func (f *Fake) Set(t time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = t
}

// Default returns c if it is a usable Clock, otherwise Real{}. This guards
// against both a plain nil interface and a typed-nil implementation
// (e.g., a nil *Fake stored in a Clock interface), either of which would
// otherwise panic on a later Now() call.
func Default(c Clock) Clock {
	if c == nil {
		return Real{}
	}
	v := reflect.ValueOf(c)
	switch v.Kind() {
	case reflect.Pointer, reflect.Interface, reflect.Map, reflect.Slice, reflect.Chan, reflect.Func:
		if v.IsNil() {
			return Real{}
		}
	}
	return c
}
