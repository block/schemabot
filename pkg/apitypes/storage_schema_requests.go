package apitypes

import (
	"fmt"
	"math"
	"time"
)

// StorageSchemaPlanRequest is the HTTP request for
// POST /api/storage/schema/plan.
//
// The request reads and changes nothing, and is a POST because the desired
// schema travels in the body: a caller asking what a database needs in order to
// match a later release sends that release's files, which the answering binary
// does not have. Every field is optional — the defaults ask the addressed
// server what its own storage needs to match its own embedded schema.
type StorageSchemaPlanRequest struct {
	// Deployment names the data plane whose storage to read. Empty reads the
	// storage of the server the request is made to.
	Deployment string `json:"deployment,omitempty"`
	// Environment is required alongside Deployment, since a deployment serves
	// one gRPC endpoint per environment.
	Environment string `json:"environment,omitempty"`
	// AllowDestructive reports the destructive statements as ones that would
	// run. It executes nothing either way: a diff never does.
	AllowDestructive bool `json:"allow_destructive,omitempty"`
	// SchemaFiles is the desired schema as file name → file contents. Empty
	// diffs against the answering binary's own embedded schema.
	SchemaFiles map[string]string `json:"schema_files,omitempty"`
	// SchemaSource says where SchemaFiles came from, in words, for the report
	// to attribute the answer to. Required with SchemaFiles and never inferred.
	SchemaSource string `json:"schema_source,omitempty"`
}

// StorageSchemaPlanResponse is the HTTP response for
// POST /api/storage/schema/plan.
type StorageSchemaPlanResponse struct {
	Report *StorageSchemaReport `json:"report"`
}

// StorageSchemaApplyRequest is the HTTP request for
// POST /api/storage/schema/apply.
//
// Every field is optional. The defaults converge the addressed server's own
// storage to its own embedded schema, which is what its next boot would
// converge; SchemaFiles is how an operator converges it to a release that is
// about to roll instead.
type StorageSchemaApplyRequest struct {
	// Deployment names the data plane whose storage to converge. Empty
	// converges the storage of the server the request is made to.
	Deployment string `json:"deployment,omitempty"`
	// Environment is required alongside Deployment, since a deployment serves
	// one gRPC endpoint per environment.
	Environment string `json:"environment,omitempty"`
	// AllowDestructive permits the destructive statements the convergence
	// would otherwise refuse. It widens the target's standing storage policy
	// and never narrows it.
	AllowDestructive bool `json:"allow_destructive,omitempty"`
	// Caller identifies the operator, so the converging instance's logs
	// attribute the convergence to a person. An authenticated identity
	// overrides it.
	Caller string `json:"caller,omitempty"`
	// TimeoutSeconds bounds the whole convergence: the advisory-lock wait, the
	// diff taken under it, and the DDL. Zero means the caller named no budget,
	// and the target then runs the convergence under the budget it boots with
	// rather than under an operator's: a caller that could not name a budget
	// is one whose wait the target cannot know, and the boot budget is the one
	// every such caller has always waited out. SchemaBot's own client names a
	// budget on every request, so a convergence runs under the operator
	// default only because the client asked for it and is waiting that long.
	// Raise it only to finish work a boot cannot, and expect to hold the
	// bootstrap advisory lock for that long — a pod booting in the window
	// fails its own lock wait. A value above the target's maximum is refused
	// rather than clamped.
	TimeoutSeconds int64 `json:"timeout_seconds,omitempty"`
	// SchemaFiles is the schema to converge to, as file name → file contents.
	// Empty converges the answering binary's own embedded schema.
	SchemaFiles map[string]string `json:"schema_files,omitempty"`
	// SchemaSource says where SchemaFiles came from, in words, for the reports
	// to attribute the convergence to. Required with SchemaFiles and never
	// inferred.
	SchemaSource string `json:"schema_source,omitempty"`
}

// StorageSchemaApplyResponse is the HTTP response for
// POST /api/storage/schema/apply. It brackets the convergence: Planned is what
// was outstanding before it ran, Remaining is what is still outstanding after.
type StorageSchemaApplyResponse struct {
	Planned   *StorageSchemaReport `json:"planned"`
	Remaining *StorageSchemaReport `json:"remaining"`
}

// DefaultStorageApplyTimeout is the budget SchemaBot's client names for a
// convergence an operator asked for, when the operator named none of their own.
// It is the deliberate path's counterpart to the boot budget, and it is far
// larger for a reason that is about who is waiting rather than about how much
// DDL there is. A boot's budget is short because a pod converging is a pod not
// yet serving; nothing is waiting on this one but the person who ran it.
//
// It is the client's default rather than the server's, and always sent, so the
// wire carries the budget the client is actually waiting for. A server that
// received no budget would have to guess how long the caller can wait, and an
// hour is the wrong guess for a caller that could not say: it runs the boot
// budget instead (see ResolveStorageApplyTimeout).
//
// The point of the larger value is that the two paths need not agree on what is
// too slow. An index over a storage table with years of history can outlast a
// boot's five minutes and still be entirely routine work to run once, ahead of
// a roll, at a terminal. Under a shared budget that statement has no path
// through SchemaBot at all and has to be run by hand against the storage
// database — which is the thing operators should not have to do.
//
// It is a ceiling, not a target: a convergence still stops at it rather than
// running unbounded, because the advisory lock it holds is what a booting pod
// waits on. MaxStorageApplyTimeout caps how far a request may raise it.
const DefaultStorageApplyTimeout = time.Hour

// MaxStorageApplyTimeout is the longest budget a storage convergence request
// may name. The cap is not about trusting the caller — the route is admin-only
// — but about the lock: a convergence holds the bootstrap advisory lock for its
// whole budget, and every pod that boots in that window fails its own lock wait
// and does not come up.
//
// It sits at the default rather than above it, so a request can lower the
// budget but not raise it, and the reason is who can end the run this bounds.
// An operator watching a convergence in their own terminal can stop it, but
// this is the ceiling on a budget that arrives in a request, and a convergence
// answering one runs with its caller's cancellation stripped: a dropped
// connection must not abandon a table copy half-done, so nothing the caller
// does afterwards ends it. For that run the budget is still the only thing
// that does. At five minutes that was nobody's problem; a mistake self-healed
// before it was worth reacting to. An hour of held lock is already the outer
// edge of what a caller should be unable to take back, and it is the trade
// that buys work a boot cannot finish. Raising this belongs with a way to stop
// a convergence the caller is not sitting in front of, not before it.
//
// A request naming more is refused rather than silently clamped, so a command
// never reports a budget it did not get.
const MaxStorageApplyTimeout = DefaultStorageApplyTimeout

// ResolveStorageApplyTimeout resolves the convergence budget a request asked
// for. Zero means the caller named none, and the answer is then unnamed — the
// budget the resolving end runs a request like that under.
//
// The two ends pass different values for it, on purpose. A client passes the
// operator default and then names the answer in the request it sends, so the
// wire always carries the budget that client is waiting for. A server passes
// the budget it boots with: a request that names no budget came from something
// that could not say how long it can wait, and the boot budget is both the one
// every such caller has always waited out and the shortest hold on the
// bootstrap lock the server can offer. Resolving an unnamed request to the
// operator default instead would hold that lock for an hour on behalf of a
// caller that gave up on it long before, which is the failure AV-11 exists to
// prevent.
//
// The bounds live beside the field rather than in the server, because both
// ends have to agree on them: the server bounds the convergence with the
// answer, and the client waits for that long before giving up. Two copies of
// this rule would drift into a client that stops waiting for work the server
// is still doing.
//
// Out of range is refused rather than clamped, in both directions. A clamp
// would answer a different question than the one asked: an operator who names
// a budget because that is what their index build needs, and is quietly given
// less, watches the convergence fail at a budget they did not choose and has
// nothing in the output to tell them why.
//
// The unnamed budget is a budget like any other, and answers to the same
// bounds. It is a constant at every call site today, so refusing one out of
// range guards a future caller rather than a live one — but this is the one
// function whose job is to keep a convergence inside MaxStorageApplyTimeout,
// and a parameter it returned unchecked would be the one way to hand a caller
// a budget past the maximum.
//
// The errors name the budget rather than the field that carried it, since each
// end spells that field differently and prefixes the refusal with its own name
// for it.
func ResolveStorageApplyTimeout(timeoutSeconds int64, unnamed time.Duration) (time.Duration, error) {
	// Bounded in seconds, before the conversion. A time.Duration counts
	// nanoseconds, so multiplying an arbitrary wire value by time.Second
	// overflows past roughly nine billion seconds and wraps — a budget far
	// above the maximum comes out small or negative and passes a bound
	// applied after the conversion, which is the one way a caller could be
	// handed a budget it never named.
	const maxSeconds = int64(MaxStorageApplyTimeout / time.Second)
	switch {
	case timeoutSeconds == 0 && (unnamed <= 0 || unnamed > MaxStorageApplyTimeout):
		return 0, fmt.Errorf("the budget for an unnamed convergence must be positive and at most %s, got %s",
			MaxStorageApplyTimeout, unnamed)
	case timeoutSeconds == 0:
		return unnamed, nil
	case timeoutSeconds < 0:
		return 0, fmt.Errorf("a convergence budget must be positive, or zero to name none and run under %s", unnamed)
	case timeoutSeconds > maxSeconds:
		return 0, fmt.Errorf("a convergence budget of %s exceeds the maximum of %s; a convergence holds the storage bootstrap lock for its whole budget, so pods booting in that window will not come up",
			describeBudgetSeconds(timeoutSeconds), MaxStorageApplyTimeout)
	}
	return time.Duration(timeoutSeconds) * time.Second, nil
}

// describeBudgetSeconds names a rejected budget the way the operator typed it.
// A duration reads better than a second count — "2h0m0s" against a maximum of
// "1h0m0s" rather than "7200s" — but only a value a time.Duration can hold can
// be rendered as one, so an absurd request falls back to its seconds.
func describeBudgetSeconds(seconds int64) string {
	if seconds <= int64(math.MaxInt64/int64(time.Second)) {
		return (time.Duration(seconds) * time.Second).String()
	}
	return fmt.Sprintf("%ds", seconds)
}
