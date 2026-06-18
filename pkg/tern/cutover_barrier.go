package tern

import "github.com/block/schemabot/pkg/storage"

// shouldAutoDeferCutover reports whether an operation-scoped copy drive must park
// at the cutover barrier automatically. It is true only for an operation of a
// multi-deployment (fan-out) apply running under the barrier cutover policy, so
// the high-risk cutover swaps can later be driven in deployment order by the
// cutover-claim path. A single-operation apply has no siblings to order, so it
// never auto-defers even when its stored cutover_policy is barrier: behaviour is
// unchanged until multi-deployment fan-out lands.
func shouldAutoDeferCutover(multiOperation bool, op *storage.ApplyOperation) bool {
	return multiOperation && op != nil && op.CutoverPolicy == storage.CutoverPolicyBarrier
}

// shouldReleaseAtCutoverBarrier reports whether an operation-scoped copy drive
// should park at the barrier *and release its claim* for the deployment-ordered
// cutover claim (OC-3). This is the automatic barrier decision only: when the
// apply was started with manual --defer-cutover, the documented manual contract
// wins — the operator holds the claim and polls for a manual cutover (subject to
// the inaction timeout) — so we must not release. effectiveCopyDriveOptions
// still keeps DeferCutover on either way, so the cutover is deferred regardless.
func shouldReleaseAtCutoverBarrier(apply *storage.Apply, multiOperation bool, op *storage.ApplyOperation) bool {
	return !apply.GetOptions().DeferCutover && shouldAutoDeferCutover(multiOperation, op)
}

// effectiveCopyDriveOptions returns the apply options that govern a copy-phase
// drive. It starts from the apply's stored options and turns on DeferCutover
// when the operation must park at the barrier (see shouldAutoDeferCutover). The
// manual per-apply --defer-cutover option stays authoritative — it is OR'd in,
// never cleared. The returned value is execution-time only and must never be
// persisted back onto the apply: the automatic decision is per operation, while
// apply.Options is shared by every deployment of the apply.
func effectiveCopyDriveOptions(apply *storage.Apply, multiOperation bool, op *storage.ApplyOperation) storage.ApplyOptions {
	opts := apply.GetOptions()
	if !opts.DeferCutover && shouldAutoDeferCutover(multiOperation, op) {
		opts.DeferCutover = true
	}
	return opts
}
