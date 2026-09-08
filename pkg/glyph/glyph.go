// Package glyph is the severity vocabulary for operator-facing output: one
// glyph per meaning, one meaning per glyph, shared by the CLI and the GitHub
// PR comment surfaces. The glyph codepoints are identical on both surfaces —
// everything that differs between them (ANSI color, cell padding, markdown
// emphasis) is markup around the glyph and stays with the surface.
//
// Two rules keep the vocabulary unambiguous at the sites that could blur it:
//
//   - Refused attaches to the refusal, never to what was refused. An apply
//     rejected over unsafe changes headlines with Refused because the apply is
//     not proceeding; the same changes disclosed at plan time, before anything
//     is refused, carry Attention.
//   - Errors in the operator's own input (a database name that resolves to
//     nothing, a config that fails validation) carry Attention — fixing the
//     input and retrying can succeed. Refused is reserved for requests
//     SchemaBot understood and will not perform, where retrying unchanged
//     refuses again.
//
// Every severity glyph occupies two terminal cells (pinned by test against
// ui.VisibleWidth), so any of them can share a padded CLI column without
// misaligning it.
//
// Apply-operation states (completed, running, cancelled, ...) are a separate
// vocabulary owned by pkg/presentation: a state glyph names where an operation
// is in its lifecycle, not how urgently an operator should react. Where a
// state is itself a severity — a failed operation — presentation takes the
// glyph from this package so the two vocabularies cannot drift apart.
package glyph

const (
	// Escalation marks destructive consent in effect: --allow-unsafe was
	// given and data will be destroyed. The operator should confirm the
	// intent — this is the last moment to stop.
	Escalation = "🚨"

	// Refused marks an operation that is not proceeding because SchemaBot or
	// the schema change engine refused it: an engine-blocked change, a
	// rejected apply, an apply blocked on a merged or closed PR. The operator
	// should read the reason; the change or the situation must change before
	// a retry can succeed.
	Refused = "⛔"

	// Failed marks an attempted operation that failed. The system already
	// stopped on its own; the operator's job is triage.
	Failed = "❌"

	// Attention marks something the operator should look at and act on
	// before proceeding — an unsafe change awaiting consent, drift, a config
	// problem, a degraded condition. Nothing has been refused yet.
	Attention = "⚠️"

	// Info marks neutral information that requires nothing of the operator.
	Info = "ℹ️"
)
