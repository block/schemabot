package storage

import "strings"

// OperatorFacingMessage rewrites a data-plane message for the operator. A remote
// deployment knows the schema change only by its own identifier, so its copy
// names something that resolves to nothing on the control plane: it reads like
// an answer about a different schema change, and it puts an internal identifier
// in front of an operator. Every remote identifier is replaced with this apply's
// operator-facing identifier.
//
// The apply's own external_id is always rewritten. Pass additional remote
// identifiers for the ones a caller addressed that do not live on the apply row
// — most importantly a claimed operation's external_id, since a
// multi-operation apply deliberately carries none on its parent.
//
// The remote identifier stays triageable: it is on the apply or operation row,
// and every path that rewrites a message logs the raw one first.
//
// Replacing an identifier everywhere it appears in free-form text is safe
// because apply identifiers are fixed-length tokens from a single generator, so
// one can contain another as a substring only if the two are equal. An engine
// that reported variable-length or caller-influenced identifiers would break
// that property and need matching on identifier shape instead.
//
// A nil receiver, an empty message, or an empty identifier returns the message
// unchanged, so a caller never has to guard.
func (a *Apply) OperatorFacingMessage(message string, additionalRemoteIDs ...string) string {
	if a == nil || message == "" || a.ApplyIdentifier == "" {
		return message
	}
	for _, remoteID := range append([]string{a.ExternalID}, additionalRemoteIDs...) {
		if remoteID == "" || remoteID == a.ApplyIdentifier {
			continue
		}
		message = strings.ReplaceAll(message, remoteID, a.ApplyIdentifier)
	}
	return message
}
