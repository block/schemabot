package inventory

import (
	"context"
	"fmt"
	"slices"
	"strings"
)

// WriterProbe inspects one assembled connection and reports whether the server
// behind it accepts writes, which server it is, and which servers it replicates
// from. A resolver whose lookup matches several candidates for one target uses
// it to find the writable one, for inventories that list both sides of a
// replicated pair (for example a standby kept in sync for a later switchover)
// under the same target.
//
// A probe reports what it observed and never decides: SelectWriter applies the
// selection rule, so every engine is held to the same one.
type WriterProbe interface {
	ProbeWriter(ctx context.Context, dsn string) (WriterStatus, error)
}

// WriterStatus is what a WriterProbe observed on one candidate.
type WriterStatus struct {
	// Writable reports whether the server accepts writes.
	Writable bool
	// ReadOnlyReason names the setting that makes the server read-only. It is
	// empty when Writable is true.
	ReadOnlyReason string
	// ServerID identifies the server instance, stable across connections to it.
	ServerID string
	// SourceIDs are the ServerIDs of the servers this one replicates from.
	SourceIDs []string
}

// WriterCandidate is one probed candidate. ID names it in errors and logs; it
// must identify the inventory record without revealing connection details.
type WriterCandidate struct {
	ID     string
	Status WriterStatus
}

// SelectWriter returns the index of the one candidate to connect to, or an
// error naming why none can be chosen.
//
// It chooses only when exactly one candidate is writable and every other
// candidate replicates from it. Requiring the replication link proves the
// candidates are copies of one database: a record attached to the target by
// mistake neither replicates from the writer nor is replicated to, so it is
// refused rather than either chosen or silently passed over. Two writable
// candidates (a switchover in progress, or a split) and no writable candidate
// are refused too, since there is no single place a change can safely land.
func SelectWriter(candidates []WriterCandidate) (int, error) {
	if len(candidates) == 0 {
		return -1, fmt.Errorf("no candidates to choose a writer from")
	}

	var writable []int
	for i, c := range candidates {
		if c.Status.Writable {
			writable = append(writable, i)
		}
	}
	switch len(writable) {
	case 0:
		return -1, fmt.Errorf("no candidate accepts writes (%s)", describeCandidates(candidates))
	case 1:
	default:
		return -1, fmt.Errorf("%d candidates accept writes, so there is no single writer (%s)", len(writable), describeCandidates(candidates))
	}

	writer := candidates[writable[0]]
	if writer.Status.ServerID == "" {
		return -1, fmt.Errorf("writable candidate %s reported no server identity, so its replicas cannot be verified", writer.ID)
	}
	for i, c := range candidates {
		if i == writable[0] {
			continue
		}
		if !slices.Contains(c.Status.SourceIDs, writer.Status.ServerID) {
			return -1, fmt.Errorf("read-only candidate %s does not replicate from writable candidate %s, so they are not proven to be copies of one database (%s)", c.ID, writer.ID, describeCandidates(candidates))
		}
	}
	return writable[0], nil
}

// describeCandidates renders each candidate's observed state for an error.
func describeCandidates(candidates []WriterCandidate) string {
	parts := make([]string, 0, len(candidates))
	for _, c := range candidates {
		state := "writable"
		if !c.Status.Writable {
			state = "read-only"
			if c.Status.ReadOnlyReason != "" {
				state += ": " + c.Status.ReadOnlyReason
			}
		}
		parts = append(parts, c.ID+" "+state)
	}
	return strings.Join(parts, "; ")
}
