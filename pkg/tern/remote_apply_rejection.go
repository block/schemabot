package tern

import (
	"context"
	"fmt"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// remoteApplyRejectionMessage is what an operator reads when a dispatch is
// refused: it is stored on the apply and rendered into the pull request.
//
// A refusal the data plane described structurally is re-composed here from
// those facts rather than passed through as the remote's own prose. Composing
// it on this side is what makes the message both safe and actionable — safe
// because it is this plane's sentence over typed fields instead of remote error
// text, which can carry dial failures and hostnames onto a public surface, and
// actionable because a pull request reference is a handle an operator can
// follow while the data plane's own identifiers resolve nowhere they can reach.
//
// Every other rejection keeps the remote's message, which is the line the data
// plane chose to return for it. fallback names the refusal for a rejection that
// carried no message at all, so the apply never fails without a reason.
//
// holderApplyID is this plane's own identifier for the holding change, already
// resolved by the caller, or "" when the holder is work this control plane did
// not start. Resolution happens outside so the message stays a pure function of
// what is known: whether a handle can be offered is a storage question, and
// composing the sentence is not.
func remoteApplyRejectionMessage(resp *ternv1.ApplyResponse, holderApplyID, fallback string) string {
	if resp == nil {
		return fallback
	}
	if message := applyConflictMessage(resp.Conflict, holderApplyID); message != "" {
		return message
	}
	if resp.ErrorMessage != "" {
		return resp.ErrorMessage
	}
	return fallback
}

// resolveConflictHolderApplyID turns the data plane's identifier for the change
// holding a database into this plane's own identifier for it, or "" when there
// is none to offer.
//
// Empty is the ordinary answer for work this control plane never dispatched — a
// direct engine run, or another control plane — and the refusal reads correctly
// without a handle. It is also the answer when the correlation cannot be read at
// all, because this runs while a refusal is already being recorded: a storage
// failure here may cost the operator the handle, never the reason they were
// refused. The failed lookup is logged against the apply being refused, so a
// message that names no holder is explainable without reproducing it.
func (c *GRPCClient) resolveConflictHolderApplyID(ctx context.Context, apply *storage.Apply, conflict *ternv1.ApplyConflict) string {
	if conflict.GetHolderExternalId() == "" {
		return ""
	}
	logAttrs := append(apply.LogAttrs(), "holder_external_id", conflict.GetHolderExternalId())
	holderApplyID, err := c.storage.ApplyOperations().ApplyIdentifierForRemoteApply(ctx, conflict.GetHolderExternalId())
	if err != nil {
		c.applyLogger(apply).ErrorContext(ctx, "refusal will not name the schema change holding the database; failed to correlate it to an apply of this control plane",
			append(logAttrs, "error", err)...)
		return ""
	}
	if holderApplyID == "" {
		c.applyLogger(apply).DebugContext(ctx, "the schema change holding the database was not dispatched by this control plane; refusing without naming an apply",
			logAttrs...)
	}
	return holderApplyID
}

// applyConflictMessage renders a structured conflict as the sentence an
// operator acts on, or empty when there is no conflict to render.
//
// The holding apply's identifier closes the message when this control plane has
// one, because it is the handle every command an operator might reach for takes
// — the pull request tells them who to talk to, the identifier lets them look at
// the change themselves. It is a separate sentence rather than another clause so
// the refusal reads the same with or without it, which is what a holder this
// control plane never dispatched leaves.
func applyConflictMessage(conflict *ternv1.ApplyConflict, holderApplyID string) string {
	if conflict == nil {
		return ""
	}
	message := fmt.Sprintf("%s is held by a schema change (%s)%s",
		conflictSubject(conflict), state.Label(conflict.BlockingState), conflictHolder(conflict))
	if hold := state.Hold(conflict.BlockingState); hold != "" {
		message += "; " + hold
	}
	message += "."
	if holderApplyID != "" {
		message += fmt.Sprintf(" The holding apply is %s.", holderApplyID)
	}
	return message
}

// conflictSubject opens the sentence with the work being held, leading with the
// table an operator recognizes. A multi-table atomic change names no table, and
// a sharded change holds only the shard's own primary, so the subject narrows
// exactly as far as the data plane could prove and no further.
func conflictSubject(conflict *ternv1.ApplyConflict) string {
	switch {
	case conflict.Table == "" && conflict.Shard == "":
		return "This database"
	case conflict.Table == "":
		return fmt.Sprintf("Shard %s of this database", conflict.Shard)
	case conflict.Shard == "":
		return fmt.Sprintf("Table %s", conflict.Table)
	default:
		return fmt.Sprintf("Table %s shard %s", conflict.Table, conflict.Shard)
	}
}

// conflictHolder names who owns the holding change. A pull request reference is
// preferred because GitHub renders it as a link an operator can follow straight
// to the change that has to finish first; a caller string is the fallback for a
// change no pull request owns. Both empty leaves the conflict described by its
// work and its state, which is everything that was proven about it.
func conflictHolder(conflict *ternv1.ApplyConflict) string {
	switch {
	case conflict.Repository != "" && conflict.PullRequest > 0:
		return fmt.Sprintf(" on %s#%d", conflict.Repository, conflict.PullRequest)
	case conflict.Caller != "":
		return fmt.Sprintf(" started by %s", conflict.Caller)
	default:
		return ""
	}
}
