package tern

// Per-boundary TableChange conversion helpers. A table change crosses three
// boundaries — engine → storage, engine → proto, and proto → storage — and
// every hop must carry the full advisory annotation set (unsafe and
// execution-mode fields). Each helper owns one hop's field mapping so a new
// annotation field is added here once per boundary instead of at every
// conversion site, and a helper that stops compiling is the signal that a
// boundary was missed.
//
// ExecutionMode and ModeReason are deliberately one-directional: they only
// travel out of the engine, and blocked verdicts are enforced when an apply
// is queued rather than re-checked engine-side. Do not add a helper that
// carries them back toward the engine without restoring an engine-side check.
//
// Identity fields that call sites derive differently (namespace scoping,
// trimming, validation, operation mapping) are passed in explicitly: the
// boundary that owns a rule keeps it.

import (
	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/engine"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/storage"
)

// storageTableChangeFromEngine converts an engine table change to its stored
// form. namespace may be empty when the containing structure already keys by
// namespace (flat plan-level lists and per-namespace table sets).
func storageTableChangeFromEngine(tc engine.TableChange, namespace string) storage.TableChange {
	return storage.TableChange{
		Namespace:     namespace,
		Table:         tc.Table,
		DDL:           tc.DDL,
		Operation:     ddl.StatementTypeToOp(tc.Operation),
		IsUnsafe:      tc.IsUnsafe,
		UnsafeReason:  tc.UnsafeReason,
		ExecutionMode: tc.ExecutionMode,
		ModeReason:    tc.ModeReason,
	}
}

// protoTableChangeFromEngine converts an engine table change to its proto
// wire form for plan responses.
func protoTableChangeFromEngine(tc engine.TableChange, namespace string) *ternv1.TableChange {
	return &ternv1.TableChange{
		Namespace:     namespace,
		TableName:     tc.Table,
		Ddl:           tc.DDL,
		ChangeType:    changeTypeToProto(tc.Operation),
		IsUnsafe:      tc.IsUnsafe,
		UnsafeReason:  tc.UnsafeReason,
		ExecutionMode: tc.ExecutionMode,
		ModeReason:    tc.ModeReason,
	}
}

// StorageTableChangeFromProto builds a storage.TableChange from a proto table
// change. The identity fields — namespace, table, DDL text, and operation —
// are passed in because each proto→storage boundary derives them under its
// own rules (trimming, fail-closed validation, change-type mapping); the
// advisory annotations are copied verbatim from the proto message.
func StorageTableChangeFromProto(ch *ternv1.TableChange, namespace, table, ddlText, operation string) storage.TableChange {
	return storage.TableChange{
		Namespace:     namespace,
		Table:         table,
		DDL:           ddlText,
		Operation:     operation,
		IsUnsafe:      ch.IsUnsafe,
		UnsafeReason:  ch.UnsafeReason,
		ExecutionMode: ch.ExecutionMode,
		ModeReason:    ch.ModeReason,
	}
}
