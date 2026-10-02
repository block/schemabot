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
	"github.com/block/schemabot/pkg/proto/ternconv"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/storage"
)

// storageTableChangeFromEngine converts an engine table change to its stored
// form. namespace may be empty when the containing structure already keys by
// namespace (flat plan-level lists and per-namespace table sets).
func storageTableChangeFromEngine(tc engine.TableChange, namespace string) storage.TableChange {
	return storage.TableChange{
		Namespace:        namespace,
		Table:            tc.Table,
		DDL:              tc.DDL,
		Operation:        ddl.StatementTypeToOp(tc.Operation),
		IsUnsafe:         tc.IsUnsafe,
		UnsafeReason:     tc.UnsafeReason,
		ExecutionMode:    tc.ExecutionMode,
		ModeReason:       tc.ModeReason,
		EstimatedRows:    tc.EstimatedRows,
		ShardCount:       tc.ShardCount,
		LargestShardRows: tc.LargestShardRows,
		EstimatedBytes:   tc.EstimatedBytes,
		CollationChanges: storageCollationChangesFromEngine(tc.CollationChanges),
	}
}

// protoTableChangeFromEngine converts an engine table change to its proto
// wire form for plan responses.
func protoTableChangeFromEngine(tc engine.TableChange, namespace string) *ternv1.TableChange {
	return &ternv1.TableChange{
		Namespace:        namespace,
		TableName:        tc.Table,
		Ddl:              tc.DDL,
		ChangeType:       ternconv.StatementTypeToChangeType(tc.Operation),
		IsUnsafe:         tc.IsUnsafe,
		UnsafeReason:     tc.UnsafeReason,
		ExecutionMode:    tc.ExecutionMode,
		ModeReason:       tc.ModeReason,
		EstimatedRows:    tc.EstimatedRows,
		ShardCount:       int32(tc.ShardCount),
		LargestShardRows: tc.LargestShardRows,
		EstimatedBytes:   tc.EstimatedBytes,
		CollationChanges: protoCollationChangesFromEngine(tc.CollationChanges),
	}
}

// StorageTableChangeFromProto builds a storage.TableChange from a proto table
// change. The identity fields — namespace, table, DDL text, and operation —
// are passed in because each proto→storage boundary derives them under its
// own rules (trimming, fail-closed validation, change-type mapping); the
// advisory annotations are copied verbatim from the proto message.
func StorageTableChangeFromProto(ch *ternv1.TableChange, namespace, table, ddlText, operation string) storage.TableChange {
	return storage.TableChange{
		Namespace:        namespace,
		Table:            table,
		DDL:              ddlText,
		Operation:        operation,
		IsUnsafe:         ch.IsUnsafe,
		UnsafeReason:     ch.UnsafeReason,
		ExecutionMode:    ch.ExecutionMode,
		ModeReason:       ch.ModeReason,
		EstimatedRows:    ch.EstimatedRows,
		ShardCount:       int(ch.ShardCount),
		LargestShardRows: ch.LargestShardRows,
		EstimatedBytes:   ch.EstimatedBytes,
		CollationChanges: storageCollationChangesFromProto(ch.CollationChanges),
	}
}

func storageCollationChangesFromEngine(changes []engine.CollationChange) []storage.CollationChange {
	if len(changes) == 0 {
		return nil
	}
	out := make([]storage.CollationChange, len(changes))
	for i, c := range changes {
		out[i] = storage.CollationChange{
			Column:         c.Column,
			From:           c.From,
			To:             c.To,
			Case:           string(c.Case),
			TrailingSpaces: string(c.TrailingSpaces),
			UniqueIndexes:  c.UniqueIndexes,
		}
	}
	return out
}

func protoCollationChangesFromEngine(changes []engine.CollationChange) []*ternv1.CollationChange {
	if len(changes) == 0 {
		return nil
	}
	out := make([]*ternv1.CollationChange, len(changes))
	for i, c := range changes {
		out[i] = &ternv1.CollationChange{
			Column:                  c.Column,
			FromCollation:           c.From,
			ToCollation:             c.To,
			CaseComparison:          string(c.Case),
			TrailingSpaceComparison: string(c.TrailingSpaces),
			UniqueIndexes:           c.UniqueIndexes,
		}
	}
	return out
}

func storageCollationChangesFromProto(changes []*ternv1.CollationChange) []storage.CollationChange {
	if len(changes) == 0 {
		return nil
	}
	out := make([]storage.CollationChange, len(changes))
	for i, c := range changes {
		out[i] = storage.CollationChange{
			Column:         c.GetColumn(),
			From:           c.GetFromCollation(),
			To:             c.GetToCollation(),
			Case:           c.GetCaseComparison(),
			TrailingSpaces: c.GetTrailingSpaceComparison(),
			UniqueIndexes:  c.GetUniqueIndexes(),
		}
	}
	return out
}
