package postgres

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"maps"

	pgplan "github.com/block/pg-sprite/pkg/plan"
	"github.com/block/pg-sprite/pkg/schemadiff"
	pgstatement "github.com/block/pg-sprite/pkg/statement"

	"github.com/block/schemabot/pkg/engine"
)

// reviewedAdoptionMetadataKey is the engine.TableChange.Metadata key under
// which the plan records the index an ADD CONSTRAINT ... USING INDEX step
// takes over, with the definition the planner matched it against. The apply
// reads the index again under the table's lock and attaches nothing when it
// no longer has that definition, so a constraint never takes over an index
// rebuilt under the same name after the plan.
const reviewedAdoptionMetadataKey = "postgres_reviewed_adoption"

// indexReplacementMetadataKey is the engine.TableChange.Metadata key under
// which the plan records, on the drop of a changed index's old definition,
// the index an earlier step built to replace it and the definition it built.
// The apply drops the old index only while that index is still valid with
// that definition, so the table is never left without the desired index.
const indexReplacementMetadataKey = "postgres_index_replacement"

// adoptedIndex returns the index sql takes over with ADD CONSTRAINT ... USING
// INDEX, and "" for a step that takes over none.
func adoptedIndex(sql string) (string, error) {
	ops, err := pgstatement.ParseOps(sql)
	if err != nil {
		return "", fmt.Errorf("parse operations: %w", err)
	}
	for _, op := range ops {
		if op.Kind == pgstatement.OpAddConstraint && op.UsingIndex && len(op.Indexes) > 0 {
			return op.Indexes[0], nil
		}
	}
	return "", nil
}

// stepAdoptionMetadata returns the metadata one rendered step of a planned
// statement carries when that step takes over an index: the planner's record
// of the index. unrecorded is set for a step that takes over an index the
// planner did not record, which the apply could not check and the caller
// blocks.
func stepAdoptionMetadata(statement pgplan.Statement, sql string) (metadata map[string]string, unrecorded bool, err error) {
	index, err := adoptedIndex(sql)
	if err != nil {
		return nil, false, err
	}
	if index == "" {
		return nil, false, nil
	}
	if statement.Adopts == nil || statement.Adopts.Index != index {
		return nil, true, nil
	}
	raw, err := json.Marshal(*statement.Adopts)
	if err != nil {
		return nil, false, fmt.Errorf("encode the reviewed adoption of index %q: %w", index, err)
	}
	return map[string]string{reviewedAdoptionMetadataKey: string(raw)}, false, nil
}

// stepReplacementMetadata returns the metadata the drop step of a replaced
// index carries: the planner's record of the index that replaces it. Every
// other step carries none.
func stepReplacementMetadata(statement pgplan.Statement, sql string) (map[string]string, error) {
	if statement.Replacement == nil {
		return nil, nil
	}
	drops, err := dropsLiveObject(sql)
	if err != nil {
		return nil, err
	}
	if !drops {
		return nil, nil
	}
	raw, err := json.Marshal(*statement.Replacement)
	if err != nil {
		return nil, fmt.Errorf("encode the replacement of index %q: %w", statement.Replacement.Index, err)
	}
	return map[string]string{indexReplacementMetadataKey: string(raw)}, nil
}

// reviewedAdoptionForApply returns the plan's record of the index a statement
// takes over with ADD CONSTRAINT ... USING INDEX, and nil for any other
// statement. An adoption is admitted only with a record that names that
// index, since without one the apply could not tell the reviewed index from
// one rebuilt under its name after the plan.
func reviewedAdoptionForApply(database string, tc engine.TableChange, statement string) (*schemadiff.IndexAdoption, error) {
	index, err := adoptedIndex(statement)
	if err != nil {
		slog.Warn("PostgreSQL apply admission could not classify a planned index adoption", "database", database, "table", tc.Table, "error", err)
		return nil, fmt.Errorf("apply PostgreSQL table %q: planned statement could not be classified", tc.Table)
	}
	if index == "" {
		return nil, nil
	}
	raw, ok := tc.Metadata[reviewedAdoptionMetadataKey]
	if !ok {
		slog.Warn("PostgreSQL apply admission refused an index adoption the plan did not record", "database", database, "table", tc.Table, "index", index)
		return nil, fmt.Errorf("apply PostgreSQL table %q: the plan has no record of the index this statement's constraint takes over; re-plan and apply again", tc.Table)
	}
	var adopts schemadiff.IndexAdoption
	if err := json.Unmarshal([]byte(raw), &adopts); err != nil || adopts.Index != index || adopts.Def == "" {
		slog.Warn("PostgreSQL apply admission rejected the reviewed index adoption record", "database", database, "table", tc.Table, "index", index, "error", err)
		return nil, fmt.Errorf("apply PostgreSQL table %q: the plan's record of the index this statement's constraint takes over is unreadable; re-plan and apply again", tc.Table)
	}
	return &adopts, nil
}

// indexReplacementForApply returns the plan's record of the index that
// replaces the one a statement drops, and nil for a drop that removes an
// index the desired schema no longer has. A record is read only for an index
// drop, and must name an index and its definition.
func indexReplacementForApply(database string, tc engine.TableChange, dropped *schemadiff.DroppedObject) (*schemadiff.IndexReplacement, error) {
	raw, ok := tc.Metadata[indexReplacementMetadataKey]
	if !ok {
		return nil, nil
	}
	if dropped == nil || dropped.Kind != schemadiff.DroppedIndex {
		slog.Warn("PostgreSQL apply admission refused an index replacement record on a statement that drops no index", "database", database, "table", tc.Table)
		return nil, fmt.Errorf("apply PostgreSQL table %q: the plan records a replacement index on a statement that drops no index; re-plan and apply again", tc.Table)
	}
	var replacement schemadiff.IndexReplacement
	if err := json.Unmarshal([]byte(raw), &replacement); err != nil || replacement.Index == "" || replacement.Def == "" {
		slog.Warn("PostgreSQL apply admission rejected the index replacement record", "database", database, "table", tc.Table, "index", dropped.Name, "error", err)
		return nil, fmt.Errorf("apply PostgreSQL table %q: the plan's record of the index that replaces %q is unreadable; re-plan and apply again", tc.Table, dropped.Name)
	}
	return &replacement, nil
}

// mergeStepMetadata copies each of extra into metadata, allocating it when
// needed.
func mergeStepMetadata(metadata map[string]string, extra ...map[string]string) map[string]string {
	for _, m := range extra {
		if len(m) == 0 {
			continue
		}
		if metadata == nil {
			metadata = make(map[string]string, len(m))
		}
		maps.Copy(metadata, m)
	}
	return metadata
}
