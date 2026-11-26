package postgres

import (
	"encoding/json"
	"fmt"
	"slices"

	pgplan "github.com/block/pg-sprite/pkg/plan"
	"github.com/block/pg-sprite/pkg/planner"
	"github.com/block/pg-sprite/pkg/schemadiff"
	pgstatement "github.com/block/pg-sprite/pkg/statement"
)

// reviewedDropMetadataKey is the engine.TableChange.Metadata key under which
// the plan records the live object a destructive step drops, as the planner
// read it. The apply checks the object against that record under the table's
// lock and drops nothing when it no longer matches, so the apply removes the
// object the operator reviewed rather than whatever holds its name by then.
const reviewedDropMetadataKey = "postgres_reviewed_drop"

// dropsLiveObject reports whether sql discards live structure, using the
// planner's own list of destructive operation shapes so the plan and the
// apply agree on which steps need a reviewed record.
func dropsLiveObject(sql string) (bool, error) {
	ops, err := pgstatement.ParseOps(sql)
	if err != nil {
		return false, fmt.Errorf("parse operations: %w", err)
	}
	destructive := planner.DestructiveOpKinds()
	for _, op := range ops {
		if slices.Contains(destructive, op.Kind) {
			return true, nil
		}
	}
	return false, nil
}

// reviewedDropMetadata is the metadata a destructive step carries from plan
// to apply.
func reviewedDropMetadata(dropped schemadiff.DroppedObject) (map[string]string, error) {
	raw, err := json.Marshal(dropped)
	if err != nil {
		return nil, fmt.Errorf("encode the reviewed drop of %s %q: %w", dropped.Kind, dropped.Name, err)
	}
	return map[string]string{reviewedDropMetadataKey: string(raw)}, nil
}

// reviewedDropFromMetadata reads back the record reviewedDropMetadata wrote.
// It returns nil when the change carries none, and an error when the record
// is present but does not name an object and its definition, since an apply
// cannot check a drop against a record that says nothing.
func reviewedDropFromMetadata(metadata map[string]string) (*schemadiff.DroppedObject, error) {
	raw, ok := metadata[reviewedDropMetadataKey]
	if !ok {
		return nil, nil
	}
	var dropped schemadiff.DroppedObject
	if err := json.Unmarshal([]byte(raw), &dropped); err != nil {
		return nil, fmt.Errorf("decode the reviewed drop: %w", err)
	}
	if dropped.Kind == "" || dropped.Name == "" || dropped.Definition == "" {
		return nil, fmt.Errorf("the reviewed drop does not name the object's kind, name, and definition")
	}
	return &dropped, nil
}

// stepDropMetadata returns the metadata one rendered step of a planned
// statement carries. A step that drops an object carries the planner's
// record of that object, and only that step does: a statement the planner
// rendered as several steps drops in one of them, and pg-sprite refuses a
// record on a step that drops nothing. unrecorded reports a dropping step
// the planner left without a record, which the apply would refuse.
func stepDropMetadata(statement pgplan.Statement, sql string) (metadata map[string]string, unrecorded bool, err error) {
	drops, err := dropsLiveObject(sql)
	if err != nil {
		return nil, false, err
	}
	if !drops {
		return nil, false, nil
	}
	record := plannedDropRecord(statement)
	if record == nil {
		return nil, true, nil
	}
	metadata, err = reviewedDropMetadata(*record)
	if err != nil {
		return nil, false, err
	}
	return metadata, false, nil
}

// plannedDropRecord returns the planner's record of what a statement drops.
// A destructive statement drops live structure the operator reviews. A SET
// NOT NULL that runs as its helper sequence drops only the helper constraint
// its first step added, which leaves the statement non-destructive, and the
// planner records that helper so the drop step still checks it removes the
// plan's own helper and not whatever took its name between the steps.
func plannedDropRecord(statement pgplan.Statement) *schemadiff.DroppedObject {
	if statement.Destructive {
		return statement.Drops
	}
	return statement.DropsHelper
}
