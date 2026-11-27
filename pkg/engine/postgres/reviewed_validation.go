package postgres

import (
	"encoding/json"
	"fmt"

	pgplan "github.com/block/pg-sprite/pkg/plan"
	"github.com/block/pg-sprite/pkg/schemadiff"
	pgstatement "github.com/block/pg-sprite/pkg/statement"

	"github.com/block/schemabot/pkg/engine"
)

// reviewedValidationMetadataKey is the engine.TableChange.Metadata key under
// which the plan records the live NOT VALID constraint a VALIDATE CONSTRAINT
// step validates, as the planner read it. The apply checks the constraint
// against that record under the validation's lock and validates nothing when
// it no longer matches, so a constraint replaced under the same name after
// the plan is never validated in the reviewed one's place.
const reviewedValidationMetadataKey = "postgres_reviewed_validation"

// validatesConstraint reports whether sql is a VALIDATE CONSTRAINT.
func validatesConstraint(sql string) (bool, error) {
	ops, err := pgstatement.ParseOps(sql)
	if err != nil {
		return false, fmt.Errorf("parse operations: %w", err)
	}
	for _, op := range ops {
		if op.Kind == pgstatement.OpValidateConstraint {
			return true, nil
		}
	}
	return false, nil
}

// stepValidationMetadata returns the metadata one rendered step of a planned
// statement carries when that step is a VALIDATE CONSTRAINT: the planner's
// record of the constraint it validates. The planner records one for a live
// NOT VALID constraint it validates in place, and for a constraint it adds
// NOT VALID and validates in a later step, since that step can run apart
// from its add after a retry or a resume. unrecorded is set for a VALIDATE
// CONSTRAINT step that carries no record, which the apply could not check
// and the caller blocks.
func stepValidationMetadata(statement pgplan.Statement, sql string) (metadata map[string]string, unrecorded bool, err error) {
	validates, err := validatesConstraint(sql)
	if err != nil {
		return nil, false, err
	}
	if !validates {
		return nil, false, nil
	}
	if statement.Validates == nil {
		return nil, true, nil
	}
	raw, err := json.Marshal(*statement.Validates)
	if err != nil {
		return nil, false, fmt.Errorf("encode the reviewed validation of constraint %q: %w", statement.Validates.Constraint, err)
	}
	return map[string]string{reviewedValidationMetadataKey: string(raw)}, false, nil
}

// reviewedValidationFromMetadata reads back the record stepValidationMetadata
// wrote. It returns nil when the change carries none, and an error when the
// record is present but does not name the constraint and its definition,
// since an apply cannot check a validation against a record that says
// nothing.
func reviewedValidationFromMetadata(metadata map[string]string) (*schemadiff.ConstraintValidation, error) {
	raw, ok := metadata[reviewedValidationMetadataKey]
	if !ok {
		return nil, nil
	}
	var validates schemadiff.ConstraintValidation
	if err := json.Unmarshal([]byte(raw), &validates); err != nil {
		return nil, fmt.Errorf("decode the reviewed validation: %w", err)
	}
	if validates.Constraint == "" || validates.Def == "" {
		return nil, fmt.Errorf("the reviewed validation does not name the constraint and its definition")
	}
	return &validates, nil
}

// reviewedValidationStep reports whether a planned step is a VALIDATE
// CONSTRAINT that carries the planner's record of the constraint it
// validates, which is what lets the apply run it under the validation bound.
// A validation with no record is blocked by the plan on its own account.
func reviewedValidationStep(change engine.TableChange) (bool, error) {
	if _, recorded := change.Metadata[reviewedValidationMetadataKey]; !recorded {
		return false, nil
	}
	validates, err := validatesConstraint(change.DDL)
	if err != nil {
		return false, fmt.Errorf("classify reviewed validation: %w", err)
	}
	return validates, nil
}
