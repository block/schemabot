package postgres

import (
	"context"
	"fmt"

	"github.com/block/pg-sprite/pkg/diffplan"
	pgplan "github.com/block/pg-sprite/pkg/plan"
	"github.com/block/pg-sprite/pkg/schemadiff"
	pgstatement "github.com/block/pg-sprite/pkg/statement"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/engine"
)

// planEnumType plans one enum types file against the live type of the same
// name in the namespace. A type that already carries every label the file
// lists plans no change. A file the live type cannot converge onto (a
// missing type, a name that holds another kind of type, a dropped, renamed
// or reordered label) fails the plan with the planner's refusal, since
// PostgreSQL cannot drop or move an enum label and no plan could reach the
// file. Every label the type lacks is one ALTER TYPE ... ADD VALUE change.
//
// INV: RV-4 — an enum change SchemaBot cannot apply is blocked at plan time.
func planEnumType(ctx context.Context, pool *pgxpool.Pool, namespace string, desired pgstatement.DesiredEnumType, parser ddl.StatementParser) ([]engine.TableChange, error) {
	report, err := diffplan.PlanEnumType(ctx, pool, namespace, desired)
	if err != nil {
		return nil, fmt.Errorf("plan PostgreSQL enum type %q: %w", desired.Name(), err)
	}
	return enumTypeChanges(namespace, report, parser)
}

// enumTypeChanges maps an enum type's planned statements to the changes the
// operator reviews, one per added label, named by the type in namespace.
// Each step is
// blocked: SchemaBot does not apply enum label additions yet, so the plan
// shows what would converge the type and how to run it, and the merge gate
// holds until the type carries the labels. A planned statement that is not a
// label addition to this type, in this namespace, is an error rather than a
// rendered change, so
// a planner that grows a new enum statement kind fails the plan instead of
// reaching apply under a name SchemaBot never reviewed.
func enumTypeChanges(namespace string, report pgplan.Report, parser ddl.StatementParser) ([]engine.TableChange, error) {
	changes := make([]engine.TableChange, 0, len(report.Statements))
	for _, statement := range report.Statements {
		if statement.Kind != schemadiff.ChangeAddEnumValue {
			return nil, fmt.Errorf("plan for enum type %q holds a statement of kind %s, which SchemaBot does not render for a type", report.Type, statement.Kind)
		}
		operation, typeName, err := parser.Classify(statement.SQL)
		if err != nil {
			return nil, fmt.Errorf("classify planned statement for enum type %q: %w", report.Type, err)
		}
		if operation != ddl.StatementAlterType || typeName != report.Type {
			return nil, fmt.Errorf("planned statement for enum type %q classified as %s on %q", report.Type, operation, typeName)
		}
		parsed, err := pgstatement.ParseOne(statement.SQL)
		if err != nil {
			return nil, fmt.Errorf("parse planned label addition for enum type %q: %w", report.Type, err)
		}
		if target := parsed.EnumType(); target != (pgstatement.TypeName{Schema: namespace, Name: report.Type}) {
			return nil, fmt.Errorf("planned statement for enum type %q in schema %q adds a label to %q in schema %q", report.Type, namespace, target.Name, target.Schema)
		}
		label := parsed.AddedEnumValue()
		if label == "" {
			return nil, fmt.Errorf("planned statement for enum type %q adds no label", report.Type)
		}
		changes = append(changes, engine.TableChange{
			Table:         report.Type,
			Operation:     ddl.StatementAlterType,
			DDL:           statement.SQL,
			ExecutionMode: engine.ExecutionModeBlocked,
			ModeReason:    engine.SanitizeBlockedCause(enumLabelNotAppliedReason(report.Type, label)),
		})
	}
	return changes, nil
}

// enumLabelNotAppliedReason is the blocked reason for a label the plan adds
// to an enum type. It names the label, so a type gaining several labels
// lists one distinct line per statement.
func enumLabelNotAppliedReason(typeName, label string) string {
	return fmt.Sprintf("adds label %q to enum type %q, which SchemaBot does not apply yet; run its ALTER TYPE outside SchemaBot, then re-plan", label, typeName)
}

// refuseEnumTypeDeclaredTwice fails the plan when two schema files in one
// namespace declare the same enum type. Each file is planned against the
// live type on its own, so two declarations would get two plans with no
// single desired label list to review. The error names the type and both
// files so the operator knows which one to remove.
func refuseEnumTypeDeclaredTwice(namespace string, files map[string]pgstatement.DesiredFile) error {
	declaredBy := make(map[string]string)
	for _, filename := range sortedKeys(files) {
		enumType, ok := files[filename].EnumType()
		if !ok {
			continue
		}
		if first, declared := declaredBy[enumType.Name()]; declared {
			return fmt.Errorf("plan PostgreSQL namespace %q: enum type %q is declared by both schema files %q and %q. Declare each type in exactly one schema file", namespace, enumType.Name(), first, filename)
		}
		declaredBy[enumType.Name()] = filename
	}
	return nil
}
