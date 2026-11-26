package postgres

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/block/pg-sprite/pkg/diffplan"
	pgplan "github.com/block/pg-sprite/pkg/plan"
	"github.com/block/pg-sprite/pkg/preflight"
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
// file. Every label the type lacks is one ALTER TYPE ... ADD VALUE change,
// executable when the connected role may alter the type and blocked with
// the grant it needs when it may not.
//
// INV: RV-4 — an enum change SchemaBot cannot apply is blocked at plan time.
func planEnumType(ctx context.Context, pool *pgxpool.Pool, database, namespace string, desired pgstatement.DesiredEnumType, parser ddl.StatementParser) ([]engine.TableChange, error) {
	report, err := diffplan.PlanEnumType(ctx, pool, namespace, desired)
	if err != nil {
		return nil, fmt.Errorf("plan PostgreSQL enum type %q: %w", desired.Name(), err)
	}
	changes, err := enumTypeChanges(database, namespace, report, parser)
	if err != nil {
		return nil, err
	}
	check := func(ctx context.Context) error {
		_, err := preflight.CheckEnumTarget(ctx, pool, pgstatement.TypeName{Schema: report.Schema, Name: report.Type})
		return err
	}
	return blockEnumTypeWithoutAccess(ctx, database, report, changes, check)
}

// enumTypeChanges maps an enum type's planned statements to the changes the
// operator reviews, one per added label, named by the type in namespace, in
// the order they must run. Each step takes the verdict the planner's route
// gives it, the same mapping a table's steps take, so a label addition the
// planner does not route to a path SchemaBot runs is blocked rather than
// applied. A planned statement that is not a label addition to this type, in
// this namespace, is an error rather than a rendered change, so a planner
// that grows a new enum statement kind fails the plan instead of reaching
// apply under a name SchemaBot never reviewed.
func enumTypeChanges(database, namespace string, report pgplan.Report, parser ddl.StatementParser) ([]engine.TableChange, error) {
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
		mode, reason := "", ""
		verdict := executionVerdict(report.FormatVersion, statement, report.Type)
		if verdict.mode != "" {
			// The table mapping words its reasons for a table, so the
			// blocked step gets a sentence of its own that names the type
			// and the label. What the planner returned goes to the log.
			mode = engine.ExecutionModeBlocked
			reason = enumLabelBlockedReason(report.Type, label)
			slog.Warn("PostgreSQL planner did not route an enum label addition to a path SchemaBot runs; the plan blocks the step",
				"database", database,
				"namespace", namespace,
				"enum_type", report.Type,
				"label", label,
				"disposition", statement.Disposition,
				"route", statement.Route,
				"backend", statement.Backend)
		}
		changes = append(changes, engine.TableChange{
			Table:         report.Type,
			Operation:     ddl.StatementAlterType,
			DDL:           statement.SQL,
			ExecutionMode: mode,
			ModeReason:    engine.SanitizeBlockedCause(reason),
		})
	}
	return changes, nil
}

// enumLabelBlockedReason is the blocked reason for a label addition the
// planner did not route to a path SchemaBot runs. It names the label, so a
// type gaining several labels lists one distinct line per statement.
func enumLabelBlockedReason(typeName, label string) string {
	return fmt.Sprintf("adding label %q to enum type %q needs an execution path SchemaBot's PostgreSQL support does not provide yet", label, typeName)
}

// blockEnumTypeWithoutAccess proves, at plan time, that the connected role
// may alter the type, so a missing grant is named on the plan instead of
// failing the apply after earlier steps ran. The apply proves it again in
// the session that executes. A refusal that names something to provision
// blocks every executable step with the grant; a type that is no longer an
// enum fails the plan, since the planner just read it; and a check that
// cannot answer fails the plan rather than leave the steps executable while
// access is unknown.
func blockEnumTypeWithoutAccess(ctx context.Context, database string, report pgplan.Report, changes []engine.TableChange, check func(context.Context) error) ([]engine.TableChange, error) {
	if !hasExecutableChanges(changes) {
		return changes, nil
	}
	err := check(ctx)
	if err == nil {
		return changes, nil
	}
	r := classifyRefusal(&enumTypeError{err: err}, report.Type)
	if r == nil || !r.provisionable {
		return nil, fmt.Errorf("check access to PostgreSQL enum type %q.%q: %w", report.Schema, report.Type, err)
	}
	slog.Info("PostgreSQL plan blocks enum label additions the engine role cannot run",
		"database", database,
		"namespace", report.Schema,
		"enum_type", report.Type,
		"refusal", r.reason)
	blockExecutableChanges(changes, r.detail)
	return changes, nil
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
			return &engine.PlanRefusedError{Err: fmt.Errorf("plan PostgreSQL namespace %q: enum type %q is declared by both schema files %q and %q. Declare each type in exactly one schema file", namespace, enumType.Name(), first, filename)}
		}
		declaredBy[enumType.Name()] = filename
	}
	return nil
}
