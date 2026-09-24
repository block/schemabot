package ddl

import ternv1 "github.com/block/schemabot/pkg/proto/ternv1"

// OpVSchemaUpdate is the operation string for Vitess vschema updates, which
// have no SQL statement type and therefore sit outside the StatementType
// vocabulary; conversions special-case it alongside the shared mapping.
const OpVSchemaUpdate = "vschema_update"

// ChangeTypeToStatementType maps proto change types represented by the shared
// DDL vocabulary. Change types outside that vocabulary are left to callers.
func ChangeTypeToStatementType(ct ternv1.ChangeType) (StatementType, bool) {
	switch ct {
	case ternv1.ChangeType_CHANGE_TYPE_CREATE:
		return StatementCreateTable, true
	case ternv1.ChangeType_CHANGE_TYPE_ALTER:
		return StatementAlterTable, true
	case ternv1.ChangeType_CHANGE_TYPE_DROP:
		return StatementDropTable, true
	case ternv1.ChangeType_CHANGE_TYPE_CREATE_INDEX:
		return StatementCreateIndex, true
	case ternv1.ChangeType_CHANGE_TYPE_DROP_INDEX:
		return StatementDropIndex, true
	case ternv1.ChangeType_CHANGE_TYPE_RENAME:
		return StatementRenameTable, true
	case ternv1.ChangeType_CHANGE_TYPE_TRUNCATE:
		return StatementTruncateTable, true
	case ternv1.ChangeType_CHANGE_TYPE_CREATE_VIEW:
		return StatementCreateView, true
	default:
		return StatementUnknown, false
	}
}

// StatementTypeToChangeType maps the shared DDL vocabulary to proto change
// types. Statements without a proto representation are reported as other.
func StatementTypeToChangeType(st StatementType) ternv1.ChangeType {
	switch st {
	case StatementCreateTable:
		return ternv1.ChangeType_CHANGE_TYPE_CREATE
	case StatementAlterTable:
		return ternv1.ChangeType_CHANGE_TYPE_ALTER
	case StatementDropTable:
		return ternv1.ChangeType_CHANGE_TYPE_DROP
	case StatementCreateIndex:
		return ternv1.ChangeType_CHANGE_TYPE_CREATE_INDEX
	case StatementDropIndex:
		return ternv1.ChangeType_CHANGE_TYPE_DROP_INDEX
	case StatementRenameTable:
		return ternv1.ChangeType_CHANGE_TYPE_RENAME
	case StatementTruncateTable:
		return ternv1.ChangeType_CHANGE_TYPE_TRUNCATE
	case StatementCreateView:
		return ternv1.ChangeType_CHANGE_TYPE_CREATE_VIEW
	default:
		return ternv1.ChangeType_CHANGE_TYPE_OTHER
	}
}
