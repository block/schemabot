// Package ternconv converts between the shared DDL vocabulary in pkg/ddl and
// the tern wire enums in ternv1. Every boundary that crosses between the two
// (API progress, API proto conversion, tern state conversion) shares these
// mappings, so a change kind added at one boundary cannot be misreported at
// another. Keeping the conversions here rather than in pkg/ddl leaves that
// package free of wire types and of the gRPC runtime they pull in.
package ternconv

import (
	"github.com/block/schemabot/pkg/ddl"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
)

// OpVSchemaUpdate is the operation string for Vitess vschema updates, which
// have no SQL statement type and therefore sit outside the StatementType
// vocabulary; conversions special-case it alongside the shared mapping.
const OpVSchemaUpdate = "vschema_update"

// ChangeTypeToStatementType maps proto change types represented by the shared
// DDL vocabulary. Change types outside that vocabulary are left to callers.
func ChangeTypeToStatementType(ct ternv1.ChangeType) (ddl.StatementType, bool) {
	switch ct {
	case ternv1.ChangeType_CHANGE_TYPE_CREATE:
		return ddl.StatementCreateTable, true
	case ternv1.ChangeType_CHANGE_TYPE_ALTER:
		return ddl.StatementAlterTable, true
	case ternv1.ChangeType_CHANGE_TYPE_DROP:
		return ddl.StatementDropTable, true
	case ternv1.ChangeType_CHANGE_TYPE_CREATE_INDEX:
		return ddl.StatementCreateIndex, true
	case ternv1.ChangeType_CHANGE_TYPE_DROP_INDEX:
		return ddl.StatementDropIndex, true
	case ternv1.ChangeType_CHANGE_TYPE_RENAME:
		return ddl.StatementRenameTable, true
	case ternv1.ChangeType_CHANGE_TYPE_TRUNCATE:
		return ddl.StatementTruncateTable, true
	case ternv1.ChangeType_CHANGE_TYPE_CREATE_VIEW:
		return ddl.StatementCreateView, true
	default:
		return ddl.StatementUnknown, false
	}
}

// StatementTypeToChangeType maps the shared DDL vocabulary to proto change
// types. Statements without a proto representation are reported as other.
func StatementTypeToChangeType(st ddl.StatementType) ternv1.ChangeType {
	switch st {
	case ddl.StatementCreateTable:
		return ternv1.ChangeType_CHANGE_TYPE_CREATE
	case ddl.StatementAlterTable:
		return ternv1.ChangeType_CHANGE_TYPE_ALTER
	case ddl.StatementDropTable:
		return ternv1.ChangeType_CHANGE_TYPE_DROP
	case ddl.StatementCreateIndex:
		return ternv1.ChangeType_CHANGE_TYPE_CREATE_INDEX
	case ddl.StatementDropIndex:
		return ternv1.ChangeType_CHANGE_TYPE_DROP_INDEX
	case ddl.StatementRenameTable:
		return ternv1.ChangeType_CHANGE_TYPE_RENAME
	case ddl.StatementTruncateTable:
		return ternv1.ChangeType_CHANGE_TYPE_TRUNCATE
	case ddl.StatementCreateView:
		return ternv1.ChangeType_CHANGE_TYPE_CREATE_VIEW
	default:
		return ternv1.ChangeType_CHANGE_TYPE_OTHER
	}
}
