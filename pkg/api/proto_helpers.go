package api

import (
	"maps"
	"strings"

	"github.com/block/spirit/pkg/statement"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/ddl"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/storage"
)

// planResponseFromProto converts a protobuf PlanResponse to an HTTP PlanResponse.
func planResponseFromProto(resp *ternv1.PlanResponse) *apitypes.PlanResponse {
	httpResp := &apitypes.PlanResponse{
		PlanID:      resp.PlanId,
		Engine:      engineName(resp.Engine),
		Changes:     []*apitypes.SchemaChangeResponse{},
		LintResults: []*apitypes.LintViolationResponse{},
		Errors:      []string{},
	}

	if len(resp.Errors) > 0 {
		httpResp.Errors = resp.Errors
	}

	for _, sc := range resp.Changes {
		apiSC := &apitypes.SchemaChangeResponse{
			Namespace: sc.Namespace,
			Metadata:  sc.Metadata,
		}
		for _, t := range sc.TableChanges {
			apiSC.TableChanges = append(apiSC.TableChanges, &apitypes.TableChangeResponse{
				TableName:    t.TableName,
				Namespace:    t.Namespace,
				DDL:          t.Ddl,
				ChangeType:   protoChangeTypeToOperation(t.ChangeType),
				IsUnsafe:     t.IsUnsafe,
				UnsafeReason: t.UnsafeReason,
			})
		}
		httpResp.Changes = append(httpResp.Changes, apiSC)
	}

	for _, w := range resp.LintViolations {
		httpResp.LintResults = append(httpResp.LintResults, &apitypes.LintViolationResponse{
			Message:  w.Message,
			Table:    w.Table,
			Column:   w.Column,
			Linter:   w.Linter,
			Severity: w.Severity,
			FixType:  w.FixType,
		})
	}

	return httpResp
}

// protoChangesToNamespaces converts proto SchemaChanges to storage namespace plan data.
func protoChangesToNamespaces(changes []*ternv1.SchemaChange) map[string]*storage.NamespacePlanData {
	result := make(map[string]*storage.NamespacePlanData)
	for _, sc := range changes {
		ns := sc.Namespace
		if ns == "" {
			ns = "default"
		}
		nsData := &storage.NamespacePlanData{}
		for _, t := range sc.TableChanges {
			nsData.Tables = append(nsData.Tables, storage.TableChange{
				Table:     t.TableName,
				DDL:       t.Ddl,
				Operation: protoChangeTypeToOperation(t.ChangeType),
			})
		}
		result[ns] = nsData
	}
	return result
}

// storageToProtoTableChanges converts storage TableChange slice to proto TableChange slice.
func storageToProtoTableChanges(changes []storage.TableChange) []*ternv1.TableChange {
	tables := make([]*ternv1.TableChange, len(changes))
	for i, c := range changes {
		tables[i] = &ternv1.TableChange{
			TableName:  c.Table,
			Ddl:        c.DDL,
			ChangeType: changeTypeToProto(c.Operation),
		}
	}
	return tables
}

// protoChangeTypeToOperation converts a proto ChangeType enum to a storage operation string.
func protoChangeTypeToOperation(ct ternv1.ChangeType) string {
	switch ct {
	case ternv1.ChangeType_CHANGE_TYPE_CREATE:
		return ddl.StatementTypeToOp(statement.StatementCreateTable)
	case ternv1.ChangeType_CHANGE_TYPE_ALTER:
		return ddl.StatementTypeToOp(statement.StatementAlterTable)
	case ternv1.ChangeType_CHANGE_TYPE_DROP:
		return ddl.StatementTypeToOp(statement.StatementDropTable)
	case ternv1.ChangeType_CHANGE_TYPE_VSCHEMA:
		return "vschema_update"
	default:
		return "other"
	}
}

// changeTypeToProto converts operation string to proto ChangeType enum.
func changeTypeToProto(op string) ternv1.ChangeType {
	if strings.EqualFold(op, "vschema_update") {
		return ternv1.ChangeType_CHANGE_TYPE_VSCHEMA
	}
	switch ddl.OpToStatementType(op) {
	case statement.StatementCreateTable:
		return ternv1.ChangeType_CHANGE_TYPE_CREATE
	case statement.StatementAlterTable:
		return ternv1.ChangeType_CHANGE_TYPE_ALTER
	case statement.StatementDropTable:
		return ternv1.ChangeType_CHANGE_TYPE_DROP
	default:
		return ternv1.ChangeType_CHANGE_TYPE_OTHER
	}
}

// protoToSchemaFiles converts proto SchemaFiles (per-keyspace with separate sql_files
// and vschema_file) to the engine's schema.SchemaFiles (per-namespace with a unified
// Files map).
func protoToSchemaFiles(sf map[string]*ternv1.SchemaFiles) schema.SchemaFiles {
	result := make(schema.SchemaFiles, len(sf))
	for ns, ksFiles := range sf {
		files := make(map[string]string, len(ksFiles.Files))
		maps.Copy(files, ksFiles.Files)
		result[ns] = &schema.Namespace{Files: files}
	}
	return result
}
