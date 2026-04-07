package api

import (
	"maps"
	"strings"

	"github.com/block/schemabot/pkg/apitypes"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/storage"
)

// planResponseFromProto converts a protobuf PlanResponse to an HTTP PlanResponse.
func planResponseFromProto(resp *ternv1.PlanResponse) *apitypes.PlanResponse {
	httpResp := &apitypes.PlanResponse{
		PlanID:       resp.PlanId,
		Engine:       engineName(resp.Engine),
		Changes:      []*apitypes.SchemaChangeResponse{},
		LintWarnings: []*apitypes.LintWarningResponse{},
		Errors:       []string{},
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
				ChangeType:   t.ChangeType.String(),
				IsUnsafe:     t.IsUnsafe,
				UnsafeReason: t.UnsafeReason,
			})
		}
		httpResp.Changes = append(httpResp.Changes, apiSC)
	}

	for _, w := range resp.LintWarnings {
		httpResp.LintWarnings = append(httpResp.LintWarnings, &apitypes.LintWarningResponse{
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
		return "create"
	case ternv1.ChangeType_CHANGE_TYPE_ALTER:
		return "alter"
	case ternv1.ChangeType_CHANGE_TYPE_DROP:
		return "drop"
	default:
		return "other"
	}
}

// changeTypeToProto converts operation string to proto ChangeType enum.
func changeTypeToProto(op string) ternv1.ChangeType {
	switch strings.ToLower(op) {
	case "create":
		return ternv1.ChangeType_CHANGE_TYPE_CREATE
	case "alter":
		return ternv1.ChangeType_CHANGE_TYPE_ALTER
	case "drop":
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
