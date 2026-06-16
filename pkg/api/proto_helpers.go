package api

import (
	"fmt"
	"maps"
	"strings"

	"github.com/block/spirit/pkg/statement"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/ddl"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/storage"
)

const vSchemaArtifactName = "vschema.json"

func pullSchemaResponseFromProto(resp *ternv1.PullSchemaResponse) *apitypes.PullSchemaResponse {
	return &apitypes.PullSchemaResponse{
		Database:    resp.Database,
		Type:        resp.Type,
		Environment: resp.Environment,
		Namespaces:  protoPulledNamespacesToAPI(resp.Namespaces),
		TableCount:  resp.TableCount,
	}
}

func protoPulledNamespacesToAPI(namespaces map[string]*ternv1.PulledNamespace) map[string]*apitypes.PulledNamespace {
	result := make(map[string]*apitypes.PulledNamespace, len(namespaces))
	for namespace, pulled := range namespaces {
		if pulled == nil {
			result[namespace] = &apitypes.PulledNamespace{Tables: map[string]string{}}
			continue
		}
		tables := make(map[string]string, len(pulled.Tables))
		maps.Copy(tables, pulled.Tables)
		artifacts := make(map[string]string, len(pulled.Artifacts))
		maps.Copy(artifacts, pulled.Artifacts)
		result[namespace] = &apitypes.PulledNamespace{Tables: tables, Artifacts: artifacts}
	}
	return result
}

func protoSchemaFilesToAPI(sf map[string]*ternv1.SchemaFiles) map[string]*apitypes.SchemaFiles {
	result := make(map[string]*apitypes.SchemaFiles, len(sf))
	for namespace, nsFiles := range sf {
		if nsFiles == nil {
			result[namespace] = &apitypes.SchemaFiles{Files: map[string]string{}}
			continue
		}
		files := make(map[string]string, len(nsFiles.Files))
		maps.Copy(files, nsFiles.Files)
		result[namespace] = &apitypes.SchemaFiles{Files: files}
	}
	return result
}

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
// SchemaChange is namespace-scoped, so duplicate namespace entries are rejected
// instead of merged or overwritten.
func protoChangesToNamespaces(changes []*ternv1.SchemaChange, schemaFiles map[string]*ternv1.SchemaFiles) (map[string]*storage.NamespacePlanData, error) {
	result := make(map[string]*storage.NamespacePlanData)
	for i, sc := range changes {
		if sc == nil {
			return nil, fmt.Errorf("schema change %d is null", i)
		}
		ns := sc.Namespace
		if ns == "" {
			ns = "default"
		}
		if _, ok := result[ns]; ok {
			return nil, fmt.Errorf("duplicate schema change namespace %q", ns)
		}
		nsData := &storage.NamespacePlanData{}
		for _, t := range sc.TableChanges {
			nsData.Tables = append(nsData.Tables, storage.TableChange{
				Table:     t.TableName,
				DDL:       t.Ddl,
				Operation: protoChangeTypeToOperation(t.ChangeType),
			})
		}
		if len(sc.OriginalFiles) > 0 {
			nsData.OriginalFiles = sc.OriginalFiles
		}
		if sc.OriginalFilesCaptured {
			nsData.OriginalFilesCaptured = true
			if nsData.OriginalFiles == nil {
				nsData.OriginalFiles = map[string]string{}
			}
		}
		if sc.Metadata["vschema_changed"] == "true" {
			if nsFiles := schemaFiles[ns]; nsFiles != nil {
				if vschema := nsFiles.Files[vSchemaArtifactName]; vschema != "" {
					nsData.Artifacts = map[string]string{vSchemaArtifactName: vschema}
				}
			}
		}
		result[ns] = nsData
	}
	return result, nil
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

// protoToSchemaFiles converts proto SchemaFiles to the engine's schema.SchemaFiles,
// copying the unified files map for each namespace. A nil namespace value yields an
// empty Files map (GetFiles is nil-safe).
func protoToSchemaFiles(sf map[string]*ternv1.SchemaFiles) schema.SchemaFiles {
	result := make(schema.SchemaFiles, len(sf))
	for ns, ksFiles := range sf {
		// A nil namespace value yields an empty Files map; GetFiles is nil-safe.
		nsFiles := ksFiles.GetFiles()
		files := make(map[string]string, len(nsFiles))
		maps.Copy(files, nsFiles)
		result[ns] = &schema.Namespace{Files: files}
	}
	return result
}
