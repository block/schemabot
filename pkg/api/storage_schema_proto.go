package api

import (
	"github.com/block/schemabot/pkg/apitypes"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/schema"
)

// A data plane owns its storage database, and an operator's workstation
// generally cannot dial it. So the control plane asks over the connection the
// two already have, and a report crosses that connection in its wire form.
// These conversions are the only place the report's shape and the proto
// message have to agree.

// StorageSchemaReportProto converts a report to its wire form, for a data
// plane answering the control plane's storage-schema RPC.
func StorageSchemaReportProto(r *StorageSchemaReport) *ternv1.StorageSchemaReport {
	if r == nil {
		return nil
	}
	return &ternv1.StorageSchemaReport{
		Dialect:            string(r.Dialect),
		Database:           r.Database,
		Host:               r.Host,
		SchemaSource:       r.SchemaSource,
		Version:            r.Version,
		Outstanding:        storageSchemaStatementsProto(r.Outstanding),
		Destructive:        storageSchemaStatementsProto(r.Destructive),
		DestructiveAllowed: r.DestructiveAllowed,
		Manual:             storageSchemaStatementsProto(r.Manual),

		ConvergenceInFlight: r.ConvergenceInFlight,
		BootRemovalPolicy:   bootRemovalPolicyProto(r.BootRemovalPolicy),
	}
}

// StorageSchemaReportFromProto converts a report back from its wire form, for
// a control plane rendering what a data plane reported. A nil message yields a
// nil report: an RPC that answered with no report at all is a different
// condition from one that reported convergence, and the caller decides which
// error to raise rather than having "converged" invented here.
func StorageSchemaReportFromProto(p *ternv1.StorageSchemaReport) *StorageSchemaReport {
	if p == nil {
		return nil
	}
	return &StorageSchemaReport{
		Dialect:            schema.Dialect(p.GetDialect()),
		Database:           p.GetDatabase(),
		Host:               p.GetHost(),
		SchemaSource:       p.GetSchemaSource(),
		Version:            p.GetVersion(),
		Outstanding:        storageSchemaStatementsFromProto(p.GetOutstanding()),
		Destructive:        storageSchemaStatementsFromProto(p.GetDestructive()),
		DestructiveAllowed: p.GetDestructiveAllowed(),
		Manual:             storageSchemaStatementsFromProto(p.GetManual()),

		ConvergenceInFlight: p.GetConvergenceInFlight(),
		BootRemovalPolicy:   bootRemovalPolicyFromProto(p.GetBootRemovalPolicy()),
	}
}

// bootRemovalPolicyProto and bootRemovalPolicyFromProto carry what a boot does
// to surplus storage state across the wire.
//
// Both map an unrecognized value to unknown rather than to preservation. A data
// plane older than the field leaves it at the zero value, and a newer one may
// send a policy this binary has no name for; in both cases the honest answer is
// that this side could not be told, and the reassuring answer is the one that
// gets an operator to pre-apply storage the next pod will drop.
func bootRemovalPolicyProto(policy apitypes.BootRemovalPolicy) ternv1.BootRemovalPolicy {
	switch policy {
	case apitypes.BootRemovalPreserves:
		return ternv1.BootRemovalPolicy_BOOT_REMOVAL_POLICY_PRESERVES
	case apitypes.BootRemovalRemoves:
		return ternv1.BootRemovalPolicy_BOOT_REMOVAL_POLICY_REMOVES
	case apitypes.BootRemovalUnknown:
		return ternv1.BootRemovalPolicy_BOOT_REMOVAL_POLICY_UNSPECIFIED
	}
	return ternv1.BootRemovalPolicy_BOOT_REMOVAL_POLICY_UNSPECIFIED
}

func bootRemovalPolicyFromProto(policy ternv1.BootRemovalPolicy) apitypes.BootRemovalPolicy {
	switch policy {
	case ternv1.BootRemovalPolicy_BOOT_REMOVAL_POLICY_PRESERVES:
		return apitypes.BootRemovalPreserves
	case ternv1.BootRemovalPolicy_BOOT_REMOVAL_POLICY_REMOVES:
		return apitypes.BootRemovalRemoves
	case ternv1.BootRemovalPolicy_BOOT_REMOVAL_POLICY_UNSPECIFIED:
		return apitypes.BootRemovalUnknown
	}
	return apitypes.BootRemovalUnknown
}

func storageSchemaStatementsProto(statements []StorageSchemaStatement) []*ternv1.StorageSchemaStatement {
	if len(statements) == 0 {
		return nil
	}
	out := make([]*ternv1.StorageSchemaStatement, 0, len(statements))
	for _, s := range statements {
		out = append(out, &ternv1.StorageSchemaStatement{
			Table:     s.Table,
			Operation: s.Operation,
			Ddl:       s.DDL,
			Reason:    s.Reason,
		})
	}
	return out
}

func storageSchemaStatementsFromProto(statements []*ternv1.StorageSchemaStatement) []StorageSchemaStatement {
	if len(statements) == 0 {
		return nil
	}
	out := make([]StorageSchemaStatement, 0, len(statements))
	for _, s := range statements {
		out = append(out, StorageSchemaStatement{
			Table:     s.GetTable(),
			Operation: s.GetOperation(),
			DDL:       s.GetDdl(),
			Reason:    s.GetReason(),
		})
	}
	return out
}
