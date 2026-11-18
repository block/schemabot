package api

import (
	"context"
	"fmt"
	"time"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
)

// schemaFileComparisonTimeout bounds one schema file comparison, so a target
// that stops answering costs the review gate a bounded wait and the file is
// treated as changed.
const schemaFileComparisonTimeout = 30 * time.Second

// CompareSchemaFileChange asks a database's target whether a pull request's
// edit to one schema file generates the same schema change on two bases: the
// file at the approved commit's merge base and at the approved commit, and at
// the head's merge base and at the head. It runs on the environment's primary
// target, the one its plans run on. The comparison runs each version on a
// scratch schema in a rolled-back transaction, so it changes nothing there.
// An error means nothing was proved.
func (s *Service) CompareSchemaFileChange(ctx context.Context, database, environment, approvedBase, approved, headBase, head string) (same bool, reason string, err error) {
	target, err := s.config.ResolvePrimaryDatabaseTarget(database, environment)
	if err != nil {
		return false, "", fmt.Errorf("resolve target for database %q environment %q: %w", database, environment, err)
	}
	client, err := s.TernClient(target.Deployment, environment)
	if err != nil {
		return false, "", fmt.Errorf("tern client for deployment %q environment %q: %w", target.Deployment, environment, err)
	}
	ctx, cancel := context.WithTimeout(ctx, schemaFileComparisonTimeout)
	defer cancel()
	resp, err := client.CompareSchemaFileChange(ctx, &ternv1.CompareSchemaFileChangeRequest{
		Database:     database,
		Type:         target.DatabaseType,
		Environment:  environment,
		Target:       target.Target,
		ApprovedBase: approvedBase,
		Approved:     approved,
		HeadBase:     headBase,
		Head:         head,
	})
	if err != nil {
		return false, "", fmt.Errorf("compare schema file for database %q environment %q on deployment %q: %w", database, environment, target.Deployment, err)
	}
	return resp.GetSame(), resp.GetReason(), nil
}
