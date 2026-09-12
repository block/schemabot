package commands

import (
	"testing"

	"github.com/alecthomas/kong"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
)

// A target is either the API path or a direct connection, never a blend of the
// two. Every refusal below is a combination that would otherwise have read a
// database the operator did not name — the failure mode these flags exist to
// prevent.
func TestStorageSchemaTargetFlags_Validate(t *testing.T) {
	tests := []struct {
		name    string
		flags   storageSchemaTargetFlags
		wantErr string
	}{
		{
			name:  "no flags reads the storage of the server the CLI is pointed at",
			flags: storageSchemaTargetFlags{},
		},
		{
			name:  "deployment with its environment",
			flags: storageSchemaTargetFlags{Deployment: "west", Environment: "production"},
		},
		{
			name:  "a DSN on its own",
			flags: storageSchemaTargetFlags{DSN: "root@tcp(127.0.0.1:3306)/schemabot"},
		},
		{
			name:  "a config file on its own",
			flags: storageSchemaTargetFlags{Config: "/etc/schemabot/config.yaml"},
		},
		{
			name:    "a deployment without an environment names no single endpoint",
			flags:   storageSchemaTargetFlags{Deployment: "west"},
			wantErr: "needs -e <environment>",
		},
		{
			name:    "an environment without a deployment selects nothing",
			flags:   storageSchemaTargetFlags{Environment: "production"},
			wantErr: "without --deployment",
		},
		{
			name:    "a deployment cannot be read through a direct DSN",
			flags:   storageSchemaTargetFlags{Deployment: "west", Environment: "production", DSN: "root@tcp(127.0.0.1:3306)/schemabot"},
			wantErr: "--deployment cannot be combined with a direct connection",
		},
		{
			name:    "an environment cannot be combined with a direct DSN",
			flags:   storageSchemaTargetFlags{Environment: "production", Config: "/etc/schemabot/config.yaml"},
			wantErr: "-e cannot be combined with a direct connection",
		},
		{
			name:    "a dialect assertion has nothing to apply to through the API",
			flags:   storageSchemaTargetFlags{Dialect: "postgres"},
			wantErr: "--dialect only applies to a direct connection",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.flags.validate()
			if tc.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

// A plan names the schema it compares the database against: one selector is
// required, and naming two is refused by the parser. The plan would say which
// schema it used either way; requiring the flag is what makes the operator
// decide before they read the plan rather than after.
func TestStoragePlanCmd_RequiresADesiredSchema(t *testing.T) {
	cmd := &StoragePlanCmd{
		storageSchemaTargetFlags: storageSchemaTargetFlags{DSN: "root@tcp(127.0.0.1:3306)/schemabot"},
	}
	err := cmd.Run(t.Context(), &Globals{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing flags: --release=STRING or --schema-dir=STRING")

	parse := func(args ...string) error {
		var cli struct {
			Plan StoragePlanCmd `cmd:"" name:"plan"`
		}
		parser, err := kong.New(&cli, kong.Name("schemabot"))
		require.NoError(t, err)
		_, err = parser.Parse(append([]string{"plan"}, args...))
		return err
	}
	require.NoError(t, parse("--release", "v1.4.0"))
	require.NoError(t, parse("--schema-dir", "."))

	err = parse("--release", "v1.4.0", "--schema-dir", ".")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "--release and --schema-dir can't be used together")
}

// Passing a DSN source is what selects the direct path, so a command can tell
// which path it is on without inspecting what happened to resolve.
func TestStorageSchemaTargetFlags_Direct(t *testing.T) {
	assert.False(t, (&storageSchemaTargetFlags{}).direct())
	assert.False(t, (&storageSchemaTargetFlags{Deployment: "west", Environment: "production"}).direct())
	assert.True(t, (&storageSchemaTargetFlags{DSN: "root@tcp(127.0.0.1:3306)/schemabot"}).direct())
	assert.True(t, (&storageSchemaTargetFlags{Config: "/etc/schemabot/config.yaml"}).direct())

	// A malformed direct source stays on the direct path, where it is refused
	// by name. Reading it as "no DSN" would quietly report the storage of the
	// server the CLI happens to point at instead of the one addressed.
	assert.True(t, (&storageSchemaTargetFlags{DSN: "   "}).direct())
	_, err := resolveStorageTarget("   ", "", "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "--dsn contains only whitespace")
}

// A refused destructive statement is not counted as applied, and is not a
// failure either: the surplus state stays in place on purpose.
func TestStorageSchemaConvergenceOutcome(t *testing.T) {
	require.NoError(t, storageSchemaConvergenceOutcome(
		&apitypes.StorageSchemaReport{Database: "schemabot", Dialect: "mysql", Converged: true}))
	require.NoError(t, storageSchemaConvergenceOutcome(&apitypes.StorageSchemaReport{
		Database:    "schemabot",
		Dialect:     "mysql",
		Destructive: []apitypes.StorageSchemaStatement{{Table: "checks", DDL: "DROP TABLE `checks_old`", Reason: "drops a table"}},
	}))

	// Manual remediation aborts the whole convergence, so an unattended run has
	// to fail rather than exit 0 having converged nothing.
	err := storageSchemaConvergenceOutcome(&apitypes.StorageSchemaReport{
		Database: "schemabot",
		Host:     "db-1.example",
		Dialect:  "postgres",
		Manual: []apitypes.StorageSchemaStatement{{
			Table:  "checks",
			DDL:    `ALTER TABLE "checks" ADD COLUMN "head_sha" varchar(64) NOT NULL`,
			Reason: "column is NOT NULL without a DEFAULT",
		}},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "schemabot on db-1.example (postgres) was not converged")
	assert.Contains(t, err.Error(), "1 change(s) need manual remediation")
}

// An error names the storage that was being read. A failure that said only
// that a read failed would leave an operator unable to tell whether they had
// reached the deployment they asked for.
func TestStorageSchemaTargetSuffix(t *testing.T) {
	assert.Empty(t, storageSchemaTargetSuffix("", ""))
	assert.Equal(t, " for deployment west in production", storageSchemaTargetSuffix("west", "production"))
}
