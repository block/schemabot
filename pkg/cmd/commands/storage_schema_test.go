package commands

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/schema"
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

// Naming a DSN source is what selects the direct path, so a command can tell
// which path it is on without inspecting what happened to resolve.
func TestStorageSchemaTargetFlags_Direct(t *testing.T) {
	assert.False(t, (&storageSchemaTargetFlags{}).direct())
	assert.False(t, (&storageSchemaTargetFlags{Deployment: "west", Environment: "production"}).direct())
	assert.True(t, (&storageSchemaTargetFlags{DSN: "root@tcp(127.0.0.1:3306)/schemabot"}).direct())
	assert.True(t, (&storageSchemaTargetFlags{Config: "/etc/schemabot/config.yaml"}).direct())
	assert.False(t, (&storageSchemaTargetFlags{DSN: "   "}).direct(),
		"whitespace names no DSN, and treating it as one would read a database from nowhere")
}

// A direct DSN's database family is inferred from its form, and an explicit
// --dialect wins over the inference. Guessing wrong here would diff a
// PostgreSQL database against MySQL schema files, so an unrecognizable DSN is
// an error naming the flag that resolves it.
func TestDirectStorageDialect(t *testing.T) {
	tests := []struct {
		name        string
		dsn         string
		dialectFlag string
		want        schema.Dialect
		wantErr     string
	}{
		{
			name: "Go MySQL driver DSN",
			dsn:  "root:secret@tcp(127.0.0.1:3306)/schemabot",
			want: schema.DialectMySQL,
		},
		{
			name: "PostgreSQL URL",
			dsn:  "postgres://schemabot@db.example:5432/schemabot?sslmode=require",
			want: schema.DialectPostgres,
		},
		{
			name: "libpq keyword string",
			dsn:  "host=db.example port=5432 dbname=schemabot sslmode=require",
			want: schema.DialectPostgres,
		},
		{
			name:        "--dialect settles a DSN whose form does not say",
			dsn:         "schemabot",
			dialectFlag: "postgres",
			want:        schema.DialectPostgres,
		},
		{
			name:        "--dialect is case-insensitive and tolerates padding",
			dsn:         "root@tcp(127.0.0.1:3306)/schemabot",
			dialectFlag: " MySQL ",
			want:        schema.DialectMySQL,
		},
		{
			name:        "a dialect SchemaBot does not store its state on",
			dsn:         "root@tcp(127.0.0.1:3306)/schemabot",
			dialectFlag: "sqlite",
			wantErr:     "unsupported storage dialect",
		},
		{
			name:    "a PostgreSQL connection string that does not parse",
			dsn:     "postgres://schemabot@db.example:notaport/schemabot",
			wantErr: "does not parse as one",
		},
		{
			name:    "a DSN in neither family",
			dsn:     "jdbc:mysql://db.example:3306/schemabot",
			wantErr: "cannot tell which database family --dsn addresses",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dialect, err := directStorageDialect(tc.dsn, tc.dialectFlag)
			if tc.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, dialect)
		})
	}
}

// A whitespace-only --dsn is refused rather than resolved from the environment
// behind the operator's back: they named a direct connection, so falling back
// to a config file would read a different database than the one they asked for.
func TestResolveStorageTarget_RefusesBlankAndConflictingSources(t *testing.T) {
	_, err := resolveStorageTarget("   ", "", "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "--dsn contains only whitespace")

	_, err = resolveStorageTarget("root@tcp(127.0.0.1:3306)/schemabot", "/etc/schemabot/config.yaml", "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "mutually exclusive")
}

// A direct DSN resolves to the database it names, with its family inferred from
// its form and the flag that supplied it recorded as the source — the source is
// what an error message names, so it must not be the DSN itself.
func TestResolveStorageTarget_DirectDSN(t *testing.T) {
	target, err := resolveStorageTarget("postgres://schemabot@db.example:5432/schemabot", "", "")
	require.NoError(t, err)
	assert.Equal(t, "postgres://schemabot@db.example:5432/schemabot", target.dsn)
	assert.Equal(t, schema.DialectPostgres, target.dialect)
	assert.Equal(t, "--dsn flag", target.source)
}

// A pasted section has to run as a script, so every statement is terminated
// exactly once whatever the differ emitted.
func TestStorageSchemaRunnableDDL(t *testing.T) {
	assert.Equal(t, "DROP TABLE `stale_state`;", storageSchemaRunnableDDL("DROP TABLE `stale_state`"))
	assert.Equal(t, "DROP TABLE `stale_state`;", storageSchemaRunnableDDL("DROP TABLE `stale_state`;"))
	assert.Equal(t, "DROP TABLE `stale_state`;", storageSchemaRunnableDDL("  DROP TABLE `stale_state` ;; "))
	assert.Equal(t, "", storageSchemaRunnableDDL(""), "nothing to terminate stays untouched")
}

// The headline is the line an operator reads first, so it names the database
// and the binary whose embedded schema produced the diff. The version matters:
// the answer is only meaningful against the schema files it was compared with.
func TestStorageSchemaHeadline(t *testing.T) {
	converged := storageSchemaHeadline(&apitypes.StorageSchemaReport{
		Database:  "schemabot",
		Dialect:   "mysql",
		Version:   "v1.2.3",
		Converged: true,
	})
	assert.Equal(t, "schemabot (mysql) is converged, against the schema embedded in v1.2.3.", converged)

	outstanding := storageSchemaHeadline(&apitypes.StorageSchemaReport{
		Database:    "schemabot",
		Dialect:     "postgres",
		Deployment:  "west",
		Environment: "production",
		Version:     "v1.2.3",
		Outstanding: []apitypes.StorageSchemaStatement{{Table: "applies"}, {Table: "checks"}},
		Destructive: []apitypes.StorageSchemaStatement{{Table: "stale_state"}},
		Manual:      []apitypes.StorageSchemaStatement{{Table: "plans"}},
	})
	assert.Equal(t,
		"schemabot (postgres) on deployment west in production needs 4 statements: "+
			"2 outstanding, 1 destructive, 1 needing manual remediation, against the schema embedded in v1.2.3.",
		outstanding)

	assert.Equal(t, "the storage database needs 1 statement: 1 outstanding.",
		storageSchemaHeadline(&apitypes.StorageSchemaReport{
			Outstanding: []apitypes.StorageSchemaStatement{{Table: "applies"}},
		}), "an unnamed database and a single statement both still read as a sentence")
}

// A converged report prints one line and nothing else: there is no section to
// read and no next step to take.
func TestRenderStorageSchemaReport_Converged(t *testing.T) {
	var out bytes.Buffer
	require.NoError(t, renderStorageSchemaReport(&out, &apitypes.StorageSchemaReport{
		Database:  "schemabot",
		Dialect:   "mysql",
		Converged: true,
	}, []string{"this hint has nothing to hint at"}))

	assert.Equal(t, "schemabot (mysql) is converged.\n", out.String())
}

// An outstanding report prints the statements bare and one per line, grouped
// under headings that say what will happen to each group, so a whole section
// can be selected and pasted into a client as it stands.
func TestRenderStorageSchemaReport_Sections(t *testing.T) {
	var out bytes.Buffer
	require.NoError(t, renderStorageSchemaReport(&out, &apitypes.StorageSchemaReport{
		Database: "schemabot",
		Dialect:  "mysql",
		Outstanding: []apitypes.StorageSchemaStatement{{
			Table:     "applies",
			Operation: "alter_table",
			DDL:       "ALTER TABLE `applies` ADD COLUMN `caller` varchar(255) NOT NULL DEFAULT ''",
		}},
		Destructive: []apitypes.StorageSchemaStatement{{
			Table:     "stale_state",
			Operation: "drop_table",
			DDL:       "DROP TABLE `stale_state`",
			Reason:    "DROP TABLE destroys data",
		}},
	}, []string{"Converge it with: schemabot storage apply"}))

	assert.Equal(t, ""+
		"schemabot (mysql) needs 2 statements: 1 outstanding, 1 destructive.\n"+
		"\n"+storageSchemaOutstandingTitle+" (1):\n"+
		"\n"+
		"ALTER TABLE `applies` ADD COLUMN `caller` varchar(255) NOT NULL DEFAULT '';\n"+
		"\nDestructive, and refused; surplus state stays in place (1):\n"+
		"\n"+
		"-- stale_state: DROP TABLE destroys data\n"+
		"DROP TABLE `stale_state`;\n"+
		"\nConverge it with: schemabot storage apply\n",
		out.String())
}

// The destructive heading says whether those statements would run, because that
// is the whole disposition of the section: refused means surplus state stays in
// place on purpose, permitted means the next convergence destroys it.
func TestStorageSchemaDestructiveTitle(t *testing.T) {
	assert.Contains(t, storageSchemaDestructiveTitle(&apitypes.StorageSchemaReport{}), "refused")
	assert.Contains(t, storageSchemaDestructiveTitle(&apitypes.StorageSchemaReport{DestructiveAllowed: true}), "permitted to run")
}

// A manual entry blocks everything behind it, so it is named as its own section
// rather than folded in with statements that will run on their own.
func TestRenderStorageSchemaReport_ManualSection(t *testing.T) {
	var out bytes.Buffer
	require.NoError(t, renderStorageSchemaReport(&out, &apitypes.StorageSchemaReport{
		Database: "schemabot",
		Dialect:  "postgres",
		Manual: []apitypes.StorageSchemaStatement{{
			Table:     "checks",
			Operation: "add_column",
			DDL:       `ALTER TABLE "checks" ADD COLUMN "head_sha" varchar(64) NOT NULL`,
			Reason:    "column is NOT NULL without a DEFAULT",
		}},
	}, nil))

	assert.Contains(t, out.String(), "Needs manual remediation before anything converges (1):")
	assert.Contains(t, out.String(), "-- checks: column is NOT NULL without a DEFAULT")
	assert.Contains(t, out.String(), `ALTER TABLE "checks" ADD COLUMN "head_sha" varchar(64) NOT NULL;`)
}

// A diff names the next step in the command form the operator invoked, and the
// direct form never echoes the DSN back — it may carry a password, and the
// operator already has it.
func TestStorageSchemaDiffHints(t *testing.T) {
	local := storageSchemaDiffHints(&StorageDiffCmd{})
	require.Len(t, local, 1)
	assert.Contains(t, local[0], "storage apply")
	assert.NotContains(t, local[0], "--deployment")

	deployment := storageSchemaDiffHints(&StorageDiffCmd{
		storageSchemaTargetFlags: storageSchemaTargetFlags{Deployment: "west", Environment: "production"},
	})
	require.Len(t, deployment, 1)
	assert.Contains(t, deployment[0], "storage apply --deployment west -e production")

	const secret = "root:hunter2@tcp(db.example:3306)/schemabot"
	dsn := storageSchemaDiffHints(&StorageDiffCmd{storageSchemaTargetFlags: storageSchemaTargetFlags{DSN: secret}})
	require.Len(t, dsn, 1)
	assert.Contains(t, dsn[0], "--dsn <the same DSN>")
	assert.NotContains(t, dsn[0], "hunter2")

	config := storageSchemaDiffHints(&StorageDiffCmd{
		storageSchemaTargetFlags: storageSchemaTargetFlags{Config: "/etc/schemabot/config.yaml"},
	})
	require.Len(t, config, 1)
	assert.Contains(t, config[0], "--config /etc/schemabot/config.yaml")
}

// A clean convergence states that nothing is left, rather than leaving an
// operator to read the absence of a section as good news.
func TestRenderStorageSchemaConvergence_Clean(t *testing.T) {
	var out bytes.Buffer
	planned := &apitypes.StorageSchemaReport{
		Database: "schemabot",
		Dialect:  "mysql",
		Outstanding: []apitypes.StorageSchemaStatement{
			{Table: "applies", DDL: "ALTER TABLE `applies` ADD COLUMN `caller` varchar(255) NOT NULL DEFAULT ''"},
			{Table: "checks", DDL: "CREATE TABLE `checks` (`id` BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY)"},
		},
	}
	remaining := &apitypes.StorageSchemaReport{Database: "schemabot", Dialect: "mysql", Converged: true}

	require.NoError(t, renderStorageSchemaConvergence(&out, planned, remaining))
	assert.Equal(t, ""+
		"Ran 2 statements against schemabot (mysql).\n"+
		"schemabot (mysql) is converged.\n",
		out.String())
}

// A convergence that left statements behind prints them with the reason they
// did not run, so the operator's next move is on the screen rather than in the
// documentation.
func TestRenderStorageSchemaConvergence_LeftBehind(t *testing.T) {
	var out bytes.Buffer
	planned := &apitypes.StorageSchemaReport{
		Database:    "schemabot",
		Dialect:     "mysql",
		Outstanding: []apitypes.StorageSchemaStatement{{Table: "applies", DDL: "ALTER TABLE `applies` ADD COLUMN `caller` varchar(255) NOT NULL DEFAULT ''"}},
	}
	remaining := &apitypes.StorageSchemaReport{
		Database: "schemabot",
		Dialect:  "mysql",
		Destructive: []apitypes.StorageSchemaStatement{{
			Table:  "stale_state",
			DDL:    "DROP TABLE `stale_state`",
			Reason: "DROP TABLE destroys data",
		}},
	}

	require.NoError(t, renderStorageSchemaConvergence(&out, planned, remaining))
	assert.Contains(t, out.String(), "Ran 1 statement against schemabot (mysql).")
	assert.Contains(t, out.String(), "DROP TABLE `stale_state`;")
	assert.Contains(t, out.String(), "--allow-destructive")
	assert.NotContains(t, out.String(), "is converged")
}

// A deployment's report is labelled with the deployment it came from, so an
// operator reading a control plane's answer can tell whose storage it describes.
func TestStorageSchemaDatabaseLabel(t *testing.T) {
	assert.Equal(t, "schemabot (mysql)", storageSchemaDatabaseLabel(&apitypes.StorageSchemaReport{
		Database: "schemabot", Dialect: "mysql",
	}))
	assert.Equal(t, "schemabot (postgres) on deployment west in production",
		storageSchemaDatabaseLabel(&apitypes.StorageSchemaReport{
			Database: "schemabot", Dialect: "postgres", Deployment: "west", Environment: "production",
		}))
	assert.Equal(t, "the storage database", storageSchemaDatabaseLabel(&apitypes.StorageSchemaReport{}),
		"a report with no database name still reads as a sentence")
}

// An error names the storage that was being read. A failure that said only
// that a read failed would leave an operator unable to tell whether they had
// reached the deployment they asked for.
func TestStorageSchemaTargetSuffix(t *testing.T) {
	assert.Empty(t, storageSchemaTargetSuffix("", ""))
	assert.Equal(t, " for deployment west in production", storageSchemaTargetSuffix("west", "production"))
}

// The exit status is the machine-readable half of a diff's answer: a
// pre-deploy gate has to tell "converged" from "statements outstanding" from
// "the read failed", and two of those three are not failures of the command.
func TestExitCodeFor(t *testing.T) {
	assert.Equal(t, 0, ExitCodeFor(nil))
	assert.Equal(t, 1, ExitCodeFor(errors.New("storage unreachable")))
	assert.Equal(t, ExitStorageSchemaOutstanding, ExitCodeFor(exitStorageSchemaOutstanding()))
	assert.Equal(t, ExitStorageSchemaOutstanding,
		ExitCodeFor(fmt.Errorf("wrapped: %w", exitStorageSchemaOutstanding())),
		"a wrapped request for a status is still honored")
	assert.Equal(t, 1, ExitCodeFor(&ExitCodeError{Err: errors.New("no status asked for")}))
}

// A status request prints nothing further: the report is already on the screen,
// and an "Error:" line under it would read as a failure of the read.
func TestSilentExitIsSilent(t *testing.T) {
	err := exitStorageSchemaOutstanding()
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrSilent)
}
