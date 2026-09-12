package commands

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/schema"
)

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
			// The Go MySQL driver's grammar is the specific one, so a valid
			// MySQL DSN reads as MySQL even where libpq's keyword parser would
			// also take it. Reading the string for "port=" or "user=" instead
			// would send this password to a PostgreSQL driver.
			name: "a MySQL password containing PostgreSQL keywords",
			dsn:  "root:p@ss/word=1&port=5432@tcp(127.0.0.1:3306)/schemabot",
			want: schema.DialectMySQL,
		},
		{
			name:    "a PostgreSQL connection URL that does not parse",
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

// --dialect is the operator's assertion about a DSN whose form does not say
// which family it belongs to, and it is only ever consulted on the direct path.
// Against a server config the same flag is a contradiction rather than a
// preference: the config states its own storage dialect, so a flag that
// disagrees is refused instead of silently losing to it or silently overriding
// it — either resolution would connect to the named database under the wrong
// family's differ.
func TestResolveStorageTarget_DialectAssertion(t *testing.T) {
	target, err := resolveStorageTarget("schemabot@tcp(db.example:3306)/schemabot", "", "mysql")
	require.NoError(t, err)
	assert.Equal(t, schema.DialectMySQL, target.dialect)

	path := writeStorageTestConfig(t, `
storage:
  dialect: postgres
  dsn: postgres://schemabot@storage-host:5432/schemabot
`)
	_, err = resolveStorageTarget("", path, "mysql")
	require.Error(t, err)
	assert.Contains(t, err.Error(), `--dialect says "mysql"`)
	assert.Contains(t, err.Error(), `configures "postgres" storage`)

	// A flag that agrees with the config is still a flag that does not apply
	// there, but it is not a contradiction, so the config's dialect stands.
	agreeing, err := resolveStorageTarget("", path, "postgres")
	require.NoError(t, err)
	assert.Equal(t, schema.DialectPostgres, agreeing.dialect)
}

// A DSN resolved from a server config carries that deployment's storage policy
// with it. A boot of that config converges under the policy, so an operator
// convergence against the same database has to as well (AV-9) — otherwise a
// deployment that allows destructive storage changes would have them refused by
// the command an operator reaches for, and permitted by the next pod that
// booted.
func TestResolveStorageTarget_CarriesConfigStoragePolicy(t *testing.T) {
	path := writeStorageTestConfig(t, `
storage:
  dialect: postgres
  dsn: postgres://schemabot@storage-host:5432/schemabot
  allow_destructive_schema_changes: true
postgres:
  statement_timeout: 45s
`)
	target, err := resolveStorageTarget("", path, "")
	require.NoError(t, err)
	assert.True(t, target.allowDestructive, "the config's standing policy travels with the target")
	require.NotNil(t, target.postgresStatementTimeout)
	assert.Equal(t, 45*time.Second, *target.postgresStatementTimeout)

	// A DSN passed on the command line has no config behind it, so it carries
	// no policy and the request flag is the only way to widen one.
	direct, err := resolveStorageTarget("postgres://schemabot@db.example:5432/schemabot", "", "")
	require.NoError(t, err)
	assert.False(t, direct.allowDestructive)
	assert.Nil(t, direct.postgresStatementTimeout)
}

// The request flag only ever widens the target's standing policy. A convergence
// that narrowed it would refuse statements the deployment's own next boot runs.
func TestStorageTargetAllowsDestructive_RequestOnlyWidens(t *testing.T) {
	permissive := &storageTarget{dialect: schema.DialectMySQL, allowDestructive: true}
	assert.True(t, permissive.allowsDestructive(false),
		"a config that allows destructive changes is not narrowed by a request without the flag")
	assert.True(t, permissive.allowsDestructive(true))

	strict := &storageTarget{dialect: schema.DialectMySQL}
	assert.False(t, strict.allowsDestructive(false))
	assert.True(t, strict.allowsDestructive(true),
		"the request flag widens a target with no standing policy")
	assert.Len(t, strict.ensureSchemaOptions(false), 2,
		"a target with no config statement budget leaves the package default in place")
	assert.Len(t, permissive.ensureSchemaOptions(false), 2)
}
