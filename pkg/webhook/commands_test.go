package webhook

import (
	"testing"

	"github.com/block/schemabot/pkg/webhook/action"
	"github.com/stretchr/testify/assert"
)

// TestCommandSpecs_CoverEveryDispatcherAction enforces that every command the
// dispatcher branches on has a spec in the registry. A spec missing here is
// the proximate cause of "schemabot $cmd" silently degrading to IsMention.
func TestCommandSpecs_CoverEveryDispatcherAction(t *testing.T) {
	required := []string{
		action.Help,
		action.Plan,
		action.Apply,
		action.ApplyConfirm,
		action.Unlock,
		action.FixLint,
		action.Stop,
		action.Cancel,
		action.Start,
		action.Release,
		action.Revert,
		action.SkipRevert,
		action.Cutover,
		action.Rollback,
		action.RollbackConfirm,
	}
	for _, name := range required {
		_, ok := specByName[name]
		assert.Truef(t, ok, "commandSpecs is missing %q", name)
	}
}

// TestCommandSpecs_FlagsRespected pins which commands opt into which flags.
// A flag mistakenly enabled here silently broadens command behavior; a flag
// mistakenly removed silently drops user input. Either change should be a
// deliberate, reviewable diff.
func TestCommandSpecs_FlagsRespected(t *testing.T) {
	cases := []struct {
		name                string
		requiresEnv         bool
		hasApplyID          bool
		supportsDB          bool
		supportsSkipRevert  bool
		supportsDefer       bool
		supportsAllowUnsafe bool
		supportsForce       bool
	}{
		{name: action.Help},
		{name: action.Plan, requiresEnv: true, supportsDB: true},
		{name: action.Apply, requiresEnv: true, supportsDB: true,
			supportsSkipRevert: true, supportsDefer: true, supportsAllowUnsafe: true},
		{name: action.ApplyConfirm, requiresEnv: true, supportsDB: true,
			supportsSkipRevert: true, supportsDefer: true, supportsAllowUnsafe: true},
		{name: action.Unlock, supportsDB: true, supportsForce: true},
		{name: action.FixLint, supportsDB: true},
		{name: action.Stop, requiresEnv: true, hasApplyID: true},
		{name: action.Cancel, requiresEnv: true, hasApplyID: true},
		{name: action.Start, requiresEnv: true, hasApplyID: true},
		{name: action.Release, requiresEnv: true, hasApplyID: true},
		{name: action.Revert, requiresEnv: true, hasApplyID: true},
		{name: action.SkipRevert, requiresEnv: true, hasApplyID: true},
		{name: action.Cutover, requiresEnv: true, hasApplyID: true},
		{name: action.Rollback, requiresEnv: true, hasApplyID: true},
		{name: action.RollbackConfirm, requiresEnv: true, supportsDefer: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			spec, ok := specByName[tc.name]
			assert.True(t, ok)
			assert.Equal(t, tc.requiresEnv, spec.RequiresEnv, "RequiresEnv")
			assert.Equal(t, tc.hasApplyID, spec.HasApplyID, "HasApplyID")
			assert.Equal(t, tc.supportsDB, spec.SupportsDB, "SupportsDB")
			assert.Equal(t, tc.supportsSkipRevert, spec.SupportsSkipRevert, "SupportsSkipRevert")
			assert.Equal(t, tc.supportsDefer, spec.SupportsDeferCutover, "SupportsDeferCutover")
			assert.Equal(t, tc.supportsAllowUnsafe, spec.SupportsAllowUnsafe, "SupportsAllowUnsafe")
			assert.Equal(t, tc.supportsForce, spec.SupportsForce, "SupportsForce")
		})
	}
}

func TestHasAutoConfirmFlag(t *testing.T) {
	p := NewCommandParser()
	assert.True(t, p.HasAutoConfirmFlag("schemabot apply -e staging -y"))
	assert.True(t, p.HasAutoConfirmFlag("schemabot apply -e staging --yes"))
	assert.False(t, p.HasAutoConfirmFlag("schemabot apply -e staging"))
	assert.False(t, p.HasAutoConfirmFlag(""))

	// A flag is a token on the command, not a substring of one: an
	// environment ending in `-y` is a legal environment name.
	assert.False(t, p.HasAutoConfirmFlag("schemabot apply -e staging-y"))
	assert.False(t, p.HasAutoConfirmFlag("schemabot apply -e prod --yes-really"))

	// Only the directive line carries flags. Prose and fenced CLI examples
	// describe the flag rather than pass it, so neither rejects the command.
	assert.False(t, p.HasAutoConfirmFlag("schemabot apply -e staging\n\nlast time I had to pass --yes locally"))
	assert.False(t, p.HasAutoConfirmFlag("schemabot apply -e staging\n\n```\nschemabot apply -e staging -y\n```\n"))
	assert.False(t, p.HasAutoConfirmFlag("we could pass --yes here"))

	// A sentence opening with the product name is not the directive line, so
	// a flag it mentions does not attach to the command that follows it.
	assert.False(t, p.HasAutoConfirmFlag("SchemaBot runs with -y in the CLI only.\n\nschemabot apply -e staging"))
	assert.True(t, p.HasAutoConfirmFlag("SchemaBot runs with -y in the CLI only.\n\nschemabot apply -e staging -y"))
}

func TestHasDatabaseFlag(t *testing.T) {
	p := NewCommandParser()
	assert.True(t, p.HasDatabaseFlag("schemabot rollback apply_abc123 -e staging -d users"))
	assert.False(t, p.HasDatabaseFlag("schemabot rollback apply_abc123 -e staging"))

	// Only the directive line carries flags. Prose and fenced CLI examples
	// describe the flag rather than pass it, so neither rejects the command.
	assert.False(t, p.HasDatabaseFlag("schemabot stop apply_abc123 -e production\n\nthe plan only touches -d accounts"))
	assert.False(t, p.HasDatabaseFlag("schemabot stop apply_abc123 -e production\n\n```\nschemabot unlock -d accounts\n```\n"))
	assert.False(t, p.HasDatabaseFlag("try passing -d accounts next time"))
}

func TestHasDeferCutoverFlag(t *testing.T) {
	p := NewCommandParser()
	assert.True(t, p.HasDeferCutoverFlag("schemabot rollback apply_abc123 -e staging --defer-cutover"))
	assert.False(t, p.HasDeferCutoverFlag("schemabot rollback apply_abc123 -e staging"))

	// Only the directive line carries flags. Prose and fenced CLI examples
	// describe the flag rather than pass it, so neither rejects the command.
	assert.False(t, p.HasDeferCutoverFlag("schemabot rollback apply_abc123 -e staging\n\nlast time we used --defer-cutover here"))
	assert.False(t, p.HasDeferCutoverFlag("schemabot rollback apply_abc123 -e staging\n\n```\nschemabot rollback-confirm -e staging --defer-cutover\n```\n"))
	assert.False(t, p.HasDeferCutoverFlag("we could pass --defer-cutover here"))
}

func TestParseTenantFlag(t *testing.T) {
	parser := NewCommandParser()

	tests := []struct {
		name   string
		body   string
		result CommandResult
	}{
		{
			name: "plan command",
			body: "schemabot plan -e staging --tenant alpha",
			result: CommandResult{
				Action:      action.Plan,
				Environment: "staging",
				Tenant:      "alpha",
				Found:       true,
				IsMention:   true,
			},
		},
		{
			name: "apply command",
			body: "schemabot apply -e production --tenant tenant-1 --allow-unsafe",
			result: CommandResult{
				Action:      action.Apply,
				Environment: "production",
				Tenant:      "tenant-1",
				AllowUnsafe: true,
				Found:       true,
				IsMention:   true,
			},
		},
		{
			name: "short flag",
			body: "schemabot plan -e staging -t alpha_1",
			result: CommandResult{
				Action:      action.Plan,
				Environment: "staging",
				Tenant:      "alpha_1",
				Found:       true,
				IsMention:   true,
			},
		},
		{
			name: "help command",
			body: "schemabot help --tenant alpha",
			result: CommandResult{
				Action:    action.Help,
				Tenant:    "alpha",
				IsHelp:    true,
				IsMention: true,
			},
		},
		{
			name: "invalid command",
			body: "schemabot wat --tenant alpha",
			result: CommandResult{
				Tenant:    "alpha",
				IsMention: true,
			},
		},
		{
			name: "missing value",
			body: "schemabot plan -e staging --tenant",
			result: CommandResult{
				Action:      action.Plan,
				Environment: "staging",
				TenantError: true,
				Found:       true,
				IsMention:   true,
			},
		},
		{
			name: "short flag missing value",
			body: "schemabot plan -e staging -t",
			result: CommandResult{
				Action:      action.Plan,
				Environment: "staging",
				TenantError: true,
				Found:       true,
				IsMention:   true,
			},
		},
		{
			name: "invalid value",
			body: "schemabot plan -e staging --tenant alpha@example",
			result: CommandResult{
				Action:      action.Plan,
				Environment: "staging",
				TenantError: true,
				Found:       true,
				IsMention:   true,
			},
		},
		{
			name: "tenant value cannot look like another flag",
			body: "schemabot apply -e staging --tenant --allow-unsafe",
			result: CommandResult{
				Action:      action.Apply,
				Environment: "staging",
				TenantError: true,
				AllowUnsafe: true,
				Found:       true,
				IsMention:   true,
			},
		},
		{
			name: "tenant prose after directive is ignored",
			body: "schemabot plan -e staging\n\nDo not use --tenant here.",
			result: CommandResult{
				Action:      action.Plan,
				Environment: "staging",
				Found:       true,
				IsMention:   true,
			},
		},
		{
			name: "tenant prose before directive is ignored",
			body: "Do not use --tenant here.\n\nschemabot plan -e staging",
			result: CommandResult{
				Action:      action.Plan,
				Environment: "staging",
				Found:       true,
				IsMention:   true,
			},
		},
		{
			name: "environment prose after directive is ignored",
			body: "schemabot plan\n\nUse -e staging later.",
			result: CommandResult{
				Action:     action.Plan,
				MissingEnv: true,
				IsMention:  true,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.result, parser.ParseCommand(tc.body))
		})
	}
}

func TestCommandSupportsDatabaseFlag(t *testing.T) {
	assert.True(t, commandSupportsDatabaseFlag(action.Plan))
	assert.True(t, commandSupportsDatabaseFlag(action.Apply))
	assert.False(t, commandSupportsDatabaseFlag(action.Rollback))
	assert.False(t, commandSupportsDatabaseFlag(action.RollbackConfirm))
	assert.False(t, commandSupportsDatabaseFlag("unknown"))
}

func TestParseCommand(t *testing.T) {
	parser := NewCommandParser()

	tests := []struct {
		name     string
		body     string
		expected CommandResult
	}{
		{
			name: "plan with environment",
			body: "schemabot plan -d Sample_DB -e StAgInG",
			expected: CommandResult{
				Action:      "plan",
				Environment: "staging",
				Database:    "sample_db",
				Found:       true,
				IsMention:   true,
			},
		},
		{
			name: "plan with production",
			body: "schemabot plan -e production",
			expected: CommandResult{
				Action:      "plan",
				Environment: "production",
				Found:       true,
				IsMention:   true,
			},
		},
		{
			name: "apply with custom environment (qa)",
			body: "schemabot apply -e qa",
			expected: CommandResult{
				Action:      "apply",
				Environment: "qa",
				Found:       true,
				IsMention:   true,
			},
		},
		{
			name: "apply with custom environment (load)",
			body: "schemabot apply -e load",
			expected: CommandResult{
				Action:      "apply",
				Environment: "load",
				Found:       true,
				IsMention:   true,
			},
		},
		{
			name: "env flag does not consume a following flag, and without a value is rejected",
			body: "schemabot apply -e --tenant alpha",
			expected: CommandResult{
				Tenant:    "alpha",
				IsMention: true,
			},
		},
		{
			name: "flag glued onto the environment value is invalid, not reinterpreted",
			body: "schemabot apply -e production--allow-unsafe",
			expected: CommandResult{
				Action:           "apply",
				EnvironmentError: true,
				Found:            true,
				IsMention:        true,
			},
		},
		{
			name: "environment value with a trailing dash is invalid",
			body: "schemabot apply -e staging-",
			expected: CommandResult{
				Action:           "apply",
				EnvironmentError: true,
				Found:            true,
				IsMention:        true,
			},
		},
		{
			name: "plan with an invalid environment value is not a multi-env plan",
			body: "schemabot plan -e production--allow-unsafe",
			expected: CommandResult{
				Action:           "plan",
				EnvironmentError: true,
				Found:            true,
				IsMention:        true,
			},
		},
		{
			name: "environment value with single dashes is valid",
			body: "schemabot apply -e prod-us-east",
			expected: CommandResult{
				Action:      "apply",
				Environment: "prod-us-east",
				Found:       true,
				IsMention:   true,
			},
		},
		{
			name: "plan with database flag",
			body: "schemabot plan -e staging -d my-database",
			expected: CommandResult{
				Action:      "plan",
				Environment: "staging",
				Database:    "my-database",
				Found:       true,
				IsMention:   true,
			},
		},
		{
			name: "apply with skip-revert",
			body: "schemabot apply -e staging --skip-revert",
			expected: CommandResult{
				Action:      "apply",
				Environment: "staging",
				Found:       true,
				IsMention:   true,
				SkipRevert:  true,
			},
		},
		{
			name: "apply with defer-cutover",
			body: "schemabot apply -e production --defer-cutover",
			expected: CommandResult{
				Action:       "apply",
				Environment:  "production",
				Found:        true,
				IsMention:    true,
				DeferCutover: true,
			},
		},
		{
			name: "help command",
			body: "schemabot help",
			expected: CommandResult{
				Action:    "help",
				IsHelp:    true,
				IsMention: true,
			},
		},
		{
			name: "unlock without -e",
			body: "schemabot unlock",
			expected: CommandResult{
				Action:    "unlock",
				Found:     true,
				IsMention: true,
			},
		},
		{
			name: "unlock with database and force",
			body: "schemabot unlock -d example-db --force",
			expected: CommandResult{
				Action:    "unlock",
				Database:  "example-db",
				Force:     true,
				Found:     true,
				IsMention: true,
			},
		},
		{
			name: "plan without -e (multi-env)",
			body: "schemabot plan",
			expected: CommandResult{
				Action:     "plan",
				IsMention:  true,
				MissingEnv: true,
			},
		},
		{
			name: "apply without -e (error)",
			body: "schemabot apply",
			expected: CommandResult{
				Action:     "apply",
				IsMention:  true,
				MissingEnv: true,
			},
		},
		{
			name: "mistyped command",
			body: "schemabot aply -e staging",
			expected: CommandResult{
				IsMention: true,
			},
		},
		{
			name: "mistyped command with apply ID",
			body: "schemabot stpo apply-abc123 -e staging",
			expected: CommandResult{
				IsMention: true,
			},
		},
		{
			name: "bare mention",
			body: "schemabot",
			expected: CommandResult{
				IsMention: true,
			},
		},
		{
			name:     "sentence opening with the product name ignored",
			body:     "Rebased onto main.\n\nSchemaBot applied the `orders` table in staging from commit `abc1234`; the staging check passes.",
			expected: CommandResult{ProseMention: true},
		},
		{
			name:     "sentence addressed to schemabot ignored",
			body:     "schemabot what's up",
			expected: CommandResult{ProseMention: true},
		},
		{
			name:     "sentence opening with a command word does not plan",
			body:     "SchemaBot plan output looks right",
			expected: CommandResult{ProseMention: true},
		},
		{
			name:     "sentence reporting an apply does not apply",
			body:     "SchemaBot apply -e staging succeeded",
			expected: CommandResult{ProseMention: true},
		},
		{
			name:     "sentence about help does not post help",
			body:     "SchemaBot help is linked below.",
			expected: CommandResult{ProseMention: true},
		},
		{
			name: "command word with trailing punctuation is not the command",
			body: "SchemaBot plan.",
			expected: CommandResult{
				IsMention: true,
			},
		},
		{
			name:     "single word with sentence punctuation that is not a command is prose",
			body:     "SchemaBot rocks!",
			expected: CommandResult{ProseMention: true},
		},
		{
			name:     "single-word sentence does not hide a command after it",
			body:     "SchemaBot planned.\n\nschemabot apply -e staging",
			expected: CommandResult{Action: "apply", Environment: "staging", Found: true, IsMention: true},
		},
		{
			name: "control command copied with its usage placeholder",
			body: "schemabot stop <apply-id> -e staging",
			expected: CommandResult{
				Action:      "stop",
				Environment: "staging",
				Found:       true,
				IsMention:   true,
			},
		},
		{
			name: "command after a sentence opening with the product name",
			body: "SchemaBot planned this earlier.\n\nschemabot apply -e staging",
			expected: CommandResult{
				Action:      "apply",
				Environment: "staging",
				Found:       true,
				IsMention:   true,
			},
		},
		{
			name: "mistyped command after a sentence opening with the product name",
			body: "SchemaBot planned this earlier.\n\nschemabot aply -e staging",
			expected: CommandResult{
				IsMention: true,
			},
		},
		{
			name:     "inline prose mention ignored",
			body:     "I onboarded schemabot in this repo.",
			expected: CommandResult{},
		},
		{
			name:     "schemabot filename ignored",
			body:     "With `schemabot.yaml` sitting at `files/migrations/`, the app uses declarative schema changes.",
			expected: CommandResult{},
		},
		{
			name:     "schemabot url ignored",
			body:     "See https://github.com/block/schemabot for details.",
			expected: CommandResult{},
		},
		{
			name:     "quoted command ignored",
			body:     "> schemabot plan -e staging",
			expected: CommandResult{},
		},
		{
			name: "command after prose on new line",
			body: "Please run this:\n\nschemabot plan -e staging",
			expected: CommandResult{
				Action:      "plan",
				Environment: "staging",
				Found:       true,
				IsMention:   true,
			},
		},
		{
			name: "command with markdown indentation",
			body: "  schemabot plan -e staging",
			expected: CommandResult{
				Action:      "plan",
				Environment: "staging",
				Found:       true,
				IsMention:   true,
			},
		},
		{
			name:     "command in fenced code block ignored",
			body:     "```sh\nschemabot plan -e staging\n```",
			expected: CommandResult{},
		},
		{
			name:     "command in tilde fenced code block ignored",
			body:     "~~~\nschemabot apply -e staging\n~~~",
			expected: CommandResult{},
		},
		{
			name:     "command in indented code block ignored",
			body:     "    schemabot plan -e staging",
			expected: CommandResult{},
		},
		{
			name: "command after fenced example",
			body: "```sh\nschemabot plan -e staging\n```\n\nschemabot plan -e production",
			expected: CommandResult{
				Action:      "plan",
				Environment: "production",
				Found:       true,
				IsMention:   true,
			},
		},
		{
			name:     "no mention",
			body:     "just a regular comment",
			expected: CommandResult{},
		},
		{
			name: "case insensitive",
			body: "SchemaBot Plan -e Staging",
			expected: CommandResult{
				Action:      "plan",
				Environment: "staging",
				Found:       true,
				IsMention:   true,
			},
		},
		{
			name: "apply-confirm",
			body: "schemabot apply-confirm -e staging",
			expected: CommandResult{
				Action:      "apply-confirm",
				Environment: "staging",
				Found:       true,
				IsMention:   true,
			},
		},
		{
			name: "stop",
			body: "schemabot stop apply_abc123 -e production",
			expected: CommandResult{
				Action:      "stop",
				ApplyID:     "apply_abc123",
				Environment: "production",
				Found:       true,
				IsMention:   true,
			},
		},
		{
			name: "start",
			body: "schemabot start apply_abc123 -e production",
			expected: CommandResult{
				Action:      "start",
				ApplyID:     "apply_abc123",
				Environment: "production",
				Found:       true,
				IsMention:   true,
			},
		},
		{
			name: "release",
			body: "schemabot release apply_abc123 -e production",
			expected: CommandResult{
				Action:      "release",
				ApplyID:     "apply_abc123",
				Environment: "production",
				Found:       true,
				IsMention:   true,
			},
		},
		{
			name: "cancel",
			body: "schemabot cancel apply_abc123 -e production",
			expected: CommandResult{
				Action:      "cancel",
				ApplyID:     "apply_abc123",
				Environment: "production",
				Found:       true,
				IsMention:   true,
			},
		},
		{
			name: "cutover",
			body: "schemabot cutover apply_abc123 -e staging",
			expected: CommandResult{
				Action:      "cutover",
				ApplyID:     "apply_abc123",
				Environment: "staging",
				Found:       true,
				IsMention:   true,
			},
		},
		{
			name: "revert with apply ID",
			body: "schemabot revert apply-957642f96d634694 -e staging",
			expected: CommandResult{
				Action:      "revert",
				ApplyID:     "apply-957642f96d634694",
				Environment: "staging",
				Found:       true,
				IsMention:   true,
			},
		},
		{
			name: "skip-revert with apply ID",
			body: "schemabot skip-revert apply-957642f96d634694 -e staging",
			expected: CommandResult{
				Action:      "skip-revert",
				ApplyID:     "apply-957642f96d634694",
				Environment: "staging",
				Found:       true,
				IsMention:   true,
			},
		},
		{
			name: "rollback with apply ID and env",
			body: "schemabot rollback apply_abc123 -e Staging",
			expected: CommandResult{
				Action:      "rollback",
				ApplyID:     "apply_abc123",
				Environment: "staging",
				Found:       true,
				IsMention:   true,
			},
		},
		{
			name: "rollback with apply ID missing env",
			body: "schemabot rollback apply_abc123",
			expected: CommandResult{
				Action:     "rollback",
				ApplyID:    "apply_abc123",
				IsMention:  true,
				MissingEnv: true,
			},
		},
		{
			name: "rollback leaves unsupported database flag out of result",
			body: "schemabot rollback apply_abc123 -e staging -d users_db",
			expected: CommandResult{
				Action:      "rollback",
				ApplyID:     "apply_abc123",
				Environment: "staging",
				Found:       true,
				IsMention:   true,
			},
		},
		{
			name: "rollback without apply ID",
			body: "schemabot rollback -e Staging",
			expected: CommandResult{
				Action:      "rollback",
				Environment: "staging",
				Found:       true,
				IsMention:   true,
			},
		},
		{
			name: "rollback without apply ID or env",
			body: "schemabot rollback",
			expected: CommandResult{
				Action:     "rollback",
				IsMention:  true,
				MissingEnv: true,
			},
		},
		{
			name: "rollback-confirm without apply ID",
			body: "schemabot rollback-confirm -e production",
			expected: CommandResult{
				Action:      "rollback-confirm",
				Environment: "production",
				Found:       true,
				IsMention:   true,
			},
		},
		{
			name: "rollback-confirm leaves unsupported database flag out of result",
			body: "schemabot rollback-confirm -e production -d users_db",
			expected: CommandResult{
				Action:      "rollback-confirm",
				Environment: "production",
				Found:       true,
				IsMention:   true,
			},
		},
		{
			name: "rollback-confirm with ignored apply ID",
			body: "schemabot rollback-confirm apply_abc123 -e production",
			expected: CommandResult{
				Action:      "rollback-confirm",
				Environment: "production",
				Found:       true,
				IsMention:   true,
			},
		},
		{
			name: "rollback-confirm missing env",
			body: "schemabot rollback-confirm apply_abc123",
			expected: CommandResult{
				Action:     "rollback-confirm",
				IsMention:  true,
				MissingEnv: true,
			},
		},
		{
			name: "database flag before env",
			body: "schemabot plan -d users_db -e staging",
			expected: CommandResult{
				Action:      "plan",
				Environment: "staging",
				Database:    "users_db",
				Found:       true,
				IsMention:   true,
			},
		},
		{
			name: "fix-lint without -e",
			body: "schemabot fix-lint",
			expected: CommandResult{
				Action:    "fix-lint",
				Found:     true,
				IsMention: true,
			},
		},
		{
			name: "fix-lint with database",
			body: "schemabot fix-lint -d users_db",
			expected: CommandResult{
				Action:    "fix-lint",
				Found:     true,
				IsMention: true,
				Database:  "users_db",
			},
		},
		{
			name: "apply with allow-unsafe",
			body: "schemabot apply -e staging --allow-unsafe",
			expected: CommandResult{
				Action:      "apply",
				Environment: "staging",
				Found:       true,
				IsMention:   true,
				AllowUnsafe: true,
			},
		},
		{
			name: "all flags combined",
			body: "schemabot apply -e production -d payments_db --defer-cutover --skip-revert --allow-unsafe",
			expected: CommandResult{
				Action:       "apply",
				Environment:  "production",
				Database:     "payments_db",
				Found:        true,
				IsMention:    true,
				SkipRevert:   true,
				DeferCutover: true,
				AllowUnsafe:  true,
			},
		},
		{
			name: "-y carries no meaning on apply",
			body: "schemabot apply -e staging -y",
			expected: CommandResult{
				Action:      "apply",
				Environment: "staging",
				Found:       true,
				IsMention:   true,
			},
		},
		{
			name: "-y alongside a supported flag leaves that flag parsed",
			body: "schemabot apply -e production --allow-unsafe -y",
			expected: CommandResult{
				Action:      "apply",
				Environment: "production",
				Found:       true,
				IsMention:   true,
				AllowUnsafe: true,
			},
		},
		{
			name: "-y carries no meaning on apply-confirm",
			body: "schemabot apply-confirm -e staging -y",
			expected: CommandResult{
				Action:      "apply-confirm",
				Environment: "staging",
				Found:       true,
				IsMention:   true,
			},
		},
		{
			name: "-y carries no meaning on plan",
			body: "schemabot plan -e staging -y",
			expected: CommandResult{
				Action:      "plan",
				Environment: "staging",
				Found:       true,
				IsMention:   true,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := parser.ParseCommand(tt.body)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// A comment often explains itself in prose before it issues a command, and
// agents posting status updates open those sentences with the product name.
// The command on its own line is the one SchemaBot runs, and nothing in the
// sentences around it reaches that command: not an environment, a database,
// a tenant, an apply ID, or a safety flag such as --allow-unsafe. A command
// that is only shown, in a code block or a quote or inline in a sentence, is
// never run.
func TestParseCommand_SentenceThenCommand(t *testing.T) {
	parser := NewCommandParser()

	tests := []struct {
		name     string
		body     string
		expected CommandResult
	}{
		{
			name:     "blank line between",
			body:     "SchemaBot planned this earlier.\n\nschemabot apply -e staging",
			expected: CommandResult{Action: "apply", Environment: "staging", Found: true, IsMention: true},
		},
		{
			name:     "no blank line between",
			body:     "SchemaBot planned this earlier.\nschemabot apply -e staging",
			expected: CommandResult{Action: "apply", Environment: "staging", Found: true, IsMention: true},
		},
		{
			name: "several sentences that open with command words",
			body: "SchemaBot planned this.\nSchemaBot plan output looks right.\n" +
				"SchemaBot apply finished in development.\n\nschemabot apply -e staging",
			expected: CommandResult{Action: "apply", Environment: "staging", Found: true, IsMention: true},
		},
		{
			name:     "CRLF line endings",
			body:     "SchemaBot planned this earlier.\r\n\r\nschemabot apply -e staging\r\n",
			expected: CommandResult{Action: "apply", Environment: "staging", Found: true, IsMention: true},
		},
		{
			name:     "environment in the sentence does not carry over",
			body:     "SchemaBot apply -e production succeeded last week.\n\nschemabot apply -e staging",
			expected: CommandResult{Action: "apply", Environment: "staging", Found: true, IsMention: true},
		},
		{
			name:     "environment only in the sentence leaves the command without one",
			body:     "SchemaBot apply -e production succeeded last week.\n\nschemabot apply",
			expected: CommandResult{Action: "apply", IsMention: true, MissingEnv: true},
		},
		{
			name:     "allow-unsafe in the sentence does not carry over",
			body:     "SchemaBot apply --allow-unsafe was needed last time.\n\nschemabot apply -e staging",
			expected: CommandResult{Action: "apply", Environment: "staging", Found: true, IsMention: true},
		},
		{
			name: "allow-unsafe on the command line applies",
			body: "SchemaBot apply --allow-unsafe was needed last time.\n\nschemabot apply -e staging --allow-unsafe",
			expected: CommandResult{
				Action: "apply", Environment: "staging", AllowUnsafe: true, Found: true, IsMention: true,
			},
		},
		{
			name:     "defer-cutover in the sentence does not carry over",
			body:     "SchemaBot apply --defer-cutover held the cutover last time.\n\nschemabot apply -e staging",
			expected: CommandResult{Action: "apply", Environment: "staging", Found: true, IsMention: true},
		},
		{
			name:     "database in the sentence does not carry over",
			body:     "SchemaBot plan -d billing showed drift yesterday.\n\nschemabot plan -e staging",
			expected: CommandResult{Action: "plan", Environment: "staging", Found: true, IsMention: true},
		},
		{
			name:     "tenant in the sentence does not carry over",
			body:     "SchemaBot plan -t west covered the other deployment.\n\nschemabot plan -e staging",
			expected: CommandResult{Action: "plan", Environment: "staging", Found: true, IsMention: true},
		},
		{
			name: "apply ID in the sentence does not carry over",
			body: "SchemaBot stop apply-abc123 -e staging worked earlier.\n\nschemabot stop apply-def456 -e staging",
			expected: CommandResult{
				Action: "stop", ApplyID: "apply-def456", Environment: "staging", Found: true, IsMention: true,
			},
		},
		{
			name:     "the first command line wins and later ones are not run",
			body:     "SchemaBot planned this.\n\nschemabot plan -e staging\nschemabot apply -e production",
			expected: CommandResult{Action: "plan", Environment: "staging", Found: true, IsMention: true},
		},
		{
			name:     "a sentence after the command does not change it",
			body:     "schemabot apply -e staging\n\nSchemaBot apply -e production --allow-unsafe comes next.",
			expected: CommandResult{Action: "apply", Environment: "staging", Found: true, IsMention: true},
		},
		{
			name:     "help after a sentence",
			body:     "SchemaBot planned this earlier.\n\nschemabot help",
			expected: CommandResult{Action: "help", IsHelp: true, IsMention: true},
		},
		{
			name:     "command in a fenced code block after a sentence is not run",
			body:     "SchemaBot planned this earlier.\n\n```\nschemabot apply -e staging\n```",
			expected: CommandResult{ProseMention: true},
		},
		{
			name:     "quoted command after a sentence is not run",
			body:     "SchemaBot planned this earlier.\n\n> schemabot apply -e staging",
			expected: CommandResult{ProseMention: true},
		},
		{
			name:     "indented code command after a sentence is not run",
			body:     "SchemaBot planned this earlier.\n\n    schemabot apply -e staging",
			expected: CommandResult{ProseMention: true},
		},
		{
			name:     "command inline in a sentence that opens with the product name is not run",
			body:     "SchemaBot planned this, so run schemabot apply -e staging next.",
			expected: CommandResult{ProseMention: true},
		},
		{
			name:     "command inline in an ordinary sentence is not run",
			body:     "Planned earlier; next step is schemabot apply -e staging.",
			expected: CommandResult{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, parser.ParseCommand(tt.body))
		})
	}
}

// agentExplanationBody is an agent's explanation of how to land a schema
// change: an imperative-style command appears only inline in a sentence, and
// the closing line opens with the product name as the subject of a sentence.
func agentExplanationBody(lastLine string) string {
	return "🤖 Heads up: this service's schema is now managed by SchemaBot (#123, merged today). " +
		"`db/migrate/` is kept as history only, so the new `20260921190000_add_column.rb` in this PR won't be picked up by SchemaBot.\n" +
		"\n" +
		"To land this change, move it into the declarative schema instead:\n" +
		"\n" +
		"1. Drop `db/migrate/20260921190000_add_column.rb`.\n" +
		"2. Edit `db/mysql/schema/app_{env}/widgets.sql` so its `CREATE TABLE` reflects the end state you want.\n" +
		"\n" +
		lastLine
}

// An explanation whose closing line opens with the product name, in any case,
// is a sentence about SchemaBot. It runs nothing and gets no answer, while the
// same comment with a command alone on a line still runs that command. The
// letter-case variants are the point: the mention match ignores case, so each
// spelling must reach the prose rule. The wording of the rest of the closing
// line is illustrative; every variant is prose for the same reason, a plain
// word after the product name.
func TestParseCommand_ExplanationOpeningWithTheProductName(t *testing.T) {
	parser := NewCommandParser()
	const withApplySteps = "will then comment the exact DDL it plans for staging and production, " +
		"and you apply it with `schemabot apply -e staging`, then `schemabot apply -e production`."
	const onThisPR = "will then comment the exact DDL it plans for staging and production on this PR."
	const readmeCovers = "will then comment the exact DDL it plans for staging and production, " +
		"and the README covers how to apply it."

	tests := []struct {
		name     string
		body     string
		expected CommandResult
	}{
		{
			name:     "closing line with inline apply commands",
			body:     agentExplanationBody("SchemaBot " + withApplySteps),
			expected: CommandResult{ProseMention: true},
		},
		{
			name:     "closing line about the comment on this PR",
			body:     agentExplanationBody("SchemaBot " + onThisPR),
			expected: CommandResult{ProseMention: true},
		},
		{
			name:     "closing line pointing at the README",
			body:     agentExplanationBody("SchemaBot " + readmeCovers),
			expected: CommandResult{ProseMention: true},
		},
		{
			name:     "title-case product name",
			body:     agentExplanationBody("Schemabot " + onThisPR),
			expected: CommandResult{ProseMention: true},
		},
		{
			name:     "upper-case product name",
			body:     agentExplanationBody("SCHEMABOT " + withApplySteps),
			expected: CommandResult{ProseMention: true},
		},
		{
			name:     "lower-case product name",
			body:     agentExplanationBody("schemabot " + onThisPR),
			expected: CommandResult{ProseMention: true},
		},
		{
			name:     "explanation sentence alone",
			body:     "SchemaBot " + onThisPR,
			expected: CommandResult{ProseMention: true},
		},
		{
			name:     "command alone on a line after the explanation runs",
			body:     agentExplanationBody("SchemaBot "+onThisPR) + "\n\nschemabot apply -e staging",
			expected: CommandResult{Action: "apply", Environment: "staging", Found: true, IsMention: true},
		},
		{
			name:     "command alone on a line",
			body:     "schemabot apply -e staging",
			expected: CommandResult{Action: "apply", Environment: "staging", Found: true, IsMention: true},
		},
		{
			name:     "capitalized command alone on a line",
			body:     "SchemaBot apply -e staging",
			expected: CommandResult{Action: "apply", Environment: "staging", Found: true, IsMention: true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parser.ParseCommand(tt.body)
			assert.Equal(t, tt.expected, got)
		})
	}
}

// The usage gates read flags from the same command line ParseCommand acts
// on, so a flag mentioned in a sentence never rejects the command below it,
// and a flag on the command line always does.
func TestFlagHelpers_ReadOnlyTheCommandLine(t *testing.T) {
	p := NewCommandParser()

	assert.False(t, p.HasDatabaseFlag("SchemaBot unlock -d billing released it before.\n\nschemabot rollback apply-abc123 -e staging"))
	assert.True(t, p.HasDatabaseFlag("SchemaBot unlock released it before.\n\nschemabot rollback apply-abc123 -e staging -d billing"))

	assert.False(t, p.HasDeferCutoverFlag("SchemaBot apply --defer-cutover held it before.\n\nschemabot rollback apply-abc123 -e staging"))
	assert.True(t, p.HasDeferCutoverFlag("SchemaBot apply held it before.\n\nschemabot rollback apply-abc123 -e staging --defer-cutover"))

	assert.False(t, p.HasAutoConfirmFlag("SchemaBot apply -e staging -y is a CLI habit.\n\nschemabot apply -e staging"))
	assert.True(t, p.HasAutoConfirmFlag("SchemaBot apply is a PR comment.\n\nschemabot apply -e staging --yes"))
}

// A line with no plain word on it is a command attempt, and every token on it
// has to be one SchemaBot accepts exactly. A token that only nearly matches,
// such as a flag with punctuation typed after it, or one that makes the
// command ambiguous, rejects the whole line with the invalid-command answer:
// the parser never trims a token into one it accepts. The rejected line is the
// directive, so a command on a later line does not run in its place.
func TestParseCommand_MalformedCommand(t *testing.T) {
	parser := NewCommandParser()
	rejected := CommandResult{IsMention: true}

	tests := []struct {
		name     string
		body     string
		expected CommandResult
	}{
		{name: "unsafe flag with a full stop", body: "schemabot apply -e staging --allow-unsafe.", expected: rejected},
		{name: "unsafe flag with a comma", body: "schemabot apply -e staging --allow-unsafe,", expected: rejected},
		{name: "flag with a value glued on", body: "schemabot apply -e staging --allow-unsafe=true", expected: rejected},
		{name: "unknown flag", body: "schemabot apply -e staging --foo", expected: rejected},
		{name: "auto-confirm flag with a full stop", body: "schemabot apply -e staging -y.", expected: rejected},
		{name: "database with a full stop", body: "schemabot apply -e staging -d billing.", expected: rejected},
		{name: "database flag without a value", body: "schemabot apply -e staging -d", expected: rejected},
		{name: "database flag followed by another flag", body: "schemabot apply -e staging -d --allow-unsafe", expected: rejected},
		{name: "apply ID with a full stop", body: "schemabot rollback apply-abc123. -e staging", expected: rejected},
		{name: "apply ID with trailing letters", body: "schemabot rollback apply-abc123xyz -e staging", expected: rejected},
		{name: "two apply IDs", body: "schemabot rollback apply-abc123 apply-def456 -e staging", expected: rejected},
		{name: "command word with a comma", body: "schemabot apply, -e staging", expected: rejected},
		{name: "unsafe flag autocorrected to an em dash", body: "schemabot apply -e staging \u2014allow-unsafe", expected: rejected},
		{name: "environment flag autocorrected to an en dash", body: "schemabot apply \u2013e staging", expected: rejected},
		{name: "optional environment copied from usage text", body: "schemabot plan [-e <env>]", expected: rejected},
		{name: "optional tenant copied from usage text", body: "schemabot rollback <apply-id> -e staging [-t <tenant>]", expected: rejected},
		{name: "environment flag without a value", body: "schemabot plan -e", expected: rejected},
		{name: "environment flag followed by another flag", body: "schemabot plan -e -d billing", expected: rejected},
		{name: "environment given twice", body: "schemabot apply -e staging -e production", expected: rejected},
		{
			name:     "tenant given twice has no single routing target",
			body:     "schemabot plan -e staging -t tenant-a --tenant tenant-b",
			expected: CommandResult{TenantError: true, IsMention: true},
		},
		{
			name:     "rejected line keeps its tenant so the right deployment answers",
			body:     "schemabot apply -e staging -t tenant-a --allow-unsafe.",
			expected: CommandResult{Tenant: "tenant-a", IsMention: true},
		},
		{
			name:     "rejected line is the directive; a later command does not run",
			body:     "schemabot apply -e staging --allow-unsafe.\n\nschemabot plan -e staging",
			expected: rejected,
		},
		{
			name:     "a bracketed plain word is a sentence, not usage text",
			body:     "SchemaBot plan [done]",
			expected: CommandResult{ProseMention: true},
		},
		{
			name:     "a plain word makes the line a sentence, not a rejected command",
			body:     "SchemaBot apply --allow-unsafe. went fine",
			expected: CommandResult{ProseMention: true},
		},
		{
			name:     "environment with a full stop gets the environment usage answer",
			body:     "schemabot apply -e staging.",
			expected: CommandResult{Action: "apply", EnvironmentError: true, Found: true, IsMention: true},
		},
		{
			name:     "flags match regardless of case",
			body:     "schemabot apply -E staging --ALLOW-UNSAFE",
			expected: CommandResult{Action: "apply", Environment: "staging", AllowUnsafe: true, Found: true, IsMention: true},
		},
		{
			name:     "long tenant spelling",
			body:     "schemabot plan -e staging --tenant tenant-a",
			expected: CommandResult{Action: "plan", Environment: "staging", Tenant: "tenant-a", Found: true, IsMention: true},
		},
		{
			name:     "apply ID with an underscore",
			body:     "schemabot rollback apply_abc123 -e staging",
			expected: CommandResult{Action: "rollback", ApplyID: "apply_abc123", Environment: "staging", Found: true, IsMention: true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, parser.ParseCommand(tt.body))
		})
	}
}

// The usage gates read flags off a well-formed command only. A malformed line
// carries no flags: it is answered as an invalid command, never as a command
// with an unsupported flag.
func TestFlagHelpers_IgnoreAMalformedCommand(t *testing.T) {
	p := NewCommandParser()

	assert.False(t, p.HasAutoConfirmFlag("schemabot apply -e staging -y --foo"))
	assert.True(t, p.HasAutoConfirmFlag("schemabot apply -e staging -y"))

	assert.False(t, p.HasDatabaseFlag("schemabot rollback-confirm -e staging -d billing --foo"))
	assert.True(t, p.HasDatabaseFlag("schemabot rollback-confirm -e staging -d billing"))

	assert.False(t, p.HasDeferCutoverFlag("schemabot plan -e staging --defer-cutover --foo"))
	assert.True(t, p.HasDeferCutoverFlag("schemabot plan -e staging --defer-cutover"))
}

// GitHub does not render an HTML comment and renders an HTML <pre> block as
// code, so a command in either is not the commenter's text and does not run.
// Text around a comment on the same line, and lines after it, are the
// commenter's own.
func TestParseCommand_HiddenAndPreformattedHTML(t *testing.T) {
	parser := NewCommandParser()

	tests := []struct {
		name     string
		body     string
		expected CommandResult
	}{
		{
			name:     "command inside a multi-line HTML comment",
			body:     "<!--\nschemabot apply -e production --allow-unsafe\n-->\nLGTM",
			expected: CommandResult{},
		},
		{
			name:     "command inside a one-line HTML comment",
			body:     "<!-- schemabot apply -e production -->",
			expected: CommandResult{},
		},
		{
			name:     "command after an HTML comment",
			body:     "<!--\nnote\n-->\nschemabot plan -e staging",
			expected: CommandResult{Action: "plan", Environment: "staging", Found: true, IsMention: true},
		},
		{
			name:     "command with an HTML comment after it on the same line",
			body:     "schemabot plan -e staging <!-- from the runbook -->",
			expected: CommandResult{Action: "plan", Environment: "staging", Found: true, IsMention: true},
		},
		{
			name:     "command with a multi-line HTML comment opening after it",
			body:     "schemabot plan -e staging <!--\nschemabot apply -e production\n-->",
			expected: CommandResult{Action: "plan", Environment: "staging", Found: true, IsMention: true},
		},
		{
			name:     "command on its own line after a sentence that opens an HTML comment",
			body:     "Rebased onto main. <!--\nnote\n-->\nschemabot plan -e staging",
			expected: CommandResult{Action: "plan", Environment: "staging", Found: true, IsMention: true},
		},
		{
			name:     "command inside an HTML pre block",
			body:     "<pre>\nschemabot apply -e production\n</pre>",
			expected: CommandResult{},
		},
		{
			name:     "command after an HTML pre block with blank lines inside",
			body:     "<PRE class=\"shell\">\n\nschemabot apply -e production\n</PRE>\nschemabot plan -e staging",
			expected: CommandResult{Action: "plan", Environment: "staging", Found: true, IsMention: true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, parser.ParseCommand(tt.body))
		})
	}
}

// Quoting a command, as GitHub's quote-reply does, shows it rather than
// issues it. Nothing inside a quote runs: not the `>` lines, not a line that
// continues the quoted paragraph without a `>` (Markdown renders it inside the
// quote), not a quoted code block, and not an HTML <blockquote>. A command the
// replier writes on its own line after the quote ends runs, and takes nothing
// from the quote.
func TestParseCommand_QuoteReply(t *testing.T) {
	parser := NewCommandParser()

	tests := []struct {
		name     string
		body     string
		expected CommandResult
	}{
		{
			name:     "quote-reply to an apply command",
			body:     "> schemabot apply -e production\n\nShould this go to staging first?",
			expected: CommandResult{},
		},
		{
			name:     "quote-reply to a whole comment with blank quoted lines",
			body:     "> SchemaBot planned this.\n>\n> schemabot apply -e production\n\nLooks good to me.",
			expected: CommandResult{},
		},
		{
			name:     "nested quote",
			body:     "> > schemabot apply -e production\n> agreed\n\nThanks",
			expected: CommandResult{},
		},
		{
			name:     "quote without a space after the marker",
			body:     ">schemabot apply -e production",
			expected: CommandResult{},
		},
		{
			name:     "quote indented by up to three spaces",
			body:     "   > schemabot apply -e production",
			expected: CommandResult{},
		},
		{
			name:     "command continuing an indented quote without a marker",
			body:     "   > Can you run this?\nschemabot apply -e production",
			expected: CommandResult{},
		},
		{
			name:     "command continuing a quoted paragraph without a marker",
			body:     "> Can you run this?\nschemabot apply -e production",
			expected: CommandResult{},
		},
		{
			name:     "command continuing a quoted command without a marker",
			body:     "> schemabot plan -e staging\nschemabot apply -e production",
			expected: CommandResult{},
		},
		{
			name:     "quoted code block",
			body:     "> ```\n> schemabot apply -e production\n> ```",
			expected: CommandResult{},
		},
		{
			name:     "quoted sentence about SchemaBot",
			body:     "> SchemaBot apply -e staging succeeded\n\nnice",
			expected: CommandResult{},
		},
		{
			name:     "HTML blockquote",
			body:     "<blockquote>\nschemabot apply -e production\n</blockquote>\n\nThoughts?",
			expected: CommandResult{},
		},
		{
			name:     "HTML blockquote on one line",
			body:     "<blockquote>schemabot apply -e production</blockquote>",
			expected: CommandResult{},
		},
		{
			name:     "HTML blockquote with blank lines inside",
			body:     "<BLOCKQUOTE>\n\nschemabot apply -e production\n\n</BLOCKQUOTE>",
			expected: CommandResult{},
		},
		{
			name:     "replier's own command after the quote ends",
			body:     "> schemabot apply -e production\n\nschemabot apply -e staging",
			expected: CommandResult{Action: "apply", Environment: "staging", Found: true, IsMention: true},
		},
		{
			name:     "quoted flags do not reach the replier's own command",
			body:     "> schemabot apply -e production --allow-unsafe\n\nschemabot apply -e staging",
			expected: CommandResult{Action: "apply", Environment: "staging", Found: true, IsMention: true},
		},
		{
			name:     "replier's own command after an HTML blockquote ends",
			body:     "<blockquote>\nschemabot apply -e production\n</blockquote>\n\nschemabot plan -e staging",
			expected: CommandResult{Action: "plan", Environment: "staging", Found: true, IsMention: true},
		},
		{
			name:     "nested HTML blockquote stays open after the inner one closes",
			body:     "<blockquote>\n<blockquote>\nearlier\n</blockquote>\nschemabot apply -e production\n</blockquote>",
			expected: CommandResult{},
		},
		{
			name:     "nested HTML blockquotes opened on one line",
			body:     "<blockquote><blockquote>earlier</blockquote>\nschemabot apply -e production\n</blockquote>",
			expected: CommandResult{},
		},
		{
			name:     "own command after a blank quoted line is outside the quote",
			body:     "> Should this go to staging first?\n>\nschemabot apply -e staging",
			expected: CommandResult{Action: "apply", Environment: "staging", Found: true, IsMention: true},
		},
		{
			name:     "own command after a closed quoted fence is outside the quote",
			body:     "> ```\n> schemabot apply -e production\n> ```\nschemabot apply -e staging",
			expected: CommandResult{Action: "apply", Environment: "staging", Found: true, IsMention: true},
		},
		{
			name:     "own command after an unclosed quoted fence is outside the quote",
			body:     "> ```\n> schemabot apply -e production\nschemabot apply -e staging",
			expected: CommandResult{Action: "apply", Environment: "staging", Found: true, IsMention: true},
		},
		{
			name:     "own command straight after a quoted fence opens is outside the quote",
			body:     "> ```\nschemabot apply -e staging",
			expected: CommandResult{Action: "apply", Environment: "staging", Found: true, IsMention: true},
		},
		{
			name:     "own command after a multi-line unclosed quoted fence is outside the quote",
			body:     "> ```\n> schemabot plan -e production\n> schemabot apply -e production\nschemabot apply -e staging",
			expected: CommandResult{Action: "apply", Environment: "staging", Found: true, IsMention: true},
		},
		{
			name:     "indented quoted line continues the quoted paragraph rather than opening a fence",
			body:     "> Can you run this?\n>     ```\nschemabot apply -e production",
			expected: CommandResult{},
		},
		{
			name:     "fence opened straight after a quote hides a command after a blank line inside it",
			body:     "> Can you run this?\n```\nearlier\n\nschemabot apply -e production\n```",
			expected: CommandResult{},
		},
		{
			name:     "HTML blockquote opened straight after a quote hides a command after a blank line inside it",
			body:     "> Can you run this?\n<blockquote>\n\nschemabot apply -e production\n</blockquote>",
			expected: CommandResult{},
		},
		{
			name:     "HTML comment opened straight after a quote hides a command after a blank line inside it",
			body:     "> Can you run this?\n<!--\n\nschemabot apply -e production\n-->",
			expected: CommandResult{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, parser.ParseCommand(tt.body))
		})
	}

	// The usage gates skip shown commands the same way, so a flag on a quoted
	// or fenced command line never attaches to the command that follows it.
	for _, shown := range []struct{ name, prefix string }{
		{"quote", "> "},
		{"quoted paragraph continuation", "> Can you run this?\n"},
		{"fence", "```\n"},
	} {
		t.Run("usage gates skip a "+shown.name, func(t *testing.T) {
			closer := ""
			if shown.name == "fence" {
				closer = "\n```"
			}
			assert.False(t, parser.HasAutoConfirmFlag(shown.prefix+"schemabot apply -e staging -y"+closer+"\n\nschemabot apply -e staging"))
			assert.False(t, parser.HasDatabaseFlag(shown.prefix+"schemabot rollback apply-abc123 -e staging -d billing"+closer+"\n\nschemabot rollback apply-abc123 -e staging"))
			assert.False(t, parser.HasDeferCutoverFlag(shown.prefix+"schemabot rollback apply-abc123 -e staging --defer-cutover"+closer+"\n\nschemabot rollback apply-abc123 -e staging"))
		})
	}
}

// `--target` narrows plan and apply to one rollout member. Its value is the
// target's name or deployment/target, and target names are opaque, so it is
// forwarded exactly as typed, never trimmed: the server matches it against the
// rollout and names the valid targets when it matches none. A missing value
// rejects the line as malformed, and the value is never read as a plain word
// that turns the line into prose.
func TestParseCommandTargetFlag(t *testing.T) {
	parser := NewCommandParser()

	tests := []struct {
		name     string
		body     string
		expected CommandResult
	}{
		{
			name: "apply narrowed to a target",
			body: "schemabot apply -e staging --target payments-002",
			expected: CommandResult{
				Action: "apply", Environment: "staging", Target: "payments-002",
				Found: true, IsMention: true,
			},
		},
		{
			name: "plan narrowed to deployment/target, case kept",
			body: "schemabot plan -e production -d payments --target prod-west/Payments_002",
			expected: CommandResult{
				Action: "plan", Environment: "production", Database: "payments", Target: "prod-west/Payments_002",
				Found: true, IsMention: true,
			},
		},
		{
			name: "plan narrowed without an environment",
			body: "schemabot plan --target payments-002",
			expected: CommandResult{
				Action: "plan", Target: "payments-002", MissingEnv: true, IsMention: true,
			},
		},
		{
			name:     "missing value",
			body:     "schemabot apply -e staging --target",
			expected: CommandResult{IsMention: true},
		},
		{
			name:     "value swallowed by the next flag",
			body:     "schemabot apply -e staging --target --allow-unsafe",
			expected: CommandResult{IsMention: true},
		},
		{
			name: "opaque target name forwarded as typed",
			body: "schemabot apply -e staging --target .payments@002",
			expected: CommandResult{
				Action: "apply", Environment: "staging", Target: ".payments@002",
				Found: true, IsMention: true,
			},
		},
		{
			name: "trailing punctuation kept, never trimmed",
			body: "schemabot apply -e staging --target payments-002.",
			expected: CommandResult{
				Action: "apply", Environment: "staging", Target: "payments-002.",
				Found: true, IsMention: true,
			},
		},
		{
			name:     "given twice",
			body:     "schemabot apply -e staging --target payments-001 --target payments-002",
			expected: CommandResult{IsMention: true},
		},
		{
			name: "command that takes no target parses without one",
			body: "schemabot apply-confirm -e staging --target payments-002",
			expected: CommandResult{
				Action: "apply-confirm", Environment: "staging", Found: true, IsMention: true,
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, parser.ParseCommand(tc.body))
		})
	}
}

func TestHasTargetFlag(t *testing.T) {
	p := NewCommandParser()
	assert.True(t, p.HasTargetFlag("schemabot apply-confirm -e staging --target payments-002"))
	assert.False(t, p.HasTargetFlag("schemabot apply-confirm -e staging"))
	assert.False(t, p.HasTargetFlag("schemabot apply-confirm -e staging\n\nlast time I passed --target payments-002"))
	assert.False(t, p.HasTargetFlag("schemabot apply-confirm -e staging\n\n```\nschemabot apply -e staging --target payments-002\n```\n"))
}

func TestCommandSupportsTargetFlag(t *testing.T) {
	assert.True(t, commandSupportsTargetFlag(action.Plan))
	assert.True(t, commandSupportsTargetFlag(action.Apply))
	assert.False(t, commandSupportsTargetFlag(action.ApplyConfirm))
	assert.False(t, commandSupportsTargetFlag(action.Rollback))
	assert.False(t, commandSupportsTargetFlag(action.Unlock))
	assert.False(t, commandSupportsTargetFlag("unknown"))
}
