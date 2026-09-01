package ui

import (
	"strings"
	"testing"

	"github.com/block/schemabot/pkg/state"
	"github.com/stretchr/testify/assert"
)

func TestFormatApproxRows(t *testing.T) {
	tests := []struct {
		n    int64
		want string
	}{
		{0, "~0"},
		{842, "~842"},
		{1_000, "~1k"},
		{15_249, "~15.2k"},
		{999_949, "~999.9k"},
		{999_950, "~1M"},
		{1_000_000, "~1M"},
		{2_340_000, "~2.3M"},
		{48_200_000, "~48.2M"},
		{999_950_000, "~1B"},
		{5_100_000_000, "~5.1B"},
		{999_950_000_000, "~1T"},
		{5_100_000_000_000, "~5.1T"},
		{-5, "~0"},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.want, FormatApproxRows(tt.n), "n=%d", tt.n)
	}
}

func TestFormatApproxBytes(t *testing.T) {
	tests := []struct {
		b    int64
		want string
	}{
		{0, "~0 B"},
		{812, "~812 B"},
		{1_000, "~1 KB"},
		{112_640, "~112.6 KB"},
		{999_949_999, "~999.9 MB"},
		{999_950_000, "~1 GB"},
		{1_130_000_000, "~1.1 GB"},
		{23_400_000_000, "~23.4 GB"},
		{999_950_000_000, "~1 TB"},
		{48_000_000_000_000, "~48 TB"},
		{999_950_000_000_000, "~1 PB"},
		{-5, "~0 B"},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.want, FormatApproxBytes(tt.b), "b=%d", tt.b)
	}
}

func TestTableStatePriority(t *testing.T) {
	tests := []struct {
		state    string
		expected int
	}{
		{state.Task.Running, 0},
		{state.Task.CuttingOver, 0},
		{state.Task.WaitingForCutover, 1},
		{state.Task.Recovering, 1},
		{state.Task.Pending, 2},
		{state.Task.Failed, 3},
		{state.Task.Stopped, 3},
		{state.Task.Completed, 4},
		{state.Task.Cancelled, 4},
		{state.Task.Reverted, 4},
		{"unknown_state", 2}, // default
	}

	for _, tt := range tests {
		t.Run(tt.state, func(t *testing.T) {
			assert.Equal(t, tt.expected, TableStatePriority(tt.state))
		})
	}
}

func TestVSchemaStatusLabel(t *testing.T) {
	assert.Equal(t, "Applying...", VSchemaStatusLabel("applying"))
	assert.Equal(t, "Applied", VSchemaStatusLabel("applied"))
	assert.Equal(t, "Failed", VSchemaStatusLabel("failed"))
	assert.Equal(t, "Cancelled", VSchemaStatusLabel("cancelled"))
	assert.Equal(t, "Stopped", VSchemaStatusLabel("stopped"))
	assert.Equal(t, "Pending", VSchemaStatusLabel(""))
	assert.Equal(t, "rolling_back", VSchemaStatusLabel("rolling_back"), "unknown status passes through")
}

func TestProgressBarActivity(t *testing.T) {
	bar := ProgressBarActivity()

	assert.Equal(t, 20, strings.Count(bar, ColorBlue))
	assert.Zero(t, strings.Count(bar, ColorEmpty))

	// Engine-driven phases (cutting over, recovering) render the activity bar in
	// one surface and a full row-copy bar in another; the two must stay
	// byte-identical so the same phase never shows two different bars.
	assert.Equal(t, ProgressBarRowCopy(100), bar)
}

func TestFormatETA(t *testing.T) {
	tests := []struct {
		name    string
		seconds int64
		want    string
	}{
		{"zero", 0, "0s"},
		{"sub-minute", 45, "45s"},
		{"exactly a minute", 60, "1m 0s"},
		{"minutes and seconds", 195, "3m 15s"},
		{"exactly an hour", 3600, "1h 0m"},
		{"hours and minutes", 3700, "1h 1m"},
		{"just under a day", 86399, "23h 59m"},
		{"exactly a day", 86400, "1d 0h"},
		{"days and hours", 180000, "2d 2h"},
		{"multi-week copy", 1814400, "21d 0h"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, FormatETA(tt.seconds))
		})
	}
}

func TestRowCopyDisplayPercent(t *testing.T) {
	assert.Equal(t, 0, RowCopyDisplayPercent(0, 0))
	assert.Equal(t, 1, RowCopyDisplayPercent(0, 42))
	assert.Equal(t, 1, RowCopyDisplayPercent(-3, 42))
	assert.Equal(t, 2, RowCopyDisplayPercent(2, 42))
	assert.Equal(t, 100, RowCopyDisplayPercent(145, 42))
}

func TestRowCopyFraction(t *testing.T) {
	assert.InDelta(t, 45.37, RowCopyFraction(45, 453_700, 1_000_000), 0.0001)
	assert.InDelta(t, 100, RowCopyFraction(99, 1_100_000, 1_000_000), 0.0001)
	assert.InDelta(t, 45, RowCopyFraction(45, 0, 0), 0.0001)
	assert.InDelta(t, 100, RowCopyFraction(145, 0, 0), 0.0001)
}

func TestFormatRowCopyPercent(t *testing.T) {
	// Row counts are the source of truth: the percent is recomputed from them
	// at two-decimal precision, whatever the whole-number percent says.
	assert.Equal(t, "45.00%", FormatRowCopyPercent(45, 450_000, 1_000_000))
	assert.Equal(t, "45.37%", FormatRowCopyPercent(45, 453_700, 1_000_000))
	assert.Equal(t, "0.03%", FormatRowCopyPercent(0, 13_186_540, 43_234_523_345))
	assert.Equal(t, "0.19%", FormatRowCopyPercent(0, 3_000, 1_604_159))
	assert.Equal(t, "100.00%", FormatRowCopyPercent(100, 1_000_000, 1_000_000))

	// Floored at 0.01% once copying has begun, and capped at 100% when the
	// copied count overshoots the estimated total.
	assert.Equal(t, "0.01%", FormatRowCopyPercent(0, 1, 1_000_000))
	assert.Equal(t, "100.00%", FormatRowCopyPercent(99, 1_100_000, 1_000_000))

	// Without row counts, the engine's whole-number percent renders clamped;
	// a copy that has begun with no total falls back to "<1%".
	assert.Equal(t, "0%", FormatRowCopyPercent(0, 0, 1_000_000))
	assert.Equal(t, "45%", FormatRowCopyPercent(45, 0, 0))
	assert.Equal(t, "100%", FormatRowCopyPercent(145, 0, 0))
	assert.Equal(t, "<1%", FormatRowCopyPercent(0, 42, 0))
}

func TestLintReasons(t *testing.T) {
	t.Run("splits engine-joined violations into cleaned messages", func(t *testing.T) {
		assert.Equal(t,
			[]string{"DROP COLUMN removes data", "Index should be invisible first"},
			LintReasons("[ERROR] unsafe: DROP COLUMN removes data; [ERROR] invisible_index_before_drop: Index should be invisible first"))
	})

	t.Run("strips severity prefix and linter name", func(t *testing.T) {
		assert.Equal(t,
			[]string{`Index "idx_status" should be made invisible`},
			LintReasons(`[ERROR] invisible_index_before_drop: Index "idx_status" should be made invisible`))
		assert.Equal(t,
			[]string{"DROP COLUMN removes data"},
			LintReasons("[WARNING] unsafe: DROP COLUMN removes data"))
	})

	t.Run("single violation yields one message", func(t *testing.T) {
		assert.Equal(t, []string{"DROP TABLE removes all data"}, LintReasons("DROP TABLE removes all data"))
	})

	t.Run("empty reason yields nothing", func(t *testing.T) {
		assert.Empty(t, LintReasons(""))
	})
}

func TestCodeQuoteIdentifiers(t *testing.T) {
	tests := []struct {
		name   string
		input  string
		expect string
	}{
		{
			name:   "double-quoted index name",
			input:  `Index "idx_status" should be made invisible before dropping`,
			expect: "Index `idx_status` should be made invisible before dropping",
		},
		{
			name:   "double-quoted type name",
			input:  `Using "varchar" as primary key is discouraged`,
			expect: "Using `varchar` as primary key is discouraged",
		},
		{
			name:   "type with length keeps parentheses",
			input:  `Using "mediumint(9)" as primary key is discouraged`,
			expect: "Using `mediumint(9)` as primary key is discouraged",
		},
		{
			name:   "multiple identifiers in one message",
			input:  `Column "created_at" of type "DATETIME" should not be the leftmost index column`,
			expect: "Column `created_at` of type `DATETIME` should not be the leftmost index column",
		},
		{
			name:   "quoted prose with spaces is left alone",
			input:  `The index "idx_a" is marked "do not drop"`,
			expect: "The index `idx_a` is marked \"do not drop\"",
		},
		{
			name:   "qualified table name",
			input:  `Table "app.users" has no primary key`,
			expect: "Table `app.users` has no primary key",
		},
		{
			name:   "enum type keeps its quoted values inside one code span",
			input:  `Column "status" in table "orders" has type "enum('active','archived')"`,
			expect: "Column `status` in table `orders` has type `enum('active','archived')`",
		},
		{
			name:   "parameterised type with a comma",
			input:  `Column "amount" in table "orders" has type "decimal(10,2)" but 2 other table(s) use type "decimal(12,4)"`,
			expect: "Column `amount` in table `orders` has type `decimal(10,2)` but 2 other table(s) use type `decimal(12,4)`",
		},
		{
			name:   "no quoted tokens passes through",
			input:  "DROP TABLE removes all data",
			expect: "DROP TABLE removes all data",
		},
		{
			name:   "quoted statement with backticked identifier gets a padded double-backtick span",
			input:  "Unsafe operation detected: \"DROP TABLE `wallet_history`\"",
			expect: "Unsafe operation detected: `` DROP TABLE `wallet_history` ``",
		},
		{
			name:   "quoted operation without backticks gets a plain code span",
			input:  `Unsafe operation detected: "TRUNCATE PARTITION"`,
			expect: "Unsafe operation detected: `TRUNCATE PARTITION`",
		},
		{
			name:   "quoted DROP COLUMN fragment keeps its backticked column",
			input:  "Unsafe operation detected: \"DROP COLUMN `email`\"",
			expect: "Unsafe operation detected: `` DROP COLUMN `email` ``",
		},
		{
			name:   "quoted type with attribute words",
			input:  `Column "qty" in table "orders" has type "int(11) unsigned"`,
			expect: "Column `qty` in table `orders` has type `int(11) unsigned`",
		},
		{
			name:   "quoted descending key part in a column list",
			input:  `Index "idx_recent" on columns ("category", "created_at DESC") is redundant`,
			expect: "Index `idx_recent` on columns (`category`, `created_at DESC`) is redundant",
		},
		{
			name:   "identifier with a doubled backtick sizes the span past the run",
			input:  "Unsafe operation detected: \"DROP COLUMN `a``b`\"",
			expect: "Unsafe operation detected: ``` DROP COLUMN `a``b` ```",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expect, CodeQuoteIdentifiers(tt.input))
		})
	}
}

// A byte count reads as a magnitude: bytes below a kibibyte, and one decimal
// place with a binary unit above it, so a table's footprint is scannable.
func TestFormatBytesBinary(t *testing.T) {
	tests := []struct {
		name   string
		input  int64
		expect string
	}{
		{name: "zero", input: 0, expect: "0 B"},
		{name: "bytes below a kibibyte", input: 1023, expect: "1023 B"},
		{name: "exactly one kibibyte", input: 1024, expect: "1.0 KiB"},
		{name: "fractional kibibytes", input: 1536, expect: "1.5 KiB"},
		{name: "mebibytes", input: 1024 * 1024, expect: "1.0 MiB"},
		{name: "gibibytes", input: 4 * 1024 * 1024 * 1024, expect: "4.0 GiB"},
		{name: "tebibytes", input: 3 * 1024 * 1024 * 1024 * 1024, expect: "3.0 TiB"},
		{name: "pebibytes", input: 2 * 1024 * 1024 * 1024 * 1024 * 1024, expect: "2.0 PiB"},
		{name: "beyond pebibytes stays readable", input: 5 * 1024 * 1024 * 1024 * 1024 * 1024 * 1024, expect: "5.0 EiB"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expect, FormatBytesBinary(tt.input))
		})
	}
}
