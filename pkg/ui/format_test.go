package ui

import (
	"math"
	"strings"
	"testing"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/state/statetest"
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
		{1_000, "~1.0 KB"},
		{112_640, "~113 KB"},
		{99_949, "~99.9 KB"},
		{99_950, "~100 KB"},
		{999_499_999, "~999 MB"},
		{999_500_000, "~1.0 GB"},
		{1_130_000_000, "~1.1 GB"},
		{23_400_000_000, "~23.4 GB"},
		{186_000_000_000, "~186 GB"},
		{186_400_000_000, "~186 GB"},
		{999_500_000_000, "~1.0 TB"},
		{48_000_000_000_000, "~48.0 TB"},
		{999_500_000_000_000, "~1.0 PB"},
		{-5, "~0 B"},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.want, FormatApproxBytes(tt.b), "b=%d", tt.b)
	}
}

// A copy line carries the table's planned size after its row counts, and
// nothing when the plan had no estimate.
func TestFormatTableSizeClause(t *testing.T) {
	bytes := int64(23_400_000_000)
	assert.Equal(t, " · ~23.4 GB", FormatTableSizeClause(&bytes))
	zero := int64(0)
	assert.Equal(t, " · ~0 B", FormatTableSizeClause(&zero))
	assert.Empty(t, FormatTableSizeClause(nil))
}

// Every task state has a pinned place in the table listing, so a state added
// to pkg/state fails here until the listing says where it goes. States the
// priority does not name, and statuses it does not recognize, list with
// pending.
func TestTableStatePriority(t *testing.T) {
	task := state.Task
	statetest.AssertRanks(t, statetest.Every(t, state.Task), map[string]int{
		task.Running:           0,
		task.CatchingUp:        0,
		task.Checksumming:      0,
		task.PostChecksum:      0,
		task.CuttingOver:       0,
		task.WaitingForCutover: 1,
		task.Recovering:        1,
		task.FailedRetryable:   1,
		task.Pending:           2,
		task.WaitingForDeploy:  2,
		task.RevertWindow:      2,
		task.Reverting:         2,
		task.Failed:            3,
		task.Stopped:           3,
		task.Completed:         4,
		task.Cancelled:         4,
		task.Reverted:          4,
	}, TableStatePriority)
	assert.Equal(t, 2, TableStatePriority("unknown_state"))
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

func TestRowCopyDisplayPercent(t *testing.T) {
	assert.Equal(t, 0, RowCopyDisplayPercent(0, 0))
	assert.Equal(t, 1, RowCopyDisplayPercent(0, 42))
	assert.Equal(t, 1, RowCopyDisplayPercent(-3, 42))
	assert.Equal(t, 2, RowCopyDisplayPercent(2, 42))
	assert.Equal(t, 100, RowCopyDisplayPercent(145, 42))
}

func TestRecoveringCopyingRows(t *testing.T) {
	assert.True(t, RecoveringCopyingRows(1_000, 0))
	assert.True(t, RecoveringCopyingRows(1_000, 99))
	assert.False(t, RecoveringCopyingRows(1_000, 100), "a finished copy is not still copying")
	assert.False(t, RecoveringCopyingRows(0, 40), "no row total yet, so no copy to report")
}

func TestChecksumDisplayPercent(t *testing.T) {
	// No total yet, or nothing verified: an empty bar.
	assert.Equal(t, 0, ChecksumDisplayPercent(0, 0))
	assert.Equal(t, 0, ChecksumDisplayPercent(500, 0))
	assert.Equal(t, 0, ChecksumDisplayPercent(0, 1_000_000))

	// At least 1% once verification has begun, so the bar shows it started.
	assert.Equal(t, 1, ChecksumDisplayPercent(4, 1_000))
	assert.Equal(t, 1, ChecksumDisplayPercent(1, 1_000_000))

	// Rounded down, so a checksum in its final stretch never fills the bar.
	assert.Equal(t, 21, ChecksumDisplayPercent(321_450, 1_466_232))
	assert.Equal(t, 99, ChecksumDisplayPercent(997_500, 1_000_000))
	assert.Equal(t, 99, ChecksumDisplayPercent(999_999, 1_000_000))
	assert.Equal(t, 100, ChecksumDisplayPercent(1_000_000, 1_000_000))

	// A verified count past the estimated total stays at 100%.
	assert.Equal(t, 100, ChecksumDisplayPercent(1_200_000, 1_000_000))
}

func TestRowCopyFraction(t *testing.T) {
	assert.InDelta(t, 45.37, RowCopyFraction(45, 453_700, 1_000_000), 0.0001)
	assert.InDelta(t, 100, RowCopyFraction(99, 1_100_000, 1_000_000), 0.0001)
	assert.InDelta(t, 45, RowCopyFraction(45, 0, 0), 0.0001)
	assert.InDelta(t, 100, RowCopyFraction(145, 0, 0), 0.0001)
	assert.InDelta(t, 100, RowCopyFraction(100, 1, 200_000), 0.0001, "a copy the engine finished short of its total is done")
}

// A copy covers the rows it copied, clamped to its total, or the whole total
// once the engine reports it finished short of the total.
func TestCopyCoveredRows(t *testing.T) {
	assert.Equal(t, int64(50_000), CopyCoveredRows(25, 50_000, 200_000))
	assert.Equal(t, int64(200_000), CopyCoveredRows(100, 1, 200_000))
	assert.Equal(t, int64(200_000), CopyCoveredRows(100, 0, 200_000))
	assert.Equal(t, int64(200_000), CopyCoveredRows(100, 250_000, 200_000))
	assert.Equal(t, int64(0), CopyCoveredRows(0, 0, 200_000))
	assert.Equal(t, int64(0), CopyCoveredRows(100, 0, 0))
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

	// Capped at 99.99% while copied rows still trail the total: "100.00%"
	// reads as done, and a copy in its final stretch is not.
	assert.Equal(t, "99.99%", FormatRowCopyPercent(99, 999_999_999, 1_000_000_000))
	assert.Equal(t, "99.99%", FormatRowCopyPercent(99, 43_234_523_344, 43_234_523_345))
	assert.Equal(t, "99.99%", FormatRowCopyPercent(99, 99_999, 100_000))

	// An engine reports 100 short of the total only when it declares the copy
	// finished, as when the copy skipped rows, so the copy reads as done.
	assert.Equal(t, "100.00%", FormatRowCopyPercent(100, 43_234_523_344, 43_234_523_345))
	assert.Equal(t, "100.00%", FormatRowCopyPercent(100, 1, 200_000))
	assert.Equal(t, "100.00%", FormatRowCopyPercent(100, 0, 200_000))

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

// An engine-reported statement renders as a single line of bounded width on
// every surface: line breaks, tabs, and control bytes fold into single spaces,
// text within the bound passes through untouched, and text past it is cut to
// exactly the bound with a trailing ellipsis.
func TestClampStatement(t *testing.T) {
	atBound := strings.Repeat("x", MaxStatementRunes)
	tests := []struct {
		name   string
		input  string
		expect string
	}{
		{name: "empty", input: "", expect: ""},
		{name: "plain statement unchanged", input: "CREATE INDEX idx ON t (c)", expect: "CREATE INDEX idx ON t (c)"},
		{name: "newlines tabs and control bytes fold to one space", input: "CREATE INDEX\r\n\tidx\x1b[31m ON t (c)", expect: "CREATE INDEX idx [31m ON t (c)"},
		{name: "surrounding whitespace dropped", input: "  ALTER TABLE t ADD c int \n", expect: "ALTER TABLE t ADD c int"},
		{name: "exactly at the bound passes through", input: atBound, expect: atBound},
		{name: "one past the bound is cut with an ellipsis", input: atBound + "y", expect: strings.Repeat("x", MaxStatementRunes-1) + "…"},
		{name: "cut counts runes not bytes", input: strings.Repeat("é", MaxStatementRunes+5), expect: strings.Repeat("é", MaxStatementRunes-1) + "…"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ClampStatement(tt.input)
			assert.Equal(t, tt.expect, got)
			assert.LessOrEqual(t, len([]rune(got)), MaxStatementRunes)
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

func TestFormatNumber(t *testing.T) {
	tests := []struct {
		name   string
		input  int64
		expect string
	}{
		{name: "zero", input: 0, expect: "0"},
		{name: "three digits are not grouped", input: 999, expect: "999"},
		{name: "first group boundary", input: 1000, expect: "1,000"},
		{name: "six digits", input: 123456, expect: "123,456"},
		{name: "seven digits", input: 1234567, expect: "1,234,567"},
		{name: "largest value", input: math.MaxInt64, expect: "9,223,372,036,854,775,807"},
		{name: "small negative", input: -999, expect: "-999"},
		{name: "negative keeps its sign out of the groups", input: -123456, expect: "-123,456"},
		{name: "negative first group boundary", input: -1000, expect: "-1,000"},
		{name: "smallest value", input: math.MinInt64, expect: "-9,223,372,036,854,775,808"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expect, FormatNumber(tt.input))
		})
	}
}

// The engine counts rows inserted during the copy, so rows copied can pass the
// estimated total. Progress shows the total and says the estimate was exceeded;
// an unknown total (zero) shows the count as reported.
func TestRowCopyEstimateHelpers(t *testing.T) {
	tests := []struct {
		name         string
		copied       int64
		total        int64
		wantClamped  int64
		wantExceeded bool
	}{
		{name: "copy under the estimate", copied: 400, total: 1000, wantClamped: 400, wantExceeded: false},
		{name: "copy exactly at the estimate", copied: 1000, total: 1000, wantClamped: 1000, wantExceeded: false},
		{name: "copy past the estimate", copied: 1200, total: 1000, wantClamped: 1000, wantExceeded: true},
		{name: "unknown total", copied: 1200, total: 0, wantClamped: 1200, wantExceeded: false},
		{name: "nothing copied yet", copied: 0, total: 1000, wantClamped: 0, wantExceeded: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.wantClamped, ClampRows(tt.copied, tt.total))
			assert.Equal(t, tt.wantExceeded, EstimateExceeded(tt.copied, tt.total))
		})
	}
}
