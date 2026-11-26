package vschema

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const describeBase = `{
  "sharded": true,
  "vindexes": {"hash": {"type": "hash"}},
  "tables": {
    "orders": {"column_vindexes": [{"column": "id", "name": "hash"}]}
  }
}`

// A VSchema change reads table by table: adding a table names that table and
// its routing, never the table above it, and removing one names only the
// table removed. Each kind of element reads in its own words.
func TestDescribeNamesEachChangedElement(t *testing.T) {
	cases := []struct {
		name             string
		current, desired string
		want             []string
	}{
		{
			name:    "a table added below an existing one",
			current: describeBase,
			desired: `{
  "sharded": true,
  "vindexes": {"hash": {"type": "hash"}},
  "tables": {
    "orders": {"column_vindexes": [{"column": "id", "name": "hash"}]},
    "orders_archive": {"column_vindexes": [{"column": "id", "name": "hash"}]}
  }
}`,
			want: []string{"adds table `orders_archive`: vindex `hash` on `id`"},
		},
		{
			name: "a table removed",
			current: `{
  "sharded": true,
  "vindexes": {"hash": {"type": "hash"}},
  "tables": {
    "orders": {"column_vindexes": [{"column": "id", "name": "hash"}]},
    "orders_archive": {"column_vindexes": [{"column": "id", "name": "hash"}]}
  }
}`,
			desired: describeBase,
			want:    []string{"removes table `orders_archive`"},
		},
		{
			name:    "a table's primary vindex replaced",
			current: describeBase,
			desired: `{
  "sharded": true,
  "vindexes": {"hash": {"type": "hash"}, "xxhash": {"type": "xxhash"}},
  "tables": {
    "orders": {"column_vindexes": [{"column": "id", "name": "xxhash"}]}
  }
}`,
			want: []string{
				"table `orders`: replaces primary vindex `hash` on `id` with `xxhash` on `id`",
				"adds vindex `xxhash`: type `xxhash`",
			},
		},
		{
			name:    "a secondary lookup vindex added",
			current: describeBase,
			desired: `{
  "sharded": true,
  "vindexes": {
    "hash": {"type": "hash"},
    "orders_email_lookup": {"type": "consistent_lookup", "owner": "orders", "params": {"table": "orders_email_idx", "from": "email", "to": "keyspace_id"}}
  },
  "tables": {
    "orders": {"column_vindexes": [{"column": "id", "name": "hash"}, {"columns": ["email"], "name": "orders_email_lookup"}]}
  }
}`,
			want: []string{
				"table `orders`: adds vindex `orders_email_lookup` on `email`",
				"adds vindex `orders_email_lookup`: type `consistent_lookup`, owned by `orders`, backed by `orders_email_idx`",
			},
		},
		{
			name:    "a table given a sequence",
			current: describeBase,
			desired: `{
  "sharded": true,
  "vindexes": {"hash": {"type": "hash"}},
  "tables": {
    "orders": {"column_vindexes": [{"column": "id", "name": "hash"}], "auto_increment": {"column": "id", "sequence": "orders_seq"}}
  }
}`,
			want: []string{"table `orders`: adds sequence `orders_seq` for `id`"},
		},
		{
			name:    "a table's column list changed",
			current: describeBase,
			desired: `{
  "sharded": true,
  "vindexes": {"hash": {"type": "hash"}},
  "tables": {
    "orders": {"column_vindexes": [{"column": "id", "name": "hash"}], "columns": [{"name": "region", "type": "VARCHAR"}]}
  }
}`,
			want: []string{"table `orders`: adds column `region`"},
		},
		{
			name:    "a vindex's type changed",
			current: describeBase,
			desired: `{
  "sharded": true,
  "vindexes": {"hash": {"type": "xxhash"}},
  "tables": {
    "orders": {"column_vindexes": [{"column": "id", "name": "hash"}]}
  }
}`,
			want: []string{"vindex `hash`: changes type from `hash` to `xxhash`"},
		},
		{
			name: "a lookup vindex's type and backing table changed together",
			current: `{
  "sharded": true,
  "vindexes": {
    "hash": {"type": "hash"},
    "orders_lookup": {"type": "lookup_hash", "owner": "orders", "params": {"table": "orders_idx", "from": "email", "to": "keyspace_id"}}
  },
  "tables": {
    "orders": {"column_vindexes": [{"column": "id", "name": "hash"}, {"columns": ["email"], "name": "orders_lookup"}]}
  }
}`,
			desired: `{
  "sharded": true,
  "vindexes": {
    "hash": {"type": "hash"},
    "orders_lookup": {"type": "consistent_lookup", "owner": "orders", "params": {"table": "orders_idx_v2", "from": "email", "to": "keyspace_id"}}
  },
  "tables": {
    "orders": {"column_vindexes": [{"column": "id", "name": "hash"}, {"columns": ["email"], "name": "orders_lookup"}]}
  }
}`,
			want: []string{"vindex `orders_lookup`: changes type from `lookup_hash` to `consistent_lookup`; changes param `table` from `orders_idx` to `orders_idx_v2`"},
		},
		{
			name:    "a table's primary vindex and column list changed together",
			current: describeBase,
			desired: `{
  "sharded": true,
  "vindexes": {"hash": {"type": "hash"}, "xxhash": {"type": "xxhash"}},
  "tables": {
    "orders": {"column_vindexes": [{"column": "id", "name": "xxhash"}], "columns": [{"name": "region", "type": "VARCHAR"}]}
  }
}`,
			want: []string{
				"table `orders`: replaces primary vindex `hash` on `id` with `xxhash` on `id`; adds column `region`",
				"adds vindex `xxhash`: type `xxhash`",
			},
		},
		{
			name: "a vindex added to a table that had none",
			current: `{
  "sharded": true,
  "vindexes": {"hash": {"type": "hash"}},
  "tables": {"orders": {}}
}`,
			desired: describeBase,
			want:    []string{"table `orders`: adds vindex `hash` on `id`"},
		},
		{
			name:    "every vindex removed from a table",
			current: describeBase,
			desired: `{
  "sharded": true,
  "vindexes": {"hash": {"type": "hash"}},
  "tables": {"orders": {}}
}`,
			want: []string{"table `orders`: removes vindex `hash` on `id`"},
		},
		{
			name:    "a sequence table and a plain table added to an unsharded keyspace",
			current: `{}`,
			desired: `{"tables": {"orders_seq": {"type": "sequence"}, "settings": {}}}`,
			want: []string{
				"adds table `orders_seq`: sequence table",
				"adds table `settings`",
			},
		},
		{
			name:    "the keyspace made sharded",
			current: `{}`,
			desired: `{"sharded": true}`,
			want:    []string{"makes the keyspace sharded"},
		},
		{
			name:    "formatting and key order only",
			current: describeBase,
			desired: `{"tables": {"orders": {"column_vindexes": [{"name": "hash", "column": "id"}]}}, "vindexes": {"hash": {"type": "hash"}}, "sharded": true}`,
			want:    nil,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Describe(tc.current, tc.desired)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestDescribeRejectsAnUnreadableDocument(t *testing.T) {
	_, err := Describe(describeBase, `{"tables": `)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse desired VSchema")
}

// A VSchema name is free text, so it renders as a code span it cannot break
// out of: line breaks and control characters cannot start a new markdown line,
// and a backtick inside the name cannot close the span early.
func TestCodeKeepsANameInsideItsSpan(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"orders", "`orders`"},
		{"odd`name", "`` odd`name ``"},
		{"two``ticks", "``` two``ticks ```"},
		{"line\nbreak", "`line break`"},
		{"tab\tand\r\nreturn", "`tab and return`"},
		{"bell\x07zero\u200bwidth", "`bellzerowidth`"},
		{"", "` `"},
	} {
		assert.Equal(t, tc.want, code(tc.in), "%q", tc.in)
	}
}

// A table's type reads in fixed words, so a type Vitess does not define is
// never echoed into the summary.
func TestTableKindNamesOnlyTheTypesVitessDefines(t *testing.T) {
	assert.Equal(t, "", tableKind(""))
	assert.Equal(t, "sequence table", tableKind("sequence"))
	assert.Equal(t, "reference table", tableKind("reference"))
	assert.Equal(t, "table of an unknown type", tableKind("**bold**\n# heading"))
}
