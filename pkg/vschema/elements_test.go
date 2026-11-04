package vschema

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Each table and vindex is its own element and everything else the keyspace
// declares is one more, each in a canonical form that ignores formatting, key
// order, and zero values Vitess does not store.
func TestElements(t *testing.T) {
	got, err := Elements(`{
  "sharded": true,
  "tables": {"orders": {"column_vindexes": [{"name": "hash", "column": "customer_id"}]}},
  "vindexes": {"hash": {"type": "hash"}}
}`)
	require.NoError(t, err)
	assert.Equal(t, map[string]string{
		"table orders":          `{"column_vindexes":[{"column":"customer_id","name":"hash"}]}`,
		"vindex hash":           `{"type":"hash"}`,
		"the keyspace settings": `{"sharded":true}`,
	}, got)

	empty, err := Elements("  ")
	require.NoError(t, err)
	unsharded, err := Elements(`{"sharded": false}`)
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"the keyspace settings": "{}"}, empty)
	assert.Equal(t, empty, unsharded, "an explicit zero value is the same as an absent one")
}
