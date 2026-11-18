package inventory

import (
	"errors"
	"testing"

	"github.com/block/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A server that cannot parse SHOW REPLICA STATUS predates it, and the probe
// says so in words an operator can act on. Any other failure stays a plain
// replication status error.
func TestReplicationStatusError(t *testing.T) {
	parse := &mysql.MySQLError{Number: 1064, Message: "You have an error in your SQL syntax"}
	var unsupported *UnsupportedServerError
	require.ErrorAs(t, replicationStatusError(parse), &unsupported)
	assert.Equal(t, "the server does not support SHOW REPLICA STATUS, which needs MySQL 8.0.22 or later", unsupported.Reason)
	assert.ErrorIs(t, unsupported, parse)

	denied := &mysql.MySQLError{Number: 1227, Message: "Access denied; you need the REPLICATION CLIENT privilege"}
	err := replicationStatusError(denied)
	assert.False(t, errors.As(err, &unsupported))
	assert.ErrorIs(t, err, denied)
	assert.Contains(t, err.Error(), "read replication status")
}
