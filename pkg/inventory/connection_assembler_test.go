package inventory

import (
	"testing"

	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMySQLConnectionAssemblerBuildsNamespaceFreeDSN(t *testing.T) {
	a := MySQLConnectionAssembler{DefaultPort: "3306", Metadata: map[string]string{"pending_drops": "false"}}

	dsn, meta, err := a.Assemble("orders.example", nil, &Credentials{Username: "ddl", Password: "secret"})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"pending_drops": "false"}, meta)

	cfg, err := mysql.ParseDSN(dsn)
	require.NoError(t, err)
	assert.Equal(t, "ddl", cfg.User)
	assert.Equal(t, "secret", cfg.Passwd)
	assert.Equal(t, "tcp", cfg.Net)
	assert.Equal(t, "orders.example:3306", cfg.Addr)
	assert.Equal(t, "", cfg.DBName, "MySQL DSN must be namespace-free; the request supplies the schema")
}

func TestMySQLConnectionAssemblerKeepsExistingPort(t *testing.T) {
	a := MySQLConnectionAssembler{DefaultPort: "3306"}

	dsn, _, err := a.Assemble("orders.example:3307", nil, &Credentials{Username: "ddl", Password: "secret"})
	require.NoError(t, err)
	cfg, err := mysql.ParseDSN(dsn)
	require.NoError(t, err)
	assert.Equal(t, "orders.example:3307", cfg.Addr)
}

func TestMySQLConnectionAssemblerPassesParams(t *testing.T) {
	a := MySQLConnectionAssembler{Params: map[string]string{"foo": "bar"}}

	dsn, _, err := a.Assemble("orders.example:3306", nil, &Credentials{Username: "ddl", Password: "secret"})
	require.NoError(t, err)
	cfg, err := mysql.ParseDSN(dsn)
	require.NoError(t, err)
	assert.Equal(t, "bar", cfg.Params["foo"])
}

// A bare IPv6 host has colons but no port; the default port must be appended
// with brackets so the resulting MySQL address is valid.
func TestMySQLConnectionAssemblerBracketsIPv6WithDefaultPort(t *testing.T) {
	a := MySQLConnectionAssembler{DefaultPort: "3306"}

	dsn, _, err := a.Assemble("2001:db8::1", nil, &Credentials{Username: "ddl", Password: "secret"})
	require.NoError(t, err)
	cfg, err := mysql.ParseDSN(dsn)
	require.NoError(t, err)
	assert.Equal(t, "[2001:db8::1]:3306", cfg.Addr)
}

func TestMySQLConnectionAssemblerKeepsBracketedIPv6Port(t *testing.T) {
	a := MySQLConnectionAssembler{DefaultPort: "3306"}

	dsn, _, err := a.Assemble("[2001:db8::1]:3307", nil, &Credentials{Username: "ddl", Password: "secret"})
	require.NoError(t, err)
	cfg, err := mysql.ParseDSN(dsn)
	require.NoError(t, err)
	assert.Equal(t, "[2001:db8::1]:3307", cfg.Addr)
}

func TestMySQLConnectionAssemblerRequiresHost(t *testing.T) {
	_, _, err := MySQLConnectionAssembler{}.Assemble("", nil, &Credentials{Username: "ddl", Password: "secret"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "host")
}

func TestMySQLConnectionAssemblerRequiresCredentials(t *testing.T) {
	_, _, err := MySQLConnectionAssembler{}.Assemble("orders.example:3306", nil, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "credentials")
}

func TestMySQLConnectionAssemblerDatabaseType(t *testing.T) {
	assert.Equal(t, "mysql", MySQLConnectionAssembler{}.DatabaseType())
}
