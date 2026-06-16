package inventory

import (
	"fmt"
	"maps"
	"net"

	"github.com/go-sql-driver/mysql"
)

// ConnectionAssembler turns a resolved endpoint and credentials into the
// connection fields of a Target for a specific database engine.
//
// It is the engine axis of resolution: written once per engine and reused
// across every inventory source, so endpoint resolution (which source) and
// credential resolution (which secret backend) stay engine-agnostic. Adding an
// engine means adding an assembler, not a new per-source resolver.
type ConnectionAssembler interface {
	// DatabaseType is the engine this assembler targets.
	DatabaseType() string
	// Assemble builds the connection fields of a Target — a DSN and/or
	// engine-specific Metadata — from a resolved endpoint host, the endpoint's
	// attributes, and credentials.
	Assemble(host string, attrs map[string]string, creds *Credentials) (dsn string, metadata map[string]string, err error)
}

// MySQLConnectionAssembler assembles a namespace-free MySQL DSN. The schema is
// injected per operation by the data plane, so the DSN carries no database name.
type MySQLConnectionAssembler struct {
	// DefaultPort is appended to the host when it has no port.
	DefaultPort string
	// Params are extra MySQL DSN parameters (for example TLS settings).
	Params map[string]string
	// Metadata is attached to every assembled target for engine-specific
	// configuration the data plane reads.
	Metadata map[string]string
}

var _ ConnectionAssembler = MySQLConnectionAssembler{}

// DatabaseType returns the MySQL engine type.
func (MySQLConnectionAssembler) DatabaseType() string { return "mysql" }

// Assemble builds a namespace-free MySQL DSN from the host and credentials. The
// endpoint attributes are unused for MySQL.
func (a MySQLConnectionAssembler) Assemble(host string, _ map[string]string, creds *Credentials) (string, map[string]string, error) {
	if host == "" {
		return "", nil, fmt.Errorf("mysql connection requires a host")
	}
	if creds == nil {
		return "", nil, fmt.Errorf("mysql connection requires credentials")
	}
	// Append the default port only when the host has none. net.SplitHostPort
	// errors when no port is present, including for bare IPv6 addresses; in that
	// case net.JoinHostPort adds the required brackets.
	if a.DefaultPort != "" {
		if _, _, err := net.SplitHostPort(host); err != nil {
			host = net.JoinHostPort(host, a.DefaultPort)
		}
	}
	cfg := mysql.NewConfig()
	cfg.User = creds.Username
	cfg.Passwd = creds.Password
	cfg.Net = "tcp"
	cfg.Addr = host
	if len(a.Params) > 0 {
		cfg.Params = maps.Clone(a.Params)
	}
	return cfg.FormatDSN(), maps.Clone(a.Metadata), nil
}
