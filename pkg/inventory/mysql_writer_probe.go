package inventory

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/block/spirit/pkg/utils"

	"github.com/block/schemabot/pkg/mysqlconn"
)

// MySQLWriterProbe is the MySQL WriterProbe. It opens a short-lived connection
// through mysqlconn, so a probe dials with the same TLS settings the schema
// change itself will use.
//
// A server is writable only when both read_only and innodb_read_only are off:
// read_only alone does not cover a server whose storage engine is read-only,
// which is how a managed replica can report read_only=0. super_read_only needs
// no check of its own, since turning it on also turns on read_only.
//
// The probe needs the REPLICATION CLIENT privilege to read SHOW REPLICA STATUS,
// the same privilege the schema change engine already requires.
type MySQLWriterProbe struct {
	// ConnectTimeout bounds the dial. The caller's context bounds the queries.
	ConnectTimeout time.Duration
}

var _ WriterProbe = MySQLWriterProbe{}

// ProbeWriter reports the read-only state, server identity, and replication
// sources of the server behind dsn.
func (p MySQLWriterProbe) ProbeWriter(ctx context.Context, dsn string) (WriterStatus, error) {
	db, err := mysqlconn.Open(dsn, mysqlconn.WithConnectTimeout(p.ConnectTimeout))
	if err != nil {
		return WriterStatus{}, fmt.Errorf("open writer probe connection: %w", err)
	}
	defer utils.CloseAndLog(db)
	db.SetMaxOpenConns(1)

	conn, err := db.Conn(ctx)
	if err != nil {
		return WriterStatus{}, fmt.Errorf("connect writer probe: %w", err)
	}
	defer utils.CloseAndLog(conn)

	var readOnly, innodbReadOnly bool
	var status WriterStatus
	if err := conn.QueryRowContext(ctx, "SELECT @@global.read_only, @@global.innodb_read_only, @@global.server_uuid").Scan(&readOnly, &innodbReadOnly, &status.ServerID); err != nil {
		return WriterStatus{}, fmt.Errorf("read read-only settings and server identity: %w", err)
	}
	switch {
	case readOnly:
		status.ReadOnlyReason = "read_only=1"
	case innodbReadOnly:
		status.ReadOnlyReason = "innodb_read_only=1"
	default:
		status.Writable = true
	}

	sources, err := mysqlReplicationSources(ctx, conn)
	if err != nil {
		return WriterStatus{}, err
	}
	status.SourceIDs = sources
	return status, nil
}

// mysqlReplicationSources returns the Source_UUID of every replication channel
// on the server. A channel that has never connected to its source reports no
// UUID and contributes nothing, so it cannot vouch for a link.
func mysqlReplicationSources(ctx context.Context, conn *sql.Conn) ([]string, error) {
	rows, err := conn.QueryContext(ctx, "SHOW REPLICA STATUS")
	if err != nil {
		return nil, fmt.Errorf("read replication status: %w", err)
	}
	defer utils.CloseAndLog(rows)

	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("read replication status columns: %w", err)
	}
	sourceUUID := -1
	for i, name := range columns {
		if name == "Source_UUID" {
			sourceUUID = i
			break
		}
	}
	if sourceUUID < 0 {
		return nil, fmt.Errorf("replication status has no Source_UUID column")
	}

	var sources []string
	for rows.Next() {
		values := make([]sql.NullString, len(columns))
		dest := make([]any, len(columns))
		for i := range values {
			dest[i] = &values[i]
		}
		if err := rows.Scan(dest...); err != nil {
			return nil, fmt.Errorf("scan replication status: %w", err)
		}
		if uuid := values[sourceUUID]; uuid.Valid && uuid.String != "" {
			sources = append(sources, uuid.String)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate replication status: %w", err)
	}
	return sources, nil
}
