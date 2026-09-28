package postgresconn

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

// registerUTCTimestamps installs utcTimestampCodec on a new connection, so
// every parameter bound to a plain timestamp on that connection is written as
// its UTC reading. It runs as the stdlib connector's after-connect hook, before
// the connection serves any statement.
func registerUTCTimestamps(_ context.Context, conn *pgx.Conn) error {
	registerUTCTimestampTypes(conn.TypeMap())
	return nil
}

// registerUTCTimestampTypes replaces the codec for timestamp (without time
// zone) and for arrays of it in m. The array type is registered again because
// an array codec holds its element type by reference, and the default
// timestamp[] still points at the default element codec.
func registerUTCTimestampTypes(m *pgtype.Map) {
	timestamp := &pgtype.Type{Name: "timestamp", OID: pgtype.TimestampOID, Codec: &utcTimestampCodec{}}
	m.RegisterType(timestamp)
	m.RegisterType(&pgtype.Type{Name: "_timestamp", OID: pgtype.TimestampArrayOID, Codec: &pgtype.ArrayCodec{ElementType: timestamp}})
}

// utcTimestampCodec is pgx's timestamp codec with one change: a parameter is
// converted to UTC before it is encoded.
//
// A plain timestamp stores a wall-clock reading and no zone, and pgx encodes a
// time.Time into one by keeping the value's own wall clock and discarding its
// location. A value stamped from time.Now() in a process whose local zone is
// not UTC would therefore be stored as a different instant, hours off from the
// one it names. SchemaBot's storage compares those columns against the
// session's now(), which this package pins to UTC, so every stored reading has
// to be a UTC one. Converting here, on the connection, makes that hold for
// every writer rather than for each call site that remembers to call UTC().
//
// Scanning is pgx's own and needs no change: a plain timestamp column reads
// back as a UTC time.Time.
type utcTimestampCodec struct {
	pgtype.TimestampCodec
}

// PlanEncode wraps pgx's timestamp encode plan so the value reaches it in UTC.
// It accepts what pgx's plan accepts: pgx unwraps time.Time, pointers to it,
// and driver.Valuer values such as sql.NullTime into a pgtype.TimestampValuer
// before asking the codec for a plan.
func (c *utcTimestampCodec) PlanEncode(m *pgtype.Map, oid uint32, format int16, value any) pgtype.EncodePlan {
	if _, ok := value.(pgtype.TimestampValuer); !ok {
		return nil
	}
	next := c.TimestampCodec.PlanEncode(m, oid, format, pgtype.Timestamp{})
	if next == nil {
		return nil
	}
	return utcTimestampEncodePlan{next: next}
}

// utcTimestampEncodePlan converts a timestamp parameter to UTC and hands it to
// pgx's own encode plan for the negotiated format.
type utcTimestampEncodePlan struct {
	next pgtype.EncodePlan
}

// Encode implements pgtype.EncodePlan.
func (p utcTimestampEncodePlan) Encode(value any, buf []byte) ([]byte, error) {
	ts, err := value.(pgtype.TimestampValuer).TimestampValue()
	if err != nil {
		return nil, fmt.Errorf("read timestamp parameter %T: %w", value, err)
	}
	ts.Time = ts.Time.UTC()
	return p.next.Encode(ts, buf)
}
