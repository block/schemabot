package postgresconn

import (
	"database/sql"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A timestamp parameter carrying a non-UTC location encodes as the UTC reading
// of the instant it names, in both wire formats and for every Go shape storage
// binds: a time.Time, a pointer to one, a sql.NullTime, pgx's own Timestamp,
// and a timestamp[] array. The expected bytes are what pgx's default codec
// produces for the same instant already converted to UTC.
func TestUTCTimestampCodecEncodesTheUTCReading(t *testing.T) {
	driverZone := time.FixedZone("UTC-5", -5*60*60)
	instant := time.Date(2026, time.September, 28, 6, 30, 0, 123456000, driverZone)
	utcReading := instant.UTC()

	defaults := pgtype.NewMap()
	pinned := pgtype.NewMap()
	registerUTCTimestampTypes(pinned)

	cases := []struct {
		name  string
		oid   uint32
		value any
		want  any
	}{
		{name: "time.Time", oid: pgtype.TimestampOID, value: instant, want: utcReading},
		{name: "pointer", oid: pgtype.TimestampOID, value: &instant, want: utcReading},
		{name: "sql.NullTime", oid: pgtype.TimestampOID, value: sql.NullTime{Time: instant, Valid: true}, want: utcReading},
		{name: "pgtype.Timestamp", oid: pgtype.TimestampOID, value: pgtype.Timestamp{Time: instant, Valid: true}, want: utcReading},
		{name: "array", oid: pgtype.TimestampArrayOID, value: []time.Time{instant}, want: []time.Time{utcReading}},
	}
	formats := map[string]int16{"text": pgtype.TextFormatCode, "binary": pgtype.BinaryFormatCode}
	for _, tc := range cases {
		for formatName, format := range formats {
			t.Run(tc.name+"/"+formatName, func(t *testing.T) {
				got, err := pinned.Encode(tc.oid, format, tc.value, nil)
				require.NoError(t, err)
				want, err := defaults.Encode(tc.oid, format, tc.want, nil)
				require.NoError(t, err)
				assert.Equal(t, want, got)
			})
		}
	}

	text, err := pinned.Encode(pgtype.TimestampOID, pgtype.TextFormatCode, instant, nil)
	require.NoError(t, err)
	assert.Equal(t, "2026-09-28 11:30:00.123456", string(text), "the stored wall clock is the UTC one")
}

// A NULL timestamp parameter stays NULL.
func TestUTCTimestampCodecKeepsNull(t *testing.T) {
	pinned := pgtype.NewMap()
	registerUTCTimestampTypes(pinned)

	got, err := pinned.Encode(pgtype.TimestampOID, pgtype.BinaryFormatCode, sql.NullTime{}, nil)
	require.NoError(t, err)
	assert.Nil(t, got)
}
