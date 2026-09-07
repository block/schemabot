package mysqlerr

import (
	"errors"
	"slices"

	blockmysql "github.com/block/mysql"
	upstreammysql "github.com/go-sql-driver/mysql"
)

// Number reports the MySQL server error code carried by err, from either MySQL
// driver linked into this binary.
//
// Two are linked, and that is not incidental. SchemaBot opens its own pools
// with block/mysql (registered as "block-mysql"), but the credential-reloading
// storage pool goes through go-mysql/hotswap-dsn-driver, which embeds upstream
// go-sql-driver/mysql and cannot be pointed at the fork. So a pool's errors are
// upstream's *mysql.MySQLError or the fork's depending on which opened it.
//
// The two structs are field-identical and carry the same codes, but they are
// distinct types in distinct packages, so errors.As against one silently
// returns false for the other. Silently is the problem: a retry classifier that
// checks only one type does not fail loudly on the other, it just stops
// recognizing deadlocks and starts surfacing them as permanent errors. Reading
// the code through here instead of asserting a driver's type at the call site
// is what keeps that from depending on which pool an error came from.
func Number(err error) (uint16, bool) {
	var blockErr *blockmysql.MySQLError
	if errors.As(err, &blockErr) {
		return blockErr.Number, true
	}
	var upstreamErr *upstreammysql.MySQLError
	if errors.As(err, &upstreamErr) {
		return upstreamErr.Number, true
	}
	return 0, false
}

// Is reports whether err is a MySQL server error with any of the given codes.
// It is the common shape of a Number call and exists so callers testing a
// fixed set of codes do not each re-derive it.
func Is(err error, codes ...uint16) bool {
	number, ok := Number(err)
	if !ok {
		return false
	}
	return slices.Contains(codes, number)
}
