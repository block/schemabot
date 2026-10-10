package mysqlerr

import (
	"errors"
	"slices"

	"github.com/block/mysql"
)

// Number reports the MySQL server error code carried by err.
//
// It exists so no call site asserts a driver's error type directly. SchemaBot
// links exactly one MySQL driver — block/mysql — and that is a property worth
// keeping rather than assuming: it stopped being true once, when the
// credential-reloading storage pool went through a hot-swap DSN driver that
// embedded upstream go-sql-driver/mysql and could not be pointed at the fork.
// The two *mysql.MySQLError structs are field-identical and carry the same
// codes, but they are distinct types in distinct packages, so errors.As against
// one silently returns false for the other. Silently is the problem: a retry
// classifier that checks only one type does not fail loudly on the other, it
// just stops recognizing deadlocks and starts surfacing them as permanent
// errors. Reading the code through here means a second driver re-entering the
// graph is a change to one function rather than a hunt through every classifier
// in the repo.
func Number(err error) (uint16, bool) {
	var mysqlErr *mysql.MySQLError
	if errors.As(err, &mysqlErr) {
		return mysqlErr.Number, true
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
