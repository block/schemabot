package spirit

import (
	"errors"

	spiritmigration "github.com/block/spirit/pkg/migration"
	"github.com/block/spirit/pkg/statement"
	"github.com/block/spirit/pkg/status"
	"github.com/go-sql-driver/mysql"

	"github.com/block/schemabot/pkg/engine"
)

// MySQL server error numbers the target returns when it rejects a statement for
// a reason that belongs to the statement or to the data already in the table.
// Re-running the identical statement against the same target reproduces each of
// them exactly, so an apply that meets one has nothing to wait for.
const (
	// The rows already in the table do not satisfy the schema being applied.
	// Only a change to the data can make the statement succeed.
	errNullNotAllowed           = 1048 // ER_BAD_NULL_ERROR
	errDuplicateEntry           = 1062 // ER_DUP_ENTRY
	errDataOutOfRange           = 1264 // ER_WARN_DATA_OUT_OF_RANGE
	errTruncatedWrongValue      = 1292 // ER_TRUNCATED_WRONG_VALUE
	errTruncatedWrongFieldValue = 1366 // ER_TRUNCATED_WRONG_VALUE_FOR_FIELD
	errDataTooLong              = 1406 // ER_DATA_TOO_LONG
	errCheckConstraintViolated  = 3819 // ER_CHECK_CONSTRAINT_VIOLATED

	// The statement contradicts the schema it is being applied to, or is not
	// valid DDL. Only a change to the schema files can make it succeed.
	errUnknownColumn        = 1054 // ER_BAD_FIELD_ERROR
	errDuplicateColumnName  = 1060 // ER_DUP_FIELDNAME
	errDuplicateKeyName     = 1061 // ER_DUP_KEYNAME
	errParse                = 1064 // ER_PARSE_ERROR
	errInvalidDefault       = 1067 // ER_INVALID_DEFAULT
	errKeyColumnNotFound    = 1072 // ER_KEY_COLUMN_DOES_NOT_EXITS
	errCantDropFieldOrKey   = 1091 // ER_CANT_DROP_FIELD_OR_KEY
	errBlobKeyWithoutLength = 1170 // ER_BLOB_KEY_WITHOUT_LENGTH
	errPrimaryCantHaveNull  = 1171 // ER_PRIMARY_CANT_HAVE_NULL

	// The target refuses to perform the operation at all. A later attempt asks
	// for the same unsupported operation and is refused the same way.
	errNotSupportedYet                  = 1235 // ER_NOT_SUPPORTED_YET
	errAlterOperationNotSupported       = 1845 // ER_ALTER_OPERATION_NOT_SUPPORTED
	errAlterOperationNotSupportedReason = 1846 // ER_ALTER_OPERATION_NOT_SUPPORTED_REASON
)

// permanentDDLErrors is the set of target rejections an apply cannot retry its
// way out of. Membership is deliberately narrow: a rejection left out of the set
// keeps the automatic retries and costs the operator only the wait they were
// already spending, while a rejection wrongly added to it ends an apply that
// would have recovered on its own. Anything the target might answer differently
// once a lock, a lagging replica, disk, or a peer transaction has moved on —
// lock wait timeouts, deadlocks, read-only, connection loss — stays out.
var permanentDDLErrors = map[uint16]struct{}{
	errNullNotAllowed:                   {},
	errDuplicateEntry:                   {},
	errDataOutOfRange:                   {},
	errTruncatedWrongValue:              {},
	errTruncatedWrongFieldValue:         {},
	errDataTooLong:                      {},
	errCheckConstraintViolated:          {},
	errUnknownColumn:                    {},
	errDuplicateColumnName:              {},
	errDuplicateKeyName:                 {},
	errParse:                            {},
	errInvalidDefault:                   {},
	errKeyColumnNotFound:                {},
	errCantDropFieldOrKey:               {},
	errBlobKeyWithoutLength:             {},
	errPrimaryCantHaveNull:              {},
	errNotSupportedYet:                  {},
	errAlterOperationNotSupported:       {},
	errAlterOperationNotSupportedReason: {},
}

// permanentDDLRejection returns the target's error number when err carries a
// rejection an apply cannot retry its way out of. The number is what names the
// rejection in the log that records why an apply skipped operator recovery.
func permanentDDLRejection(err error) (uint16, bool) {
	var mysqlErr *mysql.MySQLError
	if !errors.As(err, &mysqlErr) {
		return 0, false
	}
	if _, permanent := permanentDDLErrors[mysqlErr.Number]; !permanent {
		return 0, false
	}
	return mysqlErr.Number, true
}

// checksumRejectedUniqueIndex reports whether a failed Spirit run failed the way
// a unique index over data that is not unique fails. That rejection never
// reaches the engine as a target error: the row copy drops the duplicate rows
// instead of refusing them, so the copy succeeds and the checksum that follows
// can never be made to agree with the source. The condition is the one Spirit
// itself uses to explain the failure — the run ended inside the checksum phase
// and the batch adds a unique constraint — read from the runner's structured
// status and the parsed statements rather than from the message Spirit renders.
//
// The caller reaches this only after ruling out a cancelled context, so a stop
// during the checksum is never mistaken for the target's verdict.
//
// The condition is coarser than the failure it names, because the error that
// would separate the two is not available here: a checksum that kept finding
// row differences is the unique-index rejection, while one that errored on
// every attempt is the transient failure the retries exist for, and both
// arrive with the differences discarded. The coarse read costs a transiently
// errored checksum its retries and the operator a re-apply; the alternative
// costs a rejected unique index its whole budget, one full table copy per
// attempt. Narrow this to the differences case once the run failure carries
// which one it was.
func checksumRejectedUniqueIndex(runner *spiritmigration.Runner, parsed []*statement.AbstractStatement) bool {
	if runner.Progress().CurrentState != status.Checksum {
		return false
	}
	for _, stmt := range parsed {
		if errors.Is(stmt.AlterContainsAddUnique(), statement.ErrAlterContainsUnique) {
			return true
		}
	}
	return false
}

// classifyExecutionFailure marks err permanent when the target rejected the
// statement for a reason a later attempt cannot change, and returns it unchanged
// otherwise. Recording the classification lets a failed schema change either
// enter operator recovery or go terminal immediately: spending a recovery budget
// on a rejection that reproduces every time buys nothing and holds the target's
// active-apply slot for the whole budget.
func classifyExecutionFailure(err error) error {
	if err == nil {
		return nil
	}
	if _, permanent := permanentDDLRejection(err); !permanent {
		return err
	}
	return &engine.PermanentError{Err: err}
}
