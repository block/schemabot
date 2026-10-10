// Package targetauth classifies authentication failures from target databases.
//
// Classification happens where a target session is opened, never at a caller
// that also sees storage errors: SchemaBot's own storage is MySQL or PostgreSQL
// too, and the same driver types would classify a storage outage as a target
// failure. Wrap is therefore called only on the error from the first ping of a
// target session. The open that precedes it (mysqlconn.Open, postgresconn.Open)
// parses the DSN and never dials, so the ping is the first point at which the
// server can refuse the session.
package targetauth

import (
	"errors"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/block/schemabot/pkg/mysqlerr"
)

// Classification identifies a target authentication outcome.
//
// Each value has one meaning on every engine. Where an engine refuses to
// distinguish two conditions, the classification says so instead of guessing:
// MySQL reports a missing database to a user without a global grant as
// "access denied" (1044) so that a scoped user cannot probe which databases
// exist, and a SchemaBot target user is scoped by design. That code is
// therefore AuthNoAccess, not AuthNoDatabase; only 1049 and 3D000, which fire
// when the server has confirmed the database is absent, are AuthNoDatabase.
type Classification uint8

const (
	// NotAuth is any error that is not a target authentication failure.
	NotAuth Classification = iota
	// AuthInvalidCredentials is a rejected user or password
	// (MySQL 1045, PostgreSQL 28P01).
	AuthInvalidCredentials
	// AuthNoAccess is a refusal to let the authenticated user into the database:
	// it is missing or not granted, and on MySQL (1044) the server does not say
	// which. On PostgreSQL (42501 at the connection boundary) the database exists
	// and the role lacks CONNECT on it.
	AuthNoAccess
	// AuthNoDatabase is a database the server has confirmed does not exist
	// (MySQL 1049, PostgreSQL 3D000).
	AuthNoDatabase
)

// String returns the classification name.
func (c Classification) String() string {
	switch c {
	case AuthInvalidCredentials:
		return "AuthInvalidCredentials"
	case AuthNoAccess:
		return "AuthNoAccess"
	case AuthNoDatabase:
		return "AuthNoDatabase"
	case NotAuth:
		return "NotAuth"
	default:
		return "unknown"
	}
}

// Label returns the stable lowercase label used in outcomes and reasons.
func (c Classification) Label() string {
	switch c {
	case AuthInvalidCredentials:
		return "auth_invalid_credentials"
	case AuthNoAccess:
		return "auth_no_access"
	case AuthNoDatabase:
		return "auth_no_database"
	case NotAuth:
		return "not_auth"
	default:
		return "unknown"
	}
}

// Classify identifies authentication failures reported by supported SQL drivers.
//
// PostgreSQL 28000 (invalid_authorization_specification) is deliberately not
// mapped. A rejected password or an unknown role both arrive as 28P01; 28000
// at connection time comes from pg_hba.conf policy or an unsatisfied client
// certificate requirement, which is a transport decision, not a grant, and
// no classification here would send an operator to the right place.
//
// 42501 is also a runtime code (permission denied for a table or schema). It
// classifies as AuthNoAccess only because Wrap is called at the connection
// boundary, where 42501 can only mean permission denied for the database.
func Classify(err error) Classification {
	if number, ok := mysqlerr.Number(err); ok {
		switch number {
		case 1045:
			return AuthInvalidCredentials
		case 1044:
			return AuthNoAccess
		case 1049:
			return AuthNoDatabase
		}
	}

	var postgresErr *pgconn.PgError
	if errors.As(err, &postgresErr) {
		switch postgresErr.Code {
		case "28P01":
			return AuthInvalidCredentials
		case "42501":
			return AuthNoAccess
		case "3D000":
			return AuthNoDatabase
		}
	}

	return NotAuth
}

// Error carries an authentication classification from a target connection boundary.
type Error struct {
	Classification Classification
	Err            error
}

// Error returns the underlying driver error text.
func (e *Error) Error() string { return e.Err.Error() }

// Unwrap returns the classified driver error.
func (e *Error) Unwrap() error { return e.Err }

// Wrap marks a classified target authentication failure. Other errors are
// unchanged.
//
// Call it on the first ping of a target session and nowhere else: a ping is
// the only place a driver error can mean "the target refused this session".
// At a query boundary the same codes mean something else (42501 becomes a
// missing table privilege), and at a storage boundary the same driver types
// belong to SchemaBot's own database.
func Wrap(err error) error {
	classification := Classify(err)
	if classification == NotAuth {
		return err
	}
	return &Error{Classification: classification, Err: err}
}

// ClassificationOf returns a classification carried from a target connection boundary.
func ClassificationOf(err error) (Classification, bool) {
	var targetErr *Error
	if !errors.As(err, &targetErr) {
		return NotAuth, false
	}
	return targetErr.Classification, true
}
