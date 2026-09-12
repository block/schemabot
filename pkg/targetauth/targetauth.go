// Package targetauth classifies authentication failures from target databases.
package targetauth

import (
	"errors"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/block/schemabot/pkg/mysqlerr"
)

// Classification identifies a target authentication outcome.
type Classification uint8

const (
	NotAuth Classification = iota
	AuthInvalidCredentials
	AuthNoGrant
	AuthNoDatabase
)

// String returns the classification name.
func (c Classification) String() string {
	switch c {
	case AuthInvalidCredentials:
		return "AuthInvalidCredentials"
	case AuthNoGrant:
		return "AuthNoGrant"
	case AuthNoDatabase:
		return "AuthNoDatabase"
	case NotAuth:
		return "NotAuth"
	default:
		return "NotAuth"
	}
}

// Label returns the stable lowercase label used in outcomes and reasons.
func (c Classification) Label() string {
	switch c {
	case AuthInvalidCredentials:
		return "auth_invalid_credentials"
	case AuthNoGrant:
		return "auth_no_grant"
	case AuthNoDatabase:
		return "auth_no_database"
	case NotAuth:
		return "not_auth"
	default:
		return "not_auth"
	}
}

// Classify identifies authentication failures reported by supported SQL drivers.
func Classify(err error) Classification {
	if number, ok := mysqlerr.Number(err); ok {
		switch number {
		case 1045:
			return AuthInvalidCredentials
		case 1044:
			return AuthNoGrant
		case 1049:
			return AuthNoDatabase
		}
	}

	var postgresErr *pgconn.PgError
	if errors.As(err, &postgresErr) {
		switch postgresErr.Code {
		case "28P01":
			return AuthInvalidCredentials
		case "28000", "42501":
			return AuthNoGrant
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

// Wrap marks a classified target authentication failure. Other errors are unchanged.
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
