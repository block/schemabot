package targetauth

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"syscall"
	"testing"

	"github.com/block/mysql"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClassify(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want Classification
	}{
		{name: "MySQL invalid credentials", err: &mysql.MySQLError{Number: 1045}, want: AuthInvalidCredentials},
		{name: "MySQL no grant", err: &mysql.MySQLError{Number: 1044}, want: AuthNoGrant},
		{name: "MySQL no database", err: &mysql.MySQLError{Number: 1049}, want: AuthNoDatabase},
		{name: "PostgreSQL invalid credentials", err: &pgconn.PgError{Code: "28P01"}, want: AuthInvalidCredentials},
		{name: "PostgreSQL invalid authorization", err: &pgconn.PgError{Code: "28000"}, want: AuthNoGrant},
		{name: "PostgreSQL insufficient privilege", err: &pgconn.PgError{Code: "42501"}, want: AuthNoGrant},
		{name: "PostgreSQL no database", err: &pgconn.PgError{Code: "3D000"}, want: AuthNoDatabase},
	}

	wrappers := []struct {
		name string
		wrap func(error) error
	}{
		{name: "bare", wrap: func(err error) error { return err }},
		{name: "wrapped", wrap: func(err error) error { return fmt.Errorf("connect target: %w", err) }},
		{name: "double wrapped", wrap: func(err error) error { return fmt.Errorf("plan target: %w", fmt.Errorf("connect target: %w", err)) }},
	}

	for _, test := range tests {
		for _, wrapper := range wrappers {
			t.Run(test.name+"/"+wrapper.name, func(t *testing.T) {
				assert.Equal(t, test.want, Classify(wrapper.wrap(test.err)))
			})
		}
	}
}

func TestClassifyNotAuth(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{name: "nil"},
		{name: "generic", err: errors.New("generic failure")},
		{name: "connection refused", err: &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}},
		{name: "canceled", err: context.Canceled},
		{name: "deadline", err: context.DeadlineExceeded},
		{name: "certificate verification", err: &tls.CertificateVerificationError{Err: errors.New("unknown authority")}},
		{name: "TLS negotiation", err: errors.New("TLS negotiation failed")},
		{name: "PostgreSQL lock unavailable", err: &pgconn.PgError{Code: "55P03"}},
		{name: "PostgreSQL undefined table", err: &pgconn.PgError{Code: "42P01"}},
		{name: "MySQL duplicate entry", err: &mysql.MySQLError{Number: 1062}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, NotAuth, Classify(test.err))
			assert.Equal(t, test.err, Wrap(test.err))
		})
	}
}

func TestWrapAndClassificationOf(t *testing.T) {
	driverErr := &pgconn.PgError{Code: "28P01"}
	wrapped := Wrap(driverErr)
	require.Error(t, wrapped)

	classification, ok := ClassificationOf(fmt.Errorf("pull target schema: %w", wrapped))
	assert.True(t, ok)
	assert.Equal(t, AuthInvalidCredentials, classification)
	assert.ErrorIs(t, wrapped, driverErr)

	classification, ok = ClassificationOf(driverErr)
	assert.False(t, ok)
	assert.Equal(t, NotAuth, classification)
}

func TestClassificationStrings(t *testing.T) {
	tests := []struct {
		classification Classification
		name           string
		label          string
	}{
		{classification: AuthInvalidCredentials, name: "AuthInvalidCredentials", label: "auth_invalid_credentials"},
		{classification: AuthNoGrant, name: "AuthNoGrant", label: "auth_no_grant"},
		{classification: AuthNoDatabase, name: "AuthNoDatabase", label: "auth_no_database"},
		{classification: NotAuth, name: "NotAuth", label: "not_auth"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.name, test.classification.String())
			assert.Equal(t, test.label, test.classification.Label())
		})
	}
}
