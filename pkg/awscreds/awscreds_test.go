package awscreds

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/inventory"
)

// fakeFetcher records the account and secret name it was asked for and returns
// a configured payload, so the resolver logic can be tested without AWS.
type fakeFetcher struct {
	gotAccount string
	gotSecret  string
	payload    string
	err        error
}

func (f *fakeFetcher) FetchSecret(_ context.Context, accountID, secretName string) (string, error) {
	f.gotAccount = accountID
	f.gotSecret = secretName
	return f.payload, f.err
}

func TestResolverReadsJSONSecretForTargetAccount(t *testing.T) {
	fetch := &fakeFetcher{payload: `{"username":"spirit","password":"s3cret"}`}
	r := newResolver("aws_account_id", "ods-rds-spirit-password", fetch, nil)

	creds, err := r.ResolveCredentials(t.Context(),
		inventory.Request{Target: "orders-dsid"},
		map[string]string{"aws_account_id": "497546275604"})
	require.NoError(t, err)

	assert.Equal(t, "497546275604", fetch.gotAccount)
	assert.Equal(t, "ods-rds-spirit-password", fetch.gotSecret)
	assert.Equal(t, "spirit", creds.Username)
	assert.Equal(t, "s3cret", creds.Password)
}

// The secret name may carry a {target} placeholder for per-target secrets.
func TestResolverTemplatesSecretNameByTarget(t *testing.T) {
	fetch := &fakeFetcher{payload: `{"username":"ddl","password":"pw"}`}
	r := newResolver("aws_account_id", "schemabot/{target}/ddl", fetch, nil)

	_, err := r.ResolveCredentials(t.Context(),
		inventory.Request{Target: "orders-dsid"},
		map[string]string{"aws_account_id": "123456789012"})
	require.NoError(t, err)
	assert.Equal(t, "schemabot/orders-dsid/ddl", fetch.gotSecret)
}

func TestResolverFailsWhenAccountAttributeMissing(t *testing.T) {
	r := newResolver("aws_account_id", "secret", &fakeFetcher{payload: `{"username":"u","password":"p"}`}, nil)

	_, err := r.ResolveCredentials(t.Context(), inventory.Request{Target: "orders-dsid"}, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "aws_account_id")
}

func TestResolverPropagatesFetchError(t *testing.T) {
	r := newResolver("aws_account_id", "secret", &fakeFetcher{err: fmt.Errorf("access denied")}, nil)

	_, err := r.ResolveCredentials(t.Context(),
		inventory.Request{Target: "orders-dsid"},
		map[string]string{"aws_account_id": "123456789012"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "access denied")
}

func TestResolverFailsOnNonJSONSecret(t *testing.T) {
	r := newResolver("aws_account_id", "secret", &fakeFetcher{payload: "not-json"}, nil)

	_, err := r.ResolveCredentials(t.Context(),
		inventory.Request{Target: "orders-dsid"},
		map[string]string{"aws_account_id": "123456789012"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "JSON")
}

func TestResolverFailsOnIncompleteSecret(t *testing.T) {
	r := newResolver("aws_account_id", "secret", &fakeFetcher{payload: `{"username":"spirit"}`}, nil)

	_, err := r.ResolveCredentials(t.Context(),
		inventory.Request{Target: "orders-dsid"},
		map[string]string{"aws_account_id": "123456789012"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing a username or password")
}

// With a Decode set, the resolver interprets the fetched secret with that
// decoder instead of the default {username, password} parse — for example a
// PlanetScale token read through the same assume-role path.
func TestResolverUsesDecoder(t *testing.T) {
	fetch := &fakeFetcher{payload: `{"token":"tok-id=tok-secret"}`}
	r := newResolver("aws_account_id", "secret", fetch, inventory.DecodePlanetScaleSecret)

	creds, err := r.ResolveCredentials(t.Context(),
		inventory.Request{Target: "orders-dsid"},
		map[string]string{"aws_account_id": "123456789012"})
	require.NoError(t, err)
	assert.Empty(t, creds.Username, "a PlanetScale credential carries no username")
	assert.Equal(t, "tok-id", creds.Metadata["token_name"])
	assert.Equal(t, "tok-secret", creds.Metadata["token_value"])
}

func TestNewValidatesConfig(t *testing.T) {
	base := Config{Region: "us-west-2", RoleARN: "arn:aws:iam::{account}:role/tern-assumed", SecretName: "secret"}

	_, err := New(base)
	require.NoError(t, err)

	noRegion := base
	noRegion.Region = ""
	_, err = New(noRegion)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "region")

	noRole := base
	noRole.RoleARN = ""
	_, err = New(noRole)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "role")

	noSecret := base
	noSecret.SecretName = ""
	_, err = New(noSecret)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "secret")
}

// When no account attribute is configured, New wires the default.
func TestNewDefaultsAccountAttribute(t *testing.T) {
	r, err := New(Config{Region: "us-west-2", RoleARN: "arn:aws:iam::{account}:role/role", SecretName: "secret"})
	require.NoError(t, err)
	assert.Equal(t, defaultAccountAttribute, r.accountAttr)
}
