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
	r := newResolver("aws_account_id", "ods-rds-spirit-password", "", fetch, nil, true)

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
	r := newResolver("aws_account_id", "schemabot/{target}/ddl", "", fetch, nil, true)

	_, err := r.ResolveCredentials(t.Context(),
		inventory.Request{Target: "orders-dsid"},
		map[string]string{"aws_account_id": "123456789012"})
	require.NoError(t, err)
	assert.Equal(t, "schemabot/orders-dsid/ddl", fetch.gotSecret)
}

func TestResolverFailsWhenAccountAttributeMissing(t *testing.T) {
	r := newResolver("aws_account_id", "secret", "", &fakeFetcher{payload: `{"username":"u","password":"p"}`}, nil, true)

	_, err := r.ResolveCredentials(t.Context(), inventory.Request{Target: "orders-dsid"}, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "aws_account_id")
}

func TestResolverPropagatesFetchError(t *testing.T) {
	r := newResolver("aws_account_id", "secret", "", &fakeFetcher{err: fmt.Errorf("access denied")}, nil, true)

	_, err := r.ResolveCredentials(t.Context(),
		inventory.Request{Target: "orders-dsid"},
		map[string]string{"aws_account_id": "123456789012"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "access denied")
}

func TestResolverFailsOnNonJSONSecret(t *testing.T) {
	r := newResolver("aws_account_id", "secret", "", &fakeFetcher{payload: "not-json"}, nil, true)

	_, err := r.ResolveCredentials(t.Context(),
		inventory.Request{Target: "orders-dsid"},
		map[string]string{"aws_account_id": "123456789012"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "JSON")
}

func TestResolverFailsOnIncompleteSecret(t *testing.T) {
	r := newResolver("aws_account_id", "secret", "", &fakeFetcher{payload: `{"username":"spirit"}`}, nil, true)

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
	r := newResolver("aws_account_id", "secret", "", fetch, inventory.DecodePlanetScaleSecret, true)

	creds, err := r.ResolveCredentials(t.Context(),
		inventory.Request{Target: "orders-dsid"},
		map[string]string{"aws_account_id": "123456789012"})
	require.NoError(t, err)
	assert.Empty(t, creds.Username, "a PlanetScale credential carries no username")
	assert.Equal(t, "tok-id", creds.Metadata["token_name"])
	assert.Equal(t, "tok-secret", creds.Metadata["token_value"])
}

// The secret name may template over resolved entity attributes for per-cluster
// secret naming, not just the target.
func TestResolverTemplatesSecretNameByAttribute(t *testing.T) {
	fetch := &fakeFetcher{payload: `{"username":"ddl","password":"pw"}`}
	r := newResolver("aws_account_id", "{cluster}_schemabot_password", "", fetch, nil, true)

	_, err := r.ResolveCredentials(t.Context(),
		inventory.Request{Target: "orders-dsid"},
		map[string]string{"aws_account_id": "123456789012", "cluster": "orders-mysql"})
	require.NoError(t, err)
	assert.Equal(t, "orders-mysql_schemabot_password", fetch.gotSecret)
}

// A secret-name placeholder that the resolver did not surface fails closed
// rather than fetching a wrong (partially templated) secret name.
func TestResolverFailsOnUnresolvedSecretNameAttribute(t *testing.T) {
	r := newResolver("aws_account_id", "{cluster}_password", "", &fakeFetcher{payload: `{"username":"u","password":"p"}`}, nil, true)

	_, err := r.ResolveCredentials(t.Context(),
		inventory.Request{Target: "orders-dsid"},
		map[string]string{"aws_account_id": "123456789012"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cluster")
}

// Own-account mode (no role) reads from the caller's account, so it neither
// requires nor uses the account attribute.
func TestResolverOwnAccountModeSkipsAccountRequirement(t *testing.T) {
	fetch := &fakeFetcher{payload: `{"username":"ddl","password":"pw"}`}
	r := newResolver("aws_account_id", "schemabot_password", "", fetch, nil, false)

	creds, err := r.ResolveCredentials(t.Context(), inventory.Request{Target: "orders-dsid"}, nil)
	require.NoError(t, err)
	assert.Equal(t, "schemabot_password", fetch.gotSecret)
	assert.Empty(t, fetch.gotAccount, "own-account mode does not scope to an account")
	assert.Equal(t, "ddl", creds.Username)
}

func TestTemplateAttributes(t *testing.T) {
	assert.Equal(t, []string{"cluster"}, TemplateAttributes("{cluster}_{target}_password"))
	assert.Equal(t, []string{"app"}, TemplateAttributes("{app}_ddl"))
	assert.Empty(t, TemplateAttributes("schemabot/{target}/ddl"))
	assert.Empty(t, TemplateAttributes("static-secret"))
}

// Some conventions derive the username from an entity attribute and store only
// the password as a plain-text secret rather than a JSON payload.
func TestResolverUsernameTemplateWithPlainPassword(t *testing.T) {
	// A trailing newline (common in file-uploaded secrets) is trimmed.
	fetch := &fakeFetcher{payload: "s3cret-pw\n"}
	r := newResolver("aws_account_id", "{name}_ddl_password", "{app}_ddl", fetch, nil, false)

	creds, err := r.ResolveCredentials(t.Context(),
		inventory.Request{Target: "orders-dsid"},
		map[string]string{"app": "orders", "name": "orders-mysql"})
	require.NoError(t, err)
	assert.Equal(t, "orders-mysql_ddl_password", fetch.gotSecret)
	assert.Equal(t, "orders_ddl", creds.Username)
	assert.Equal(t, "s3cret-pw", creds.Password)
}

// A username template referencing an attribute the resolver did not surface
// fails closed rather than producing a partial username.
func TestResolverFailsOnUnresolvedUsernameAttribute(t *testing.T) {
	r := newResolver("aws_account_id", "secret", "{app}_ddl", &fakeFetcher{payload: "pw"}, nil, false)

	_, err := r.ResolveCredentials(t.Context(), inventory.Request{Target: "orders-dsid"}, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "app")
}

// In username-template mode an empty secret is a missing password, not a valid
// credential.
func TestResolverUsernameTemplateRejectsEmptyPassword(t *testing.T) {
	r := newResolver("aws_account_id", "secret", "{app}_ddl", &fakeFetcher{payload: ""}, nil, false)

	_, err := r.ResolveCredentials(t.Context(),
		inventory.Request{Target: "orders-dsid"},
		map[string]string{"app": "orders"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "password")
}

func TestNewRejectsUsernameWithDecode(t *testing.T) {
	_, err := New(Config{Region: "us-west-2", SecretName: "secret", Username: "{app}_ddl", Decode: inventory.DecodePlanetScaleSecret})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "mutually exclusive")
}

// Assume-role failures carry the target AWS account id so cross-account
// problems stay diagnosable; own-account mode has no account to report.
func TestResolverFetchErrorIncludesAccountInAssumeRoleMode(t *testing.T) {
	assumeRole := newResolver("aws_account_id", "secret", "", &fakeFetcher{err: fmt.Errorf("access denied")}, nil, true)
	_, err := assumeRole.ResolveCredentials(t.Context(),
		inventory.Request{Target: "orders-dsid"},
		map[string]string{"aws_account_id": "123456789012"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "123456789012")

	ownAccount := newResolver("aws_account_id", "secret", "", &fakeFetcher{err: fmt.Errorf("access denied")}, nil, false)
	_, err = ownAccount.ResolveCredentials(t.Context(), inventory.Request{Target: "orders-dsid"}, nil)
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "in account")
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

	// RoleARN is optional: without it, secrets are read from the caller's own
	// account, so New succeeds.
	noRole := base
	noRole.RoleARN = ""
	_, err = New(noRole)
	require.NoError(t, err)

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
