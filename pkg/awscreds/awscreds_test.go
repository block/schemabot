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
	calls      int
	gotAccount string
	gotRegion  string
	gotSecret  string
	payload    string
	err        error
}

func (f *fakeFetcher) FetchSecret(_ context.Context, accountID, region, secretName string) (string, error) {
	f.calls++
	f.gotAccount = accountID
	f.gotRegion = region
	f.gotSecret = secretName
	return f.payload, f.err
}

func TestResolverReadsJSONSecretForTargetAccount(t *testing.T) {
	fetch := &fakeFetcher{payload: `{"username":"ddl","password":"s3cret"}`}
	r := newResolver("aws_account_id", "ddl-password", "", fetch, nil, true)

	creds, err := r.ResolveCredentials(t.Context(),
		inventory.Request{Target: "orders-dsid"},
		map[string]string{"aws_account_id": "111111111111"})
	require.NoError(t, err)

	assert.Equal(t, "111111111111", fetch.gotAccount)
	assert.Equal(t, "ddl-password", fetch.gotSecret)
	assert.Equal(t, "ddl", creds.Username)
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
	r := newResolver("aws_account_id", "secret", "", &fakeFetcher{payload: `{"username":"ddl"}`}, nil, true)

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
	// A length operator does not change which attribute the placeholder references.
	assert.Equal(t, []string{"app"}, TemplateAttributes("{app:24}_ddl"))
}

// A ":N" length operator truncates the resolved value so a derived username fits
// the database's identifier limit even when the source attribute is longer (e.g.
// an app name longer than MySQL's 32-character cap).
func TestResolverUsernameLengthOperatorTruncates(t *testing.T) {
	fetch := &fakeFetcher{payload: "pw"}
	r := newResolver("aws_account_id", "secret", "{app:24}_ddl", fetch, nil, false)

	creds, err := r.ResolveCredentials(t.Context(),
		inventory.Request{Target: "orders-dsid"},
		map[string]string{"app": "abcdefghijklmnopqrstuvwxyz0123"}) // 30 chars
	require.NoError(t, err)
	assert.Equal(t, "abcdefghijklmnopqrstuvwx_ddl", creds.Username) // 24 + "_ddl"
}

// A value already within the limit is left intact by the length operator.
func TestResolverLengthOperatorLeavesShortValueIntact(t *testing.T) {
	fetch := &fakeFetcher{payload: "pw"}
	r := newResolver("aws_account_id", "secret", "{app:24}_ddl", fetch, nil, false)

	creds, err := r.ResolveCredentials(t.Context(),
		inventory.Request{Target: "orders-dsid"},
		map[string]string{"app": "orders"})
	require.NoError(t, err)
	assert.Equal(t, "orders_ddl", creds.Username)
}

// The length operator also applies to secret-name templates.
func TestResolverSecretNameLengthOperatorTruncates(t *testing.T) {
	fetch := &fakeFetcher{payload: `{"username":"u","password":"p"}`}
	r := newResolver("aws_account_id", "schemabot/{cluster:8}/ddl", "", fetch, nil, true)

	_, err := r.ResolveCredentials(t.Context(),
		inventory.Request{Target: "orders-dsid"},
		map[string]string{"aws_account_id": "123456789012", "cluster": "production-cluster"})
	require.NoError(t, err)
	assert.Equal(t, "schemabot/producti/ddl", fetch.gotSecret)
}

// A zero or unparseable length operator is a configuration error, not a silent
// passthrough that would produce a wrong identifier.
func TestResolverFailsOnInvalidLengthOperator(t *testing.T) {
	// Zero, non-numeric, negative, and empty operators are all configuration
	// errors. A malformed operator must still be recognized as a placeholder and
	// rejected, never rendered literally into the username.
	for _, tmpl := range []string{"{app:0}_ddl", "{app:nope}_ddl", "{app:-1}_ddl", "{app:}_ddl"} {
		r := newResolver("aws_account_id", "secret", tmpl, &fakeFetcher{payload: "pw"}, nil, false)

		_, err := r.ResolveCredentials(t.Context(),
			inventory.Request{Target: "orders-dsid"},
			map[string]string{"app": "orders"})
		require.Error(t, err, tmpl)
		assert.Contains(t, err.Error(), "invalid length operator", tmpl)
	}
}

// Truncation counts characters, not bytes, so a multi-byte value is never cut
// mid-character.
func TestTruncateToRunesCountsCharacters(t *testing.T) {
	assert.Equal(t, "héllo", truncateToRunes("héllo-world", 5))
	assert.Equal(t, "abc", truncateToRunes("abc", 5))
	assert.Equal(t, "", truncateToRunes("abc", 0))
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

// A cluster's credential secret is provisioned in the region the cluster runs
// in. With a region attribute, a us-east-1 cluster's secret is read from
// us-east-1 even though other targets of the same resolver live in us-west-2.
func TestResolverReadsSecretInEntityRegion(t *testing.T) {
	fetch := &fakeFetcher{payload: `{"username":"ddl","password":"s3cret"}`}
	r := newResolver("aws_account_id", "ddl-password", "", fetch, nil, true)
	r.regionAttr = "aws_region"

	creds, err := r.ResolveCredentials(t.Context(),
		inventory.Request{Target: "inventory-dsid"},
		map[string]string{"aws_account_id": "222222222222", "aws_region": "us-east-1"})
	require.NoError(t, err)

	assert.Equal(t, "222222222222", fetch.gotAccount)
	assert.Equal(t, "us-east-1", fetch.gotRegion)
	assert.Equal(t, "ddl-password", fetch.gotSecret)
	assert.Equal(t, "ddl", creds.Username)
}

// A target whose entity does not name a region fails before any fetch: reading
// the secret from some other region would be a guess.
func TestResolverFailsWhenRegionAttributeMissing(t *testing.T) {
	fetch := &fakeFetcher{payload: `{"username":"u","password":"p"}`}
	r := newResolver("aws_account_id", "secret", "", fetch, nil, true)
	r.regionAttr = "aws_region"

	_, err := r.ResolveCredentials(t.Context(),
		inventory.Request{Target: "orders-dsid"},
		map[string]string{"aws_account_id": "111111111111"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), `target "orders-dsid" has no "aws_region" attribute`)
	assert.Equal(t, 0, fetch.calls)
}

// A fixed region is used for every target, whatever region its entity reports.
func TestResolverFixedRegionIgnoresEntityRegion(t *testing.T) {
	fetch := &fakeFetcher{payload: `{"username":"u","password":"p"}`}
	r := newResolver("aws_account_id", "secret", "", fetch, nil, true)
	r.region = "us-west-2"

	_, err := r.ResolveCredentials(t.Context(),
		inventory.Request{Target: "orders-dsid"},
		map[string]string{"aws_account_id": "111111111111", "aws_region": "us-east-1"})
	require.NoError(t, err)
	assert.Equal(t, "us-west-2", fetch.gotRegion)
}

// A fetch failure names the account and region it read from, so a secret that
// exists in one region but not another is diagnosable from the error alone.
func TestResolverFetchErrorIncludesRegion(t *testing.T) {
	r := newResolver("aws_account_id", "ddl-password", "", &fakeFetcher{err: fmt.Errorf("ResourceNotFoundException")}, nil, true)
	r.regionAttr = "aws_region"

	_, err := r.ResolveCredentials(t.Context(),
		inventory.Request{Target: "inventory-dsid"},
		map[string]string{"aws_account_id": "222222222222", "aws_region": "us-east-1"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), `target "inventory-dsid" in account 222222222222, region us-east-1`)
}

// Assumed-role clients are cached per account and region: a client built for
// one region is never reused to read another region's secret.
func TestAssumeRoleFetcherCachesClientPerAccountAndRegion(t *testing.T) {
	r, err := New(Config{RegionAttribute: "aws_region", RoleARN: "arn:aws:iam::{account}:role/role", SecretName: "secret"})
	require.NoError(t, err)
	f, ok := r.fetch.(*assumeRoleFetcher)
	require.True(t, ok)

	west := f.clientFor("111111111111", "us-west-2")
	east := f.clientFor("111111111111", "us-east-1")
	assert.NotSame(t, west, east)
	assert.Same(t, west, f.clientFor("111111111111", "us-west-2"))
	assert.Equal(t, "us-west-2", west.Options().Region)
	assert.Equal(t, "us-east-1", east.Options().Region)
	assert.NotSame(t, west, f.clientFor("222222222222", "us-west-2"))
}

// Own-account clients are cached per region the same way.
func TestOwnAccountFetcherCachesClientPerRegion(t *testing.T) {
	r, err := New(Config{RegionAttribute: "aws_region", SecretName: "secret"})
	require.NoError(t, err)
	f, ok := r.fetch.(*ownAccountFetcher)
	require.True(t, ok)

	west := f.clientForRegion("us-west-2")
	east := f.clientForRegion("us-east-1")
	assert.NotSame(t, west, east)
	assert.Same(t, west, f.clientForRegion("us-west-2"))
	assert.Equal(t, "us-east-1", east.Options().Region)
}

func TestNewValidatesConfig(t *testing.T) {
	base := Config{Region: "us-west-2", RoleARN: "arn:aws:iam::{account}:role/tern-assumed", SecretName: "secret"}

	_, err := New(base)
	require.NoError(t, err)

	noRegion := base
	noRegion.Region = ""
	_, err = New(noRegion)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "region or region attribute is required")

	// A region attribute stands in for the fixed region.
	regionAttr := noRegion
	regionAttr.RegionAttribute = "aws_region"
	_, err = New(regionAttr)
	require.NoError(t, err)

	bothRegions := base
	bothRegions.RegionAttribute = "aws_region"
	_, err = New(bothRegions)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "mutually exclusive")

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
