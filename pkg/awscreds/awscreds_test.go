package awscreds

import (
	"context"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	smtypes "github.com/aws/aws-sdk-go-v2/service/secretsmanager/types"
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

// testRoleARN switches a test resolver into assume-role mode.
const testRoleARN = "arn:aws:iam::{account}:role/ddl"

// testResolver builds a Resolver over a fake fetcher through the same
// validation New applies, so a test cannot construct a configuration New would
// refuse.
func testResolver(t *testing.T, cfg Config, fetch secretFetcher) *Resolver {
	t.Helper()
	r, err := newResolver(cfg, fetch)
	require.NoError(t, err)
	return r
}

func TestResolverReadsJSONSecretForTargetAccount(t *testing.T) {
	fetch := &fakeFetcher{payload: `{"username":"ddl","password":"s3cret"}`}
	r := testResolver(t, Config{Region: "us-west-2", RoleARN: testRoleARN, SecretName: "ddl-password"}, fetch)

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
	r := testResolver(t, Config{Region: "us-west-2", RoleARN: testRoleARN, SecretName: "schemabot/{target}/ddl"}, fetch)

	_, err := r.ResolveCredentials(t.Context(),
		inventory.Request{Target: "orders-dsid"},
		map[string]string{"aws_account_id": "123456789012"})
	require.NoError(t, err)
	assert.Equal(t, "schemabot/orders-dsid/ddl", fetch.gotSecret)
}

func TestResolverFailsWhenAccountAttributeMissing(t *testing.T) {
	r := testResolver(t, Config{Region: "us-west-2", RoleARN: testRoleARN, SecretName: "secret"}, &fakeFetcher{payload: `{"username":"u","password":"p"}`})

	_, err := r.ResolveCredentials(t.Context(), inventory.Request{Target: "orders-dsid"}, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "aws_account_id")
}

func TestResolverPropagatesFetchError(t *testing.T) {
	r := testResolver(t, Config{Region: "us-west-2", RoleARN: testRoleARN, SecretName: "secret"}, &fakeFetcher{err: fmt.Errorf("access denied")})

	_, err := r.ResolveCredentials(t.Context(),
		inventory.Request{Target: "orders-dsid"},
		map[string]string{"aws_account_id": "123456789012"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "access denied")
}

func TestResolverFailsOnNonJSONSecret(t *testing.T) {
	r := testResolver(t, Config{Region: "us-west-2", RoleARN: testRoleARN, SecretName: "secret"}, &fakeFetcher{payload: "not-json"})

	_, err := r.ResolveCredentials(t.Context(),
		inventory.Request{Target: "orders-dsid"},
		map[string]string{"aws_account_id": "123456789012"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "JSON")
}

func TestResolverFailsOnIncompleteSecret(t *testing.T) {
	r := testResolver(t, Config{Region: "us-west-2", RoleARN: testRoleARN, SecretName: "secret"}, &fakeFetcher{payload: `{"username":"ddl"}`})

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
	r := testResolver(t, Config{Region: "us-west-2", RoleARN: testRoleARN, SecretName: "secret", Decode: inventory.DecodePlanetScaleSecret}, fetch)

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
	r := testResolver(t, Config{Region: "us-west-2", RoleARN: testRoleARN, SecretName: "{cluster}_schemabot_password"}, fetch)

	_, err := r.ResolveCredentials(t.Context(),
		inventory.Request{Target: "orders-dsid"},
		map[string]string{"aws_account_id": "123456789012", "cluster": "orders-mysql"})
	require.NoError(t, err)
	assert.Equal(t, "orders-mysql_schemabot_password", fetch.gotSecret)
}

// A secret-name placeholder that the resolver did not surface fails closed
// rather than fetching a wrong (partially templated) secret name.
func TestResolverFailsOnUnresolvedSecretNameAttribute(t *testing.T) {
	r := testResolver(t, Config{Region: "us-west-2", RoleARN: testRoleARN, SecretName: "{cluster}_password"}, &fakeFetcher{payload: `{"username":"u","password":"p"}`})

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
	r := testResolver(t, Config{Region: "us-west-2", SecretName: "schemabot_password"}, fetch)

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
	r := testResolver(t, Config{Region: "us-west-2", SecretName: "secret", Username: "{app:24}_ddl"}, fetch)

	creds, err := r.ResolveCredentials(t.Context(),
		inventory.Request{Target: "orders-dsid"},
		map[string]string{"app": "abcdefghijklmnopqrstuvwxyz0123"}) // 30 chars
	require.NoError(t, err)
	assert.Equal(t, "abcdefghijklmnopqrstuvwx_ddl", creds.Username) // 24 + "_ddl"
}

// A value already within the limit is left intact by the length operator.
func TestResolverLengthOperatorLeavesShortValueIntact(t *testing.T) {
	fetch := &fakeFetcher{payload: "pw"}
	r := testResolver(t, Config{Region: "us-west-2", SecretName: "secret", Username: "{app:24}_ddl"}, fetch)

	creds, err := r.ResolveCredentials(t.Context(),
		inventory.Request{Target: "orders-dsid"},
		map[string]string{"app": "orders"})
	require.NoError(t, err)
	assert.Equal(t, "orders_ddl", creds.Username)
}

// The length operator also applies to secret-name templates.
func TestResolverSecretNameLengthOperatorTruncates(t *testing.T) {
	fetch := &fakeFetcher{payload: `{"username":"u","password":"p"}`}
	r := testResolver(t, Config{Region: "us-west-2", RoleARN: testRoleARN, SecretName: "schemabot/{cluster:8}/ddl"}, fetch)

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
		r := testResolver(t, Config{Region: "us-west-2", SecretName: "secret", Username: tmpl}, &fakeFetcher{payload: "pw"})

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
	r := testResolver(t, Config{Region: "us-west-2", SecretName: "{name}_ddl_password", Username: "{app}_ddl"}, fetch)

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
	r := testResolver(t, Config{Region: "us-west-2", SecretName: "secret", Username: "{app}_ddl"}, &fakeFetcher{payload: "pw"})

	_, err := r.ResolveCredentials(t.Context(), inventory.Request{Target: "orders-dsid"}, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "app")
}

// In username-template mode an empty secret is a missing password, not a valid
// credential.
func TestResolverUsernameTemplateRejectsEmptyPassword(t *testing.T) {
	r := testResolver(t, Config{Region: "us-west-2", SecretName: "secret", Username: "{app}_ddl"}, &fakeFetcher{payload: ""})

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
	assumeRole := testResolver(t, Config{Region: "us-west-2", RoleARN: testRoleARN, SecretName: "secret"}, &fakeFetcher{err: fmt.Errorf("access denied")})
	_, err := assumeRole.ResolveCredentials(t.Context(),
		inventory.Request{Target: "orders-dsid"},
		map[string]string{"aws_account_id": "123456789012"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "123456789012")

	ownAccount := testResolver(t, Config{Region: "us-west-2", SecretName: "secret"}, &fakeFetcher{err: fmt.Errorf("access denied")})
	_, err = ownAccount.ResolveCredentials(t.Context(), inventory.Request{Target: "orders-dsid"}, nil)
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "in account")
}

// regionResolver reads in us-west-2 by default and can also call Secrets
// Manager in us-east-1, placing each target by its aws_region attribute.
func regionResolver(t *testing.T, fetch secretFetcher) *Resolver {
	t.Helper()
	return testResolver(t, Config{
		Region:           "us-west-2",
		RegionAttribute:  "aws_region",
		ReachableRegions: []string{"us-east-1"},
		RoleARN:          testRoleARN,
		SecretName:       "ddl-password",
	}, fetch)
}

// accountInRegion returns entity attributes placing a target in an account and
// a cluster region.
func accountInRegion(account, region string) map[string]string {
	return map[string]string{"aws_account_id": account, "aws_region": region}
}

// A data plane that can call Secrets Manager in its cluster's region reads the
// secret there: a us-east-1 cluster's secret is read in us-east-1 and a
// us-west-2 cluster's in us-west-2, from one resolver.
func TestResolverReadsSecretInReachableClusterRegion(t *testing.T) {
	for _, region := range []string{"us-east-1", "us-west-2"} {
		fetch := &fakeFetcher{payload: `{"username":"ddl","password":"s3cret"}`}
		creds, err := regionResolver(t, fetch).ResolveCredentials(t.Context(),
			inventory.Request{Target: "inventory-dsid"}, accountInRegion("222222222222", region))
		require.NoError(t, err, region)

		assert.Equal(t, "222222222222", fetch.gotAccount, region)
		assert.Equal(t, region, fetch.gotRegion, region)
		assert.Equal(t, "ddl-password", fetch.gotSecret, region)
		assert.Equal(t, "ddl", creds.Username, region)
	}
}

// A cluster in a region the data plane cannot call has its secret read in the
// home region, where it is expected as a replica.
func TestResolverReadsSecretInHomeRegionWhenClusterRegionUnreachable(t *testing.T) {
	fetch := &fakeFetcher{payload: `{"username":"ddl","password":"s3cret"}`}
	_, err := regionResolver(t, fetch).ResolveCredentials(t.Context(),
		inventory.Request{Target: "inventory-dsid"}, accountInRegion("222222222222", "eu-west-1"))
	require.NoError(t, err)
	assert.Equal(t, "us-west-2", fetch.gotRegion)
}

// A target whose entity does not name its cluster's region fails before any
// fetch: reading the secret in the home region would be a guess about where the
// target runs.
func TestResolverFailsWhenRegionAttributeMissing(t *testing.T) {
	fetch := &fakeFetcher{payload: `{"username":"u","password":"p"}`}
	_, err := regionResolver(t, fetch).ResolveCredentials(t.Context(),
		inventory.Request{Target: "orders-dsid"},
		map[string]string{"aws_account_id": "111111111111"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), `target "orders-dsid" has no "aws_region" attribute naming the region of its cluster`)
	assert.Equal(t, 0, fetch.calls)
}

// A region attribute whose value is not a region name fails before any fetch,
// naming the value, rather than as an opaque endpoint error from the SDK.
func TestResolverFailsOnMalformedEntityRegion(t *testing.T) {
	fetch := &fakeFetcher{payload: `{"username":"u","password":"p"}`}
	_, err := regionResolver(t, fetch).ResolveCredentials(t.Context(),
		inventory.Request{Target: "orders-dsid"}, accountInRegion("111111111111", "us-east"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), `target "orders-dsid" has "aws_region" attribute "us-east", which is not an AWS region name`)
	assert.Equal(t, 0, fetch.calls)
}

func TestIsRegionName(t *testing.T) {
	for _, name := range []string{"us-east-1", "us-west-2", "eu-central-1", "ap-southeast-4", "us-gov-west-1", "cn-north-1", "us-isob-east-1"} {
		assert.True(t, isRegionName(name), name)
	}
	for _, name := range []string{"", "us-east", "useast1", "US-EAST-1", "us-east-1 ", "us-east-1a", "-us-east-1"} {
		assert.False(t, isRegionName(name), name)
	}
}

// Without a region attribute every secret is read in the home region, whatever
// region the target's entity reports.
func TestResolverHomeRegionIgnoresEntityRegion(t *testing.T) {
	fetch := &fakeFetcher{payload: `{"username":"u","password":"p"}`}
	r := testResolver(t, Config{Region: "us-west-2", RoleARN: testRoleARN, SecretName: "secret"}, fetch)

	_, err := r.ResolveCredentials(t.Context(),
		inventory.Request{Target: "orders-dsid"}, accountInRegion("111111111111", "us-east-1"))
	require.NoError(t, err)
	assert.Equal(t, "us-west-2", fetch.gotRegion)
}

// secretNotFound is the error Secrets Manager returns for a secret that does
// not exist in the region it was read in, wrapped as the SDK wraps it.
func secretNotFound() error {
	return fmt.Errorf("operation error Secrets Manager: GetSecretValue: %w",
		&smtypes.ResourceNotFoundException{Message: aws.String("Secrets Manager can't find the specified secret.")})
}

// A secret missing from the home region, for a cluster in a region the data
// plane cannot call, is reported with the fix: replicate the secret to the home
// region, or make the cluster's region reachable.
func TestResolverSecretMissingFromHomeRegionNamesTheFix(t *testing.T) {
	fetch := &fakeFetcher{err: secretNotFound()}
	_, err := regionResolver(t, fetch).ResolveCredentials(t.Context(),
		inventory.Request{Target: "inventory-dsid"}, accountInRegion("222222222222", "eu-west-1"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), `fetch secret "ddl-password" for target "inventory-dsid" in account 222222222222, region us-west-2`)
	assert.Contains(t, err.Error(), "the target's cluster is in eu-west-1, which is not a reachable region, so its secret is read in us-west-2: replicate the secret to us-west-2, or list eu-west-1 as a reachable region if this data plane can call Secrets Manager there")

	var notFound *smtypes.ResourceNotFoundException
	assert.ErrorAs(t, err, &notFound)
}

// The replication fix is offered only where it applies: not when the secret
// was read in the cluster's own region, and not for a failure other than a
// missing secret.
func TestResolverFetchErrorOmitsReplicaFixWhenItDoesNotApply(t *testing.T) {
	cases := map[string]struct {
		region string
		err    error
	}{
		"missing in the cluster's own region": {region: "us-east-1", err: secretNotFound()},
		"access denied in the home region":    {region: "eu-west-1", err: fmt.Errorf("AccessDeniedException")},
	}
	for name, tc := range cases {
		_, err := regionResolver(t, &fakeFetcher{err: tc.err}).ResolveCredentials(t.Context(),
			inventory.Request{Target: "inventory-dsid"}, accountInRegion("222222222222", tc.region))
		require.Error(t, err, name)
		assert.Contains(t, err.Error(), `target "inventory-dsid" in account 222222222222`, name)
		assert.NotContains(t, err.Error(), "replicate", name)
	}
}

// Assumed-role credentials are cached per account and assumed through STS in
// the home region, so reading a secret in another region needs only that
// region's Secrets Manager. Clients are cached per account and region: a client
// built for one region is never reused to read another region's secret.
func TestAssumeRoleFetcherSharesCredentialsAcrossRegions(t *testing.T) {
	r, err := New(Config{Region: "us-west-2", RegionAttribute: "aws_region", ReachableRegions: []string{"us-east-1"}, RoleARN: "arn:aws:iam::{account}:role/role", SecretName: "secret"})
	require.NoError(t, err)
	f, ok := r.fetch.(*assumeRoleFetcher)
	require.True(t, ok)
	assert.Equal(t, "us-west-2", f.awsCfg.Region, "roles are assumed in the home region")

	west := f.clientFor("111111111111", "us-west-2")
	east := f.clientFor("111111111111", "us-east-1")
	assert.NotSame(t, west, east)
	assert.Same(t, west, f.clientFor("111111111111", "us-west-2"))
	assert.Equal(t, "us-west-2", west.Options().Region)
	assert.Equal(t, "us-east-1", east.Options().Region)
	assert.Same(t, west.Options().Credentials, east.Options().Credentials)

	other := f.clientFor("222222222222", "us-west-2")
	assert.NotSame(t, west, other)
	assert.NotSame(t, west.Options().Credentials, other.Options().Credentials)
}

// Own-account clients are cached per region the same way.
func TestOwnAccountFetcherCachesClientPerRegion(t *testing.T) {
	r, err := New(Config{Region: "us-west-2", RegionAttribute: "aws_region", ReachableRegions: []string{"us-east-1"}, SecretName: "secret"})
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

	// A region attribute alone keeps every read in the home region and names the
	// cluster's region when a secret is missing.
	regionAttr := base
	regionAttr.RegionAttribute = "aws_region"
	_, err = New(regionAttr)
	require.NoError(t, err)

	crossRegion := regionAttr
	crossRegion.ReachableRegions = []string{"us-east-1", "eu-west-1"}
	_, err = New(crossRegion)
	require.NoError(t, err)

	// RoleARN is optional: without it, secrets are read from the caller's own
	// account, so New succeeds.
	noRole := base
	noRole.RoleARN = ""
	_, err = New(noRole)
	require.NoError(t, err)

	cases := map[string]func(*Config){
		"region is required":                              func(c *Config) { c.Region = "" },
		`region "us-west" is not an AWS region name`:      func(c *Config) { c.Region = "us-west" },
		"secret name is required":                         func(c *Config) { c.SecretName = "" },
		"reachable regions require a region attribute":    func(c *Config) { c.ReachableRegions = []string{"us-east-1"} },
		`reachable region "us-east" is not an AWS region`: func(c *Config) { c.RegionAttribute = "aws_region"; c.ReachableRegions = []string{"us-east"} },
		`reachable region "us-west-2" is the home region`: func(c *Config) { c.RegionAttribute = "aws_region"; c.ReachableRegions = []string{"us-west-2"} },
		`reachable region "us-east-1" is listed more than once`: func(c *Config) {
			c.RegionAttribute = "aws_region"
			c.ReachableRegions = []string{"us-east-1", "us-east-1"}
		},
	}
	for want, mutate := range cases {
		cfg := base
		mutate(&cfg)
		_, err := New(cfg)
		require.Error(t, err, want)
		assert.Contains(t, err.Error(), want)
	}
}

// When no account attribute is configured, New wires the default.
func TestNewDefaultsAccountAttribute(t *testing.T) {
	r, err := New(Config{Region: "us-west-2", RoleARN: "arn:aws:iam::{account}:role/role", SecretName: "secret"})
	require.NoError(t, err)
	assert.Equal(t, defaultAccountAttribute, r.accountAttr)
}
