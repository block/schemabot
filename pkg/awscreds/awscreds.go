// Package awscreds resolves database credentials from AWS Secrets Manager.
//
// It implements inventory.CredentialResolver. The secret name is templated over
// the resolved target and its entity attributes, so one configuration can locate
// per-target or per-cluster secrets. By default it reads from the caller's own
// AWS account; when a role ARN is configured it assumes a per-target role first,
// so a single data plane can read secrets across many AWS accounts. Secrets are
// read in the data plane's home region, or in the region of the target's own
// cluster when that region is one the data plane is configured to reach.
//
// The fetched secret is interpreted in one of three ways: by a configured decoder
// (for example a PlanetScale token); as a JSON {username, password} payload (the
// default); or, when a username template is configured, as a plain-text password
// with the username rendered from the template — for conventions that derive the
// username from entity attributes and store only the password. Credential values
// come from Secrets Manager, never from the inventory source.
package awscreds

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	smtypes "github.com/aws/aws-sdk-go-v2/service/secretsmanager/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"

	"github.com/block/schemabot/pkg/inventory"
	"github.com/block/schemabot/pkg/secrets"
)

// defaultAccountAttribute is the endpoint attribute holding the target's AWS
// account id when one is not configured.
const defaultAccountAttribute = "aws_account_id"

// templatePlaceholderRe matches "{name}" placeholders in a template, with an
// optional ":N" length operator (e.g. "{app:24}"). "{target}" is the request
// target; any other name is an entity attribute resolved for the target. When a
// length operator is present, the resolved value is truncated to at most N
// characters — used to fit database identifier limits (e.g. MySQL's 32-character
// username cap, Postgres's 63) when an attribute can exceed them. The operator is
// captured loosely (any text after the colon) so a malformed one like "{app:x}"
// is still recognized as a placeholder and rejected at render, rather than
// rendering literally.
var templatePlaceholderRe = regexp.MustCompile(`\{([a-zA-Z0-9_]+)(:[^}]*)?\}`)

// Config configures a Resolver.
type Config struct {
	// AWSConfig is the base AWS config used to assume roles and build Secrets
	// Manager clients.
	AWSConfig aws.Config
	// Region is the data plane's home region, and is required. Roles are assumed
	// through STS here, and a target's secret is read here unless its cluster is
	// in one of ReachableRegions.
	Region string
	// RegionAttribute names the entity attribute holding the region of each
	// target's cluster (e.g. "aws_region"). Setting it lets a target whose
	// cluster is in one of ReachableRegions have its secret read in that region,
	// and lets a secret missing from the home region be reported against the
	// cluster's region. A target whose entity has no value for it, or a value
	// that is not a region name, fails resolution.
	RegionAttribute string
	// ReachableRegions lists the regions, besides Region, whose Secrets Manager
	// this data plane can call. A target whose cluster is in one of them has its
	// secret read there; a target whose cluster is in any other region has its
	// secret read in Region, so the secret must be replicated there. Requires
	// RegionAttribute.
	ReachableRegions []string
	// RoleARN is the IAM role assumed in the target account. When empty, secrets
	// are read from the caller's own account without assuming a role. When set, it
	// may contain an "{account}" placeholder, replaced with the target's AWS
	// account id. Using a full ARN (rather than a bare role name) keeps the
	// partition and role path explicit, so non-commercial partitions (aws-us-gov,
	// aws-cn) work — e.g. "arn:aws:iam::{account}:role/tern-assumed".
	RoleARN string
	// ExternalID is an optional STS AssumeRole external id, required by some
	// cross-account trust policies. It is only used when RoleARN is set.
	ExternalID string
	// SecretName is the Secrets Manager secret id. It may contain placeholders:
	// "{target}" (the request target) and "{attribute}" (any resolved entity
	// attribute), replaced to locate per-target or per-cluster secrets. A
	// placeholder may carry a ":N" length operator (e.g. "{cluster:20}") that
	// truncates the value to at most N characters.
	SecretName string
	// AccountAttribute is the endpoint attribute holding the target's AWS account
	// id. Defaults to "aws_account_id". Only required when RoleARN is set.
	AccountAttribute string
	// Username, when set, is a template (over "{target}" and "{attribute}"
	// placeholders) that renders the database username, and the fetched secret is
	// treated as the plain-text password rather than a JSON payload. Use this for
	// conventions that derive the username from entity attributes (e.g.
	// "{app}_ddl") and store only the password. A placeholder may carry a ":N"
	// length operator (e.g. "{app:24}_ddl") so the rendered username fits the
	// database's identifier limit when an attribute can exceed it. Mutually
	// exclusive with Decode.
	Username string
	// Decode, when set, interprets the fetched secret into Credentials (for
	// example a PlanetScale token). When nil (and no Username template is set) the
	// secret is parsed as a JSON {username, password} payload.
	Decode inventory.SecretDecoder
}

// Resolver resolves credentials from Secrets Manager, optionally via per-account
// assumed roles.
type Resolver struct {
	accountAttr    string
	region         string
	regionAttr     string
	reachable      map[string]bool
	secretName     string
	usernameTmpl   string
	fetch          secretFetcher
	decode         inventory.SecretDecoder
	requireAccount bool
}

var _ inventory.CredentialResolver = (*Resolver)(nil)

// secretFetcher reads a raw secret value from a region, optionally scoped to a
// target AWS account (ignored by backends that read from the caller's own
// account).
type secretFetcher interface {
	FetchSecret(ctx context.Context, accountID, region, secretName string) (string, error)
}

// New builds a Resolver. With a role ARN it assumes a per-account role to read
// secrets across accounts; without one it reads from the caller's own account.
func New(cfg Config) (*Resolver, error) {
	// No role: read from the caller's own account, with no STS call. A role
	// switches on per-account assume-role so one data plane can read secrets
	// across many accounts; the target account then comes from an attribute.
	// Building a fetcher makes no AWS call; newResolver validates cfg first.
	// The base config carries the home region, which is where roles are assumed.
	awsCfg := cfg.AWSConfig
	awsCfg.Region = cfg.Region
	var fetch secretFetcher
	if cfg.RoleARN == "" {
		fetch = &ownAccountFetcher{awsCfg: awsCfg, clients: make(map[string]*secretsmanager.Client)}
	} else {
		fetch = &assumeRoleFetcher{
			awsCfg:     awsCfg,
			roleARN:    cfg.RoleARN,
			externalID: cfg.ExternalID,
			creds:      make(map[string]aws.CredentialsProvider),
			clients:    make(map[accountRegion]*secretsmanager.Client),
		}
	}
	return newResolver(cfg, fetch)
}

// newResolver validates cfg and constructs a Resolver over a given fetcher, so
// tests can inject a fake that does not call AWS while building the resolver
// exactly as New does.
func newResolver(cfg Config, fetch secretFetcher) (*Resolver, error) {
	switch {
	case cfg.Region == "":
		return nil, fmt.Errorf("region is required")
	case cfg.SecretName == "":
		return nil, fmt.Errorf("secret name is required")
	case cfg.Username != "" && cfg.Decode != nil:
		return nil, fmt.Errorf("username template and decode are mutually exclusive")
	case len(cfg.ReachableRegions) > 0 && cfg.RegionAttribute == "":
		return nil, fmt.Errorf("reachable regions require a region attribute naming each target's cluster region")
	}
	if !isRegionName(cfg.Region) {
		return nil, fmt.Errorf("region %q is not an AWS region name", cfg.Region)
	}
	reachable, err := reachableRegions(cfg.Region, cfg.ReachableRegions)
	if err != nil {
		return nil, err
	}
	accountAttr := cfg.AccountAttribute
	if accountAttr == "" {
		accountAttr = defaultAccountAttribute
	}
	return &Resolver{
		accountAttr:    accountAttr,
		region:         cfg.Region,
		regionAttr:     cfg.RegionAttribute,
		reachable:      reachable,
		secretName:     cfg.SecretName,
		usernameTmpl:   cfg.Username,
		fetch:          fetch,
		decode:         cfg.Decode,
		requireAccount: cfg.RoleARN != "",
	}, nil
}

// regionNameRe matches the shape of an AWS region name across partitions:
// "us-east-1", "us-gov-west-1", "cn-north-1".
var regionNameRe = regexp.MustCompile(`^[a-z]{2,}(-[a-z]+)+-[0-9]+$`)

// isRegionName reports whether s has the shape of an AWS region name. A value
// that does not would otherwise surface later as an opaque endpoint or signing
// failure from the SDK.
func isRegionName(s string) bool {
	return regionNameRe.MatchString(s)
}

// reachableRegions returns the set of regions a secret may be read in: the home
// region and every listed one. Each listed region must be a region name, other
// than the home region, and listed once.
func reachableRegions(home string, listed []string) (map[string]bool, error) {
	reachable := map[string]bool{home: true}
	for _, region := range listed {
		switch {
		case !isRegionName(region):
			return nil, fmt.Errorf("reachable region %q is not an AWS region name", region)
		case region == home:
			return nil, fmt.Errorf("reachable region %q is the home region; list only the other regions this data plane can call", region)
		case reachable[region]:
			return nil, fmt.Errorf("reachable region %q is listed more than once", region)
		}
		reachable[region] = true
	}
	return reachable, nil
}

// TemplateAttributes returns the entity attribute names referenced by a template
// — every "{placeholder}" except "{target}" — so callers can ensure the resolver
// surfaces them.
func TemplateAttributes(template string) []string {
	var attrs []string
	for _, m := range templatePlaceholderRe.FindAllStringSubmatch(template, -1) {
		if m[1] != "target" {
			attrs = append(attrs, m[1])
		}
	}
	return attrs
}

// ResolveCredentials fetches the secret for the target and interprets it via the
// configured decoder, a username template (plain-text password), or a JSON
// {username, password} payload. It fails closed: a missing required account
// attribute, an unresolved template placeholder, a fetch failure, an unparseable
// secret, or a missing username/password are all errors.
func (r *Resolver) ResolveCredentials(ctx context.Context, req inventory.Request, attrs map[string]string) (*inventory.Credentials, error) {
	accountID := attrs[r.accountAttr]
	if r.requireAccount && accountID == "" {
		return nil, fmt.Errorf("target %q has no %q attribute for assume-role credential resolution", req.Target, r.accountAttr)
	}
	region, clusterRegion, err := r.readRegion(req.Target, attrs)
	if err != nil {
		return nil, err
	}

	secretName, err := renderTemplate("secret name", r.secretName, req.Target, attrs)
	if err != nil {
		return nil, err
	}
	// Render the username template up front so a misconfiguration fails before the
	// fetch rather than after it.
	var username string
	if r.usernameTmpl != "" {
		username, err = renderTemplate("username", r.usernameTmpl, req.Target, attrs)
		if err != nil {
			return nil, err
		}
	}

	where := targetContext(req.Target, accountID, region)
	raw, err := r.fetch.FetchSecret(ctx, accountID, region, secretName)
	if err != nil {
		if missingReplica(err, clusterRegion, region) {
			return nil, fmt.Errorf("fetch secret %q for %s: %w; the target's cluster is in %s, which is not a reachable region, so its secret is read in %s: replicate the secret to %s, or list %s as a reachable region if this data plane can call Secrets Manager there",
				secretName, where, err, clusterRegion, region, region, clusterRegion)
		}
		return nil, fmt.Errorf("fetch secret %q for %s: %w", secretName, where, err)
	}

	if r.decode != nil {
		creds, err := r.decode(raw)
		if err != nil {
			return nil, fmt.Errorf("decode secret %q for %s: %w", secretName, where, err)
		}
		return creds, nil
	}

	// Username template mode: the secret is the plain-text password. Trim
	// surrounding whitespace, which a stored password rarely intends but a secret
	// pasted or uploaded from a file commonly carries (a trailing newline), so the
	// failure mode is a clear config error here rather than an opaque auth failure.
	if r.usernameTmpl != "" {
		if username == "" {
			return nil, fmt.Errorf("username template %q for %s resolved to an empty username", r.usernameTmpl, where)
		}
		password := strings.TrimSpace(raw)
		if password == "" {
			return nil, fmt.Errorf("secret %q for %s is empty (expected a password)", secretName, where)
		}
		return &inventory.Credentials{Username: username, Password: password}, nil
	}

	var parsed struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if err := json.Unmarshal([]byte(raw), &parsed); err != nil {
		return nil, fmt.Errorf("parse secret %q for %s as JSON {username, password}: %w", secretName, where, err)
	}
	if parsed.Username == "" || parsed.Password == "" {
		return nil, fmt.Errorf("secret %q for %s is missing a username or password", secretName, where)
	}
	return &inventory.Credentials{Username: parsed.Username, Password: parsed.Password}, nil
}

// readRegion returns the region to read the target's secret in, and the region
// of the target's cluster when a region attribute is configured. The secret is
// read in the cluster's region when that region is reachable, and in the home
// region otherwise. A missing or malformed cluster region fails rather than
// being read in the home region, since it means the inventory cannot say where
// the target runs.
func (r *Resolver) readRegion(target string, attrs map[string]string) (read, cluster string, err error) {
	if r.regionAttr == "" {
		return r.region, "", nil
	}
	cluster = attrs[r.regionAttr]
	if cluster == "" {
		return "", "", fmt.Errorf("target %q has no %q attribute naming the region of its cluster", target, r.regionAttr)
	}
	if !isRegionName(cluster) {
		return "", "", fmt.Errorf("target %q has %q attribute %q, which is not an AWS region name", target, r.regionAttr, cluster)
	}
	if r.reachable[cluster] {
		return cluster, cluster, nil
	}
	return r.region, cluster, nil
}

// missingReplica reports whether a fetch failed because the secret is missing
// from the home region it was read in on behalf of a cluster in an unreachable
// region: the case where the secret is expected as a replica in the home region.
func missingReplica(err error, clusterRegion, readRegion string) bool {
	readInHomeForUnreachableCluster := clusterRegion != "" && clusterRegion != readRegion
	if !readInHomeForUnreachableCluster {
		return false
	}
	var notFound *smtypes.ResourceNotFoundException
	return errors.As(err, &notFound)
}

// targetContext describes the target for error messages, including its AWS
// account id when assume-role mode resolved one (own-account mode has none) and
// the region the secret was read from, so a secret missing from one region of
// one account is diagnosable from the error alone.
func targetContext(target, accountID, region string) string {
	if accountID != "" {
		return fmt.Sprintf("target %q in account %s, region %s", target, accountID, region)
	}
	return fmt.Sprintf("target %q in region %s", target, region)
}

// renderTemplate replaces "{target}" with the request target and every other
// "{attribute}" placeholder with the resolved attribute value, applying an
// optional ":N" length operator that truncates the value to at most N
// characters. It fails closed if any referenced attribute was not resolved or a
// length operator is invalid (zero, or too large to parse). what labels the
// template in errors.
func renderTemplate(what, tmpl, target string, attrs map[string]string) (string, error) {
	var missing, invalid []string
	rendered := templatePlaceholderRe.ReplaceAllStringFunc(tmpl, func(match string) string {
		groups := templatePlaceholderRe.FindStringSubmatch(match)
		key, lengthOp := groups[1], groups[2]

		var value string
		switch {
		case key == "target":
			value = target
		case attrs[key] != "":
			value = attrs[key]
		default:
			missing = append(missing, key)
			return match
		}

		if lengthOp == "" {
			return value
		}
		// lengthOp includes the leading colon (e.g. ":24"); the spec after it must
		// be a positive integer. Anything else (":", ":x", ":0") is a config error.
		maxLen, err := strconv.Atoi(lengthOp[1:])
		if err != nil || maxLen < 1 {
			invalid = append(invalid, match)
			return match
		}
		return truncateToRunes(value, maxLen)
	})
	if len(invalid) > 0 {
		return "", fmt.Errorf("%s template %q for target %q has invalid length operator(s): %s", what, tmpl, target, strings.Join(invalid, ", "))
	}
	if len(missing) > 0 {
		return "", fmt.Errorf("%s template %q for target %q references unresolved attribute(s): %s", what, tmpl, target, strings.Join(missing, ", "))
	}
	return rendered, nil
}

// truncateToRunes returns at most maxLen runes of s. Database identifier limits
// count characters, so truncating by rune (not byte) keeps multi-byte values
// from being cut mid-character.
func truncateToRunes(s string, maxLen int) string {
	if utf8.RuneCountInString(s) <= maxLen {
		return s
	}
	return string([]rune(s)[:maxLen])
}

// getSecretValue reads a secret's value from Secrets Manager (string or binary).
func getSecretValue(ctx context.Context, client *secretsmanager.Client, secretName string) (string, error) {
	resp, err := client.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{
		SecretId: aws.String(secretName),
	})
	if err != nil {
		return "", fmt.Errorf("get secret value %q: %w", secretName, err)
	}
	return secrets.ValueFromGetSecretOutput(resp, secretName)
}

// ownAccountFetcher reads secrets from the caller's own account — no STS
// AssumeRole — caching one Secrets Manager client per region. The account id is
// ignored.
type ownAccountFetcher struct {
	awsCfg aws.Config

	mu      sync.Mutex
	clients map[string]*secretsmanager.Client
}

// FetchSecret returns the raw secret string from the caller's own account.
func (f *ownAccountFetcher) FetchSecret(ctx context.Context, _, region, secretName string) (string, error) {
	return getSecretValue(ctx, f.clientForRegion(region), secretName)
}

func (f *ownAccountFetcher) clientForRegion(region string) *secretsmanager.Client {
	f.mu.Lock()
	defer f.mu.Unlock()

	if c, ok := f.clients[region]; ok {
		return c
	}
	regionalCfg := f.awsCfg
	regionalCfg.Region = region
	c := secretsmanager.NewFromConfig(regionalCfg)
	f.clients[region] = c
	return c
}

// accountRegion keys an assumed-role client: the role is per account and the
// Secrets Manager endpoint is per region.
type accountRegion struct {
	accountID string
	region    string
}

// assumeRoleFetcher reads secrets via Secrets Manager using per-account
// assumed-role credentials. It caches one credential provider per account, so a
// role is assumed once however many regions its secrets are read in, and one
// client per account and region.
type assumeRoleFetcher struct {
	// awsCfg carries the home region, where every role is assumed. Credentials
	// from a regional STS endpoint are valid in every region, so reading a
	// secret in another region needs that region's Secrets Manager but not its
	// STS.
	awsCfg     aws.Config
	roleARN    string
	externalID string

	mu      sync.Mutex
	creds   map[string]aws.CredentialsProvider
	clients map[accountRegion]*secretsmanager.Client
}

// FetchSecret returns the raw secret string from the target account.
func (f *assumeRoleFetcher) FetchSecret(ctx context.Context, accountID, region, secretName string) (string, error) {
	return getSecretValue(ctx, f.clientFor(accountID, region), secretName)
}

func (f *assumeRoleFetcher) clientFor(accountID, region string) *secretsmanager.Client {
	f.mu.Lock()
	defer f.mu.Unlock()

	key := accountRegion{accountID: accountID, region: region}
	if c, ok := f.clients[key]; ok {
		return c
	}

	creds, ok := f.creds[accountID]
	if !ok {
		roleARN := strings.ReplaceAll(f.roleARN, "{account}", accountID)
		creds = aws.NewCredentialsCache(
			stscreds.NewAssumeRoleProvider(sts.NewFromConfig(f.awsCfg), roleARN, func(aro *stscreds.AssumeRoleOptions) {
				if f.externalID != "" {
					aro.ExternalID = aws.String(f.externalID)
				}
			}),
		)
		f.creds[accountID] = creds
	}

	regionalCfg := f.awsCfg
	regionalCfg.Region = region
	c := secretsmanager.NewFromConfig(regionalCfg, func(o *secretsmanager.Options) {
		o.Credentials = creds
	})
	f.clients[key] = c
	return c
}
