// Package awscreds resolves database credentials from AWS Secrets Manager.
//
// It implements inventory.CredentialResolver. The secret name is templated over
// the resolved target and its entity attributes, so one configuration can locate
// per-target or per-cluster secrets. By default it reads from the caller's own
// AWS account; when a role ARN is configured it assumes a per-target role first,
// so a single data plane can read secrets across many AWS accounts.
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
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/aws/aws-sdk-go-v2/service/sts"

	"github.com/block/schemabot/pkg/inventory"
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
	// Region is the region for Secrets Manager (and assumed-role sessions, when a
	// role is configured).
	Region string
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
	secretName     string
	usernameTmpl   string
	fetch          secretFetcher
	decode         inventory.SecretDecoder
	requireAccount bool
}

var _ inventory.CredentialResolver = (*Resolver)(nil)

// secretFetcher reads a raw secret value, optionally scoped to a target AWS
// account (ignored by backends that read from the caller's own account).
type secretFetcher interface {
	FetchSecret(ctx context.Context, accountID, secretName string) (string, error)
}

// New builds a Resolver. With a role ARN it assumes a per-account role to read
// secrets across accounts; without one it reads from the caller's own account.
func New(cfg Config) (*Resolver, error) {
	switch {
	case cfg.Region == "":
		return nil, fmt.Errorf("region is required")
	case cfg.SecretName == "":
		return nil, fmt.Errorf("secret name is required")
	case cfg.Username != "" && cfg.Decode != nil:
		return nil, fmt.Errorf("username template and decode are mutually exclusive")
	}
	accountAttr := cfg.AccountAttribute
	if accountAttr == "" {
		accountAttr = defaultAccountAttribute
	}

	// No role: read from the caller's own account with a single client. A role
	// switches on per-account assume-role so one data plane can read secrets
	// across many accounts; the target account then comes from an attribute.
	if cfg.RoleARN == "" {
		regionalCfg := cfg.AWSConfig
		regionalCfg.Region = cfg.Region
		fetch := &ownAccountFetcher{client: secretsmanager.NewFromConfig(regionalCfg)}
		return newResolver(accountAttr, cfg.SecretName, cfg.Username, fetch, cfg.Decode, false), nil
	}

	fetch := &assumeRoleFetcher{
		awsCfg:     cfg.AWSConfig,
		region:     cfg.Region,
		roleARN:    cfg.RoleARN,
		externalID: cfg.ExternalID,
		clients:    make(map[string]*secretsmanager.Client),
	}
	return newResolver(accountAttr, cfg.SecretName, cfg.Username, fetch, cfg.Decode, true), nil
}

// newResolver constructs a Resolver over a given fetcher, so tests can inject a
// fake that does not call AWS.
func newResolver(accountAttr, secretName, usernameTmpl string, fetch secretFetcher, decode inventory.SecretDecoder, requireAccount bool) *Resolver {
	return &Resolver{
		accountAttr:    accountAttr,
		secretName:     secretName,
		usernameTmpl:   usernameTmpl,
		fetch:          fetch,
		decode:         decode,
		requireAccount: requireAccount,
	}
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

	where := targetContext(req.Target, accountID)
	raw, err := r.fetch.FetchSecret(ctx, accountID, secretName)
	if err != nil {
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

// targetContext describes the target for error messages, including its AWS
// account id when assume-role mode resolved one (own-account mode has none), so
// cross-account failures stay diagnosable.
func targetContext(target, accountID string) string {
	if accountID != "" {
		return fmt.Sprintf("target %q in account %s", target, accountID)
	}
	return fmt.Sprintf("target %q", target)
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

// getSecretString reads a string secret value, rejecting binary secrets.
func getSecretString(ctx context.Context, client *secretsmanager.Client, secretName string) (string, error) {
	resp, err := client.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{
		SecretId: aws.String(secretName),
	})
	if err != nil {
		return "", fmt.Errorf("get secret value: %w", err)
	}
	if resp.SecretString == nil {
		return "", fmt.Errorf("secret %q has no string value (binary secrets are not supported)", secretName)
	}
	return *resp.SecretString, nil
}

// ownAccountFetcher reads secrets from the caller's own account using a single
// Secrets Manager client — no STS AssumeRole. The account id is ignored.
type ownAccountFetcher struct {
	client *secretsmanager.Client
}

// FetchSecret returns the raw secret string from the caller's own account.
func (f *ownAccountFetcher) FetchSecret(ctx context.Context, _ string, secretName string) (string, error) {
	return getSecretString(ctx, f.client, secretName)
}

// assumeRoleFetcher reads secrets via Secrets Manager using per-account
// assumed-role credentials, caching one client per account to avoid repeated
// STS AssumeRole calls.
type assumeRoleFetcher struct {
	awsCfg     aws.Config
	region     string
	roleARN    string
	externalID string

	mu      sync.Mutex
	clients map[string]*secretsmanager.Client
}

// FetchSecret returns the raw secret string from the target account.
func (f *assumeRoleFetcher) FetchSecret(ctx context.Context, accountID, secretName string) (string, error) {
	return getSecretString(ctx, f.clientForAccount(accountID), secretName)
}

func (f *assumeRoleFetcher) clientForAccount(accountID string) *secretsmanager.Client {
	f.mu.Lock()
	defer f.mu.Unlock()

	if c, ok := f.clients[accountID]; ok {
		return c
	}

	// Make the configured region authoritative for both the STS and Secrets
	// Manager clients, regardless of the base config's region.
	regionalCfg := f.awsCfg
	regionalCfg.Region = f.region

	roleARN := strings.ReplaceAll(f.roleARN, "{account}", accountID)
	c := secretsmanager.NewFromConfig(regionalCfg, func(o *secretsmanager.Options) {
		o.Credentials = aws.NewCredentialsCache(
			stscreds.NewAssumeRoleProvider(sts.NewFromConfig(regionalCfg), roleARN, func(aro *stscreds.AssumeRoleOptions) {
				if f.externalID != "" {
					aro.ExternalID = aws.String(f.externalID)
				}
			}),
		)
	})
	f.clients[accountID] = c
	return c
}
