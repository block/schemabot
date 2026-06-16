// Package awscreds resolves database credentials from AWS Secrets Manager,
// assuming a per-target IAM role first so a single data plane can read secrets
// across many AWS accounts.
//
// It implements inventory.CredentialResolver: the target's AWS account comes
// from an endpoint attribute (surfaced by endpoint resolution), the resolver
// assumes a role in that account, reads the secret, and parses it as a JSON
// {username, password} payload. Credential values come from Secrets Manager,
// never from the inventory source.
package awscreds

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/aws/aws-sdk-go-v2/service/sts"

	"github.com/block/schemabot/pkg/inventory"
)

// defaultAccountAttribute is the endpoint attribute holding the target's AWS
// account id when one is not configured.
const defaultAccountAttribute = "aws_account_id"

// Config configures a Resolver.
type Config struct {
	// AWSConfig is the base AWS config used to assume roles and build Secrets
	// Manager clients.
	AWSConfig aws.Config
	// Region is the region for assumed-role sessions and Secrets Manager.
	Region string
	// RoleARN is the IAM role assumed in the target account. It may contain an
	// "{account}" placeholder, replaced with the target's AWS account id. Using a
	// full ARN (rather than a bare role name) keeps the partition and role path
	// explicit, so non-commercial partitions (aws-us-gov, aws-cn) work — e.g.
	// "arn:aws:iam::{account}:role/tern-assumed".
	RoleARN string
	// ExternalID is an optional STS AssumeRole external id, required by some
	// cross-account trust policies.
	ExternalID string
	// SecretName is the Secrets Manager secret id. It may contain a "{target}"
	// placeholder, replaced with the request target for per-target secrets.
	SecretName string
	// AccountAttribute is the endpoint attribute holding the target's AWS account
	// id. Defaults to "aws_account_id".
	AccountAttribute string
	// Decode, when set, interprets the fetched secret into Credentials (for
	// example a PlanetScale token). When nil the secret is parsed as a JSON
	// {username, password} payload.
	Decode inventory.SecretDecoder
}

// Resolver resolves credentials from Secrets Manager via per-account assumed
// roles.
type Resolver struct {
	accountAttr string
	secretName  string
	fetch       secretFetcher
	decode      inventory.SecretDecoder
}

var _ inventory.CredentialResolver = (*Resolver)(nil)

// secretFetcher reads a raw secret value for a target AWS account.
type secretFetcher interface {
	FetchSecret(ctx context.Context, accountID, secretName string) (string, error)
}

// New builds a Resolver backed by per-account assumed-role Secrets Manager
// clients.
func New(cfg Config) (*Resolver, error) {
	switch {
	case cfg.Region == "":
		return nil, fmt.Errorf("region is required")
	case cfg.RoleARN == "":
		return nil, fmt.Errorf("role ARN is required")
	case cfg.SecretName == "":
		return nil, fmt.Errorf("secret name is required")
	}
	accountAttr := cfg.AccountAttribute
	if accountAttr == "" {
		accountAttr = defaultAccountAttribute
	}
	fetch := &assumeRoleFetcher{
		awsCfg:     cfg.AWSConfig,
		region:     cfg.Region,
		roleARN:    cfg.RoleARN,
		externalID: cfg.ExternalID,
		clients:    make(map[string]*secretsmanager.Client),
	}
	return newResolver(accountAttr, cfg.SecretName, fetch, cfg.Decode), nil
}

// newResolver constructs a Resolver over a given fetcher, so tests can inject a
// fake that does not call AWS.
func newResolver(accountAttr, secretName string, fetch secretFetcher, decode inventory.SecretDecoder) *Resolver {
	return &Resolver{accountAttr: accountAttr, secretName: secretName, fetch: fetch, decode: decode}
}

// ResolveCredentials reads the target's account from attrs, fetches the secret
// from that account, and parses it as a JSON {username, password} payload. It
// fails closed: a missing account attribute, a fetch failure, an unparseable
// secret, or a missing username/password are all errors.
func (r *Resolver) ResolveCredentials(ctx context.Context, req inventory.Request, attrs map[string]string) (*inventory.Credentials, error) {
	accountID := attrs[r.accountAttr]
	if accountID == "" {
		return nil, fmt.Errorf("target %q has no %q attribute for assume-role credential resolution", req.Target, r.accountAttr)
	}

	secretName := strings.ReplaceAll(r.secretName, "{target}", req.Target)
	raw, err := r.fetch.FetchSecret(ctx, accountID, secretName)
	if err != nil {
		return nil, fmt.Errorf("fetch secret %q for target %q in account %s: %w", secretName, req.Target, accountID, err)
	}

	if r.decode != nil {
		creds, err := r.decode(raw)
		if err != nil {
			return nil, fmt.Errorf("decode secret %q for target %q in account %s: %w", secretName, req.Target, accountID, err)
		}
		return creds, nil
	}

	var parsed struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if err := json.Unmarshal([]byte(raw), &parsed); err != nil {
		return nil, fmt.Errorf("parse secret %q for target %q as JSON {username, password}: %w", secretName, req.Target, err)
	}
	if parsed.Username == "" || parsed.Password == "" {
		return nil, fmt.Errorf("secret %q for target %q is missing a username or password", secretName, req.Target)
	}
	return &inventory.Credentials{Username: parsed.Username, Password: parsed.Password}, nil
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
	client := f.clientForAccount(accountID)
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
