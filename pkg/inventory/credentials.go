package inventory

import (
	"context"
	"fmt"
	"strings"

	"github.com/block/schemabot/pkg/secrets"
)

// Credentials is a resolved database credential. The values never originate
// from the inventory source (for example Etre); they come from a credential
// backend such as an environment variable, a mounted file, or a secrets
// manager.
type Credentials struct {
	Username string
	Password string
	// Metadata carries engine-specific credential material (for example a
	// Vitess access token) without widening the struct per engine.
	Metadata map[string]string
}

// SecretDecoder interprets a raw secret value into Credentials. It lets a
// credential resolver fetch a secret one way — a reference, or an assumed-role
// Secrets Manager read — and interpret it per engine without coupling the fetch
// to the format: a plain password for MySQL, or a PlanetScale token for Vitess.
type SecretDecoder func(raw string) (*Credentials, error)

// CredentialResolver resolves credentials for a target, independently of
// endpoint resolution so the two can be configured and overridden separately.
//
// It may use attributes surfaced by endpoint resolution — for example an AWS
// account id to assume a role, or a cluster name to build a per-cluster secret
// name — but those attributes are inputs for locating the credential, never the
// secret values themselves.
type CredentialResolver interface {
	ResolveCredentials(ctx context.Context, req Request, attrs map[string]string) (*Credentials, error)
}

// SecretRefCredentialResolver resolves credentials from a configured username
// and a password secret reference.
//
// The reference is resolved through pkg/secrets (env:, file:, secretsmanager:,
// or a literal) on every call, so a rotated file-mounted secret — for example
// one materialized by the External Secrets Operator — is picked up without a
// restart. The reference may contain a "{target}" placeholder, replaced with
// the request target, for per-target secret naming. This resolver does not use
// endpoint attributes; the secrets-manager resolver that does (assume-role,
// per-cluster names) plugs into the same interface.
type SecretRefCredentialResolver struct {
	Username    string
	PasswordRef string
	// Decode, when set, interprets the resolved secret value into Credentials
	// (for example a PlanetScale token). When nil the secret is used directly as
	// the password alongside the configured username.
	Decode SecretDecoder
}

var _ CredentialResolver = SecretRefCredentialResolver{}

// ResolveCredentials resolves the password reference fresh on every call.
func (r SecretRefCredentialResolver) ResolveCredentials(_ context.Context, req Request, _ map[string]string) (*Credentials, error) {
	if r.PasswordRef == "" {
		return nil, fmt.Errorf("password reference is required for target %q", req.Target)
	}
	ref := strings.ReplaceAll(r.PasswordRef, "{target}", req.Target)
	password, err := secrets.Resolve(ref, "")
	if err != nil {
		return nil, fmt.Errorf("resolve password for target %q: %w", req.Target, err)
	}
	// secrets.Resolve returns "" without error when an env: var or file: is
	// unset/empty. Fail closed rather than authenticate with a blank password,
	// which would otherwise surface as an opaque DB auth error later.
	if password == "" {
		return nil, fmt.Errorf("password reference %q resolved to an empty value for target %q", ref, req.Target)
	}
	if r.Decode != nil {
		creds, err := r.Decode(password)
		if err != nil {
			return nil, fmt.Errorf("decode secret for target %q: %w", req.Target, err)
		}
		return creds, nil
	}
	return &Credentials{Username: r.Username, Password: password}, nil
}
