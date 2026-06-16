package inventory

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSecretRefCredentialResolverLiteralPassword(t *testing.T) {
	r := SecretRefCredentialResolver{Username: "spirit", PasswordRef: "ddl-secret"}

	creds, err := r.ResolveCredentials(t.Context(), Request{Target: "orders-dsid"}, nil)
	require.NoError(t, err)
	assert.Equal(t, "spirit", creds.Username)
	assert.Equal(t, "ddl-secret", creds.Password)
}

// The password reference may carry a {target} placeholder so a deployment can
// name per-target secrets without a templating engine.
func TestSecretRefCredentialResolverTemplatesTarget(t *testing.T) {
	t.Setenv("ORDERS_DSID_PW", "from-env")
	r := SecretRefCredentialResolver{Username: "spirit", PasswordRef: "env:{target}_PW"}

	creds, err := r.ResolveCredentials(t.Context(), Request{Target: "ORDERS_DSID"}, nil)
	require.NoError(t, err)
	assert.Equal(t, "from-env", creds.Password)
}

func TestSecretRefCredentialResolverRequiresPasswordRef(t *testing.T) {
	r := SecretRefCredentialResolver{Username: "spirit"}

	_, err := r.ResolveCredentials(t.Context(), Request{Target: "orders-dsid"}, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "password reference is required")
}

// An env: reference to an unset variable resolves to "" without error; the
// resolver must fail closed rather than return a blank password.
func TestSecretRefCredentialResolverRejectsEmptyResolvedPassword(t *testing.T) {
	r := SecretRefCredentialResolver{Username: "spirit", PasswordRef: "env:UNSET_DDL_PASSWORD"}

	_, err := r.ResolveCredentials(t.Context(), Request{Target: "orders-dsid"}, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "empty value")
}

// With a Decode set, the resolved secret is interpreted by the decoder — here a
// PlanetScale token — rather than used directly as the password.
func TestSecretRefCredentialResolverDecodes(t *testing.T) {
	r := SecretRefCredentialResolver{
		PasswordRef: `{"token":"tok-id=tok-secret"}`,
		Decode:      DecodePlanetScaleSecret,
	}

	creds, err := r.ResolveCredentials(t.Context(), Request{Target: "orders-dsid"}, nil)
	require.NoError(t, err)
	assert.Empty(t, creds.Password)
	assert.Equal(t, "tok-id", creds.Metadata[MetadataTokenName])
	assert.Equal(t, "tok-secret", creds.Metadata[MetadataTokenValue])
}
