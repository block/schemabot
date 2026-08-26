package postgres

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
)

// TestCACertPath pins the CA-reference contract: an absent reference and the
// embedded RDS bundle both rely on the automatic RDS trust (no path), a file
// reference resolves to the named bundle, and anything else is refused so an
// unrecognized CA can never silently downgrade verification.
func TestCACertPath(t *testing.T) {
	tests := []struct {
		name     string
		metadata map[string]string
		want     string
		wantErr  string
	}{
		{name: "absent reference trusts the embedded RDS roots"},
		{
			name:     "embedded reference resolves to no path",
			metadata: map[string]string{metadataCARef: caRefEmbeddedRDSGlobal},
		},
		{
			name:     "file reference resolves to its path",
			metadata: map[string]string{metadataCARef: "file:/etc/ssl/rds.pem"},
			want:     "/etc/ssl/rds.pem",
		},
		{
			name:     "file reference with no path is refused",
			metadata: map[string]string{metadataCARef: "file:"},
			wantErr:  "names no path",
		},
		{
			name:     "relative file reference is refused",
			metadata: map[string]string{metadataCARef: "file:ca.pem"},
			wantErr:  "requires an absolute path",
		},
		{
			name:     "unknown reference is refused",
			metadata: map[string]string{metadataCARef: "vault:some-ca"},
			wantErr:  "unsupported reference",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := caCertPath(&engine.Credentials{Metadata: tt.metadata})
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

// A bundle the validation connection cannot trust is refused before any dial:
// a missing file and a file with no parseable certificates both fail closed,
// while a readable bundle yields the pinning option and no path yields none.
func TestValidationRootCAs(t *testing.T) {
	t.Run("no path needs no options", func(t *testing.T) {
		opts, err := validationRootCAs("")
		require.NoError(t, err)
		assert.Empty(t, opts)
	})

	t.Run("missing bundle fails closed", func(t *testing.T) {
		_, err := validationRootCAs(filepath.Join(t.TempDir(), "absent.pem"))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "read PostgreSQL CA bundle")
	})

	t.Run("bundle with no certificates fails closed", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "empty.pem")
		require.NoError(t, os.WriteFile(path, []byte("not a certificate"), 0o600))
		_, err := validationRootCAs(path)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "contains no usable certificates")
	})

	t.Run("readable bundle yields the pinning option", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "ca.pem")
		require.NoError(t, os.WriteFile(path, testCAPEM(t), 0o600))
		opts, err := validationRootCAs(path)
		require.NoError(t, err)
		assert.Len(t, opts, 1)
	})
}

func TestPlanRefusesUnknownCAReferenceBeforeConnecting(t *testing.T) {
	eng := New()
	_, err := eng.Plan(t.Context(), &engine.PlanRequest{
		Database: "app",
		Credentials: &engine.Credentials{
			DSN:      "postgres://localhost/app",
			Metadata: map[string]string{metadataCARef: "vault:some-ca"},
		},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported reference")
}

func TestApplyRefusesUnknownCAReferenceAtAcceptance(t *testing.T) {
	eng := New()
	_, err := eng.Apply(t.Context(), &engine.ApplyRequest{
		Database: "app",
		Changes: []engine.SchemaChange{{
			Namespace: "public",
			TableChanges: []engine.TableChange{{
				Table: "users", DDL: "ALTER TABLE public.users ADD COLUMN email text",
			}},
		}},
		Credentials: &engine.Credentials{
			DSN:      "postgres://localhost/app",
			Metadata: map[string]string{metadataCARef: "vault:some-ca"},
		},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported reference")
}

// A file-referenced bundle the pool could never trust — missing here — refuses
// the apply at acceptance instead of failing later inside the background
// drive.
func TestApplyRefusesUnreadableCABundleAtAcceptance(t *testing.T) {
	eng := New()
	_, err := eng.Apply(t.Context(), &engine.ApplyRequest{
		Database: "app",
		Changes: []engine.SchemaChange{{
			Namespace: "public",
			TableChanges: []engine.TableChange{{
				Table: "users", DDL: "ALTER TABLE public.users ADD COLUMN email text",
			}},
		}},
		Credentials: &engine.Credentials{
			DSN:      "postgres://localhost/app",
			Metadata: map[string]string{metadataCARef: "file:" + filepath.Join(t.TempDir(), "absent.pem")},
		},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "read PostgreSQL CA bundle")
}

// testCAPEM returns a self-signed CA certificate in PEM form.
func testCAPEM(t *testing.T) []byte {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "schemabot test ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, pub, priv)
	require.NoError(t, err)
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}
