package serve

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"log/slog"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
)

// writeTestCertPair generates a self-signed certificate and key, writes them
// as PEM files under dir, and returns their paths. The certificate doubles as
// the CA bundle: registration only parses the material, it does not verify a
// chain.
func writeTestCertPair(t *testing.T, dir string) (certPath, keyPath string) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	now := time.Now()
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "schemabot-test"},
		NotBefore:    now.Add(-time.Hour),
		NotAfter:     now.Add(time.Hour),
	}
	certDER, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)

	certPath = filepath.Join(dir, "tls.crt")
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	require.NoError(t, os.WriteFile(certPath, certPEM, 0o600))

	keyDER, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)
	keyPath = filepath.Join(dir, "tls.key")
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	require.NoError(t, os.WriteFile(keyPath, keyPEM, 0o600))

	return certPath, keyPath
}

// Without a planetscale.mtls block the server starts without registering any
// process-wide TLS config, leaving per-database TLS settings in charge.
func TestRegisterPlanetScaleMTLSAbsentIsNoOp(t *testing.T) {
	cfg := &api.ServerConfig{}
	require.NoError(t, registerPlanetScaleMTLS(cfg, slog.New(slog.DiscardHandler)))
}

// A configured planetscale.mtls block with readable certificate material
// registers successfully, so every MySQL connection the Vitess engine opens
// presents the client identity.
func TestRegisterPlanetScaleMTLSValidMaterial(t *testing.T) {
	certPath, keyPath := writeTestCertPair(t, t.TempDir())
	cfg := &api.ServerConfig{
		PlanetScale: api.PlanetScaleConfig{
			MTLS: &api.PlanetScaleMTLSConfig{
				CABundle:   certPath,
				ClientCert: certPath,
				ClientKey:  keyPath,
			},
		},
	}
	require.NoError(t, registerPlanetScaleMTLS(cfg, slog.New(slog.DiscardHandler)))
}

// Unreadable certificate material is a hard startup error: a worker must not
// come up and then fail (or silently degrade) on every Vitess connection.
func TestRegisterPlanetScaleMTLSUnreadableMaterialFails(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing.pem")
	cfg := &api.ServerConfig{
		PlanetScale: api.PlanetScaleConfig{
			MTLS: &api.PlanetScaleMTLSConfig{
				CABundle:   missing,
				ClientCert: missing,
				ClientKey:  missing,
			},
		},
	}
	err := registerPlanetScaleMTLS(cfg, slog.New(slog.DiscardHandler))
	require.ErrorContains(t, err, "register PlanetScale mTLS")
	require.ErrorContains(t, err, missing)
}

// Build registers planetscale.mtls before opening storage, so bad certificate
// material fails startup immediately with an mTLS error, not a downstream
// connection error.
func TestBuildFailsFastOnBadPlanetScaleMTLS(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing.pem")
	cfg := &api.ServerConfig{
		PlanetScale: api.PlanetScaleConfig{
			MTLS: &api.PlanetScaleMTLSConfig{
				CABundle:   missing,
				ClientCert: missing,
				ClientKey:  missing,
			},
		},
	}
	_, err := Build(t.Context(), cfg, WithLogger(slog.New(slog.DiscardHandler)))
	require.ErrorContains(t, err, "register PlanetScale mTLS")
}
