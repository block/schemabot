package planetscale

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// writeCertPair generates a self-signed certificate for commonName and writes
// the certificate and key PEM files to certPath and keyPath, overwriting any
// existing files so a test can rotate the pair in place.
func writeCertPair(t *testing.T, certPath, keyPath, commonName string) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	now := time.Now()
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: commonName},
		NotBefore:    now.Add(-time.Hour),
		NotAfter:     now.Add(time.Hour),
	}
	certDER, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	require.NoError(t, os.WriteFile(certPath, certPEM, 0o600))

	keyDER, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	require.NoError(t, os.WriteFile(keyPath, keyPEM, 0o600))
}

// The handshake callback reads the client certificate pair from disk on every
// call, so rotating the files on the same paths changes the identity presented
// on new connections without a process restart.
func TestClientCertificateLoaderReloadsRotatedPair(t *testing.T) {
	dir := t.TempDir()
	certPath := filepath.Join(dir, "tls.crt")
	keyPath := filepath.Join(dir, "tls.key")
	writeCertPair(t, certPath, keyPath, "before-rotation")

	load := clientCertificateLoader(certPath, keyPath)

	before, err := load(nil)
	require.NoError(t, err)
	beforeLeaf, err := x509.ParseCertificate(before.Certificate[0])
	require.NoError(t, err)
	require.Equal(t, "before-rotation", beforeLeaf.Subject.CommonName)

	writeCertPair(t, certPath, keyPath, "after-rotation")

	after, err := load(nil)
	require.NoError(t, err)
	afterLeaf, err := x509.ParseCertificate(after.Certificate[0])
	require.NoError(t, err)
	require.Equal(t, "after-rotation", afterLeaf.Subject.CommonName)
}

// A pair that has become unreadable fails the handshake with the underlying
// path in the error, rather than silently presenting a stale identity.
func TestClientCertificateLoaderUnreadablePairFails(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing.pem")
	load := clientCertificateLoader(missing, missing)

	_, err := load(nil)
	require.ErrorContains(t, err, "reload PlanetScale mTLS client certificate")
	require.ErrorContains(t, err, missing)
}
