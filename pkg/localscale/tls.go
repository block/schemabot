package localscale

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"time"
)

// TLSBundle holds the paths to generated TLS certificates for branch proxies.
type TLSBundle struct {
	// CAPath is the path to the CA certificate PEM file.
	CAPath string
	// ClientCertPath is the path to the client certificate PEM file (mTLS only).
	ClientCertPath string
	// ClientKeyPath is the path to the client private key PEM file (mTLS only).
	ClientKeyPath string
	// TLSConfig is the server-side TLS configuration for net.Listener wrapping.
	TLSConfig *tls.Config
}

// generateTLSBundle creates a self-signed CA and server certificate for branch
// proxy TLS. When mtls is true, also generates a client certificate and
// configures the server to require client cert verification.
func generateTLSBundle(dir string, mtls bool) (*TLSBundle, error) {
	// Generate CA key and certificate.
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate CA key: %w", err)
	}

	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{Organization: []string{"LocalScale"}, CommonName: "LocalScale CA"},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		IsCA:                  true,
		BasicConstraintsValid: true,
	}

	caCertDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		return nil, fmt.Errorf("create CA certificate: %w", err)
	}

	caCert, err := x509.ParseCertificate(caCertDER)
	if err != nil {
		return nil, fmt.Errorf("parse CA certificate: %w", err)
	}

	// Generate server key and certificate signed by the CA.
	serverKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate server key: %w", err)
	}

	serverTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{Organization: []string{"LocalScale"}, CommonName: "localhost"},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
	}

	serverCertDER, err := x509.CreateCertificate(rand.Reader, serverTemplate, caCert, &serverKey.PublicKey, caKey)
	if err != nil {
		return nil, fmt.Errorf("create server certificate: %w", err)
	}

	// Write PEM files.
	caPath := filepath.Join(dir, "ca.pem")
	if err := writePEM(caPath, "CERTIFICATE", caCertDER); err != nil {
		return nil, fmt.Errorf("write CA cert: %w", err)
	}

	serverKeyDER, err := x509.MarshalECPrivateKey(serverKey)
	if err != nil {
		return nil, fmt.Errorf("marshal server key: %w", err)
	}

	serverCertPair, err := tls.X509KeyPair(
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: serverCertDER}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: serverKeyDER}),
	)
	if err != nil {
		return nil, fmt.Errorf("create server TLS keypair: %w", err)
	}

	// Build server TLS config.
	tlsCfg := &tls.Config{
		Certificates: []tls.Certificate{serverCertPair},
		MinVersion:   tls.VersionTLS12,
	}

	bundle := &TLSBundle{
		CAPath:    caPath,
		TLSConfig: tlsCfg,
	}

	// For mTLS: generate client cert and require client auth on the server.
	if mtls {
		clientKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			return nil, fmt.Errorf("generate client key: %w", err)
		}

		clientTemplate := &x509.Certificate{
			SerialNumber: big.NewInt(3),
			Subject:      pkix.Name{Organization: []string{"LocalScale"}, CommonName: "localscale-client"},
			NotBefore:    time.Now(),
			NotAfter:     time.Now().Add(24 * time.Hour),
			KeyUsage:     x509.KeyUsageDigitalSignature,
			ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		}

		clientCertDER, err := x509.CreateCertificate(rand.Reader, clientTemplate, caCert, &clientKey.PublicKey, caKey)
		if err != nil {
			return nil, fmt.Errorf("create client certificate: %w", err)
		}

		clientCertPath := filepath.Join(dir, "client-cert.pem")
		clientKeyPath := filepath.Join(dir, "client-key.pem")

		if err := writePEM(clientCertPath, "CERTIFICATE", clientCertDER); err != nil {
			return nil, fmt.Errorf("write client cert: %w", err)
		}

		clientKeyDER, err := x509.MarshalECPrivateKey(clientKey)
		if err != nil {
			return nil, fmt.Errorf("marshal client key: %w", err)
		}
		if err := writePEM(clientKeyPath, "EC PRIVATE KEY", clientKeyDER); err != nil {
			return nil, fmt.Errorf("write client key: %w", err)
		}

		bundle.ClientCertPath = clientCertPath
		bundle.ClientKeyPath = clientKeyPath

		// Server requires and verifies client certs.
		caPool := x509.NewCertPool()
		caPool.AddCert(caCert)
		tlsCfg.ClientCAs = caPool
		tlsCfg.ClientAuth = tls.RequireAndVerifyClientCert
	}

	return bundle, nil
}

func writePEM(path, blockType string, data []byte) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	if err := pem.Encode(f, &pem.Block{Type: blockType, Bytes: data}); err != nil {
		return fmt.Errorf("encode PEM: %w (close: %w)", err, f.Close())
	}
	return f.Close()
}
