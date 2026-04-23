package planetscale

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"sync/atomic"

	mysql "github.com/go-sql-driver/mysql"
)

// mtlsConfigName is the Go MySQL driver TLS config name registered by RegisterMTLS.
// Used for all PlanetScale MySQL connections (branches and vtgate).
const mtlsConfigName = "planetscale"

// mtlsRegistered tracks whether RegisterMTLS has been called.
// Accessed from multiple goroutines (branch apply + vtgate progress polling).
var mtlsRegistered atomic.Bool

// TLSConfigName returns the Go MySQL driver TLS config name registered by
// RegisterMTLS. Use this in DSN strings (e.g., "user:pass@tcp(host)/?tls=<name>")
// for any connection that needs PlanetScale mTLS certificates.
func TLSConfigName() string {
	return mtlsConfigName
}

// MTLSConfig holds paths to the TLS certificates for mutual TLS authentication
// with PlanetScale branch endpoints.
type MTLSConfig struct {
	// CABundlePath is the path to the CA bundle PEM file for verifying the
	// server's certificate. Required.
	CABundlePath string

	// ClientCertPath is the path to the client certificate PEM file.
	// Required for mutual TLS (mTLS).
	ClientCertPath string

	// ClientKeyPath is the path to the client private key PEM file.
	// Required for mutual TLS (mTLS).
	ClientKeyPath string
}

// RegisterMTLS registers a TLS configuration with the Go MySQL driver for
// mutual TLS authentication with PlanetScale endpoints. The engine applies
// the config automatically for all MySQL connections (branches and vtgate).
//
// Example usage:
//
//	err := planetscale.RegisterMTLS(planetscale.MTLSConfig{
//	    CABundlePath:   "/etc/planetscale/ca-bundle.pem",
//	    ClientCertPath: "/etc/planetscale/client-cert.pem",
//	    ClientKeyPath:  "/etc/planetscale/client-key.pem",
//	})
func RegisterMTLS(cfg MTLSConfig) error {
	caBundlePEM, err := os.ReadFile(cfg.CABundlePath)
	if err != nil {
		return fmt.Errorf("read CA bundle %s: %w", cfg.CABundlePath, err)
	}

	rootCAs := x509.NewCertPool()
	if !rootCAs.AppendCertsFromPEM(caBundlePEM) {
		return fmt.Errorf("no valid certificates found in CA bundle %s", cfg.CABundlePath)
	}

	clientCert, err := tls.LoadX509KeyPair(cfg.ClientCertPath, cfg.ClientKeyPath)
	if err != nil {
		return fmt.Errorf("load client certificate: %w", err)
	}

	tlsCfg := &tls.Config{
		RootCAs:      rootCAs,
		Certificates: []tls.Certificate{clientCert},
		MinVersion:   tls.VersionTLS12,
	}

	if err := mysql.RegisterTLSConfig(mtlsConfigName, tlsCfg); err != nil {
		return fmt.Errorf("register TLS config: %w", err)
	}
	mtlsRegistered.Store(true)
	return nil
}
