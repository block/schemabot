package postgres

import (
	"crypto/x509"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/block/pg-sprite/pkg/dbconn"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/postgresconn"
)

// Credentials metadata contract for PostgreSQL certificate-authority
// references. The inventory PostgreSQL connection assembler writes the key;
// the engine keeps its own copy of the strings because the engine interface
// deliberately carries engine-specific data as opaque metadata.
const (
	// metadataCARef names the certificate authority the target's
	// verify-full DSN is verified against.
	metadataCARef = "postgres_ca_ref"
	// caRefEmbeddedRDSGlobal selects the embedded AWS RDS global CA
	// bundle. Each connection layer vendors its own snapshot of the
	// AWS-published artifact, so the trusted roots can drift between them
	// until a dependency bump converges the copies.
	caRefEmbeddedRDSGlobal = "embedded:rds-global"
	// caRefFilePrefix selects a PEM bundle at an absolute path on the
	// local filesystem, spelled file:<absolute-path>.
	caRefFilePrefix = "file:"
)

// caCertPath resolves the credentials' CA reference to the bundle path
// pg-sprite's pool trusts. An absent reference and the embedded RDS bundle
// both resolve to no path: pg-sprite auto-verifies RDS/Aurora endpoints with
// its embedded bundle when no path is set, and non-RDS targets keep whatever
// trust their DSN asked for. A reference the engine cannot honor is refused —
// an unrecognized CA must never silently downgrade to a different trust root.
// A file reference must be absolute: a relative path would resolve against
// the server's working directory and could name an unintended file. A file
// reference also requires a DSN that verifies the server certificate: under
// any other sslmode the validation connection would never consult the pinned
// roots, so a trust requirement the engine cannot enforce refuses up front
// rather than silently degrading.
func caCertPath(creds *engine.Credentials) (string, error) {
	ref := creds.Metadata[metadataCARef]
	switch {
	case ref == "" || ref == caRefEmbeddedRDSGlobal:
		return "", nil
	case strings.HasPrefix(ref, caRefFilePrefix):
		path := strings.TrimPrefix(ref, caRefFilePrefix)
		if path == "" {
			return "", fmt.Errorf("resolve PostgreSQL CA reference %q: file reference names no path", ref)
		}
		if !filepath.IsAbs(path) {
			return "", fmt.Errorf("resolve PostgreSQL CA reference %q: file reference requires an absolute path", ref)
		}
		verifies, err := postgresconn.VerifiesServerCertificate(creds.DSN)
		if err != nil {
			return "", fmt.Errorf("resolve PostgreSQL CA reference %q: %w", ref, err)
		}
		if !verifies {
			return "", fmt.Errorf("resolve PostgreSQL CA reference %q: DSN sslmode does not verify the server certificate, so the pinned bundle would never be consulted; use sslmode=verify-full or verify-ca", ref)
		}
		return path, nil
	default:
		return "", fmt.Errorf("resolve PostgreSQL CA reference: unsupported reference %q", ref)
	}
}

// spritePoolConfig builds the pg-sprite pool configuration the plan, apply,
// and pull dial sites share: the normalized DSN plus the CA bundle path the pool
// verifies the target against — empty when the embedded RDS trust or the
// DSN's own settings apply. Routing every pool through one constructor keeps
// the dial sites from drifting apart in what they trust.
func spritePoolConfig(dsn, caPath string) (dbconn.Config, error) {
	normalized, err := postgresconn.ConnectionDSN(dsn)
	if err != nil {
		return dbconn.Config{}, fmt.Errorf("normalize PostgreSQL DSN for pg-sprite pool: %w", err)
	}
	return dbconn.Config{URL: normalized, CACertPath: caPath}, nil
}

// validationRootCAs builds the postgresconn options that pin the validation
// connection to the bundle a file: reference names — the same path handed to
// the pg-sprite pool, so the two connection paths cannot verify against
// different roots. With no bundle path there is nothing to pin: the
// validation connection keeps the trust its DSN and the connection layer
// provide, and the pg-sprite pool applies its own RDS auto-trust. A bundle
// that cannot be read or parsed is refused here, before any dial.
func validationRootCAs(caPath string) ([]postgresconn.Option, error) {
	if caPath == "" {
		return nil, nil
	}
	roots, err := loadCABundle(caPath)
	if err != nil {
		return nil, err
	}
	return []postgresconn.Option{postgresconn.WithRootCAs(roots)}, nil
}

// loadCABundle reads and parses the PEM bundle at caPath, failing closed on a
// bundle that cannot be read or holds no usable certificates — a connection
// must never proceed with less trust than the reference named.
func loadCABundle(caPath string) (*x509.CertPool, error) {
	pem, err := os.ReadFile(caPath)
	if err != nil {
		return nil, fmt.Errorf("read PostgreSQL CA bundle %q: %w", caPath, err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("PostgreSQL CA bundle %q contains no usable certificates", caPath)
	}
	return roots, nil
}
