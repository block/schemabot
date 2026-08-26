package postgres

import (
	"crypto/x509"
	"fmt"
	"os"
	"path/filepath"
	"strings"

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
	// caRefEmbeddedRDSGlobal selects the embedded AWS RDS global bundle
	// that pg-sprite's pool and postgresconn both carry.
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
// the server's working directory and could name an unintended file.
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
		return path, nil
	default:
		return "", fmt.Errorf("resolve PostgreSQL CA reference: unsupported reference %q", ref)
	}
}

// validationRootCAs builds the postgresconn options that make the validation
// connection verify against the same CA bundle as the pg-sprite pool. With no
// bundle path there is nothing to pin: postgresconn already trusts the
// embedded RDS roots for RDS targets whose DSN requests verification. A
// bundle that cannot be read or parsed is refused here, before any dial.
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
