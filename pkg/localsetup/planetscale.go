package localsetup

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	ps "github.com/planetscale/planetscale-go/planetscale"

	"github.com/block/schemabot/pkg/psclient"
	"github.com/block/schemabot/pkg/schema"
)

// Target is the database setup reads before anything is registered. DSN is the
// application connection; the PlanetScale fields address a Vitess database,
// whose keyspaces and deploy requests live behind the PlanetScale API rather
// than the vtgate connection.
type Target struct {
	Engine string
	DSN    string

	// Database is the PlanetScale database name. Organization owns it. Token is
	// the raw service token in name:value form, and APIURL overrides the public
	// PlanetScale endpoint for compatible private or emulated APIs.
	Database     string
	Organization string
	Token        string
	APIURL       string
}

// Every Vitess database has a main branch; setup reads production schema from it.
const planetScaleMainBranch = "main"

// CheckPlanetScale verifies the organization, database name, and token with a
// read-only keyspace listing. It creates nothing.
func CheckPlanetScale(ctx context.Context, target Target) error {
	_, err := listPlanetScaleKeyspaces(ctx, target)
	return err
}

func listPlanetScaleKeyspaces(ctx context.Context, target Target) ([]string, error) {
	if strings.TrimSpace(target.Organization) == "" {
		return nil, fmt.Errorf("enter the PlanetScale organization before checking the API")
	}
	if strings.TrimSpace(target.Database) == "" {
		return nil, fmt.Errorf("name the database before checking the PlanetScale API")
	}
	name, value, err := parsePlanetScaleToken(target.Token)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	client, err := psclient.NewPSClientWithBaseURL(name, value, target.APIURL)
	if err != nil {
		slog.DebugContext(ctx, "create setup PlanetScale client failed", "organization", target.Organization, "database", target.Database, "error", err)
		return nil, &setupConnectionError{message: "we couldn’t prepare the PlanetScale client; check the API address", cause: err}
	}
	keyspaces, err := client.ListKeyspaces(ctx, &ps.ListKeyspacesRequest{Organization: target.Organization, Database: target.Database, Branch: planetScaleMainBranch})
	if err != nil {
		slog.DebugContext(ctx, "list setup keyspaces failed", "organization", target.Organization, "database", target.Database, "error", err)
		return nil, &setupConnectionError{message: "we couldn’t reach that database on PlanetScale; check the organization, database name, and token, then try again", cause: err}
	}
	names := make([]string, 0, len(keyspaces))
	for _, keyspace := range keyspaces {
		if keyspace == nil {
			slog.WarnContext(ctx, "skipping nil keyspace from PlanetScale", "organization", target.Organization, "database", target.Database)
			continue
		}
		if keyspace.Name == "" {
			return nil, fmt.Errorf("PlanetScale returned a keyspace with no name for database %s", target.Database)
		}
		if !schema.IsReservedPullNamespaceForDialect(schema.DialectMySQL, keyspace.Name) {
			names = append(names, keyspace.Name)
		}
	}
	return names, nil
}

// A PlanetScale service token is stored as name:value in one variable, the
// same shape the server reads from token_secret_ref.
func parsePlanetScaleToken(raw string) (string, string, error) {
	name, value, ok := strings.Cut(strings.TrimSpace(raw), ":")
	if !ok || strings.TrimSpace(name) == "" || strings.TrimSpace(value) == "" {
		return "", "", fmt.Errorf("your PlanetScale token variable needs the name:value format of a service token")
	}
	return strings.TrimSpace(name), strings.TrimSpace(value), nil
}
