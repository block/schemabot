package client

import (
	"context"
	"fmt"
	"os"

	"github.com/block/schemabot/pkg/localruntime"
)

// ResolveLocalConnection is called only for API commands. Explicit remote
// endpoint overrides retain precedence and never start a local process.
func ResolveLocalConnection(ctx context.Context, endpoint, profileFlag, token, version string) (*localruntime.Connection, error) {
	if endpoint != "" || os.Getenv("SCHEMABOT_ENDPOINT") != "" {
		return nil, nil
	}
	profile, err := GetProfile(profileFlag)
	if err != nil {
		return nil, err
	}
	if profile.LocalRuntime == "" {
		return nil, nil
	}
	if profile.Endpoint != "" || profile.Token != "" || profile.RefreshToken != "" || profile.OIDC != nil || token != "" || os.Getenv("SCHEMABOT_TOKEN") != "" {
		return nil, fmt.Errorf("local runtime profile cannot be combined with endpoint or authentication overrides; select a remote endpoint explicitly instead")
	}
	dir, err := localruntime.Directory(profile.LocalRuntime)
	if err != nil {
		return nil, err
	}
	binary, err := os.Executable()
	if err != nil {
		return nil, err
	}
	connection, err := (localruntime.Manager{Dir: dir, Binary: binary, Version: version}).Ensure(ctx)
	if err != nil {
		return nil, err
	}
	return &connection, nil
}
