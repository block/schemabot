package client

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/block/spirit/pkg/utils"
	"gopkg.in/yaml.v3"
)

// Config represents the global SchemaBot CLI configuration.
type Config struct {
	DefaultProfile string             `yaml:"default_profile,omitempty"`
	Profiles       map[string]Profile `yaml:"profiles"`
}

// Profile represents a named configuration profile.
type Profile struct {
	Endpoint string `yaml:"endpoint"`
	// Token is the cached Bearer token for this profile's endpoint, written by
	// `schemabot login`. Scoping it to a profile keeps a token bound to the
	// server it was issued for, so it is never sent to a different endpoint.
	Token string `yaml:"token,omitempty"`
	// RefreshToken renews Token without another browser login. TokenExpiry is the
	// Unix time (seconds) Token expires; 0 means unknown (no proactive refresh).
	RefreshToken string `yaml:"refresh_token,omitempty"`
	TokenExpiry  int64  `yaml:"token_expiry,omitempty"`
	// OIDC holds the public-client settings `schemabot login` uses to run the
	// browser auth-code flow for this profile's server. Optional; absent until a
	// server has OIDC enabled.
	OIDC *OIDCLogin `yaml:"oidc,omitempty"`
}

// OIDCLogin holds the OIDC public-client settings for `schemabot login`. The
// values mirror the server's OIDC provider registration: the issuer URL, the
// CLI public client ID, and the loopback port of the redirect URI registered
// for that client.
type OIDCLogin struct {
	Issuer       string `yaml:"issuer"`
	ClientID     string `yaml:"client_id"`
	RedirectPort int    `yaml:"redirect_port,omitempty"`
}

// ConfigPath returns the path to the config file (~/.schemabot/config.yaml).
func ConfigPath() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("get home directory: %w", err)
	}
	return filepath.Join(home, ".schemabot", "config.yaml"), nil
}

// LoadConfig loads the global configuration from ~/.schemabot/config.yaml.
// Returns an empty config if the file doesn't exist.
func LoadConfig() (*Config, error) {
	path, err := ConfigPath()
	if err != nil {
		return nil, err
	}

	// Open once and read the permission bits from the same file descriptor we
	// read the token from, so the credential check below applies to the exact
	// file loaded — not a different file swapped in between a separate stat and
	// read.
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return &Config{Profiles: make(map[string]Profile)}, nil
		}
		return nil, fmt.Errorf("open config %s: %w", path, err)
	}
	defer utils.CloseAndLog(f)

	info, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat config %s: %w", path, err)
	}

	data, err := io.ReadAll(f)
	if err != nil {
		return nil, fmt.Errorf("read config %s: %w", path, err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config %s: %w", path, err)
	}

	if cfg.Profiles == nil {
		cfg.Profiles = make(map[string]Profile)
	}

	// A config that caches a token is a credential file: refuse to use it if it
	// is accessible by other users, so a leaked-permission token fails closed.
	if configHasToken(&cfg) {
		if err := requireSecureConfigMode(path, info.Mode()); err != nil {
			return nil, err
		}
	}

	return &cfg, nil
}

// configHasToken reports whether any profile carries a cached token.
func configHasToken(cfg *Config) bool {
	for _, p := range cfg.Profiles {
		if p.Token != "" || p.RefreshToken != "" {
			return true
		}
	}
	return false
}

// requireSecureConfigMode verifies the config file is not readable or writable
// by group or other users. The mode comes from an fstat on the open file the
// token was read from, so the check applies to that exact file. On Windows the
// reported mode is synthetic, so file ACLs are relied on instead and the check
// is skipped.
func requireSecureConfigMode(path string, mode os.FileMode) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	if perm := mode.Perm(); perm&0o077 != 0 {
		return fmt.Errorf("config file %s caches a token but is accessible by other users (mode %#o); restrict it with: chmod 600 %s", path, perm, path)
	}
	return nil
}

// SaveConfig saves the configuration to ~/.schemabot/config.yaml.
func SaveConfig(cfg *Config) error {
	path, err := ConfigPath()
	if err != nil {
		return err
	}

	// Create directory if needed
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return fmt.Errorf("create config directory: %w", err)
	}

	data, err := yaml.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("marshal config: %w", err)
	}

	if err := os.WriteFile(path, data, 0600); err != nil {
		return fmt.Errorf("write config: %w", err)
	}

	return nil
}

// ResolveProfileName returns the active profile name using the standard
// precedence: the --profile flag, then SCHEMABOT_PROFILE, then the configured
// default profile, then "default". It does not check that the profile exists.
func ResolveProfileName(cfg *Config, profileFlag string) string {
	if profileFlag != "" {
		return profileFlag
	}
	if env := os.Getenv("SCHEMABOT_PROFILE"); env != "" {
		return env
	}
	if cfg.DefaultProfile != "" {
		return cfg.DefaultProfile
	}
	return "default"
}

// GetProfile loads and returns the resolved profile (see ResolveProfileName for
// the name precedence, which includes the final fallback to "default"). An
// explicitly requested profile (via --profile or SCHEMABOT_PROFILE) that does
// not exist is an error; an unrequested missing profile yields an empty Profile.
func GetProfile(profileFlag string) (*Profile, error) {
	cfg, err := LoadConfig()
	if err != nil {
		return nil, err
	}

	// Track whether the user explicitly requested a profile (flag or env), so we
	// can error if it doesn't exist instead of silently falling back.
	explicit := profileFlag != "" || os.Getenv("SCHEMABOT_PROFILE") != ""
	profileName := ResolveProfileName(cfg, profileFlag)

	profile, ok := cfg.Profiles[profileName]
	if !ok {
		if explicit {
			return nil, fmt.Errorf("unknown profile %q", profileName)
		}
		return &Profile{}, nil
	}

	return &profile, nil
}

// ResolveEndpointWithProfile returns the endpoint to use, checking in order:
// 1. Explicit --endpoint flag
// 2. SCHEMABOT_ENDPOINT environment variable
// 3. Profile endpoint (from --profile flag, SCHEMABOT_PROFILE env, or default profile)
// 4. schemabot.yaml endpoint (if provided)
func ResolveEndpointWithProfile(endpointFlag, profileFlag string, configEndpoint ...string) (string, error) {
	// 1. Explicit flag
	if endpointFlag != "" {
		return trimSlash(endpointFlag), nil
	}

	// 2. Environment variable
	if env := os.Getenv("SCHEMABOT_ENDPOINT"); env != "" {
		return trimSlash(env), nil
	}

	// 3. Profile
	profile, err := GetProfile(profileFlag)
	if err != nil {
		return "", err
	}
	if profile.Endpoint != "" {
		return trimSlash(profile.Endpoint), nil
	}

	// 4. Config file (schemabot.yaml)
	if len(configEndpoint) > 0 && configEndpoint[0] != "" {
		return trimSlash(configEndpoint[0]), nil
	}

	return "", nil
}

// ResolveToken returns the Bearer token to authenticate with, checking in order:
// 1. Explicit --token flag
// 2. SCHEMABOT_TOKEN environment variable
// 3. Cached token on the resolved profile (from `schemabot login`)
// An empty result means no token is configured, which is correct against a
// server with auth disabled.
//
// A cached profile token is bound to the endpoint it was issued for. The
// endpoint override (--endpoint flag or SCHEMABOT_ENDPOINT) is passed in so the
// profile token is attached only when the request is actually going to that
// profile's endpoint: an override pointing elsewhere must never receive a token
// issued for a different server. An explicit --token / SCHEMABOT_TOKEN always
// wins, since the caller is supplying a credential for whatever endpoint they
// chose.
func ResolveToken(tokenFlag, endpointFlag, profileFlag string) (string, error) {
	if tokenFlag != "" {
		return tokenFlag, nil
	}
	if env := os.Getenv("SCHEMABOT_TOKEN"); env != "" {
		return env, nil
	}
	profile, err := GetProfile(profileFlag)
	if err != nil {
		return "", err
	}
	return profileToken(profile, endpointFlag), nil
}

// profileToken returns the cached token to send to endpointFlag, or "" when no
// token is cached or an endpoint override points away from the profile's own
// endpoint — a token must never be sent to a server it was not issued for.
func profileToken(profile *Profile, endpointFlag string) string {
	if profile.Token == "" {
		return ""
	}
	endpointOverride := endpointFlag
	if endpointOverride == "" {
		endpointOverride = os.Getenv("SCHEMABOT_ENDPOINT")
	}
	if endpointOverride != "" && trimSlash(endpointOverride) != trimSlash(profile.Endpoint) {
		return ""
	}
	return profile.Token
}

// tokenRefreshSkew renews a token slightly before its real expiry so a request
// is not sent with a token that lapses mid-flight.
const tokenRefreshSkew = 60 * time.Second

// unixExpiry converts a token expiry to the stored Unix-seconds form, returning
// 0 ("unknown, do not proactively refresh") when the provider omits an expiry.
func unixExpiry(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.Unix()
}

// ResolveBearerToken resolves the Bearer token like ResolveToken, but renews an
// expired profile-cached token when a refresh token and OIDC settings are
// present, persisting the rotated token. Flag/env tokens are returned as-is.
//
// Error contract for the caller: a non-empty token with a non-nil error is a
// non-fatal warning (the returned token is the best available — e.g. a stale
// token when refresh failed — so the command can still run and the user can
// re-authenticate); an empty token with a non-nil error is a hard failure.
func ResolveBearerToken(ctx context.Context, tokenFlag, endpointFlag, profileFlag string) (string, error) {
	if tokenFlag != "" {
		return tokenFlag, nil
	}
	if env := os.Getenv("SCHEMABOT_TOKEN"); env != "" {
		return env, nil
	}

	cfg, err := LoadConfig()
	if err != nil {
		return "", err
	}
	profileName := ResolveProfileName(cfg, profileFlag)
	profile, ok := cfg.Profiles[profileName]
	if !ok {
		// Mirror GetProfile: an explicitly requested missing profile is an error;
		// an unrequested missing profile simply has no token.
		if profileFlag != "" || os.Getenv("SCHEMABOT_PROFILE") != "" {
			return "", fmt.Errorf("unknown profile %q", profileName)
		}
		return "", nil
	}

	token := profileToken(&profile, endpointFlag)
	if token == "" {
		return "", nil
	}

	// Refresh only when the expiry is known and (nearly) reached.
	if profile.TokenExpiry == 0 || time.Until(time.Unix(profile.TokenExpiry, 0)) > tokenRefreshSkew {
		return token, nil
	}
	if profile.RefreshToken == "" || profile.OIDC == nil {
		return token, fmt.Errorf("token for profile %q is expired or about to expire and cannot be refreshed; run `schemabot login`", profileName)
	}

	result, err := RefreshToken(ctx, LoginConfig{Issuer: profile.OIDC.Issuer, ClientID: profile.OIDC.ClientID}, profile.RefreshToken)
	if err != nil {
		return token, fmt.Errorf("could not refresh the token for profile %q (run `schemabot login`): %w", profileName, err)
	}

	profile.Token = result.IDToken
	if result.RefreshToken != "" {
		profile.RefreshToken = result.RefreshToken
	}
	// Always set expiry, clearing it when the provider omits one, so a stale
	// value can't make every subsequent command refresh immediately.
	profile.TokenExpiry = unixExpiry(result.Expiry)
	cfg.Profiles[profileName] = profile
	if err := SaveConfig(cfg); err != nil {
		// The refreshed token is usable for this run even if it could not be saved.
		return result.IDToken, fmt.Errorf("could not persist the refreshed token for profile %q: %w", profileName, err)
	}
	return result.IDToken, nil
}

func trimSlash(s string) string {
	if len(s) > 0 && s[len(s)-1] == '/' {
		return s[:len(s)-1]
	}
	return s
}
