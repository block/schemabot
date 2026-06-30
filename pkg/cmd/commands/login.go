package commands

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"time"

	"github.com/block/schemabot/pkg/cmd/client"
)

// loginTimeout bounds the whole interactive login, including the wait for the
// user to authenticate in the browser.
const loginTimeout = 5 * time.Minute

// defaultRedirectPort is the loopback port used for the OIDC redirect when the
// profile and flags do not set one. It must match a redirect URI registered for
// the CLI public client, so an operator can override it per profile.
const defaultRedirectPort = 8765

// LoginCmd logs in via OIDC (browser auth-code + PKCE) and caches the resulting
// token on the active profile, so subsequent commands authenticate to an
// auth-enabled server without a manually supplied --token.
type LoginCmd struct {
	Issuer       string `help:"OIDC issuer URL (overrides the profile's oidc.issuer)"`
	ClientID     string `name:"client-id" help:"OIDC public client ID for the CLI (overrides the profile's oidc.client_id)"`
	RedirectPort int    `name:"redirect-port" help:"Loopback port for the OIDC redirect URI (overrides the profile's oidc.redirect_port)"`
	NoBrowser    bool   `name:"no-browser" help:"Print the login URL instead of opening a browser (for headless or remote sessions)"`

	// Test seams: nil in production, where they default to the real
	// implementations below.
	loginFn     func(context.Context, client.LoginConfig, client.BrowserOpener) (*client.LoginResult, error)
	openBrowser client.BrowserOpener
}

// Run executes the login command.
func (cmd *LoginCmd) Run(g *Globals) error {
	cfg, err := client.LoadConfig()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	// Log in against an already-configured profile: the token is bound to the
	// profile's endpoint, so refuse to cache one on a missing or endpoint-less
	// profile (which would otherwise create a dangling, unusable entry).
	profileName := client.ResolveProfileName(cfg, g.Profile)
	profile, ok := cfg.Profiles[profileName]
	if !ok {
		return fmt.Errorf("profile %q is not configured; run `schemabot configure` to set its endpoint before logging in", profileName)
	}
	if profile.Endpoint == "" {
		return fmt.Errorf("profile %q has no endpoint; run `schemabot configure` to set it before logging in", profileName)
	}

	loginCfg, err := resolveLoginConfig(cmd.Issuer, cmd.ClientID, cmd.RedirectPort, &profile)
	if err != nil {
		return err
	}

	open := cmd.openBrowser
	if open == nil {
		open = launchBrowser
		if cmd.NoBrowser {
			open = printAuthURL
		}
	}
	loginFn := cmd.loginFn
	if loginFn == nil {
		loginFn = client.Login
	}

	ctx, cancel := context.WithTimeout(context.Background(), loginTimeout)
	defer cancel()

	result, err := loginFn(ctx, loginCfg, open)
	if err != nil {
		return fmt.Errorf("log in to %s: %w", loginCfg.Issuer, err)
	}

	// Cache the ID token: it carries the user identity and groups the server's
	// OIDC authorizer validates, and its aud is the CLI client ID the server is
	// configured to accept. The refresh token and expiry let the CLI renew it
	// without another browser login.
	profile.Token = result.IDToken
	profile.RefreshToken = result.RefreshToken
	// Clear expiry when the provider omits one so a stale value can't make the
	// new token look immediately expired to the refresh path.
	profile.TokenExpiry = 0
	if !result.Expiry.IsZero() {
		profile.TokenExpiry = result.Expiry.Unix()
	}
	cfg.Profiles[profileName] = profile
	if err := client.SaveConfig(cfg); err != nil {
		return fmt.Errorf("save token to profile %q: %w", profileName, err)
	}

	if who := userFromIDToken(result.IDToken); who != "" {
		fmt.Printf("Logged in as %s. Token cached for profile %q.\n", who, profileName)
	} else {
		fmt.Printf("Logged in. Token cached for profile %q.\n", profileName)
	}
	return nil
}

// resolveLoginConfig merges login settings from flags (highest precedence) and
// the profile's oidc block, applying the default redirect port, and fails when a
// required value is missing.
func resolveLoginConfig(flagIssuer, flagClientID string, flagRedirectPort int, profile *client.Profile) (client.LoginConfig, error) {
	var oidc client.OIDCLogin
	if profile != nil && profile.OIDC != nil {
		oidc = *profile.OIDC
	}

	issuer := flagIssuer
	if issuer == "" {
		issuer = oidc.Issuer
	}
	clientID := flagClientID
	if clientID == "" {
		clientID = oidc.ClientID
	}
	port := flagRedirectPort
	if port == 0 {
		port = oidc.RedirectPort
	}
	if port == 0 {
		port = defaultRedirectPort
	}

	var missing []string
	if issuer == "" {
		missing = append(missing, "issuer")
	}
	if clientID == "" {
		missing = append(missing, "client ID")
	}
	if len(missing) > 0 {
		return client.LoginConfig{}, fmt.Errorf(
			"OIDC %s not configured for this profile; set it under `oidc:` in the profile or pass --issuer/--client-id",
			strings.Join(missing, " and "))
	}

	return client.LoginConfig{
		Issuer:       issuer,
		ClientID:     clientID,
		RedirectPort: port,
	}, nil
}

// printAuthURL prints the authorization URL for the user to open manually. It is
// the --no-browser opener and the fallback the launcher reuses, so the user can
// always complete login by pasting the URL (e.g. on a headless or remote host).
func printAuthURL(url string) error {
	fmt.Fprintf(os.Stderr, "Open this URL in your browser to log in:\n\n  %s\n\n", url)
	return nil
}

// launchBrowser prints the authorization URL and opens it in the system browser.
func launchBrowser(url string) error {
	if err := printAuthURL(url); err != nil {
		return err
	}

	var name string
	var args []string
	switch runtime.GOOS {
	case "darwin":
		name, args = "open", []string{url}
	case "windows":
		name, args = "rundll32", []string{"url.dll,FileProtocolHandler", url}
	default:
		name, args = "xdg-open", []string{url}
	}
	// Background, not the login context: the launcher hands off to the browser
	// and exits, so the launch must not be killed when the login deadline fires.
	launcher := exec.CommandContext(context.Background(), name, args...)
	if err := launcher.Start(); err != nil {
		return fmt.Errorf("launch browser via %s: %w", name, err)
	}
	// The launcher exits as soon as it dispatches to the browser; we don't need
	// its result, so release the process handle rather than waiting on it.
	if err := launcher.Process.Release(); err != nil {
		return fmt.Errorf("release browser launcher process: %w", err)
	}
	return nil
}

// userFromIDToken extracts a human-readable identifier from the ID token for the
// "logged in as" confirmation. It is best-effort and display-only — the server
// verifies the token — so an unparseable token yields an empty string and the
// caller simply omits the name.
func userFromIDToken(idToken string) string {
	parts := strings.Split(idToken, ".")
	if len(parts) < 2 {
		return ""
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return ""
	}
	var claims struct {
		Email string `json:"email"`
		Name  string `json:"name"`
		Sub   string `json:"sub"`
	}
	if err := json.Unmarshal(payload, &claims); err != nil {
		return ""
	}
	switch {
	case claims.Email != "":
		return claims.Email
	case claims.Name != "":
		return claims.Name
	default:
		return claims.Sub
	}
}
