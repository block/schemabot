package commands

import (
	"context"
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

	profileName := client.ResolveProfileName(cfg, g.Profile)
	profile := cfg.Profiles[profileName]

	loginCfg, err := resolveLoginConfig(cmd.Issuer, cmd.ClientID, cmd.RedirectPort, &profile)
	if err != nil {
		return err
	}

	open := cmd.openBrowser
	if open == nil {
		open = openBrowser
	}
	loginFn := cmd.loginFn
	if loginFn == nil {
		loginFn = client.Login
	}

	ctx, cancel := context.WithTimeout(context.Background(), loginTimeout)
	defer cancel()

	fmt.Fprintf(os.Stderr, "Opening your browser to log in to %s…\n", loginCfg.Issuer)
	result, err := loginFn(ctx, loginCfg, open)
	if err != nil {
		return fmt.Errorf("log in to %s: %w", loginCfg.Issuer, err)
	}

	// Cache the ID token: it carries the user identity and groups the server's
	// OIDC authorizer validates, and its aud is the CLI client ID the server is
	// configured to accept.
	profile.Token = result.IDToken
	cfg.Profiles[profileName] = profile
	if cfg.DefaultProfile == "" {
		cfg.DefaultProfile = profileName
	}
	if err := client.SaveConfig(cfg); err != nil {
		return fmt.Errorf("save token to profile %q: %w", profileName, err)
	}

	fmt.Printf("Logged in. Token cached for profile %q.\n", profileName)
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

// openBrowser opens the system browser at the authorization URL and also prints
// it, so the user can complete login by pasting the URL if the browser does not
// launch (e.g. over SSH).
func openBrowser(url string) error {
	fmt.Fprintf(os.Stderr, "If your browser does not open, visit this URL to continue:\n  %s\n", url)

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
	// Background, not the login context: the launcher forks the browser and
	// returns immediately, so the launch must not be killed when the login flow's
	// deadline fires.
	if err := exec.CommandContext(context.Background(), name, args...).Start(); err != nil {
		return fmt.Errorf("launch browser via %s: %w", name, err)
	}
	return nil
}
