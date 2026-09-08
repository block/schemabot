package commands

import (
	"bufio"
	"fmt"
	"net/url"
	"os"
	"sort"
	"strings"

	"github.com/block/schemabot/pkg/cmd/client"
	"github.com/block/schemabot/pkg/cmd/cliname"
)

// ConfigureCmd configures CLI settings.
type ConfigureCmd struct {
	Setup ConfigureSetupCmd `cmd:"" default:"withargs" hidden:"" help:"Configure a profile interactively"`
	Show  ConfigureShowCmd  `cmd:"" help:"Show current configuration"`
}

// ConfigureSetupCmd is the default configure command (interactive profile setup).
type ConfigureSetupCmd struct{}

// Run executes the configure command (interactive profile setup).
func (cmd *ConfigureSetupCmd) Run(g *Globals) error {
	profileName := g.Profile
	if profileName == "" {
		profileName = "default"
	}

	// Load existing config
	cfg, err := client.LoadConfig()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	// Get existing profile values for defaults
	existingProfile := cfg.Profiles[profileName]

	reader := bufio.NewReader(os.Stdin)

	// Prompt for endpoint
	defaultEndpoint := existingProfile.Endpoint
	if defaultEndpoint == "" {
		defaultEndpoint = "http://localhost:13370"
	}
	fmt.Printf("SchemaBot endpoint [%s]: ", defaultEndpoint)
	endpoint, _ := reader.ReadString('\n')
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" {
		endpoint = defaultEndpoint
	}

	profile, loginCleared := reconfiguredProfile(existingProfile, endpoint)
	cfg.Profiles[profileName] = profile
	if loginCleared {
		fmt.Printf("\nEndpoint changed; the cached login for %s was cleared. Run `%s login` to sign in to the new endpoint.\n", existingProfile.Endpoint, cliname.Name())
	}

	// If this is the first profile or named "default", set as default
	if cfg.DefaultProfile == "" || profileName == "default" {
		cfg.DefaultProfile = profileName
	}

	// Save config
	if err := client.SaveConfig(cfg); err != nil {
		return fmt.Errorf("save config: %w", err)
	}

	configPath, _ := client.ConfigPath()
	fmt.Printf("\nProfile '%s' saved to %s\n", profileName, configPath)

	if cfg.DefaultProfile == profileName {
		fmt.Printf("\nThis is your default profile. You can now run:\n")
		fmt.Printf("  %s plan -s ./schema -e staging\n", cliname.Name())
	} else {
		fmt.Printf("\nTo use this profile:\n")
		fmt.Printf("  %s plan -s ./schema -e staging --profile %s\n", cliname.Name(), profileName)
		fmt.Printf("\nOr set as default:\n")
		fmt.Printf("  export SCHEMABOT_PROFILE=%s\n", profileName)
	}

	return nil
}

// reconfiguredProfile returns the profile to save once the operator has chosen
// an endpoint, and reports whether a cached login was dropped. Everything
// `login` wrote survives an unchanged endpoint. A changed endpoint drops the
// cached token, its refresh token, and its expiry: a token is bound to the
// server that issued it and must never be sent to a different one. The oidc
// settings are kept either way, since `login` can still use or override them.
func reconfiguredProfile(existing client.Profile, endpoint string) (profile client.Profile, loginCleared bool) {
	profile = existing
	profile.Endpoint = endpoint
	hasLogin := existing.Token != "" || existing.RefreshToken != ""
	if hasLogin && !sameEndpoint(existing.Endpoint, endpoint) {
		profile.Token = ""
		profile.RefreshToken = ""
		profile.TokenExpiry = 0
		loginCleared = true
	}
	return profile, loginCleared
}

// sameEndpoint reports whether two configured endpoints address the same
// server. It exists because the operator retypes the endpoint at the prompt,
// so the same server routinely arrives spelled differently — with or without a
// trailing slash, with the host in a different case — and an exact string
// compare would read those as a move and sign the operator out of a server
// they never left.
//
// It only collapses differences the URL grammar says are not part of the
// address: the scheme and host are case-insensitive, and a trailing slash is
// not a path segment. Everything else, including the port, still counts as a
// different server. An endpoint that will not parse falls back to exact
// equality, which errs toward clearing a login rather than carrying a token to
// a server that did not issue it.
func sameEndpoint(a, b string) bool {
	if a == b {
		return true
	}
	normalizedA, okA := normalizeEndpoint(a)
	normalizedB, okB := normalizeEndpoint(b)
	if !okA || !okB {
		return false
	}
	return normalizedA == normalizedB
}

// normalizeEndpoint rewrites an endpoint into the form sameEndpoint compares,
// reporting false when it cannot be parsed as a URL.
func normalizeEndpoint(raw string) (string, bool) {
	parsed, err := url.Parse(raw)
	if err != nil {
		return "", false
	}
	parsed.Scheme = strings.ToLower(parsed.Scheme)
	parsed.Host = strings.ToLower(parsed.Host)
	parsed.Path = strings.TrimRight(parsed.Path, "/")
	return parsed.String(), true
}

// ConfigureShowCmd displays the current configuration.
type ConfigureShowCmd struct{}

// Run executes the configure show command.
func (cmd *ConfigureShowCmd) Run(g *Globals) error {
	cfg, err := client.LoadConfig()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	configPath, _ := client.ConfigPath()

	// Determine active profile name
	activeProfileName := g.Profile
	activeSource := "flag"
	if activeProfileName == "" {
		activeProfileName = os.Getenv("SCHEMABOT_PROFILE")
		activeSource = "env"
	}
	if activeProfileName == "" {
		activeProfileName = cfg.DefaultProfile
		activeSource = "config"
	}
	if activeProfileName == "" {
		activeProfileName = "default"
		activeSource = "default"
	}

	fmt.Println("SchemaBot Configuration")
	fmt.Println()
	fmt.Printf("  Config file: %s\n", configPath)
	fmt.Println()

	// Show how profile was determined
	fmt.Printf("  Active profile: %s", activeProfileName)
	switch activeSource {
	case "flag":
		fmt.Printf(" (from --profile flag)\n")
	case "env":
		fmt.Printf(" (from SCHEMABOT_PROFILE env)\n")
	case "config":
		fmt.Printf(" (from config default_profile)\n")
	default:
		fmt.Printf(" (default)\n")
	}

	// Show endpoint resolution
	endpoint, _ := client.ResolveEndpointWithProfile("", g.Profile)
	if envEndpoint := os.Getenv("SCHEMABOT_ENDPOINT"); envEndpoint != "" {
		fmt.Printf("  Endpoint: %s (from SCHEMABOT_ENDPOINT env)\n", envEndpoint)
	} else if endpoint != "" {
		fmt.Printf("  Endpoint: %s (from profile)\n", endpoint)
	} else {
		fmt.Printf("  Endpoint: (not configured)\n")
	}

	// List all profiles
	fmt.Println()
	fmt.Println("  Profiles:")
	if len(cfg.Profiles) == 0 {
		fmt.Println("    (none configured)")
	} else {
		// Sort profile names for consistent output
		names := make([]string, 0, len(cfg.Profiles))
		for name := range cfg.Profiles {
			names = append(names, name)
		}
		sort.Strings(names)

		for _, name := range names {
			profile := cfg.Profiles[name]
			marker := "  "
			if name == activeProfileName {
				marker = "* "
			}
			fmt.Printf("    %s%s: %s\n", marker, name, profile.Endpoint)
		}
	}

	return nil
}
