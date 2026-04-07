package commands

import (
	"bufio"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/block/schemabot/pkg/cmd/client"
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

	// Update profile
	cfg.Profiles[profileName] = client.Profile{
		Endpoint: endpoint,
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
		fmt.Printf("  schemabot plan -s ./schema -e staging\n")
	} else {
		fmt.Printf("\nTo use this profile:\n")
		fmt.Printf("  schemabot plan -s ./schema -e staging --profile %s\n", profileName)
		fmt.Printf("\nOr set as default:\n")
		fmt.Printf("  export SCHEMABOT_PROFILE=%s\n", profileName)
	}

	return nil
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
