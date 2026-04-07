package commands

import (
	"fmt"
	"strings"

	"github.com/block/schemabot/pkg/cmd/client"
	"github.com/block/schemabot/pkg/cmd/templates"
)

// settingInfo holds metadata for a known setting.
type settingInfo struct {
	Description string
	Default     string
}

// Known settings with descriptions and defaults
var knownSettings = map[string]settingInfo{
	"spirit_debug_logs": {
		Description: "Enable verbose Spirit debug logs (replication events, etc.)",
		Default:     "false",
	},
	"require_review": {
		Description: "Require PR approval before schemabot apply (checks CODEOWNERS, falls back to any non-self approval)",
		Default:     "false",
	},
}

// SettingsCmd manages server settings (git-style get/set/list).
type SettingsCmd struct {
	Args []string `arg:"" optional:"" help:"[key] [value] - list all, get, or set a setting"`
}

// Run executes the settings command.
func (cmd *SettingsCmd) Run(g *Globals) error {
	ep, err := resolveEndpoint(g.Endpoint, g.Profile)
	if err != nil {
		return err
	}

	switch len(cmd.Args) {
	case 0:
		// List all settings
		return listSettings(ep)
	case 1:
		// Get a setting
		return getSetting(ep, cmd.Args[0])
	default:
		// Set a setting
		key := cmd.Args[0]
		value := strings.Join(cmd.Args[1:], " ")
		return setSetting(ep, key, value)
	}
}

// validateSettingKey checks if a key is a known setting.
// Supports scoped keys like "require_review:owner/repo".
func validateSettingKey(key string) error {
	baseKey := key
	if before, _, ok := strings.Cut(key, ":"); ok {
		baseKey = before
	}
	if _, ok := knownSettings[baseKey]; !ok {
		var keys []string
		for k := range knownSettings {
			keys = append(keys, k)
		}
		return fmt.Errorf("unknown setting: %s (valid: %s)", key, strings.Join(keys, ", "))
	}
	return nil
}

func listSettings(endpoint string) error {
	settings, err := client.ListSettings(endpoint)
	if err != nil {
		return err
	}

	// Build map of current values
	currentValues := make(map[string]string)
	for _, s := range settings {
		currentValues[s.Key] = s.Value
	}

	fmt.Printf("%sSettings%s\n\n", templates.ANSIBold, templates.ANSIReset)

	// Show all known settings with their current values
	for key, info := range knownSettings {
		value := currentValues[key]
		if value == "" {
			value = fmt.Sprintf("%s%s (default)%s", templates.ANSIDim, info.Default, templates.ANSIReset)
		}
		fmt.Printf("  %s = %s\n", key, value)
		fmt.Printf("    %s%s%s\n\n", templates.ANSIDim, info.Description, templates.ANSIReset)
	}

	return nil
}

func getSetting(endpoint, key string) error {
	value, err := client.GetSetting(endpoint, key)
	if err != nil {
		return err
	}

	if value == "" {
		fmt.Printf("%s: (not set)\n", key)
	} else {
		fmt.Printf("%s = %s\n", key, value)
	}
	return nil
}

func setSetting(endpoint, key, value string) error {
	if err := validateSettingKey(key); err != nil {
		return err
	}

	if err := client.SetSetting(endpoint, key, value); err != nil {
		return err
	}

	fmt.Printf("%s%s set to %s%s\n", templates.ANSIGreen, key, value, templates.ANSIReset)
	return nil
}
