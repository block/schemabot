package commands

import (
	"fmt"
	"slices"
	"strings"

	"github.com/block/schemabot/pkg/cmd/client"
	"github.com/block/schemabot/pkg/cmd/internal/templates"
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
}

// SettingsCmd manages server settings (git-style get/set/list).
type SettingsCmd struct {
	Args []string `arg:"" optional:"" help:"[key] [value] - list all, get, or set a setting"`
	JSON bool     `help:"Output as JSON"`
}

// settingJSON is one setting as `settings --json` reports it.
type settingJSON struct {
	Key   string `json:"key"`
	Value string `json:"value"`
	// Set is true when a non-empty value is stored for the key, the same
	// rule the text views use. When it is false, Value is the setting's
	// documented default.
	Set         bool   `json:"set"`
	Default     string `json:"default,omitempty"`
	Description string `json:"description,omitempty"`
}

// settingsListJSON is the `settings --json` document for the full list.
type settingsListJSON struct {
	Settings []settingJSON `json:"settings"`
}

// newSettingJSON describes key from its stored value. An empty value reads
// as unset on every path, list, get and set alike, as it does in the text
// views. An unknown key carries no default or description.
func newSettingJSON(key, stored string) settingJSON {
	info := knownSettings[key]
	out := settingJSON{Key: key, Value: stored, Set: stored != "", Default: info.Default, Description: info.Description}
	if !out.Set {
		out.Value = info.Default
	}
	return out
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
		return listSettings(ep, cmd.JSON)
	case 1:
		// Get a setting
		return getSetting(ep, cmd.Args[0], cmd.JSON)
	default:
		// Set a setting
		key := cmd.Args[0]
		value := strings.Join(cmd.Args[1:], " ")
		return setSetting(ep, key, value, cmd.JSON)
	}
}

// validateSettingKey checks if a key is a known setting.
func validateSettingKey(key string) error {
	if _, ok := knownSettings[key]; !ok {
		var keys []string
		for k := range knownSettings {
			keys = append(keys, k)
		}
		return fmt.Errorf("unknown setting: %s (valid: %s)", key, strings.Join(keys, ", "))
	}
	return nil
}

func listSettings(endpoint string, outputJSON bool) error {
	var settings []*client.Setting
	err := withLoading("Loading settings...", !outputJSON, func() error {
		var loadErr error
		settings, loadErr = client.ListSettings(endpoint)
		return loadErr
	})
	if err != nil {
		return err
	}

	// Build map of current values
	currentValues := make(map[string]string)
	for _, s := range settings {
		currentValues[s.Key] = s.Value
	}

	if outputJSON {
		keys := make([]string, 0, len(knownSettings))
		for key := range knownSettings {
			keys = append(keys, key)
		}
		slices.Sort(keys)
		doc := settingsListJSON{Settings: make([]settingJSON, 0, len(keys))}
		for _, key := range keys {
			doc.Settings = append(doc.Settings, newSettingJSON(key, currentValues[key]))
		}
		return writeJSON(doc)
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

func getSetting(endpoint, key string, outputJSON bool) error {
	var value string
	err := withLoading("Loading setting...", !outputJSON, func() error {
		var loadErr error
		value, loadErr = client.GetSetting(endpoint, key)
		return loadErr
	})
	if err != nil {
		return err
	}

	if outputJSON {
		return writeJSON(newSettingJSON(key, value))
	}

	if value == "" {
		fmt.Printf("%s: (not set)\n", key)
	} else {
		fmt.Printf("%s = %s\n", key, value)
	}
	return nil
}

func setSetting(endpoint, key, value string, outputJSON bool) error {
	if err := validateSettingKey(key); err != nil {
		return err
	}

	if err := withLoading("Updating setting...", !outputJSON, func() error {
		return client.SetSetting(endpoint, key, value)
	}); err != nil {
		return err
	}

	if outputJSON {
		return writeJSON(newSettingJSON(key, value))
	}

	fmt.Printf("%s%s set to %s%s\n", templates.ANSIGreen, key, value, templates.ANSIReset)
	return nil
}
