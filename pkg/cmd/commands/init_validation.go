package commands

import (
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"unicode"
)

func initNamespaceInput(value string, original []string) []string {
	if value == strings.Join(original, ", ") {
		return slices.Clone(original)
	}
	names := strings.Split(value, ",")
	for i := range names {
		names[i] = strings.TrimSpace(names[i])
	}
	return names
}

func initTerminalText(value string) string {
	for _, r := range value {
		if unicode.IsControl(r) {
			return strconv.Quote(value)
		}
	}
	return value
}

func initSchemaReuse(path string) (bool, error) {
	entries, err := os.ReadDir(path)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("cannot read schema directory %q: %w", path, err)
	}
	if len(entries) == 0 {
		return false, nil
	}
	if _, err := LoadCLIConfig(path); err != nil {
		return false, fmt.Errorf("choose an empty folder or a directory with a valid schemabot.yaml: %w", err)
	}
	return true, nil
}
