package api

import (
	"os"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// requiredGitHubAppPermissions is every permission an existing install
// already grants. Adding one here makes every deployment need it on upgrade;
// gate the behavior behind a server config key instead and mark the
// permission optional in the setup guide (AV-17).
var requiredGitHubAppPermissions = []string{
	"Checks",
	"Commit statuses",
	"Contents",
	"Issues",
	"Members",
	"Merge queues",
	"Metadata",
	"Pull requests",
}

var (
	permissionRow   = regexp.MustCompile(`^\| \*\*([^*]+)\*\* \| [^|]+ \| (.*) \|$`)
	backtickedValue = regexp.MustCompile("`([a-z0-9_.-]+)`")
)

// Every permission the GitHub App setup guide lists is either one existing
// installs already grant or is optional and names the server config key that
// turns on the behavior needing it, so an upgrade never needs a new grant.
func TestGitHubAppPermissionsAreRequiredOrGatedByConfig(t *testing.T) {
	data, err := os.ReadFile("../../docs/github-app-setup.md")
	require.NoError(t, err)
	keys := serverConfigYAMLKeys(reflect.TypeFor[ServerConfig]())

	var listed []string
	for line := range strings.SplitSeq(string(data), "\n") {
		match := permissionRow.FindStringSubmatch(line)
		if match == nil {
			continue
		}
		permission, usedFor := match[1], match[2]
		listed = append(listed, permission)
		if !strings.HasPrefix(usedFor, "Optional.") {
			assert.Contains(t, requiredGitHubAppPermissions, permission,
				"%s is not optional, so every install would need it on upgrade; mark it optional and name the config key that turns it on", permission)
			continue
		}
		var gate string
		for _, value := range backtickedValue.FindAllStringSubmatch(usedFor, -1) {
			if keys[value[1]] {
				gate = value[1]
				break
			}
		}
		assert.NotEmpty(t, gate, "optional permission %s must name the server config key that turns on the behavior needing it", permission)
	}
	assert.Subset(t, listed, requiredGitHubAppPermissions, "the setup guide lists every required permission")
	assert.Contains(t, listed, "Administration")
}

// serverConfigYAMLKeys collects every YAML key in the server config, nested
// blocks included.
func serverConfigYAMLKeys(typ reflect.Type) map[string]bool {
	keys := map[string]bool{}
	var walk func(reflect.Type)
	seen := map[reflect.Type]bool{}
	walk = func(typ reflect.Type) {
		for typ.Kind() == reflect.Pointer || typ.Kind() == reflect.Slice || typ.Kind() == reflect.Map {
			typ = typ.Elem()
		}
		if typ.Kind() != reflect.Struct || seen[typ] {
			return
		}
		seen[typ] = true
		for field := range typ.Fields() {
			name, _, _ := strings.Cut(field.Tag.Get("yaml"), ",")
			if name != "" && name != "-" {
				keys[name] = true
			}
			walk(field.Type)
		}
	}
	walk(typ)
	return keys
}
