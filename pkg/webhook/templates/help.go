// Package templates provides markdown templates for GitHub PR comments.
package templates

// commandReference returns the compact command table used by help and error messages.
func commandReference() string {
	return `| Command | Description |
|---------|-------------|
| ` + "`schemabot plan [-e <env>]`" + ` | Preview schema changes |
| ` + "`schemabot apply -e <env>`" + ` | Plan, lock, and apply after safety rechecks |
| ` + "`schemabot apply-confirm -e <env>`" + ` | Confirm a downgraded locked plan |
| ` + "`schemabot unlock`" + ` | Release lock and discard plan |
| ` + "`schemabot stop <apply-id> -e <env>`" + ` | Stop an in-progress deployment |
| ` + "`schemabot cancel <apply-id> -e <env>`" + ` | Permanently cancel an in-progress deployment |
| ` + "`schemabot start <apply-id> -e <env>`" + ` | Resume a stopped deployment |
| ` + "`schemabot release <apply-id> -e <env>`" + ` | Release a paused rollout to proceed |
| ` + "`schemabot cutover <apply-id> -e <env>`" + ` | Complete a deferred cutover |
| ` + "`schemabot rollback <apply-id> -e <env>`" + ` | Generate a rollback plan |
| ` + "`schemabot rollback-confirm -e <env>`" + ` | Execute a rollback |

**Options**: ` + "`-e <env>`" + ` environment, ` + "`-d <db>`" + ` database, ` + "`-t, --tenant <name>`" + ` deployment routing, ` + "`--defer-cutover`" + `, ` + "`--allow-unsafe`" + `, ` + "`--skip-revert`" + ` (Vitess)

**Quick start**: ` + "`plan`" + ` → ` + "`apply`" + `
`
}

// RenderHelpComment generates the help message listing all available commands.
func RenderHelpComment() string {
	return "## 📚 SchemaBot Help\n\n" + commandReference()
}
