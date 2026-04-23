// Package templates provides markdown templates for GitHub PR comments.
package templates

// commandReference returns the compact command table used by help and error messages.
func commandReference() string {
	return `| Command | Description |
|---------|-------------|
| ` + "`schemabot plan [-e <env>]`" + ` | Preview schema changes |
| ` + "`schemabot apply -e <env>`" + ` | Plan, lock, and confirm deployment |
| ` + "`schemabot apply-confirm -e <env>`" + ` | Execute a locked plan |
| ` + "`schemabot unlock`" + ` | Release lock and discard plan |
| ` + "`schemabot stop <apply-id>`" + ` | Stop an in-progress deployment |
| ` + "`schemabot start <apply-id>`" + ` | Resume a stopped deployment |
| ` + "`schemabot cutover <apply-id>`" + ` | Complete a deferred cutover |
| ` + "`schemabot rollback <apply-id>`" + ` | Generate a rollback plan |
| ` + "`schemabot rollback-confirm <apply-id>`" + ` | Execute a rollback |

**Options**: ` + "`-e <env>`" + ` environment, ` + "`-d <db>`" + ` database, ` + "`--defer-cutover`" + `, ` + "`--allow-unsafe`" + `, ` + "`--skip-revert`" + ` (Vitess)

**Quick start**: ` + "`plan`" + ` → ` + "`apply`" + ` → ` + "`apply-confirm`" + `
`
}

// RenderHelpComment generates the help message listing all available commands.
func RenderHelpComment() string {
	return "## 📚 SchemaBot Help\n\n" + commandReference()
}
