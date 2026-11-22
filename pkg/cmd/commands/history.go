package commands

import "fmt"

// HistoryCmd lists every apply recorded for one database, newest first. It is
// the same list `status -d` prints, under the name operators reach for first,
// and it takes the database either as an argument or with -d.
type HistoryCmd struct {
	DatabaseArg string `arg:"" optional:"" name:"database" help:"Database to show the apply history of"`
	Database    string `short:"d" help:"Database to show the apply history of (same as the argument)"`
	Environment string `short:"e" help:"Only show applies in this environment"`
	JSON        bool   `help:"Output as JSON"`
}

// Run prints the database's apply history.
func (cmd *HistoryCmd) Run(g *Globals) error {
	database, err := cmd.database()
	if err != nil {
		return err
	}
	ep, err := resolveEndpoint(g.Endpoint, g.Profile)
	if err != nil {
		return err
	}
	return showDatabaseHistory(ep, database, cmd.Environment, cmd.JSON, !cmd.JSON)
}

// database resolves the database from the argument or -d, refusing a call
// that names none or two different ones.
func (cmd *HistoryCmd) database() (string, error) {
	switch {
	case cmd.DatabaseArg == "" && cmd.Database == "":
		return "", fmt.Errorf("name the database: history <database> -e <environment>")
	case cmd.DatabaseArg != "" && cmd.Database != "" && cmd.DatabaseArg != cmd.Database:
		return "", fmt.Errorf("the argument names database %q and -d names %q; give one", cmd.DatabaseArg, cmd.Database)
	case cmd.DatabaseArg != "":
		return cmd.DatabaseArg, nil
	default:
		return cmd.Database, nil
	}
}
