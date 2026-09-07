package commands

import (
	"context"
	"fmt"

	"net"
	"os"
	"strconv"
	"strings"

	"github.com/block/schemabot/pkg/localsetup"
	tea "github.com/charmbracelet/bubbletea"

	"github.com/block/mysql"
	"github.com/jackc/pgx/v5"
)

// Show only destination fields, never a reconstructed DSN or parser error.
// Quoting also prevents database names from injecting terminal controls.
func initConnectionSummary(engine, ref string) string {
	ref = strings.TrimSpace(ref)
	if !initVariable.MatchString(ref) {
		return "Enter env:VARIABLE_NAME to find your connection."
	}
	dsn := os.Getenv(strings.TrimPrefix(ref, "env:"))
	if dsn == "" {
		return "This variable isn’t set yet. Set it before continuing setup."
	}
	var host, database string
	switch engine {
	case "mysql":
		cfg, err := mysql.ParseDSN(dsn)
		if err != nil {
			return "Connection found; couldn’t read its destination. Check the connection string."
		}
		host, database = cfg.Addr, cfg.DBName
	case "postgres":
		cfg, err := pgx.ParseConfig(dsn)
		if err != nil {
			return "Connection found; couldn’t read its destination. Check the connection string."
		}
		host, database = net.JoinHostPort(cfg.Host, strconv.Itoa(int(cfg.Port))), cfg.Database
	default:
		return "Connection variable found."
	}
	return fmt.Sprintf("Host: %q\nDatabase: %q", host, database)
}

type initConnectionMsg struct {
	generation int
	err        error
}

func (m *initWizard) checkConnection() tea.Cmd {
	m.checkingConnection = true
	m.connectionChecked = false
	m.generation++
	generation := m.generation
	ctx := m.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, m.cancelDiscovery = context.WithCancel(ctx)
	engine, dsn := m.fields[0].value, os.Getenv(strings.TrimPrefix(strings.TrimSpace(m.input.Value()), "env:"))
	check := m.check
	if check == nil {
		check = localsetup.CheckConnection
	}
	return func() tea.Msg { return initConnectionMsg{generation, check(ctx, engine, dsn)} }
}
