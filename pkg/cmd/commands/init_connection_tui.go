package commands

import (
	"context"
	"fmt"

	"net"
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
	dsn, err := resolveInitConnection(strings.TrimSpace(ref))
	if err != nil {
		return "Choose a connection source to continue."
	}
	return initConnectionDestination(engine, dsn)
}

func initConnectionDestination(engine, dsn string) string {
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
	if m.cancelDiscovery != nil {
		m.cancelDiscovery()
	}
	ctx, cancel := context.WithCancel(ctx)
	m.cancelDiscovery = cancel
	engine := m.fields[0].value
	dsn, err := m.resolveConnection(strings.TrimSpace(m.input.Value()))
	if err != nil {
		return func() tea.Msg { return initConnectionMsg{generation: generation, err: err} }
	}
	check := m.check
	if check == nil {
		check = localsetup.CheckConnection
	}
	return func() tea.Msg { defer cancel(); return initConnectionMsg{generation, check(ctx, engine, dsn)} }
}
