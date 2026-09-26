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
	case "mysql", "vitess":
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

func initTokenSummary(ref string) string {
	raw, err := resolveInitConnection(ref)
	if err != nil {
		return "Choose an environment variable or file containing your PlanetScale service token."
	}
	name, value, ok := strings.Cut(raw, ":")
	if !ok || strings.TrimSpace(name) == "" || strings.TrimSpace(value) == "" {
		return "Use a PlanetScale service token in name:value format."
	}
	return fmt.Sprintf("Token: %q", strings.TrimSpace(name))
}

type initConnectionAdvanceMsg struct {
	generation, step int
}

type initConnectionMsg struct {
	generation int
	err        error
}

func (m *initWizard) checkConnection() tea.Cmd {
	m.err = ""
	if m.step == stepDSN {
		m.applicationConnected = false
	}
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
	engine := m.fields[stepEngine].value
	dsn, err := m.resolveConnection(strings.TrimSpace(m.input.Value()))
	if err != nil {
		return func() tea.Msg { return initConnectionMsg{generation: generation, err: err} }
	}
	if m.step == stepAPIToken {
		target := m.planetScaleTarget(dsn)
		return func() tea.Msg { defer cancel(); return initConnectionMsg{generation, m.checkAPI(ctx, target)} }
	}
	if m.step == stepStorageDSN {
		engine = initStorageDialect(engine)
	}
	check := m.check
	if check == nil {
		check = localsetup.CheckConnection
	}
	return func() tea.Msg { defer cancel(); return initConnectionMsg{generation, check(ctx, engine, dsn)} }
}
