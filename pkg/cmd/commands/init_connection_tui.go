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

// Show the token's name, never its value: the name identifies the service
// token in PlanetScale's console.
func initTokenSummary(ref string) string {
	ref = strings.TrimSpace(ref)
	if !initVariable.MatchString(ref) {
		return "Enter env:VARIABLE_NAME to find your PlanetScale service token."
	}
	raw := os.Getenv(strings.TrimPrefix(ref, "env:"))
	if raw == "" {
		return "This variable isn’t set yet. Set it to name:value before continuing setup."
	}
	name, _, ok := strings.Cut(raw, ":")
	if !ok || strings.TrimSpace(name) == "" {
		return "Token found; it should look like name:value. Check the variable."
	}
	return fmt.Sprintf("Token: %q", strings.TrimSpace(name))
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
	secret := os.Getenv(strings.TrimPrefix(strings.TrimSpace(m.input.Value()), "env:"))
	if m.step == stepAPIToken {
		target := m.planetScaleTarget(secret)
		checkAPI := m.checkAPI
		if checkAPI == nil {
			checkAPI = localsetup.CheckPlanetScale
		}
		return func() tea.Msg { defer cancel(); return initConnectionMsg{generation, checkAPI(ctx, target)} }
	}
	engine := m.engine()
	if m.step == stepStorageDSN {
		engine = initStorageDialect(engine)
	}
	check := m.check
	if check == nil {
		check = localsetup.CheckConnection
	}
	return func() tea.Msg { defer cancel(); return initConnectionMsg{generation, check(ctx, engine, secret)} }
}
