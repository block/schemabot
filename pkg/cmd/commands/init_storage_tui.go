package commands

import (
	"strings"

	"github.com/charmbracelet/lipgloss"
)

func (m *initWizard) storageChoiceView() string {
	bold := m.renderer.NewStyle().Bold(true)
	muted := m.renderer.NewStyle().Foreground(lipgloss.AdaptiveColor{Light: "#59636E", Dark: "#9DA7B3"})
	blue := m.renderer.NewStyle().Foreground(lipgloss.AdaptiveColor{Light: "#0969DA", Dark: "#79C0FF"})
	var b strings.Builder
	if m.applicationConnected {
		green := m.renderer.NewStyle().Foreground(lipgloss.AdaptiveColor{Light: "#1A7F37", Dark: "#3FB950"})
		b.WriteString(green.Render("✓ Application database connected") + "\n\n")
	}
	b.WriteString(bold.Render("Where should SchemaBot store its own data?") + "\n\n")
	for _, choice := range []struct {
		label, hint string
		integrated  bool
	}{
		{"Integrated", "A simple setup for a single database project.", true},
		{"Standalone", "A good fit for teams managing multiple databases.", false},
	} {
		label := "  " + choice.label
		if choice.integrated == m.integrated {
			label = blue.Render("› " + choice.label)
		}
		b.WriteString(label + "\n  " + muted.Render(choice.hint) + "\n\n")
	}
	engine := "MySQL"
	if m.fields[0].value == "postgres" {
		engine = "PostgreSQL"
	}
	if m.integrated {
		b.WriteString("Your " + engine + " server\n├── Your application database\n│   └── Your application’s tables\n└── schemabot (new database)\n    └── SchemaBot’s own tables\n\n")
	} else {
		b.WriteString("Your application’s server\n└── Your application database\n    └── Your application’s tables\n\nSeparate server\n└── SchemaBot’s database\n    └── SchemaBot’s own tables\n\n")
		b.WriteString("Next, connect an existing database for SchemaBot’s own data.\n\n")
	}
	b.WriteString(muted.Render("↑/↓ choose · enter continue · shift+tab back · esc cancel"))
	return b.String()
}
