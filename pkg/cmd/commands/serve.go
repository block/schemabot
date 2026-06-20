package commands

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/serve"
)

// ServeCmd starts the SchemaBot HTTP API server.
type ServeCmd struct{}

// Run loads the server configuration from the environment and starts the
// server. The command is a thin wrapper over serve.Run, which holds the server
// implementation so it can also be embedded by other processes.
func (cmd *ServeCmd) Run(g *Globals) error {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: logLevel(),
	})).With("schemabot_version", g.Version)
	slog.SetDefault(logger)

	// Load server configuration from YAML file
	serverConfig, err := api.LoadServerConfig()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	return serve.Run(context.Background(), serverConfig,
		serve.WithLogger(logger),
		serve.WithBuildInfo(g.Version, g.Commit, g.Date),
	)
}

func logLevel() slog.Level {
	switch os.Getenv("LOG_LEVEL") {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
