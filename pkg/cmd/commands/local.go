package commands

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"

	"github.com/block/spirit/pkg/utils"
	"gopkg.in/yaml.v3"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/localruntime"
	"github.com/block/schemabot/pkg/serve"
)

// LocalCmd contains local runtime operations.
type LocalCmd struct {
	Managed LocalManagedCmd `cmd:"" hidden:"" help:"Internal supervised runtime child"`
	Status  LocalStatusCmd  `cmd:"" help:"Inspect local runtime without starting it"`
	Stop    LocalStopCmd    `cmd:"" help:"Drain and stop local runtime"`
	Serve   LocalServeCmd   `cmd:"" help:"Run an authenticated local runtime in the foreground"`
}

// LocalServeCmd runs with explicit configuration and a private bearer token.
type LocalServeCmd struct {
	Config    string `required:"" type:"path" help:"Local runtime YAML configuration"`
	TokenFile string `required:"" type:"path" help:"Private file containing a randomly generated bearer token"`
	Listen    string `default:"127.0.0.1:0" help:"Numeric loopback address and port (0 allocates a port)"`
}

// Run leaves stdout for the ready record and writes diagnostic logs to stderr.
func (cmd *LocalServeCmd) Run(ctx context.Context, g *Globals) error {
	data, err := readPrivateLocalFile(cmd.Config)
	if err != nil {
		return fmt.Errorf("read local configuration: %w", err)
	}

	cfg, err := parseLocalConfig(data)
	if err != nil {
		return err
	}
	token, err := readPrivateLocalFile(cmd.TokenFile)
	if err != nil {
		return fmt.Errorf("read local token: %w", err)
	}
	return runLocalServer(ctx, cfg, strings.TrimSpace(string(token)), cmd.Listen, g, func(endpoint string) error {
		return json.NewEncoder(os.Stdout).Encode(struct {
			State    string `json:"state"`
			Endpoint string `json:"endpoint"`
		}{State: "ready", Endpoint: endpoint})
	})
}

// Check the same descriptor we read so permission validation cannot race a
// replacement between stat and open. Neither configuration nor tokens are logged.
func readPrivateLocalFile(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	defer utils.CloseAndLog(f)
	info, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("inspect %s: %w", path, err)
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("%s must be a regular file", path)
	}
	if info.Mode().Perm()&0077 != 0 {
		return nil, fmt.Errorf("%s must be private; run chmod 600 on this file", path)
	}
	data, err := io.ReadAll(io.LimitReader(f, (1<<20)+1))
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	if len(data) > 1<<20 {
		return nil, fmt.Errorf("%s exceeds the 1 MiB file size limit", path)
	}
	return data, nil
}

func parseLocalConfig(data []byte) (api.ServerConfig, error) {
	var cfg api.ServerConfig
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil {
		return cfg, fmt.Errorf("parse local configuration: %w", err)
	}
	var extra any
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		return cfg, fmt.Errorf("local configuration must contain one YAML document")
	}
	return cfg, nil
}

func runLocalServer(ctx context.Context, cfg api.ServerConfig, token, address string, g *Globals, ready func(string) error) error {
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: logLevel()}))
	return serve.RunLocal(ctx, cfg, serve.LocalOptions{Address: address, Token: token, Ready: ready}, serve.WithLogger(logger), serve.WithBuildInfo(g.Version, g.Commit, g.Date))
}

type LocalManagedCmd struct {
	Directory  string `required:"" type:"path"`
	Generation string `required:""`
}

func (cmd *LocalManagedCmd) Run(ctx context.Context, g *Globals) error {
	return localruntime.Run(ctx, cmd.Directory, cmd.Generation, func(ctx context.Context, path, token string, ready func(string) error) error {
		data, err := localruntime.ReadPrivate(path)
		if err != nil {
			return err
		}
		cfg, err := parseLocalConfig(data)
		if err != nil {
			return err
		}
		return runLocalServer(ctx, cfg, token, "127.0.0.1:0", g, ready)
	})
}

type LocalStatusCmd struct {
	ID string `arg:"" help:"Runtime ID"`
}

func (cmd *LocalStatusCmd) Run(ctx context.Context) error {
	dir, err := localruntime.Directory(cmd.ID)
	if err != nil {
		return err
	}
	r, err := (localruntime.Manager{Dir: dir}).Status(ctx)
	if err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(r)
}

type LocalStopCmd struct {
	ID string `arg:"" help:"Runtime ID"`
}

func (cmd *LocalStopCmd) Run(ctx context.Context) error {
	dir, err := localruntime.Directory(cmd.ID)
	if err != nil {
		return err
	}
	if err := (localruntime.Manager{Dir: dir}).Stop(ctx); err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(struct {
		State string `json:"state"`
	}{State: "stopped"})
}
