package serve

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/auth"
	"github.com/block/schemabot/pkg/schema"
)

// LocalConfig is the explicit configuration for a local MySQL runtime.
// Keeping the surface limited prevents service listeners and GitHub integrations
// from being enabled accidentally by a copied server configuration.
type LocalConfig struct {
	Storage   api.StorageConfig             `yaml:"storage"`
	Databases map[string]api.DatabaseConfig `yaml:"databases"`
}

func (c LocalConfig) serverConfig() (*api.ServerConfig, error) {
	cfg := &api.ServerConfig{Storage: c.Storage, Databases: c.Databases}
	if c.Storage.DSN == "" && c.Storage.DSNFrom == nil {
		return nil, fmt.Errorf("local runtime requires explicit storage.dsn or storage.dsn_from")
	}
	if c.Storage.AllowDestructiveSchemaChanges {
		return nil, fmt.Errorf("local runtime storage cannot enable destructive schema changes")
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("validate local configuration: %w", err)
	}
	dialect, err := c.Storage.ResolveDialect()
	if err != nil {
		return nil, fmt.Errorf("resolve local storage dialect: %w", err)
	}
	if dialect != schema.DialectMySQL {
		return nil, fmt.Errorf("local runtime requires MySQL storage")
	}
	storageDSN, err := cfg.StorageDSN()
	if err != nil {
		return nil, fmt.Errorf("resolve local storage: %w", err)
	}
	storageName, err := localDatabaseName(storageDSN)
	if err != nil {
		return nil, fmt.Errorf("local storage: %w", err)
	}
	for name, db := range c.Databases {
		if db.Type != "mysql" {
			return nil, fmt.Errorf("local database %q must use MySQL", name)
		}
		for environment, target := range db.Environments {
			if !target.HasLocalDSN() {
				return nil, fmt.Errorf("local database %q environment %q requires a direct DSN", name, environment)
			}
			dsn, err := target.ResolveDSN()
			if err != nil {
				return nil, fmt.Errorf("resolve local database %q environment %q: %w", name, environment, err)
			}
			targetName, err := localDatabaseName(dsn)
			if err != nil {
				return nil, fmt.Errorf("local database %q environment %q: %w", name, environment, err)
			}
			// Require a distinct name even across hosts: aliases and tunnels cannot
			// turn an address comparison into proof that storage is a separate database.
			if strings.EqualFold(storageName, targetName) {
				return nil, fmt.Errorf("local storage database must have a different name from target %q", name)
			}
		}
	}
	return cfg, nil
}

func localDatabaseName(dsn string) (string, error) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return "", fmt.Errorf("invalid MySQL DSN")
	}
	if cfg.DBName == "" {
		return "", fmt.Errorf("MySQL DSN must name a database")
	}
	return cfg.DBName, nil
}

// LocalOptions controls the foreground host. Ready runs after storage and the
// operator are initialized; its endpoint contains the actual allocated port.
type LocalOptions struct {
	Address string
	Token   string
	Ready   func(endpoint string) error
}

// RunLocal hosts the normal API and operator on an authenticated loopback
// listener. Cancellation drains the HTTP server and existing operator shutdown
// path; it does not issue an operator stop or cancel any stored apply.
func RunLocal(ctx context.Context, config LocalConfig, local LocalOptions, opts ...Option) (runErr error) {
	cfg, err := config.serverConfig()
	if err != nil {
		return err
	}
	o := options{logger: slog.Default()}
	for _, opt := range opts {
		opt(&o)
	}
	authorizer, err := auth.NewLocalAuthorizer(local.Token, o.logger)
	if err != nil {
		return fmt.Errorf("configure local authentication: %w", err)
	}
	address := local.Address
	if address == "" {
		address = "127.0.0.1:0"
	}
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("local listen address: %w", err)
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return fmt.Errorf("local listen address must use a numeric loopback IP")
	}
	// Reserve the address before Build can bootstrap storage or Start can claim work.
	var lc net.ListenConfig
	listener, err := lc.Listen(ctx, "tcp", address)
	if err != nil {
		return fmt.Errorf("listen for local runtime: %w", err)
	}
	defer func() {
		if err := listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			runErr = errors.Join(runErr, err)
		}
	}()
	opts = append(opts, func(o *options) { o.authorizer = authorizer })
	srv, err := Build(ctx, cfg, opts...)
	if err != nil {
		return fmt.Errorf("build local runtime: %w", err)
	}
	// Only Close cancels the operator, after it begins draining claims. Passing
	// the command context to Start would let a signal end drives before their
	// claims are retained for orderly release.
	runtimeCtx, cancelRuntime := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelRuntime()
	defer func() { runErr = errors.Join(runErr, srv.Close()) }()
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("local startup canceled: %w", err)
	}
	server := &http.Server{
		Handler: srv.Handler(), ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout: 30 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second,
	}
	srv.Start(runtimeCtx)
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.Serve(listener) }()
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			runErr = errors.Join(runErr, fmt.Errorf("drain local HTTP server: %w", err), server.Close())
		}
	}()
	endpoint := "http://" + listener.Addr().String()
	o.logger.Info("local runtime ready", "endpoint", endpoint)
	if local.Ready != nil {
		if err := local.Ready(endpoint); err != nil {
			return fmt.Errorf("report local runtime readiness: %w", err)
		}
	}
	select {
	case <-ctx.Done():
		o.logger.Info("local runtime shutting down")
		return nil
	case err := <-serveErr:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("serve local runtime: %w", err)
	}
}
