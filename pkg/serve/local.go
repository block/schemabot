package serve

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/auth"
)

// validateLocalConfig keeps local hosting explicit while leaving engine,
// routing, and storage selection to the shared server configuration.
func validateLocalConfig(cfg *api.ServerConfig) error {
	if cfg.Storage.DSN == "" && cfg.Storage.DSNFrom == nil {
		return fmt.Errorf("local runtime requires explicit storage.dsn or storage.dsn_from")
	}
	if cfg.Storage.AllowDestructiveSchemaChanges {
		return fmt.Errorf("local runtime storage cannot enable destructive schema changes")
	}
	if cfg.Auth.Type != "" && cfg.Auth.Type != "none" {
		return fmt.Errorf("local runtime uses its private credential; service authentication must not be configured")
	}
	if cfg.GitHub.PrivateKey != "" || len(cfg.Apps) > 0 {
		return fmt.Errorf("local runtime must not configure GitHub Apps")
	}
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("validate local configuration: %w", err)
	}
	return cfg.ValidateStorageIsolation()
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
func RunLocal(ctx context.Context, config api.ServerConfig, local LocalOptions, opts ...Option) (runErr error) {
	if err := validateLocalConfig(&config); err != nil {
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
	srv, err := Build(ctx, &config, opts...)
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
