package commands

import (
	"context"
	"fmt"

	"github.com/block/schemabot/pkg/local"
)

// LocalCmd manages the local background server.
type LocalCmd struct {
	Status LocalStatusCmd `cmd:"" default:"withargs" help:"Show local server status."`
	Stop   LocalStopCmd   `cmd:"" help:"Stop the local server."`
	Reset  LocalResetCmd  `cmd:"" help:"Stop the server and drop the storage database."`
}

// LocalStatusCmd shows local server status.
type LocalStatusCmd struct{}

func (cmd *LocalStatusCmd) Run(g *Globals) error {
	ctx := context.Background()
	if local.IsRunning(ctx) {
		pid := local.ReadPID()
		fmt.Printf("Local server: running (PID %d, port %s)\n", pid, local.GetServerPort())
		fmt.Printf("Storage: %s\n", local.RedactedStorageDSN())
		fmt.Printf("Logs: ~/.schemabot/server.log\n")
	} else {
		fmt.Println("Local server: not running")
		fmt.Println("Run any command (e.g., schemabot plan) to auto-start.")
	}
	return nil
}

// LocalStopCmd stops the local server.
type LocalStopCmd struct{}

func (cmd *LocalStopCmd) Run(g *Globals) error {
	if err := local.Stop(); err != nil {
		return err
	}
	fmt.Println("Local server stopped.")
	return nil
}

// LocalResetCmd stops the server and drops the storage database.
type LocalResetCmd struct{}

func (cmd *LocalResetCmd) Run(g *Globals) error {
	// Stop server first
	_ = local.Stop()

	// Drop the storage database
	if err := local.DropStorage(); err != nil {
		return fmt.Errorf("drop storage: %w", err)
	}
	fmt.Printf("Local server stopped and %s database dropped.\n", local.StorageDatabase)
	return nil
}
