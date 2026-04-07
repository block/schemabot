//go:build integration

// Package integration contains integration tests that exercise multiple components
// together — some use Docker containers (e.g. MySQL via testcontainers), others
// instantiate server structs and gRPC clients in-process.
package integration

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"log"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/storage/mysqlstore"
	"github.com/block/schemabot/pkg/tern"
	"github.com/block/schemabot/pkg/testutil"
)

var (
	// Target MySQL - where Spirit applies schema changes
	targetDSN string

	// Tern storage MySQL - used by gRPC server (simulates remote Tern's storage)
	ternStorageDSN string

	// SchemaBot storage MySQL - plans, tasks, applies, locks
	schemabotDSN string

	// gRPC server backed by LocalClient
	grpcAddr   string
	grpcClient tern.Client

	cleanupFuncs []func()
)

func TestMain(m *testing.M) {
	ctx := context.Background()

	// Start MySQL container for target databases (where Spirit applies schema changes)
	targetContainer, err := startMySQLContainer(ctx, "target-e2e-mysql", "target_test", nil)
	if err != nil {
		log.Fatalf("Failed to start target MySQL container: %v", err)
	}
	cleanupFuncs = append(cleanupFuncs, func() {
		if os.Getenv("DEBUG") == "" {
			_ = targetContainer.Terminate(ctx)
		}
	})

	host, err := testutil.ContainerHost(ctx, targetContainer)
	if err != nil {
		log.Fatalf("Failed to get target container host: %v", err)
	}
	port, err := testutil.ContainerPort(ctx, targetContainer, "3306")
	if err != nil {
		log.Fatalf("Failed to get target container port: %v", err)
	}
	targetDSN = fmt.Sprintf("root:testpassword@tcp(%s:%d)/target_test?parseTime=true", host, port)

	// Start MySQL container for Tern storage (simulates remote Tern's separate storage)
	ternStorageContainer, err := startMySQLContainer(ctx, "tern-storage-e2e-mysql", "tern_storage", &schema.MySQLFS)
	if err != nil {
		log.Fatalf("Failed to start Tern storage MySQL container: %v", err)
	}
	cleanupFuncs = append(cleanupFuncs, func() {
		if os.Getenv("DEBUG") == "" {
			_ = ternStorageContainer.Terminate(ctx)
		}
	})

	ternHost, err := testutil.ContainerHost(ctx, ternStorageContainer)
	if err != nil {
		log.Fatalf("Failed to get Tern storage container host: %v", err)
	}
	ternPort, err := testutil.ContainerPort(ctx, ternStorageContainer, "3306")
	if err != nil {
		log.Fatalf("Failed to get Tern storage container port: %v", err)
	}
	ternStorageDSN = fmt.Sprintf("root:testpassword@tcp(%s:%d)/tern_storage?parseTime=true", ternHost, ternPort)

	// Start MySQL container for SchemaBot storage (plans, tasks, applies, locks)
	schemabotContainer, err := startMySQLContainer(ctx, "schemabot-e2e-mysql", "schemabot_test", &schema.MySQLFS)
	if err != nil {
		log.Fatalf("Failed to start SchemaBot MySQL container: %v", err)
	}
	cleanupFuncs = append(cleanupFuncs, func() {
		if os.Getenv("DEBUG") == "" {
			_ = schemabotContainer.Terminate(ctx)
		}
	})

	sbHost, err := testutil.ContainerHost(ctx, schemabotContainer)
	if err != nil {
		log.Fatalf("Failed to get SchemaBot container host: %v", err)
	}
	sbPort, err := testutil.ContainerPort(ctx, schemabotContainer, "3306")
	if err != nil {
		log.Fatalf("Failed to get SchemaBot container port: %v", err)
	}
	schemabotDSN = fmt.Sprintf("root:testpassword@tcp(%s:%d)/schemabot_test?parseTime=true", sbHost, sbPort)

	// Start gRPC server backed by LocalClient with its own storage (simulates remote Tern)
	grpcAddr, err = startTernGRPC(ctx, targetDSN, ternStorageDSN)
	if err != nil {
		log.Fatalf("Failed to start Tern gRPC server: %v", err)
	}

	// Create gRPC client (simulating SchemaBot talking to external Tern over gRPC)
	grpcClient, err = tern.NewGRPCClient(tern.Config{Address: grpcAddr})
	if err != nil {
		log.Fatalf("Failed to create gRPC client: %v", err)
	}
	cleanupFuncs = append(cleanupFuncs, func() { _ = grpcClient.Close() })

	code := m.Run()

	// Cleanup in reverse order
	for i := len(cleanupFuncs) - 1; i >= 0; i-- {
		cleanupFuncs[i]()
	}

	os.Exit(code)
}

func startMySQLContainer(ctx context.Context, baseName, dbName string, schemaFS *embed.FS) (testcontainers.Container, error) {
	req := testcontainers.ContainerRequest{
		Name:         containerName(baseName),
		Image:        "mysql:8.0",
		ExposedPorts: []string{"3306/tcp"},
		Env: map[string]string{
			"MYSQL_ROOT_PASSWORD": "testpassword",
			"MYSQL_DATABASE":      dbName,
		},
		WaitingFor: wait.ForAll(
			wait.ForLog("ready for connections").WithOccurrence(2).WithStartupTimeout(60*time.Second),
			wait.ForListeningPort("3306/tcp"),
		),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
		Reuse:            os.Getenv("DEBUG") != "",
	})
	if err != nil {
		return nil, err
	}

	// Apply schema if provided
	if schemaFS != nil {
		host, err := testutil.ContainerHost(ctx, container)
		if err != nil {
			_ = container.Terminate(ctx)
			return nil, fmt.Errorf("get container host: %w", err)
		}
		port, err := testutil.ContainerPort(ctx, container, "3306")
		if err != nil {
			_ = container.Terminate(ctx)
			return nil, fmt.Errorf("get container port: %w", err)
		}
		dsn := fmt.Sprintf("root:testpassword@tcp(%s:%d)/%s?parseTime=true&multiStatements=true", host, port, dbName)

		db, err := sql.Open("mysql", dsn)
		if err != nil {
			_ = container.Terminate(ctx)
			return nil, fmt.Errorf("open db for schema: %w", err)
		}
		defer func() { _ = db.Close() }()

		// Wait for MySQL to be ready to accept connections
		var pingErr error
		for range 30 {
			if pingErr = db.PingContext(ctx); pingErr == nil {
				break
			}
			time.Sleep(500 * time.Millisecond)
		}
		if pingErr != nil {
			_ = container.Terminate(ctx)
			return nil, fmt.Errorf("MySQL not ready after 15s: %w", pingErr)
		}

		if err := applySchemaFS(ctx, db, *schemaFS); err != nil {
			_ = container.Terminate(ctx)
			return nil, fmt.Errorf("apply schema: %w", err)
		}
	}

	return container, nil
}

func applySchemaFS(ctx context.Context, db *sql.DB, schemaFS embed.FS) error {
	entries, err := schemaFS.ReadDir("mysql")
	if err != nil {
		return fmt.Errorf("read schema directory: %w", err)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		content, err := schemaFS.ReadFile("mysql/" + entry.Name())
		if err != nil {
			return fmt.Errorf("read schema file %s: %w", entry.Name(), err)
		}
		// Extract table name from CREATE TABLE statement to drop first (idempotent for reused containers)
		contentStr := string(content)
		if tableName := extractTableName(contentStr); tableName != "" {
			if _, err := db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)); err != nil {
				return fmt.Errorf("drop table %s: %w", tableName, err)
			}
		}
		if _, err := db.ExecContext(ctx, contentStr); err != nil {
			return fmt.Errorf("execute schema %s: %w", entry.Name(), err)
		}
	}

	return nil
}

// extractTableName extracts the table name from a CREATE TABLE statement.
func extractTableName(content string) string {
	re := regexp.MustCompile(`(?i)CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)`)
	matches := re.FindStringSubmatch(content)
	if len(matches) >= 2 {
		return matches[1]
	}
	return ""
}

// startTernGRPC starts a gRPC server backed by LocalClient (simulates a remote Tern service).
// This proves SchemaBot can communicate with any service implementing the Tern proto over gRPC.
// Uses separate storage (ternStorageDSN) from SchemaBot to simulate production architecture.
func startTernGRPC(ctx context.Context, targetDSN, storageDSN string) (grpcAddress string, err error) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	// Create the testdb database on the target MySQL for Spirit tests
	targetDB, err := sql.Open("mysql", targetDSN+"&multiStatements=true")
	if err != nil {
		return "", fmt.Errorf("open target db: %w", err)
	}
	if _, err := targetDB.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS testdb"); err != nil {
		_ = targetDB.Close()
		return "", fmt.Errorf("create testdb: %w", err)
	}
	_ = targetDB.Close()

	// Build DSN for testdb (replace database name in DSN)
	testdbDSN := strings.Replace(targetDSN, "/target_test", "/testdb", 1)

	// Open Tern storage (separate from SchemaBot storage, simulates production architecture)
	storageDB, err := sql.Open("mysql", storageDSN)
	if err != nil {
		return "", fmt.Errorf("open storage db: %w", err)
	}
	storage := mysqlstore.New(storageDB)
	cleanupFuncs = append(cleanupFuncs, func() { _ = storageDB.Close() })

	// Create LocalClient backed by Tern storage
	localClient, err := tern.NewLocalClient(tern.LocalConfig{
		Database:  "testdb",
		Type:      "mysql",
		TargetDSN: testdbDSN,
	}, storage, logger)
	if err != nil {
		return "", fmt.Errorf("create local client: %w", err)
	}
	cleanupFuncs = append(cleanupFuncs, func() { _ = localClient.Close() })

	// Wrap LocalClient in gRPC server (simulates remote Tern)
	grpcSrv := grpc.NewServer()
	ternGRPCServer := newGRPCServer(localClient)
	registerGRPCServer(ternGRPCServer, grpcSrv)
	cleanupFuncs = append(cleanupFuncs, func() { grpcSrv.GracefulStop() })

	// Start gRPC server on random port
	grpcListener, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "localhost:0")
	if err != nil {
		return "", fmt.Errorf("listen grpc: %w", err)
	}
	grpcAddress = grpcListener.Addr().String()

	go func() { _ = grpcSrv.Serve(grpcListener) }()

	// Wait for server to be ready
	conn, err := grpc.NewClient(grpcAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return "", fmt.Errorf("create gRPC client: %w", err)
	}
	defer func() { _ = conn.Close() }()
	readyCtx, readyCancel := context.WithTimeout(ctx, 5*time.Second)
	defer readyCancel()
	conn.Connect()
	for conn.GetState() != connectivity.Ready {

		if !conn.WaitForStateChange(readyCtx, conn.GetState()) {
			return "", fmt.Errorf("gRPC server not ready: context expired")
		}
	}

	return grpcAddress, nil
}

// containerName generates a unique container name based on the git branch.
func containerName(base string) string {
	out, err := exec.CommandContext(context.Background(), "git", "rev-parse", "--abbrev-ref", "HEAD").Output()
	if err != nil {
		return base
	}
	branch := strings.TrimSpace(string(out))
	if branch == "" || branch == "HEAD" {
		return base
	}
	sanitized := strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' {
			return r
		}
		return '-'
	}, branch)
	return base + "-" + sanitized
}
