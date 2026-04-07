// Package secrets provides secret resolution for SchemaBot.
//
// Secret references use a prefix:value format:
//
//	env:VAR_NAME                      → reads $VAR_NAME environment variable
//	file:/path/to/secret              → reads from file (trims whitespace)
//	secretsmanager:secret-name        → reads from AWS Secrets Manager
//	secretsmanager:secret-name#key    → reads specific key from JSON secret
//	literal-value                     → returned as-is
//
// # Custom Resolvers
//
// OSS users can register custom resolvers for their own secret backends:
//
//	// Register a HashiCorp Vault resolver
//	secrets.Register("vault", func(ref string) (string, error) {
//	    return vaultClient.Logical().Read(ref)
//	})
//
//	// Then reference secrets as: vault:secret/data/myapp#password
//	dsn, err := secrets.Resolve("vault:secret/data/myapp#password", "")
//
// Custom resolvers are checked before built-in resolvers, allowing you to
// override default behavior if needed.
package secrets

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
)

// ResolverFunc is a function that resolves a secret reference to its value.
// The ref parameter has the prefix already stripped. For example,
// when resolving "vault:secret/myapp", the resolver receives "secret/myapp".
type ResolverFunc func(ref string) (string, error)

var (
	customResolvers = make(map[string]ResolverFunc)
	resolverMu      sync.RWMutex
)

// Register adds a custom resolver for the given prefix.
// Custom resolvers are checked before built-in resolvers.
//
// Example:
//
//	secrets.Register("vault", func(ref string) (string, error) {
//	    return vaultClient.Get(ref)
//	})
func Register(prefix string, resolver ResolverFunc) {
	resolverMu.Lock()
	defer resolverMu.Unlock()
	customResolvers[prefix] = resolver
}

// Unregister removes a custom resolver. Mainly useful for testing.
func Unregister(prefix string) {
	resolverMu.Lock()
	defer resolverMu.Unlock()
	delete(customResolvers, prefix)
}

// Resolve resolves a secret value, handling special prefixes:
//   - Custom registered resolvers (checked first)
//   - "env:VAR_NAME" reads from the specified environment variable
//   - "file:/path/to/secret" reads from a file (trimming whitespace)
//   - "secretsmanager:secret-name" reads from AWS Secrets Manager
//   - "secretsmanager:secret-name#key" reads a specific key from a JSON secret
//   - Direct value is returned as-is
//
// If value is empty, falls back to the fallbackEnvVar environment variable.
func Resolve(value string, fallbackEnvVar string) (string, error) {
	if value == "" {
		return os.Getenv(fallbackEnvVar), nil
	}

	// Check for custom resolvers first
	if before, after, ok := strings.Cut(value, ":"); ok {
		prefix := before
		resolverMu.RLock()
		resolver, ok := customResolvers[prefix]
		resolverMu.RUnlock()
		if ok {
			return resolver(after)
		}
	}

	// Check for "env:" prefix
	if strings.HasPrefix(value, "env:") {
		return os.Getenv(value[4:]), nil
	}

	// Check for "file:" prefix
	if strings.HasPrefix(value, "file:") {
		return resolveFile(value[5:])
	}

	// Check for "secretsmanager:" prefix
	if strings.HasPrefix(value, "secretsmanager:") {
		return resolveSecretsManager(value[15:])
	}

	return value, nil
}

// resolveFile reads a secret from a file, trimming whitespace.
func resolveFile(path string) (string, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read secret file %s: %w", path, err)
	}
	// Trim whitespace (secrets files often have trailing newlines)
	return strings.TrimSpace(string(content)), nil
}

// resolveSecretsManager fetches a secret from AWS Secrets Manager.
// Format: "secret-name" or "secret-name#json-key"
func resolveSecretsManager(ref string) (string, error) {
	// Parse secret name and optional JSON key
	secretName, jsonKey, _ := strings.Cut(ref, "#")

	// Create AWS config (uses default credential chain: env vars, IAM role, etc.)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return "", fmt.Errorf("load AWS config: %w", err)
	}

	client := secretsmanager.NewFromConfig(cfg)

	// Fetch the secret
	result, err := client.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{
		SecretId: &secretName,
	})
	if err != nil {
		return "", fmt.Errorf("get secret %q: %w", secretName, err)
	}

	secretValue := ""
	if result.SecretString != nil {
		secretValue = *result.SecretString
	} else {
		return "", fmt.Errorf("secret %q has no string value (binary secrets not supported)", secretName)
	}

	// If no JSON key specified, return the whole secret
	if jsonKey == "" {
		return secretValue, nil
	}

	// Parse as JSON and extract the key
	var data map[string]any
	if err := json.Unmarshal([]byte(secretValue), &data); err != nil {
		return "", fmt.Errorf("parse secret %q as JSON: %w", secretName, err)
	}

	val, ok := data[jsonKey]
	if !ok {
		return "", fmt.Errorf("key %q not found in secret %q", jsonKey, secretName)
	}

	// Convert to string
	switch v := val.(type) {
	case string:
		return v, nil
	default:
		return fmt.Sprintf("%v", v), nil
	}
}
