package api

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"sort"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"

	"github.com/block/schemabot/pkg/awscreds"
	"github.com/block/schemabot/pkg/etre"
	"github.com/block/schemabot/pkg/inventory"
	"github.com/block/schemabot/pkg/secrets"
	"github.com/block/schemabot/pkg/storage"
)

// BuildResolver builds the configured inventory.Resolver — the Etre dynamic
// backend(s) or the static inventory. It fails closed on an ambiguous config
// (both backends configured) and on an empty one (neither configured), so an
// embedder calling this directly gets the same guarantees the server enforces
// rather than a silent backend preference.
func (c TargetResolverConfig) BuildResolver(ctx context.Context, logger *slog.Logger) (inventory.Resolver, error) {
	etreConfigured := len(c.Etre) > 0
	staticConfigured := c.Configured()
	switch {
	case etreConfigured && staticConfigured:
		return nil, fmt.Errorf("target_resolver configures both etre and static targets; per-target overrides are not yet supported — use one")
	case etreConfigured:
		return buildEtreResolvers(ctx, c.Etre, logger)
	case staticConfigured:
		resolver, err := inventory.NewStaticResolver(c.StaticInventory())
		if err != nil {
			return nil, fmt.Errorf("build static target resolver: %w", err)
		}
		logger.Info("gRPC server routing by static target resolver", "targets", len(c.Targets))
		return resolver, nil
	default:
		return nil, fmt.Errorf("target_resolver configures neither etre nor static targets")
	}
}

// buildEtreResolvers builds the Etre-backed resolver(s). A single block resolves
// every request directly (engine selected by its database_type). Multiple blocks
// compose into a TypeRoutingResolver keyed by database type so one data plane
// serves several engines at once and the request's database type selects the
// engine. It fails closed when multiple blocks omit a database_type or collide on
// one, so an ambiguous routing config surfaces at startup, not first request.
func buildEtreResolvers(ctx context.Context, etres []EtreConfig, logger *slog.Logger) (inventory.Resolver, error) {
	if len(etres) == 1 {
		resolver, err := buildEtreResolver(ctx, etres[0], logger)
		if err != nil {
			return nil, fmt.Errorf("build etre resolver: %w", err)
		}
		logEtreResolver(logger, etres[0])
		return resolver, nil
	}

	byType := make(map[string]inventory.Resolver, len(etres))
	for _, cfg := range etres {
		switch {
		case cfg.DatabaseType == "":
			return nil, fmt.Errorf("target_resolver.etre: database_type is required for each resolver when more than one is configured")
		case byType[cfg.DatabaseType] != nil:
			return nil, fmt.Errorf("target_resolver.etre: more than one resolver configured for database_type %q", cfg.DatabaseType)
		}
		resolver, err := buildEtreResolver(ctx, cfg, logger)
		if err != nil {
			return nil, fmt.Errorf("build etre resolver for database_type %q: %w", cfg.DatabaseType, err)
		}
		logEtreResolver(logger, cfg)
		byType[cfg.DatabaseType] = resolver
	}

	router, err := inventory.NewTypeRoutingResolver(byType)
	if err != nil {
		return nil, fmt.Errorf("build per-type etre resolver routing: %w", err)
	}
	routedTypes := make([]string, 0, len(byType))
	for databaseType := range byType {
		routedTypes = append(routedTypes, databaseType)
	}
	sort.Strings(routedTypes)
	logger.Info("gRPC server routing by per-type etre resolvers", "database_types", routedTypes)
	return router, nil
}

// logEtreResolver records that an Etre resolver was built, with the identifiers
// an operator needs to confirm routing at startup.
func logEtreResolver(logger *slog.Logger, cfg EtreConfig) {
	logger.Info("gRPC server etre resolver configured",
		"database_type", cfg.DatabaseType, "entity_type", cfg.EntityType,
		"target_label", cfg.TargetLabel, "credentials", credentialType(cfg.Credentials))
}

// buildEtreResolver assembles the Etre-backed resolver from config: the Etre
// query client, the engine-specific connection assembler, and the configured
// credential resolver. Lazily-validated fields (host, credentials) are checked
// here so a misconfiguration fails at startup, not first request.
func buildEtreResolver(ctx context.Context, cfg EtreConfig, logger *slog.Logger) (inventory.Resolver, error) {
	assembler, decode, err := etreAssembler(cfg)
	if err != nil {
		return nil, err
	}
	creds, err := buildCredentialResolver(ctx, cfg.Credentials, decode)
	if err != nil {
		return nil, err
	}
	addr, err := secrets.Resolve(cfg.Addr, "")
	if err != nil {
		return nil, fmt.Errorf("resolve target_resolver.etre.addr: %w", err)
	}
	// A secret ref (env:/file:/secretsmanager:) can resolve to "" without error;
	// surface that as a clear config error rather than a generic downstream one.
	if addr == "" {
		return nil, fmt.Errorf("target_resolver.etre.addr resolved to an empty value")
	}
	client, err := etre.New(etre.Config{Addr: addr, EntityType: cfg.EntityType, Logger: logger})
	if err != nil {
		return nil, fmt.Errorf("build etre client: %w", err)
	}
	return etre.NewEtreResolver(etre.EtreResolverConfig{
		Client:          client,
		TargetLabel:     cfg.TargetLabel,
		Labels:          cfg.Labels,
		EnvLabel:        cfg.EnvLabel,
		HostField:       cfg.MySQL.HostField,
		AttributeFields: resolverAttributeFields(cfg),
		Credentials:     creds,
		Assembler:       assembler,
	})
}

// etreAssembler selects the engine-specific connection assembler for the
// configured database type, plus the secret decoder that backend needs. MySQL
// builds a namespace-free DSN from the host; Vitess assembles PlanetScale API
// metadata and decodes a token secret rather than a username/password. A new
// engine (postgres, strata) is a new case here; an unsupported type fails closed.
func etreAssembler(cfg EtreConfig) (inventory.ConnectionAssembler, inventory.SecretDecoder, error) {
	switch cfg.DatabaseType {
	case "":
		return nil, nil, fmt.Errorf("target_resolver.etre.database_type is required (%q or %q)", storage.DatabaseTypeMySQL, storage.DatabaseTypeVitess)
	case storage.DatabaseTypeMySQL:
		if cfg.MySQL.HostField == "" {
			return nil, nil, fmt.Errorf("target_resolver.etre.mysql.host_field is required for the %q engine", storage.DatabaseTypeMySQL)
		}
		return inventory.MySQLConnectionAssembler{DefaultPort: cfg.MySQL.DefaultPort}, nil, nil
	case storage.DatabaseTypeVitess:
		return inventory.VitessConnectionAssembler{
			OrganizationAttribute: cfg.Vitess.OrganizationAttribute,
			APIURL:                cfg.Vitess.APIURL,
		}, inventory.DecodePlanetScaleSecret, nil
	default:
		return nil, nil, fmt.Errorf("target_resolver.etre.database_type %q is not supported", cfg.DatabaseType)
	}
}

const (
	credentialTypeSecretRef = "secret_ref"
	credentialTypeAWSSM     = "awssm"
)

// credentialType returns the configured credential backend, defaulting to
// secret_ref.
func credentialType(cfg EtreCredentialsConfig) string {
	if cfg.Type == "" {
		return credentialTypeSecretRef
	}
	return cfg.Type
}

// buildCredentialResolver builds the configured credential backend. Each backend
// is one inventory.CredentialResolver implementation; the data plane is not
// coupled to any single secret store.
func buildCredentialResolver(ctx context.Context, cfg EtreCredentialsConfig, decode inventory.SecretDecoder) (inventory.CredentialResolver, error) {
	switch credentialType(cfg) {
	case credentialTypeSecretRef:
		if cfg.PasswordRef == "" {
			return nil, fmt.Errorf("target_resolver.etre.credentials.password_ref is required")
		}
		// A decoder (for example a Vitess token) produces the full credential from
		// the secret, so no separate username is configured; require a username
		// only for the plain username + password form.
		if decode == nil && cfg.Username == "" {
			return nil, fmt.Errorf("target_resolver.etre.credentials.username is required")
		}
		return inventory.SecretRefCredentialResolver{Username: cfg.Username, PasswordRef: cfg.PasswordRef, Decode: decode}, nil

	case credentialTypeAWSSM:
		// Validate required fields with config-path context before loading AWS
		// config, so a misconfiguration fails fast and actionably instead of
		// after (potentially slow) credential-chain resolution. role_arn is
		// optional: without it the backend reads from the caller's own account.
		switch {
		case cfg.Region == "":
			return nil, fmt.Errorf("target_resolver.etre.credentials.region is required for the awssm backend")
		case cfg.SecretName == "":
			return nil, fmt.Errorf("target_resolver.etre.credentials.secret_name is required for the awssm backend")
		case cfg.Username != "" && decode != nil:
			return nil, fmt.Errorf("target_resolver.etre.credentials.username (plain-password secrets) cannot be combined with a token-decoding engine such as vitess")
		}
		awsCfg, err := awsconfig.LoadDefaultConfig(ctx)
		if err != nil {
			return nil, fmt.Errorf("load AWS config for target_resolver.etre.credentials: %w", err)
		}
		resolver, err := awscreds.New(awscreds.Config{
			AWSConfig:        awsCfg,
			Region:           cfg.Region,
			RoleARN:          cfg.RoleARN,
			ExternalID:       cfg.ExternalID,
			SecretName:       cfg.SecretName,
			AccountAttribute: cfg.AccountAttribute,
			Username:         cfg.Username,
			Decode:           decode,
		})
		if err != nil {
			return nil, fmt.Errorf("build target_resolver.etre.credentials awssm resolver: %w", err)
		}
		return resolver, nil

	default:
		return nil, fmt.Errorf("unknown target_resolver.etre.credentials.type %q (want %q or %q)", cfg.Type, credentialTypeSecretRef, credentialTypeAWSSM)
	}
}

// resolverAttributeFields returns the entity attribute fields the resolver must
// surface so the assembler and credential backend can locate their inputs: the
// Vitess organization attribute and the assume-role backend's account attribute,
// alongside any explicitly configured fields.
func resolverAttributeFields(cfg EtreConfig) []string {
	fields := append([]string(nil), cfg.AttributeFields...)
	// Engines that resolve a connection attribute rather than a host surface it
	// here. A new such engine adds a branch; host-based engines (mysql) add none.
	if cfg.DatabaseType == storage.DatabaseTypeVitess {
		orgAttr := cfg.Vitess.OrganizationAttribute
		if orgAttr == "" {
			orgAttr = inventory.MetadataOrganization
		}
		fields = ensureField(fields, orgAttr)
	}
	if credentialType(cfg.Credentials) == credentialTypeAWSSM {
		// Assume-role mode (role_arn set) resolves the target account from an
		// attribute; own-account mode does not need it.
		if cfg.Credentials.RoleARN != "" {
			accountAttr := cfg.Credentials.AccountAttribute
			if accountAttr == "" {
				accountAttr = "aws_account_id"
			}
			fields = ensureField(fields, accountAttr)
		}
		// The secret name and username may template over resolved attributes;
		// surface those so the resolver fetches them for the credential backend.
		for _, attr := range awscreds.TemplateAttributes(cfg.Credentials.SecretName) {
			fields = ensureField(fields, attr)
		}
		for _, attr := range awscreds.TemplateAttributes(cfg.Credentials.Username) {
			fields = ensureField(fields, attr)
		}
	}
	return fields
}

// ensureField appends field unless it is already present.
func ensureField(fields []string, field string) []string {
	if slices.Contains(fields, field) {
		return fields
	}
	return append(fields, field)
}
