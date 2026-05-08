#!/bin/bash
set -euo pipefail

# Bootstrap a PlanetScale database for SchemaBot with sharded + unsharded keyspaces.
#
# Prerequisites:
#   - pscale CLI installed: brew install planetscale/tap/pscale
#   - Authenticated: pscale auth login
#
# Usage:
#   ./bootstrap-planetscale.sh --org <org> [--database <name>] <command>
#
# Commands:
#   create    Create database, keyspaces, service token, and vtgate password
#   status    Show database status and cost estimate
#   delete    Delete database (with confirmation)
#
# Examples:
#   ./bootstrap-planetscale.sh --org my-org create
#   ./bootstrap-planetscale.sh --org my-org --database mydb --shards 4 create
#   ./bootstrap-planetscale.sh --org my-org status
#   ./bootstrap-planetscale.sh --org my-org delete

# ============================================================================
# Defaults
# ============================================================================

PS_ORG=""
PS_DATABASE="commerce"
PS_REGION="us-west"
PS_CLUSTER_SIZE="PS-10"
PS_SHARDED_SHARD_COUNT=2
PS_COST_PER_SHARD=39

# ============================================================================
# Parse flags
# ============================================================================

while [[ $# -gt 0 ]]; do
    case $1 in
        --help|-h)
            COMMAND="--help"
            break
            ;;
        --org)
            PS_ORG="$2"
            shift 2
            ;;
        --database)
            PS_DATABASE="$2"
            shift 2
            ;;
        --region)
            PS_REGION="$2"
            shift 2
            ;;
        --cluster-size)
            PS_CLUSTER_SIZE="$2"
            shift 2
            ;;
        --shards)
            PS_SHARDED_SHARD_COUNT="$2"
            shift 2
            ;;
        *)
            break
            ;;
    esac
done

COMMAND="${1:-}"
PS_SHARDED_KEYSPACE="${PS_DATABASE}_sharded"
TODAY=$(date +%Y-%m-%d)

if [ -z "$PS_ORG" ]; then
    echo "Error: --org <org-name> is required"
    echo "Usage: $0 --org <org-name> [options] <command>"
    echo "Run '$0 --help' for details."
    exit 1
fi

if [ -z "$COMMAND" ]; then
    echo "Error: command required (create, status, delete)"
    echo "Usage: $0 --org <org-name> [options] <command>"
    exit 1
fi

if ! command -v pscale &> /dev/null; then
    echo "Error: pscale CLI not installed"
    echo "Install: brew install planetscale/tap/pscale"
    echo "Then:    pscale auth login"
    exit 1
fi

PSCALE="pscale --org $PS_ORG"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m'

log()     { echo -e "${BLUE}[INFO]${NC} $1"; }
success() { echo -e "${GREEN}[OK]${NC} $1"; }
warn()    { echo -e "${YELLOW}[WARN]${NC} $1"; }
error()   { echo -e "${RED}[ERROR]${NC} $1"; exit 1; }

# ============================================================================
# create
# ============================================================================

cmd_create() {
    log "Creating PlanetScale database '$PS_DATABASE' in org '$PS_ORG'..."
    echo ""

    if $PSCALE database show "$PS_DATABASE" &>/dev/null; then
        error "Database '$PS_DATABASE' already exists in org '$PS_ORG'"
    fi

    # Step 1: Create database (creates default unsharded keyspace with same name)
    log "Step 1/5: Creating database '$PS_DATABASE' (unsharded keyspace, $PS_CLUSTER_SIZE)..."
    $PSCALE database create "$PS_DATABASE" \
        --region "$PS_REGION" \
        --cluster-size "$PS_CLUSTER_SIZE" \
        --wait
    success "Database created with unsharded keyspace '$PS_DATABASE'"

    # Step 2: Create sharded keyspace
    log "Step 2/5: Creating sharded keyspace '$PS_SHARDED_KEYSPACE' ($PS_SHARDED_SHARD_COUNT shards, $PS_CLUSTER_SIZE each)..."
    $PSCALE keyspace create "$PS_DATABASE" main "$PS_SHARDED_KEYSPACE" \
        --shards "$PS_SHARDED_SHARD_COUNT" \
        --cluster-size "$PS_CLUSTER_SIZE" \
        --wait
    success "Sharded keyspace created"

    # Step 3: Create service token
    local token_name="schemabot-${PS_DATABASE}--${TODAY}"
    log "Step 3/5: Creating service token '$token_name'..."
    local token_json
    token_json=$($PSCALE service-token create --name "$token_name" --format json)
    local token_id token_secret
    token_id=$(echo "$token_json" | jq -re '.id') || error "Failed to parse service token ID from output"
    token_secret=$(echo "$token_json" | jq -re '.token') || error "Failed to parse service token secret from output"
    success "Service token created: $token_id"

    # Step 4: Grant permissions
    log "Step 4/5: Granting database permissions..."
    $PSCALE service-token add-access "$token_id" \
        approve_deploy_request \
        connect_branch \
        create_branch \
        create_comment \
        create_deploy_request \
        delete_branch_password \
        read_branch \
        read_comment \
        read_database \
        read_deploy_request \
        write_branch_vschema \
        --database "$PS_DATABASE"
    success "Permissions granted (11 access types)"

    # Step 5: Create vtgate password for progress polling (SHOW VITESS_MIGRATIONS)
    local vtgate_name="schemabot-vtgate--${TODAY}"
    log "Step 5/5: Creating vtgate password '$vtgate_name'..."
    local vtgate_json
    vtgate_json=$($PSCALE password create "$PS_DATABASE" main "$vtgate_name" --role reader --format json)
    local vtgate_host vtgate_user vtgate_pass
    vtgate_host=$(echo "$vtgate_json" | jq -re '.access_host_url') || error "Failed to parse vtgate host from output"
    vtgate_user=$(echo "$vtgate_json" | jq -re '.username') || error "Failed to parse vtgate username from output"
    vtgate_pass=$(echo "$vtgate_json" | jq -re '.plain_text') || error "Failed to parse vtgate password from output"
    success "Vtgate password created"

    # Summary
    local total_shards=$((1 + PS_SHARDED_SHARD_COUNT))
    local total_cost=$((total_shards * PS_COST_PER_SHARD))
    local service_token="${token_id}:${token_secret}"
    local vtgate_dsn="${vtgate_user}:${vtgate_pass}@tcp(${vtgate_host}:3306)/"

    echo ""
    echo "═══════════════════════════════════════════════════════════════"
    echo "  PlanetScale Setup Complete"
    echo "═══════════════════════════════════════════════════════════════"
    echo ""
    echo "  Database:     $PS_DATABASE"
    echo "  Organization: $PS_ORG"
    echo "  Region:       $PS_REGION"
    echo ""
    echo "  Keyspaces:"
    echo "    - $PS_DATABASE (unsharded, 1 shard)"
    echo "    - $PS_SHARDED_KEYSPACE (sharded, $PS_SHARDED_SHARD_COUNT shards)"
    echo ""
    echo "  Cost: ~\$$total_cost/mo ($total_shards shards × \$$PS_COST_PER_SHARD)"
    echo ""
    echo "  Credentials (save these — they cannot be retrieved later):"
    echo ""
    echo "    Service token (for token_secret_ref):"
    echo "      $service_token"
    echo ""
    echo "    Vtgate DSN (for dsn):"
    echo "      $vtgate_dsn"
    echo ""
    echo "  Next steps:"
    echo "    1. Store credentials in Secrets Manager:"
    echo "       ../scripts/setup-planetscale-token.sh \\"
    echo "         --token '$service_token' \\"
    echo "         --vtgate-dsn '$vtgate_dsn'"
    echo ""
    echo "    2. Update config.yaml organization field:"
    echo "       organization: \"$PS_ORG\""
    echo ""
    echo "    3. Redeploy SchemaBot:"
    echo "       ../scripts/deploy.sh"
    echo ""
    echo "═══════════════════════════════════════════════════════════════"
}

# ============================================================================
# status
# ============================================================================

cmd_status() {
    echo ""
    echo "═══════════════════════════════════════════════════════════════"
    echo "  PlanetScale Status"
    echo "═══════════════════════════════════════════════════════════════"
    echo ""

    local total_shards=$((1 + PS_SHARDED_SHARD_COUNT))
    local total_cost=$((total_shards * PS_COST_PER_SHARD))

    local ps_status
    ps_status=$($PSCALE database show "$PS_DATABASE" --format json 2>/dev/null | jq -r '.state' || echo "not-found")

    echo -e "  Database:     $PS_DATABASE"
    echo -e "  Organization: $PS_ORG"
    echo ""

    case "$ps_status" in
        ready)
            echo -e "  Status: ${GREEN}running${NC}"
            echo ""
            echo "  Keyspaces:"
            echo "    - $PS_DATABASE (unsharded, 1 shard)"
            echo "    - $PS_SHARDED_KEYSPACE (sharded, $PS_SHARDED_SHARD_COUNT shards)"
            echo ""
            echo "  Cost: ~\$$total_cost/mo ($total_shards shards × \$$PS_COST_PER_SHARD)"
            ;;
        not-found)
            echo -e "  Status: ${YELLOW}not found${NC}"
            echo ""
            echo "  Run '$0 --org $PS_ORG create' to create the database"
            ;;
        *)
            echo "  Status: $ps_status"
            ;;
    esac

    echo ""
    echo "═══════════════════════════════════════════════════════════════"
    echo ""
}

# ============================================================================
# delete
# ============================================================================

cmd_delete() {
    if ! $PSCALE database show "$PS_DATABASE" &>/dev/null; then
        error "Database '$PS_DATABASE' not found in org '$PS_ORG'"
    fi

    local total_shards=$((1 + PS_SHARDED_SHARD_COUNT))
    local total_cost=$((total_shards * PS_COST_PER_SHARD))

    echo ""
    warn "This will DELETE the PlanetScale database: $PS_DATABASE"
    warn "Organization: $PS_ORG"
    warn "Monthly savings: ~\$$total_cost"
    echo ""
    read -p "Type the database name to confirm: " confirm

    if [ "$confirm" != "$PS_DATABASE" ]; then
        error "Cancelled — input did not match database name"
    fi

    log "Deleting $PS_DATABASE..."
    $PSCALE database delete "$PS_DATABASE" --force
    success "Database deleted"
}

# ============================================================================
# Main
# ============================================================================

case "$COMMAND" in
    create) cmd_create ;;
    status) cmd_status ;;
    delete) cmd_delete ;;
    --help|-h)
        echo "Bootstrap a PlanetScale database for SchemaBot"
        echo ""
        echo "Usage: $0 --org <org> [options] <command>"
        echo ""
        echo "Commands:"
        echo "  create    Create database, keyspaces, service token, and vtgate password"
        echo "  status    Show database status and cost estimate"
        echo "  delete    Delete database (with confirmation)"
        echo ""
        echo "Required:"
        echo "  --org <org>            PlanetScale organization"
        echo ""
        echo "Options:"
        echo "  --database <name>      Database name (default: commerce)"
        echo "  --region <region>      PlanetScale region (default: us-west)"
        echo "  --cluster-size <size>  Cluster size (default: PS-10, \$${PS_COST_PER_SHARD}/shard/mo)"
        echo "  --shards <n>           Shards for sharded keyspace (default: 2)"
        echo ""
        echo "Prerequisites:"
        echo "  brew install planetscale/tap/pscale"
        echo "  pscale auth login"
        echo ""
        echo "Database structure:"
        echo "  <database>          — unsharded keyspace (1 shard)"
        echo "  <database>_sharded  — sharded keyspace (<n> shards)"
        echo ""
        exit 0
        ;;
    *)
        echo "Unknown command: $COMMAND"
        echo "Usage: $0 --org <org> [options] <command>"
        echo "Commands: create, status, delete"
        exit 1
        ;;
esac
