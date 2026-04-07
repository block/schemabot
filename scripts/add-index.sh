#!/bin/bash
# Add one index to each schema file by duplicating the last index.
#
# Usage:
#   ./scripts/add-index.sh         # Add one index to each table
#   ./scripts/add-index.sh reset   # Reset to minimal schema

set -e

SCHEMA_DIR="${SCHEMA_DIR:-./examples/mysql/schema/testapp}"

if [ "$1" = "reset" ]; then
    cat > "$SCHEMA_DIR/users.sql" << 'EOF'
CREATE TABLE users (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    email VARCHAR(255) NOT NULL,
    full_name VARCHAR(255) NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_email (email),
    INDEX idx_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
EOF

    cat > "$SCHEMA_DIR/orders.sql" << 'EOF'
CREATE TABLE orders (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    total_cents BIGINT NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_user_id (user_id),
    INDEX idx_status (status),
    INDEX idx_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
EOF

    cat > "$SCHEMA_DIR/products.sql" << 'EOF'
CREATE TABLE products (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT NULL,
    price_cents BIGINT NOT NULL,
    sku VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_name (name),
    INDEX idx_sku (sku),
    INDEX idx_price (price_cents)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
EOF

    echo "Reset all schema files"
    exit 0
fi

# Add one index to each file using Python for reliability
python3 << 'PYEOF'
import os
import re
import sys

schema_dir = os.environ.get('SCHEMA_DIR', './examples/mysql/schema/testapp')

for filename in os.listdir(schema_dir):
    if not filename.endswith('.sql'):
        continue

    filepath = os.path.join(schema_dir, filename)
    table = filename.replace('.sql', '')

    with open(filepath, 'r') as f:
        content = f.read()

    # Find all INDEX lines
    index_pattern = r'^    INDEX (\w+)'
    indexes = re.findall(index_pattern, content, re.MULTILINE)

    if not indexes:
        continue

    last_idx = indexes[-1]

    # Generate new index name
    match = re.match(r'(.+?)(\d+)$', last_idx)
    if match:
        new_idx = f"{match.group(1)}{int(match.group(2)) + 1}"
    else:
        new_idx = f"{last_idx}1"

    # Find the last INDEX line and its content
    last_index_match = list(re.finditer(r'^(    INDEX ' + re.escape(last_idx) + r'.+)$', content, re.MULTILINE))[-1]
    last_index_line = last_index_match.group(1)

    # Create new index line
    new_index_line = last_index_line.replace(last_idx, new_idx)

    # Ensure last INDEX has comma
    if not last_index_line.rstrip().endswith(','):
        content = content[:last_index_match.end()] + ',' + content[last_index_match.end():]

    # Insert new index before ) ENGINE
    content = re.sub(r'(\n\) ENGINE)', f'\n{new_index_line}\\1', content)

    with open(filepath, 'w') as f:
        f.write(content)

    print(f"{table}: {new_idx}")
PYEOF
