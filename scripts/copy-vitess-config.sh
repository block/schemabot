#!/usr/bin/env bash
#
# Copy Vitess config files from Go module cache into deploy/local/vitess-config/
# for the LocalScale Docker image build. These are runtime configs (init_db.sql,
# mycnf templates) that vtcombo needs — we extract them at build time rather than
# vendoring them in the repo.
set -euo pipefail

VITESS_MOD=$(go list -m -f '{{.Dir}}' vitess.io/vitess)
DEST=deploy/local/vitess-config

[ -d "$DEST" ] && chmod -R u+w "$DEST"
rm -rf "$DEST"
cp -r "$VITESS_MOD/config" "$DEST"
chmod -R u+w "$DEST"
# Remove Go build files that aren't needed at runtime
rm -f "$DEST/embed.go" "$DEST/gomysql.pc.tmpl"

echo "Copied Vitess config from $VITESS_MOD/config to $DEST"
