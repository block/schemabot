#!/usr/bin/env bash
# Requires Node.js, Playwright, a browser, and ImageMagick.
set -euo pipefail
cd "$(dirname "$0")/.."
node scripts/render-pr-demo.cjs
