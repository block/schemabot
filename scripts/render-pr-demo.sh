#!/usr/bin/env bash
# Assemble the PR walkthrough screenshots into a steady, readable README demo.
# Run after render-pr-mockups.py. Requires ImageMagick; keeps pr-demo.gif intact.
set -euo pipefail
cd "$(dirname "$0")/.."
command -v magick >/dev/null || { echo 'ImageMagick (magick) is required' >&2; exit 1; }
demo_tmp=$(mktemp -d)
trap 'rm -rf "$demo_tmp"' EXIT
frames=()
for image in assets/pr-mockups/pre-merge-{1-plan,2-apply,3-status,4-applied,5-checks,6-merged}.png; do
  frame="$demo_tmp/$(basename "$image")"
  # Preserve the original scale across frames, including narrower check panels.
  # The fixed canvas keeps the README below the image from moving between steps.
  magick "$image" -resize 60% -background white -gravity northwest \
    -extent 1002x1120 "$frame"
  frames+=("$frame")
done
magick -delay 600 -loop 0 "${frames[@]}" -layers Optimize assets/pr-workflow-demo.gif
printf 'Wrote assets/pr-workflow-demo.gif (six stages, six seconds each)\n'
