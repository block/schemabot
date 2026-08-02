#!/usr/bin/env bash
# Pre-pull external Docker images with bounded retry.
#
# Why this exists: GitHub-hosted runners share NAT egress and routinely hit
# registry throttling or transient outages (e.g. 502s from the Docker Hub
# token endpoint). When that happens mid-job, the failure surfaces either as
# a BuildKit "failed to authorize" error during an image build or as many
# parallel testcontainer "context deadline exceeded" test failures — both
# noisy and hard to attribute.
#
# Running this as its own step before builds/tests turns those flakes into a
# single named, retryable, fail-fast step. Both the dockerd-integrated
# BuildKit builder and testcontainers resolve images from the local daemon
# store first, so warming the cache here prevents the mid-job registry
# round-trips entirely on the happy path.
#
# Usage: prepull-images.sh [<image:tag> ...] [--dockerfile <path>] ...
#
# --dockerfile <path> extracts the external base images from the FROM lines
# of the given Dockerfile (skipping build-stage references and scratch), so
# workflow steps stay in sync with Dockerfile tag bumps automatically.

set -euo pipefail

if [[ $# -eq 0 ]]; then
  echo "usage: $0 [<image:tag> ...] [--dockerfile <path>] ..." >&2
  exit 2
fi

# Collect the external base images referenced by a Dockerfile's FROM lines.
# Skips references to earlier build stages (FROM ... AS <stage>) and scratch.
dockerfile_images() {
  local dockerfile="$1"
  awk '
    toupper($1) == "FROM" {
      # First non-flag token after FROM is the image or stage reference.
      for (i = 2; i <= NF; i++) {
        if ($i !~ /^--/) { ref = $i; break }
      }
      # Record stage aliases so later FROM <stage> lines are skipped.
      for (i = 2; i < NF; i++) {
        if (toupper($i) == "AS") { stages[$(i + 1)] = 1 }
      }
      if (ref != "scratch" && !(ref in stages)) { print ref }
    }
  ' "$dockerfile" | sort -u
}

images=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --dockerfile)
      if [[ $# -lt 2 ]]; then
        echo "--dockerfile requires a path argument" >&2
        exit 2
      fi
      if [[ ! -f "$2" ]]; then
        echo "dockerfile not found: $2" >&2
        exit 2
      fi
      while IFS= read -r image; do
        images+=("$image")
      done < <(dockerfile_images "$2")
      shift 2
      ;;
    *)
      images+=("$1")
      shift
      ;;
  esac
done

if [[ ${#images[@]} -eq 0 ]]; then
  echo "no images resolved from arguments" >&2
  exit 2
fi

# 3 attempts with linear backoff is enough headroom for transient registry
# slowness without masking a real outage; if all 3 fail the job exits with a
# clear "failed to pull" error before any build or test starts.
max_attempts=3

pull_with_retry() {
  local image="$1"
  local attempt sleep_seconds
  for attempt in $(seq 1 "$max_attempts"); do
    echo "::group::docker pull ${image} (attempt ${attempt}/${max_attempts})"
    if docker pull "$image"; then
      docker image inspect "$image" >/dev/null
      echo "::endgroup::"
      return 0
    fi
    echo "::endgroup::"
    if [[ "$attempt" -lt "$max_attempts" ]]; then
      sleep_seconds=$((attempt * 10))
      echo "pull failed for ${image}; retrying in ${sleep_seconds}s"
      sleep "$sleep_seconds"
    fi
  done
  echo "failed to pull ${image} after ${max_attempts} attempts" >&2
  return 1
}

for image in "${images[@]}"; do
  pull_with_retry "$image"
done
