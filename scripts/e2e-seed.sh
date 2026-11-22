#!/usr/bin/env bash
# Seeds the test-e2e-local environment: applies the MySQL and Vitess example
# schemas to staging, then production, before the e2e tests run. The MySQL pair
# runs in the background while the Vitess pair runs, so the two overlap.
#
# Every seed apply runs under a time bound of E2E_SEED_APPLY_TIMEOUT seconds, so
# one that stalls fails the setup with a named error instead of holding the job
# until the CI runner cancels it. A runner cancellation skips the failure-only
# log capture steps, and it lands before `go test` starts, so the test binary's
# own timeout and goroutine dump never fire either; failing here keeps the stall
# inside the path that collects evidence.
#
# When a VSchema seed or the Vitess pair fails, the MySQL pair is stopped and
# reaped before the script exits, so no seed apply outlives the setup it
# belonged to.
#
# E2E_DC is the docker compose command for the e2e stack. The Makefile passes it
# in, so the failure log dump reads the same compose project the tests run in.
#
# Run from the repository root. scripts/test_e2e_seed.py covers this script.
set -uo pipefail

limit=${E2E_SEED_APPLY_TIMEOUT-180}
# Digits only, at most six of them, with at least one that is not zero. perl
# reads `0`, `00` and an empty value alike as alarm(0), which turns the bound
# off. alarm also narrows its argument to a C int, so a value too large to fit
# wraps to zero or a negative number and turns the bound off too.
case "$limit" in
  '' | *[!0-9]*) limit_ok=false ;;
  *[1-9]*) limit_ok=true ;;
  *) limit_ok=false ;;
esac
if [ "${#limit}" -gt 6 ]; then
  limit_ok=false
fi
if [ "$limit_ok" != true ]; then
  echo "E2E_SEED_APPLY_TIMEOUT must be a whole number of seconds from 1 to 999999, got '$limit'" >&2
  exit 2
fi

if [ -z "${E2E_DC:-}" ]; then
  echo "E2E_DC must name the docker compose command for the e2e stack" >&2
  exit 2
fi
read -ra compose <<<"$E2E_DC"

endpoint=http://localhost:14370
localscale=http://localhost:14374

# seed_apply <label> <schemabot apply args...> runs one seed apply under the
# bound. It exits 124 when the bound ends the apply, otherwise with the apply's
# status. perl does the timing because it ships on macOS and the CI runners,
# where coreutils `timeout` is not guaranteed. It stops the apply with SIGTERM,
# then SIGKILL after a grace period, rather than through an exec'd alarm: a Go
# program ignores SIGALRM unless it asks for it. A SIGTERM or SIGINT sent to
# perl stops the apply the same way, and perl exits only once the apply has.
# perl installs its handlers before it forks, so a signal that lands while it is
# starting the apply still stops it. The child restores the default handlers
# before it execs the apply.
#
# Callers run it in the background, and it execs perl so the background PID is
# perl's: signalling the subshell instead would leave perl and the apply
# running.
seed_apply() {
  local label=$1
  shift
  exec perl -e '
    my ($limit, $label, @cmd) = @ARGV;
    my ($pid, $stopping, $timed_out) = (0, 0, 0);
    # $pid is 0 before the fork and in the child. Then stop only records the
    # request, and the parent acts on it once it has the apply PID.
    sub stop {
      return if $stopping;
      $stopping = 1;
      return unless $pid;
      kill("TERM", $pid);
      alarm(10);
    }
    $SIG{TERM} = $SIG{INT} = sub { stop() };
    $SIG{ALRM} = sub {
      if ($stopping) { kill("KILL", $pid) if $pid; return }
      $timed_out = 1;
      stop();
    };
    my $child = fork() // die "fork: $!\n";
    if ($child == 0) {
      $SIG{TERM} = $SIG{INT} = $SIG{ALRM} = "DEFAULT";
      exec(@cmd) or die "exec $cmd[0]: $!\n";
    }
    $pid = $child;
    alarm($limit);
    if ($stopping) {
      kill("TERM", $pid);
      alarm(10);
    }
    my $reaped;
    do { $reaped = waitpid($pid, 0) } while ($reaped == -1 && $!{EINTR});
    my $status = $?;
    alarm(0);
    if ($timed_out) {
      print STDERR "seed apply ($label) did not finish within ${limit}s; failing the e2e setup\n";
      exit(124);
    }
    exit(128 + ($status & 127)) if $status & 127;
    exit($status >> 8);
  ' "$limit" "$label" ./bin/schemabot apply "$@" --endpoint "$endpoint" -y --allow-unsafe -o log
}

# stop_seed_applies stops and reaps every seed apply this shell has started
# and not yet reaped. It reads them from the job table rather than from a
# variable set after the launch, so an apply started just before a signal
# arrives is still on the list.
stop_seed_applies() {
  local pids
  pids=$(jobs -p)
  [ -n "$pids" ] || return 0
  # jobs -p prints one PID per line; the unquoted expansions split them.
  kill $pids 2>/dev/null
  wait $pids
}

# seed_pair <engine> <schema dir> applies one schema to staging, then
# production. A SIGTERM stops the apply in flight and waits for it to exit, so
# whoever stops the pair knows nothing it started is still running.
seed_pair() {
  local engine=$1 schema=$2 env
  trap 'stop_seed_applies; exit 143' TERM
  for env in staging production; do
    seed_apply "$engine-$env" -s "$schema" -e "$env" &
    wait "$!" || return
  done
}

# seed_vschemas loads each example keyspace's VSchema into both LocalScale
# orgs. It stops at the first one LocalScale rejects and returns non-zero, so
# the Vitess applies never run against a keyspace without its VSchema.
seed_vschemas() {
  local ks org status
  for ks in testapp testapp_sharded; do
    [ -f "examples/vitess/schema/$ks/vschema.json" ] || continue
    for org in localscale-staging localscale-production; do
      curl -sSf -X POST "$localscale/admin/seed-vschema" \
        -H "Content-Type: application/json" \
        -d "{\"org\":\"$org\",\"database\":\"testapp-vitess\",\"keyspace\":\"$ks\",\"vschema\":$(cat "examples/vitess/schema/$ks/vschema.json")}" || {
        status=$?
        echo "seeding the $ks VSchema for $org failed (curl exit $status); failing the e2e setup" >&2
        return 1
      }
    done
  done
}

# fail_setup prints the LocalScale logs, stops and reaps the MySQL pair, and
# fails the setup.
fail_setup() {
  echo "=== LocalScale logs ==="
  "${compose[@]}" logs localscale --tail=200
  kill "$mysql_pid" 2>/dev/null
  wait "$mysql_pid"
  exit 1
}

seed_pair mysql examples/mysql/schema/testapp &
mysql_pid=$!

seed_vschemas || fail_setup
(seed_pair vitess examples/vitess/schema) || fail_setup
wait "$mysql_pid"
