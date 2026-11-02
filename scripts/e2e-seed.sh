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
# When the Vitess pair fails, the MySQL pair is stopped and reaped before the
# script exits, so no seed apply outlives the setup it belonged to.
#
# Run from the repository root. scripts/test_e2e_seed.py covers this script.
set -uo pipefail

limit=${E2E_SEED_APPLY_TIMEOUT-180}
case "$limit" in
  '' | *[!0-9]* | 0)
    echo "E2E_SEED_APPLY_TIMEOUT must be a positive whole number of seconds, got '$limit'" >&2
    exit 2
    ;;
esac

endpoint=http://localhost:14370
localscale=http://localhost:14374

# seed_apply <label> <schemabot apply args...> runs one seed apply under the
# bound. It exits 124 when the bound ends the apply, otherwise with the apply's
# status. perl does the timing because it ships on macOS and the CI runners,
# where coreutils `timeout` is not guaranteed. It stops the apply with SIGTERM,
# then SIGKILL after a grace period, rather than through an exec'd alarm: a Go
# program ignores SIGALRM unless it asks for it. A SIGTERM or SIGINT sent to
# perl stops the apply the same way, and perl exits only once the apply has.
#
# Callers run it in the background, and it execs perl so the background PID is
# perl's: signalling the subshell instead would leave perl and the apply
# running.
seed_apply() {
  local label=$1
  shift
  exec perl -e '
    my ($limit, $label, @cmd) = @ARGV;
    my $pid = fork() // die "fork: $!\n";
    if ($pid == 0) { exec(@cmd) or die "exec $cmd[0]: $!\n" }
    my ($stopping, $timed_out) = (0, 0);
    sub stop {
      return if $stopping;
      $stopping = 1;
      kill("TERM", $pid);
      alarm(10);
    }
    $SIG{TERM} = $SIG{INT} = sub { stop() };
    $SIG{ALRM} = sub {
      if ($stopping) { kill("KILL", $pid); return }
      $timed_out = 1;
      stop();
    };
    alarm($limit);
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

# seed_pair <engine> <schema dir> applies one schema to staging, then
# production. A SIGTERM stops the apply in flight and waits for it to exit, so
# whoever stops the pair knows nothing it started is still running.
seed_pair() {
  local engine=$1 schema=$2 env
  in_flight=
  trap 'if [ -n "$in_flight" ]; then kill "$in_flight" 2>/dev/null; wait "$in_flight"; fi; exit 143' TERM
  for env in staging production; do
    seed_apply "$engine-$env" -s "$schema" -e "$env" &
    in_flight=$!
    wait "$in_flight" || return
    in_flight=
  done
}

seed_vschemas() {
  local ks org
  for ks in testapp testapp_sharded; do
    [ -f "examples/vitess/schema/$ks/vschema.json" ] || continue
    for org in localscale-staging localscale-production; do
      curl -sf -X POST "$localscale/admin/seed-vschema" \
        -H "Content-Type: application/json" \
        -d "{\"org\":\"$org\",\"database\":\"testapp-vitess\",\"keyspace\":\"$ks\",\"vschema\":$(cat "examples/vitess/schema/$ks/vschema.json")}"
    done
  done
}

seed_pair mysql examples/mysql/schema/testapp &
mysql_pid=$!

seed_vschemas

if ! (seed_pair vitess examples/vitess/schema); then
  echo "=== LocalScale logs ==="
  docker compose -f deploy/e2e/docker-compose.yml logs localscale --tail=200
  kill "$mysql_pid" 2>/dev/null
  wait "$mysql_pid"
  exit 1
fi
wait "$mysql_pid"
