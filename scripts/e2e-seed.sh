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
# When a VSchema seed or the Vitess pair fails, the MySQL pair is stopped before
# the script exits. A seed apply that does not succeed, whether the bound or a
# sibling's failure stopped it or its CLI failed, is also cancelled on the
# server and given time to settle, so no seed apply keeps changing a database
# after the setup it belonged to.
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

# Bounds on cancelling a seed apply the script stopped: how long the cancel
# command may take, and how long the apply may then take to settle.
cancel_limit=30
settle_limit=60

# Each seed apply's output goes to a file here, printed once the apply ends, so
# the script can read the apply ID back and the MySQL and Vitess output does not
# interleave.
seed_logs=$(mktemp -d "${TMPDIR:-/tmp}/e2e-seed.XXXXXX")
trap 'rm -rf "$seed_logs"' EXIT

# bounded <seconds> <description> <command...> runs a command under a time
# bound. It exits 124 when the bound ends the command, otherwise with the
# command's status. perl does the timing because it ships on macOS and the CI
# runners, where coreutils `timeout` is not guaranteed. It stops the command
# with SIGTERM, then SIGKILL after a grace period, rather than through an exec'd
# alarm: a Go program ignores SIGALRM unless it asks for it. A SIGTERM or SIGINT
# sent to perl stops the command the same way, and perl exits only once the
# command has. perl installs its handlers before it forks, so a signal that
# lands while it is starting the command still stops it. The child restores the
# default handlers before it execs the command.
#
# It execs perl, so run it in a subshell or in the background. In the
# background, the PID is perl's: signalling the subshell instead would leave
# perl and the command running.
bounded() {
  exec perl -e '
    my ($limit, $what, @cmd) = @ARGV;
    my ($pid, $stopping, $timed_out) = (0, 0, 0);
    # $pid is 0 before the fork and in the child. Then stop only records the
    # request, and the parent acts on it once it has the command PID.
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
      print STDERR "$what did not finish within ${limit}s; failing the e2e setup\n";
      exit(124);
    }
    exit(128 + ($status & 127)) if $status & 127;
    exit($status >> 8);
  ' "$@"
}

# seed_apply <label> <env> <schema dir> runs one seed apply under the bound,
# with its output in the label's log file. Callers run it in the background.
seed_apply() {
  local label=$1 env=$2 schema=$3
  bounded "$limit" "seed apply ($label)" \
    ./bin/schemabot apply -s "$schema" -e "$env" --endpoint "$endpoint" -y --allow-unsafe -o log \
    >"$seed_logs/$label.log"
}

show_seed_output() {
  local log=$seed_logs/$1.log
  [ -f "$log" ] || return 0
  echo "--- seed apply ($1) ---"
  cat "$log"
}

# seed_apply_settled <apply ID> prints "settled <state>" when the apply and
# every table under it have reached a state no driver will write again, and
# "unsettled <state>" otherwise. A settled parent alone is not proof its work
# stopped: one failed table fails the apply while a sibling table keeps copying.
# Operation rows, which a multi-deployment apply carries, are checked the same
# way. A response it cannot read counts as unsettled.
seed_apply_settled() {
  curl -sS --max-time 5 "$endpoint/api/progress/apply/$1" | perl -MJSON::PP -0777 -ne '
    sub norm { my $s = lc(shift // ""); $s =~ s/^state_//; $s }
    my %settled = map { $_ => 1 } qw(completed failed cancelled reverted);
    my $p = eval { decode_json($_) };
    if (ref $p ne "HASH") { print "unsettled unreadable"; exit }
    my $state = norm($p->{state});
    my $open = !$settled{$state}
      || grep({ !$settled{norm($_->{state})} } @{ $p->{operations} // [] })
      || grep({ !$settled{norm($_->{status})} } @{ $p->{tables} // [] });
    print $open ? "unsettled $state" : "settled $state";
  '
}

# cancel_seed_apply <label> <env> cancels a seed apply that did not succeed,
# whether the script stopped it or its CLI failed, and waits for it to settle.
# The CLI exiting only ends its watch: the apply belongs to the server, which
# keeps running it until it is cancelled through the server's durable control
# path. For an apply that has already settled, the cancel does nothing and the
# settle check passes at once.
cancel_seed_apply() {
  local label=$1 env=$2 id result deadline
  [ -n "$label" ] || return 0
  show_seed_output "$label"
  id=$(sed -n 's/^Apply started: //p' "$seed_logs/$label.log" 2>/dev/null | tail -n 1)
  if [ -z "$id" ]; then
    echo "seed apply ($label) printed no apply ID, so the script cannot tell whether the server accepted it and cannot cancel it there" >&2
    return 1
  fi
  echo "Cancelling seed apply ($label) $id on the server"
  if ! (bounded "$cancel_limit" "cancel of seed apply ($label) $id" \
    ./bin/schemabot cancel "$id" -e "$env" --endpoint "$endpoint" -y); then
    echo "cancel of seed apply ($label) $id failed; it is still running on the server" >&2
    return 1
  fi
  deadline=$((SECONDS + settle_limit))
  while :; do
    result=$(seed_apply_settled "$id")
    case "$result" in
      settled\ *)
        echo "Seed apply ($label) $id settled as ${result#settled }"
        return 0
        ;;
    esac
    if [ "$SECONDS" -ge "$deadline" ]; then
      echo "seed apply ($label) $id did not settle within ${settle_limit}s of its cancel; the server still reports '${result#unsettled }' or unfinished table work" >&2
      return 1
    fi
    sleep 1
  done
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
# production. When a seed apply does not succeed, or a SIGTERM stops the pair,
# the apply is stopped and then cancelled on the server, so whoever stops the
# pair knows nothing it started is still changing a database.
seed_pair() {
  local engine=$1 schema=$2 env status
  label='' seed_env=''
  trap 'stop_seed_applies; cancel_seed_apply "$label" "$seed_env"; exit 143' TERM
  for env in staging production; do
    label=$engine-$env seed_env=$env
    seed_apply "$label" "$env" "$schema" &
    wait "$!"
    status=$?
    if [ "$status" -ne 0 ]; then
      # The cancel runs to completion even if a SIGTERM arrives meanwhile;
      # the trap would otherwise end it before the apply settles.
      trap : TERM
      cancel_seed_apply "$label" "$env"
      return "$status"
    fi
    show_seed_output "$label"
    label=''
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
