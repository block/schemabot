#!/usr/bin/env bash
# Runs one e2e seed apply under a time bound, so a seed apply that stalls fails
# the setup with a named error instead of holding the job until the CI runner
# cancels it. A runner cancellation skips the failure-only log capture steps,
# and it lands before `go test` starts, so the test binary's own timeout and
# goroutine dump never fire either; failing here keeps the stall inside the path
# that collects evidence.
#
# Usage: scripts/e2e-seed-apply.sh <seconds> <label> <schemabot apply args...>
#
# Exits 124 when the bound ends the apply, otherwise with the apply's status.
# perl does the timing because it ships on macOS and the CI runners, where
# coreutils `timeout` is not guaranteed. It signals the apply with SIGTERM, then
# SIGKILL after a grace period, rather than through an exec'd alarm: a Go
# program ignores SIGALRM unless it asks for it.
set -uo pipefail

if [ "$#" -lt 3 ]; then
  echo "usage: $0 <seconds> <label> <schemabot apply args...>" >&2
  exit 2
fi
limit=$1
label=$2
shift 2

perl -e '
  my ($limit, @cmd) = @ARGV;
  my $pid = fork() // die "fork: $!\n";
  if ($pid == 0) { exec(@cmd) or die "exec $cmd[0]: $!\n" }
  my $timed_out = 0;
  $SIG{ALRM} = sub {
    if ($timed_out) { kill("KILL", $pid); return }
    $timed_out = 1;
    kill("TERM", $pid);
    alarm(10);
  };
  alarm($limit);
  waitpid($pid, 0);
  my $status = $?;
  alarm(0);
  exit(124) if $timed_out;
  exit(128 + ($status & 127)) if $status & 127;
  exit($status >> 8);
' "$limit" ./bin/schemabot apply "$@"
rc=$?

if [ "$rc" -eq 124 ]; then
  echo "seed apply ($label) did not finish within ${limit}s; failing the e2e setup" >&2
fi
exit "$rc"
