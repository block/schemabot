"""Regression checks for scripts/e2e-seed.sh. Run with python3 -B -m unittest discover -s scripts -p 'test_e2e_seed.py'.

Each case runs the real seed script against a fake schemabot binary, so the
time bound, the failure paths, and the cleanup of a seed apply still in flight
are exercised without containers.
"""

import os
import pathlib
import signal
import subprocess
import tempfile
import textwrap
import time
import unittest

SCRIPTS = pathlib.Path(__file__).resolve().parent
REPO = SCRIPTS.parent
SEED = SCRIPTS / "e2e-seed.sh"

# Bound on one run of the seed script. A run that reaches it means a seed apply
# the script should have stopped kept it waiting.
RUN_DEADLINE = 30

# Records each seed apply it is asked to run and prints its apply ID the way
# the real CLI does once the server accepts it, unless FAKE_NO_ID names it.
# Then it hangs, fails, or succeeds as the FAKE_* lists say:
#   FAKE_HANG        the watch never returns.
#   FAKE_FAIL        the apply fails on the server, which settles it, and the
#                    CLI exits 1.
#   FAKE_WATCH_FAIL  the CLI exits 1 while the apply keeps running on the
#                    server.
# A failing seed waits until every hanging seed has started, so the failure
# always lands while a seed apply is in flight. A cancel of a running apply is
# recorded, so the fake progress API reports it cancelled. A cancel of a
# settled apply does nothing, as the real command does.
FAKE_SCHEMABOT = textwrap.dedent(
    """\
    #!/usr/bin/env bash
    if [ "$1" = cancel ]; then
      # Invoked as: cancel <apply id> -e <env> ...
      if grep -qx "$2" "$FAKE_STATE/settled-failed" 2>/dev/null; then
        echo "Schema change already terminal (state: failed) - nothing to cancel"
        exit 0
      fi
      echo "$2" >> "$FAKE_STATE/cancelled"
      exit 0
    fi
    # Invoked as: apply -s <schema dir> -e <env> ...
    case "$3" in *mysql*) engine=mysql ;; *) engine=vitess ;; esac
    label="$engine-$5"
    echo "$label" >> "$FAKE_STATE/started"
    echo $$ > "$FAKE_STATE/$label.pid"
    case " ${FAKE_NO_ID:-} " in *" $label "*) ;; *) echo "Apply started: apply-$label" ;; esac
    case " ${FAKE_HANG:-} " in *" $label "*) exec sleep 300 ;; esac
    wait_for_hanging_seeds() {
      for hanging in ${FAKE_HANG:-}; do
        for _ in $(seq 1 100); do
          [ -f "$FAKE_STATE/$hanging.pid" ] && break
          sleep 0.1
        done
      done
    }
    case " ${FAKE_FAIL:-} " in
      *" $label "*)
        wait_for_hanging_seeds
        echo "apply-$label" >> "$FAKE_STATE/settled-failed"
        exit 1
        ;;
    esac
    case " ${FAKE_WATCH_FAIL:-} " in
      *" $label "*)
        wait_for_hanging_seeds
        exit 1
        ;;
    esac
    exit 0
    """
)

FAKE_DOCKER = '#!/usr/bin/env bash\necho "fake docker $*"\n'
# Answers the progress API: "failed" for an apply that failed on the server,
# "cancelled" for one the fake CLI cancelled, and "running" otherwise. For an
# apply FAKE_TABLE_LAG names, the first answer after its cancel still shows a
# table running under the cancelled parent. Each answer is counted. Counts each VSchema it is asked to seed,
# and rejects them all when FAKE_CURL_FAIL is set, the way curl -f reports an
# HTTP error. Like a failing seed apply, it waits for every hanging seed to
# start before it fails.
FAKE_CURL = textwrap.dedent(
    """\
    #!/usr/bin/env bash
    url="${*: -1}"
    case "$url" in
      */api/progress/apply/*)
        id="${url##*/}"
        echo "$id" >> "$FAKE_STATE/progress-polls"
        if grep -qx "$id" "$FAKE_STATE/settled-failed" 2>/dev/null; then
          echo '{"state": "failed", "tables": [{"status": "failed"}]}'
        elif grep -qx "$id" "$FAKE_STATE/cancelled" 2>/dev/null; then
          if [ "$id" = "${FAKE_TABLE_LAG:-}" ] && [ ! -f "$FAKE_STATE/table-lag-seen" ]; then
            touch "$FAKE_STATE/table-lag-seen"
            echo '{"state": "cancelled", "tables": [{"status": "cancelled"}, {"status": "running"}]}'
          else
            echo '{"state": "cancelled", "tables": [{"status": "cancelled"}]}'
          fi
        else
          echo '{"state": "running", "tables": [{"status": "running"}]}'
        fi
        exit 0
        ;;
    esac
    echo seed-vschema >> "$FAKE_STATE/curl"
    if [ -n "${FAKE_CURL_FAIL:-}" ]; then
      for hanging in ${FAKE_HANG:-}; do
        for _ in $(seq 1 100); do
          [ -f "$FAKE_STATE/$hanging.pid" ] && break
          sleep 0.1
        done
      done
      echo "curl: (22) The requested URL returned error: 400" >&2
      exit 22
    fi
    exit 0
    """
)

# A compose command with a project flag, so a test can tell the script used
# E2E_DC rather than a compose command of its own.
E2E_DC = "docker compose -f deploy/e2e/docker-compose.yml -p seed-test"


def write_executable(path, content):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)
    path.chmod(0o755)


def process_alive(pid):
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


def stop_leftover_seeds(state):
    for pid_file in state.glob("*.pid"):
        pid = int(pid_file.read_text())
        if process_alive(pid):
            os.kill(pid, signal.SIGKILL)


class SeedRun:
    def __init__(self, returncode, output, elapsed, state):
        self.returncode = returncode
        self.output = output
        self.elapsed = elapsed
        self.state = state

    def started(self):
        path = self.state / "started"
        return path.read_text().split() if path.exists() else []

    def pid(self, label):
        return int((self.state / f"{label}.pid").read_text())

    def cancelled(self):
        path = self.state / "cancelled"
        return path.read_text().split() if path.exists() else []

    def progress_polls(self, apply_id):
        path = self.state / "progress-polls"
        return path.read_text().split().count(apply_id) if path.exists() else 0


class E2ESeedTest(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = pathlib.Path(tmp.name)
        write_executable(self.root / "bin" / "schemabot", FAKE_SCHEMABOT)
        write_executable(self.root / "fakebin" / "docker", FAKE_DOCKER)
        write_executable(self.root / "fakebin" / "curl", FAKE_CURL)
        (self.root / "examples").symlink_to(REPO / "examples")

    def run_seed(self, timeout="30", hang="", fail="", watch_fail="", no_id="", table_lag="", curl_fail="", compose=E2E_DC):
        state = pathlib.Path(tempfile.mkdtemp(dir=self.root))
        env = dict(os.environ)
        env.update(
            PATH=f"{self.root / 'fakebin'}{os.pathsep}{env['PATH']}",
            E2E_SEED_APPLY_TIMEOUT=timeout,
            E2E_DC=compose,
            FAKE_STATE=str(state),
            FAKE_HANG=hang,
            FAKE_FAIL=fail,
            FAKE_WATCH_FAIL=watch_fail,
            FAKE_NO_ID=no_id,
            FAKE_TABLE_LAG=table_lag,
            FAKE_CURL_FAIL=curl_fail,
        )
        # Output goes to a file rather than a pipe: a seed apply the script
        # failed to stop would hold a pipe open and turn the assertion below
        # into a hang.
        output = state / "output"
        self.addCleanup(stop_leftover_seeds, state)
        start = time.monotonic()
        with output.open("w") as out:
            proc = subprocess.run(
                [str(SEED)],
                cwd=self.root,
                env=env,
                stdout=out,
                stderr=subprocess.STDOUT,
                timeout=RUN_DEADLINE,
            )
        elapsed = time.monotonic() - start
        return SeedRun(proc.returncode, output.read_text(), elapsed, state)

    def assert_not_running(self, run, label):
        self.assertFalse(
            process_alive(run.pid(label)),
            f"seed apply {label} is still running after the seed script exited",
        )

    def test_every_seed_apply_succeeds(self):
        run = self.run_seed()
        self.assertEqual(run.returncode, 0, run.output)
        self.assertEqual(
            sorted(run.started()),
            ["mysql-production", "mysql-staging", "vitess-production", "vitess-staging"],
        )
        self.assertNotIn("LocalScale logs", run.output)
        self.assertEqual(run.cancelled(), [])

    def test_vitess_staging_failure_stops_the_mysql_seed_in_flight(self):
        run = self.run_seed(hang="mysql-staging", fail="vitess-staging")
        self.assertEqual(run.returncode, 1, run.output)
        self.assertIn("=== LocalScale logs ===", run.output)
        self.assertIn(f"fake {E2E_DC} logs localscale --tail=200", run.output)
        self.assert_not_running(run, "mysql-staging")
        self.assertEqual(run.cancelled(), ["apply-mysql-staging"])
        self.assertIn("Seed apply (mysql-staging) apply-mysql-staging settled as cancelled", run.output)
        self.assertNotIn("mysql-production", run.started())
        self.assertNotIn("vitess-production", run.started())
        self.assertLess(run.elapsed, 20, "the seed script waited on the hanging MySQL seed instead of stopping it")

    def test_vitess_production_failure_stops_the_mysql_seed_in_flight(self):
        run = self.run_seed(hang="mysql-production", fail="vitess-production")
        self.assertEqual(run.returncode, 1, run.output)
        self.assertIn("=== LocalScale logs ===", run.output)
        self.assert_not_running(run, "mysql-production")
        self.assertEqual(run.cancelled(), ["apply-mysql-production"])
        self.assertIn("vitess-staging", run.started())
        self.assertLess(run.elapsed, 20, "the seed script waited on the hanging MySQL seed instead of stopping it")

    # LocalScale rejects the first VSchema while the MySQL seed is still
    # running. The setup fails on that rejection, before any Vitess apply runs
    # against a keyspace without its VSchema, and stops the MySQL seed.
    def test_a_rejected_vschema_fails_the_setup_before_the_vitess_seeds(self):
        run = self.run_seed(hang="mysql-staging", curl_fail="1")
        self.assertEqual(run.returncode, 1, run.output)
        self.assertIn(
            "seeding the testapp VSchema for localscale-staging failed (curl exit 22); failing the e2e setup",
            run.output,
        )
        self.assertIn(f"fake {E2E_DC} logs localscale --tail=200", run.output)
        self.assertEqual(len((run.state / "curl").read_text().splitlines()), 1)
        self.assertNotIn("vitess-staging", run.started())
        self.assert_not_running(run, "mysql-staging")
        self.assertEqual(run.cancelled(), ["apply-mysql-staging"])
        self.assertLess(run.elapsed, 20, "the seed script waited on the hanging MySQL seed instead of stopping it")

    def test_mysql_failure_fails_the_setup(self):
        run = self.run_seed(fail="mysql-production")
        self.assertEqual(run.returncode, 1, run.output)
        self.assertIn("vitess-production", run.started())
        self.assertEqual(run.cancelled(), [], "a seed apply that failed on the server is already settled")
        self.assertIn("Seed apply (mysql-production) apply-mysql-production settled as failed", run.output)

    # The CLI's watch fails while its apply is still running on the server.
    # The script cancels the apply there and waits for it to settle, rather
    # than leaving it running after the setup fails.
    def test_a_failed_watch_cancels_the_apply_still_running_on_the_server(self):
        run = self.run_seed(watch_fail="vitess-staging")
        self.assertEqual(run.returncode, 1, run.output)
        self.assertEqual(run.cancelled(), ["apply-vitess-staging"])
        self.assertIn("Seed apply (vitess-staging) apply-vitess-staging settled as cancelled", run.output)
        self.assertNotIn("vitess-production", run.started())

    # The parent reads cancelled while one of its tables is still running. The
    # script keeps waiting until the table settles too.
    def test_a_cancelled_parent_with_a_running_table_is_not_yet_settled(self):
        run = self.run_seed(timeout="1", hang="vitess-staging", table_lag="apply-vitess-staging")
        self.assertEqual(run.returncode, 1, run.output)
        self.assertEqual(run.cancelled(), ["apply-vitess-staging"])
        self.assertEqual(run.progress_polls("apply-vitess-staging"), 2)
        self.assertIn("Seed apply (vitess-staging) apply-vitess-staging settled as cancelled", run.output)

    # The CLI is stopped before it printed an apply ID. The script says it
    # cannot tell whether the server accepted the apply, rather than claiming
    # nothing is running there.
    def test_a_stopped_apply_with_no_id_is_reported_as_unresolved(self):
        run = self.run_seed(timeout="1", hang="vitess-staging", no_id="vitess-staging")
        self.assertEqual(run.returncode, 1, run.output)
        self.assertEqual(run.cancelled(), [])
        self.assertIn(
            "seed apply (vitess-staging) printed no apply ID, so the script cannot tell whether the server accepted it and cannot cancel it there",
            run.output,
        )

    def test_a_stalled_seed_apply_fails_at_the_bound(self):
        run = self.run_seed(timeout="1", hang="vitess-staging")
        self.assertEqual(run.returncode, 1, run.output)
        self.assertIn("seed apply (vitess-staging) did not finish within 1s; failing the e2e setup", run.output)
        self.assertIn("=== LocalScale logs ===", run.output)
        self.assert_not_running(run, "vitess-staging")
        self.assertEqual(run.cancelled(), ["apply-vitess-staging"])
        self.assertIn("Seed apply (vitess-staging) apply-vitess-staging settled as cancelled", run.output)
        self.assertNotIn("vitess-production", run.started())

    # perl reads zero, an empty value, and anything too large for a C int as
    # alarm(0) or a negative alarm, both of which leave the seed unbounded.
    def test_a_timeout_that_would_not_bound_the_seed_is_rejected(self):
        for timeout in ["0", "00", "000", "", "abc", "-5", "1.5", "1000000", "2147483648", "4294967296"]:
            with self.subTest(timeout=timeout):
                run = self.run_seed(timeout=timeout)
                self.assertEqual(run.returncode, 2, run.output)
                self.assertIn(
                    f"E2E_SEED_APPLY_TIMEOUT must be a whole number of seconds from 1 to 999999, got '{timeout}'",
                    run.output,
                )
                self.assertEqual(run.started(), [])

    def test_the_largest_accepted_timeout_runs_the_seed(self):
        run = self.run_seed(timeout="999999")
        self.assertEqual(run.returncode, 0, run.output)
        self.assertEqual(len(run.started()), 4)

    def test_a_missing_compose_command_is_rejected(self):
        run = self.run_seed(compose="")
        self.assertEqual(run.returncode, 2, run.output)
        self.assertIn("E2E_DC must name the docker compose command for the e2e stack", run.output)
        self.assertEqual(run.started(), [])


if __name__ == "__main__":
    unittest.main()
