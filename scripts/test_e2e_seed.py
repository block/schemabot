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

# Records each seed apply it is asked to run, then hangs, fails, or succeeds as
# FAKE_HANG and FAKE_FAIL say. A failing seed waits until every hanging seed has
# started, so the failure always lands while a seed apply is in flight.
FAKE_SCHEMABOT = textwrap.dedent(
    """\
    #!/usr/bin/env bash
    # Invoked as: apply -s <schema dir> -e <env> ...
    case "$3" in *mysql*) engine=mysql ;; *) engine=vitess ;; esac
    label="$engine-$5"
    echo "$label" >> "$FAKE_STATE/started"
    echo $$ > "$FAKE_STATE/$label.pid"
    case " ${FAKE_HANG:-} " in *" $label "*) exec sleep 300 ;; esac
    case " ${FAKE_FAIL:-} " in
      *" $label "*)
        for hanging in ${FAKE_HANG:-}; do
          for _ in $(seq 1 100); do
            [ -f "$FAKE_STATE/$hanging.pid" ] && break
            sleep 0.1
          done
        done
        exit 1
        ;;
    esac
    exit 0
    """
)

FAKE_DOCKER = '#!/usr/bin/env bash\necho "fake docker $*"\n'
FAKE_CURL = "#!/usr/bin/env bash\nexit 0\n"


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


class E2ESeedTest(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = pathlib.Path(tmp.name)
        write_executable(self.root / "bin" / "schemabot", FAKE_SCHEMABOT)
        write_executable(self.root / "fakebin" / "docker", FAKE_DOCKER)
        write_executable(self.root / "fakebin" / "curl", FAKE_CURL)
        (self.root / "examples").symlink_to(REPO / "examples")

    def run_seed(self, timeout="30", hang="", fail=""):
        state = pathlib.Path(tempfile.mkdtemp(dir=self.root))
        env = dict(os.environ)
        env.update(
            PATH=f"{self.root / 'fakebin'}{os.pathsep}{env['PATH']}",
            E2E_SEED_APPLY_TIMEOUT=timeout,
            FAKE_STATE=str(state),
            FAKE_HANG=hang,
            FAKE_FAIL=fail,
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

    def test_vitess_staging_failure_stops_the_mysql_seed_in_flight(self):
        run = self.run_seed(hang="mysql-staging", fail="vitess-staging")
        self.assertEqual(run.returncode, 1, run.output)
        self.assertIn("=== LocalScale logs ===", run.output)
        self.assertIn("fake docker compose -f deploy/e2e/docker-compose.yml logs localscale --tail=200", run.output)
        self.assert_not_running(run, "mysql-staging")
        self.assertNotIn("mysql-production", run.started())
        self.assertNotIn("vitess-production", run.started())
        self.assertLess(run.elapsed, 20, "the seed script waited on the hanging MySQL seed instead of stopping it")

    def test_vitess_production_failure_stops_the_mysql_seed_in_flight(self):
        run = self.run_seed(hang="mysql-production", fail="vitess-production")
        self.assertEqual(run.returncode, 1, run.output)
        self.assertIn("=== LocalScale logs ===", run.output)
        self.assert_not_running(run, "mysql-production")
        self.assertIn("vitess-staging", run.started())
        self.assertLess(run.elapsed, 20, "the seed script waited on the hanging MySQL seed instead of stopping it")

    def test_mysql_failure_fails_the_setup(self):
        run = self.run_seed(fail="mysql-production")
        self.assertEqual(run.returncode, 1, run.output)
        self.assertIn("vitess-production", run.started())

    def test_a_stalled_seed_apply_fails_at_the_bound(self):
        run = self.run_seed(timeout="1", hang="vitess-staging")
        self.assertEqual(run.returncode, 1, run.output)
        self.assertIn("seed apply (vitess-staging) did not finish within 1s; failing the e2e setup", run.output)
        self.assertIn("=== LocalScale logs ===", run.output)
        self.assert_not_running(run, "vitess-staging")
        self.assertNotIn("vitess-production", run.started())

    def test_a_timeout_that_is_not_a_positive_number_is_rejected(self):
        for timeout in ["0", "", "abc", "-5", "1.5"]:
            with self.subTest(timeout=timeout):
                run = self.run_seed(timeout=timeout)
                self.assertEqual(run.returncode, 2, run.output)
                self.assertIn(
                    f"E2E_SEED_APPLY_TIMEOUT must be a positive whole number of seconds, got '{timeout}'",
                    run.output,
                )
                self.assertEqual(run.started(), [])


if __name__ == "__main__":
    unittest.main()
