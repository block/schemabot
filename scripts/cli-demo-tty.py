#!/usr/bin/env python3
"""Give a local CLI demo terminal input without opening the user's terminal."""
import os
import pty
import subprocess
import sys


def main():
    master, slave = pty.openpty()
    try:
        # Confirmation is buffered until the command reads it. Keep the master
        # open so the automatic watcher can use the same terminal afterward.
        # A command that asks for no confirmation is given no input, since the
        # watcher would read it as key presses.
        answer = os.environ.get("CLI_DEMO_INPUT", "yes\n")
        if answer:
            os.write(master, answer.encode())
        result = subprocess.run(
            sys.argv[1:], stdin=slave, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, timeout=20, check=False,
        )
        sys.stdout.buffer.write(result.stdout)
        sys.stderr.buffer.write(result.stderr)
        return result.returncode
    finally:
        os.close(slave)
        os.close(master)


if __name__ == "__main__":
    sys.exit(main())
