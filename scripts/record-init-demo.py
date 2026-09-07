#!/usr/bin/env python3
"""Record the real wizard and first plan with disposable, preconfigured DSNs.

Requires DATABASE_URL and SCHEMABOT_STORAGE_DSN pointing at demo databases.
The target must contain shop.users(id, email varchar(255)). No apply is issued.
"""
import argparse
import difflib
import errno
import json
import os
from pathlib import Path
import pty
import select
import subprocess
import tempfile
import time

parser = argparse.ArgumentParser()
parser.add_argument('--binary', required=True)
parser.add_argument('--output', required=True)
args = parser.parse_args()
binary = str(Path(args.binary).resolve())
work = Path(tempfile.mkdtemp(prefix='schemabot-init-demo-'))
(work / 'home').mkdir()
env = dict(os.environ, HOME=str(work / 'home'), SCHEMABOT_PROFILE='', SCHEMABOT_ENDPOINT='', SCHEMABOT_TOKEN='', TERM='xterm-256color')
master, slave = pty.openpty()
process = subprocess.Popen([binary, 'init'], cwd=work, env=env, stdin=slave, stdout=slave, stderr=slave)
os.close(slave)
steps = [('Database engine', 'mysql'), ('Database name:', 'shop'), ('Environment [', ''), ('Database connection variable', ''), ('Separate state database variable', ''), ('Namespaces (', ''), ('Schema directory [', ''), ('Connection profile [', ''), ('Continue?', 'y')]
events = []
text = ''
seen = 0
start = time.monotonic()
try:
    while time.monotonic() - start < 90:
        ready, _, _ = select.select([master], [], [], .1)
        if ready:
            try:
                data = os.read(master, 65536)
            except OSError as exc:
                if exc.errno == errno.EIO:
                    break
                raise
            if not data:
                break
            chunk = data.decode('utf-8', errors='replace').replace('\r\n', '\n')
            text += chunk
            events.append({'time': round(time.monotonic() - start, 3), 'text': chunk})
            if seen < len(steps) and steps[seen][0] in text:
                time.sleep(.12)
                os.write(master, (steps[seen][1] + '\n').encode())
                seen += 1
        if process.poll() is not None and not ready:
            break
    process.wait(timeout=5)
    if process.returncode:
        raise RuntimeError(text)
    schema = work / 'schema/shop/users.sql'
    before = schema.read_text()
    after = before.replace('varchar(255)', 'varchar(320)')
    if before == after:
        raise RuntimeError('demo table does not have the expected email column')
    schema.write_text(after)
    plan = subprocess.run([binary, 'plan', '-s', 'schema', '-e', 'development'], cwd=work, env=env, text=True, capture_output=True, check=True)
    output = {'wizard': text, 'events': events, 'diff': ''.join(difflib.unified_diff(before.splitlines(True), after.splitlines(True), fromfile='schema/shop/users.sql', tofile='schema/shop/users.sql')), 'plan': plan.stdout, 'plan_stderr': plan.stderr}
    Path(args.output).write_text(json.dumps(output, indent=2))
    print(text)
    print(plan.stdout)
finally:
    os.close(master)
    if process.poll() is None:
        process.terminate()
        process.wait(timeout=5)
    subprocess.run([binary, 'local', 'stop', 'local'], cwd=work, env=env, capture_output=True, timeout=35)
    # Leave the private work directory for troubleshooting; no credentials are
    # copied to the committed recording (only environment-variable references).
    print('Private demo workspace:', work)
