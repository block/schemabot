#!/usr/bin/env python3
"""Record the real wizard and first plan with disposable, preconfigured DSNs.

Requires pyte (pip install pyte), DATABASE_URL and SCHEMABOT_STORAGE_DSN pointing at demo databases.
The target must contain shop.users(id, email varchar(255)). No apply is issued.
"""
import argparse
import difflib
import errno
import fcntl
import struct
import termios
import pyte
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
env = dict(os.environ, HOME=str(work / 'home'), SCHEMABOT_PROFILE='', SCHEMABOT_ENDPOINT='', SCHEMABOT_TOKEN='', TERM='xterm-256color', COLORTERM='truecolor', CLICOLOR_FORCE='1', NO_COLOR='', COLORFGBG='0;15')
master, slave = pty.openpty()
fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack("HHHH", 24, 88, 0, 0))
screen = pyte.Screen(88, 24)
stream = pyte.Stream(screen)
process = subprocess.Popen([binary, 'init'], cwd=work, env=env, stdin=slave, stdout=slave, stderr=slave)
os.close(slave)
steps = [('Database engine', '\r'), ('Database name', 'shop\r'), ('Ready when you are', '\r')]
frames = []
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
            chunk = data.decode('utf-8', errors='replace')
            text += chunk
            if '\x1b]11;' in chunk:
                os.write(master, b'\x1b]11;rgb:ffff/ffff/ffff\x1b\\')
            stream.feed(chunk)
            visible = '\n'.join(screen.display)
            rows = []
            for row in range(screen.lines):
                spans = []
                for col in range(screen.columns):
                    cell = screen.buffer[row][col]
                    style = [cell.fg, cell.bold]
                    if spans and spans[-1]['style'] == style:
                        spans[-1]['text'] += cell.data
                    else:
                        spans.append({'style': style, 'text': cell.data})
                rows.append(spans)
            frames.append({'time': round(time.monotonic() - start, 3), 'rows': rows})
            if seen < len(steps) and steps[seen][0] in visible:
                print('Recording:', steps[seen][0], flush=True)
                time.sleep(.7 if seen < 2 else 2)
                os.write(master, steps[seen][1].encode())
                seen += 1
        if process.poll() is not None and not ready:
            break
    if process.poll() is None:
        print('Last terminal screen:', '\n'.join(screen.display), flush=True)
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
    output = {'wizard': '\n'.join(screen.display).strip(), 'wizard_frames': [f for f in frames if any('SchemaBot' in ''.join(s['text'] for s in r) for r in f['rows']) or f['time'] > 1], 'diff': ''.join(difflib.unified_diff(before.splitlines(True), after.splitlines(True), fromfile='schema/shop/users.sql', tofile='schema/shop/users.sql')), 'plan': plan.stdout, 'plan_stderr': plan.stderr}
    Path(args.output).write_text(json.dumps(output, indent=2))
    print('\n'.join(screen.display))
    print(plan.stdout)
finally:
    os.close(master)
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)
    subprocess.run([binary, 'local', 'stop', 'local'], cwd=work, env=env, capture_output=True, timeout=35)
    # Leave the private work directory for troubleshooting; no credentials are
    # copied to the committed recording (only environment-variable references).
    print('Private demo workspace:', work)
