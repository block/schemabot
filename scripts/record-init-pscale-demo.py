#!/usr/bin/env python3
"""Record production wizard screens with fictional pscale responses.

Requires pyte, as does record-init-demo.py. No live account is accessed. The
recording ends at setup review, before starting a runtime or importing a schema.
"""
import json
import os
from pathlib import Path
import subprocess
import tempfile
import pyte

root = Path(__file__).resolve().parent.parent
with tempfile.TemporaryDirectory(prefix='pscale-demo-') as tmp:
    scenes = Path(tmp) / 'scenes.json'
    env = dict(os.environ, SCHEMABOT_PSCALE_DEMO=str(scenes))
    subprocess.run(['go', 'test', './pkg/cmd/commands', '-run', '^TestInitPScaleDemo$', '-count=1'], cwd=root, env=env, check=True)
    recorded = json.loads(scenes.read_text())
for scene in recorded:
    screen = pyte.Screen(88, 24)
    stream = pyte.Stream(screen)
    stream.feed(scene.pop('screen').replace('\n', '\r\n'))
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
        while spans and not spans[-1]['text'].rstrip():
            spans.pop()
        if spans:
            spans[-1]['text'] = spans[-1]['text'].rstrip()
        rows.append(spans or [{'style': ['default', False], 'text': ' '}])
    scene['rows'] = rows
path = root / 'assets/src/init-pscale-demo-recording.json'
path.write_text('{\n"engine":"vitess",\n"init_command":"schemabot init",\n"setup_scenes":[\n' + ',\n'.join(json.dumps(s, separators=(',', ':')) for s in recorded) + '\n]}\n')
print(path)
