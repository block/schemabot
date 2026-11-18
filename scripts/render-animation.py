#!/usr/bin/env python3
"""Render a deterministic HTML animation to an animated GIF.

The HTML file draws one frame as a pure function of t in [0,1], read from the
URL hash (#t=0.42). This script sweeps t across N frames, screenshots each with
headless Chrome, and assembles the GIF with ImageMagick. Sources live under
assets/src/, outputs under assets/.

Usage:
    scripts/render-animation.py SRC.html OUT.gif [--frames 48] [--fps 8]
                                [--width 760] [--height 362] [--hold 12]
"""

import argparse
import os
import shutil
import subprocess
import sys
import tempfile


def fail(msg):
    print(f"error: {msg}", file=sys.stderr)
    sys.exit(1)


def find_chrome():
    candidates = [os.environ.get("CHROME", "")]
    candidates += [
        "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
        "/Applications/Chromium.app/Contents/MacOS/Chromium",
    ]
    candidates += [shutil.which(n) or "" for n in ("google-chrome", "chromium", "chromium-browser", "chrome")]
    for c in candidates:
        if c and os.path.exists(c):
            return c
    fail("no Chrome/Chromium found; set CHROME=/path/to/chrome")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("src")
    ap.add_argument("out")
    ap.add_argument("--frames", type=int, default=48)
    ap.add_argument("--fps", type=int, default=8)
    ap.add_argument("--width", type=int, default=760)
    ap.add_argument("--height", type=int, default=362)
    ap.add_argument("--hold", type=int, default=12, help="extra copies of the last frame, so the end state lingers")
    args = ap.parse_args()

    chrome = find_chrome()
    magick = shutil.which("magick") or fail("ImageMagick 'magick' not found")
    src = os.path.abspath(args.src)
    if not os.path.exists(src):
        fail(f"{src} not found")

    with tempfile.TemporaryDirectory() as work:
        frames = []
        for i in range(args.frames):
            t = i / (args.frames - 1)
            png = os.path.join(work, f"f{i:04d}.png")
            subprocess.run(
                [chrome, "--headless=new", "--disable-gpu", "--hide-scrollbars", "--force-device-scale-factor=1",
                 f"--window-size={args.width},{args.height}", f"--screenshot={png}", f"file://{src}#t={t:.4f}"],
                check=True, capture_output=True,
            )
            frames.append(png)
        frames += [frames[-1]] * args.hold
        delay = str(round(100 / args.fps))
        subprocess.run(
            [magick, "-delay", delay, "-loop", "0", *frames, "-layers", "Optimize", os.path.abspath(args.out)],
            check=True,
        )
    print(f"{args.out}: {os.path.getsize(args.out) // 1024} KB, {args.frames}+{args.hold} frames at {args.fps} fps")


if __name__ == "__main__":
    main()
