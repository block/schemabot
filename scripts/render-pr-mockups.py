#!/usr/bin/env python3
"""Render PR timeline mock-ups for the docs from the real comment templates.

Every comment body comes verbatim from TEMPLATES.md, the snapshot the binary
regenerates and CI checks for drift. The images are manually regenerated
snapshots: run `make docs-assets` after changing the templates they show.
GitHub's own markdown API renders each body to HTML (the same renderer a
PR uses, syntax highlighting included), a thin GitHub-styled timeline frame is
wrapped around the comments, and headless Chrome screenshots the result.

Requirements: `gh` (authenticated), Google Chrome or Chromium, and ImageMagick
(`magick`) for trimming. Output lands in assets/pr-mockups/ by default.

Usage:
    scripts/render-pr-mockups.py [--scenario NAME] [--out DIR] [--list]
"""

import argparse
import html
import os
import re
import shutil
import subprocess
import sys
import tempfile

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TEMPLATES = os.path.join(REPO_ROOT, "TEMPLATES.md")
BOT_AVATAR = os.path.join(REPO_ROOT, "assets", "schemabot-avatar-200.png")
DEFAULT_OUT = os.path.join(REPO_ROOT, "assets", "pr-mockups")

# Author and repo placeholders in TEMPLATES.md are swapped for neutral ones so
# the mock-ups read as a generic PR.
SUBSTITUTIONS = {
    "@jackjackbits": "@octocat",
    "acme/myapp#42": "acme/shop#42",
}

# A frame is one screenshot of the PR timeline. Items are, in order:
#   ("bot", anchor, when)                 a SchemaBot comment, body from TEMPLATES.md
#   ("human", text, when, reactions)      an operator comment, plain text
#   ("event", text)                       a timeline event line
# `panel` is an optional box under the timeline: "checks" (every check green)
# or "merged" (the post-merge box). A frame may have items, a panel, or both.
SCENARIOS = {
    "pre-merge": [
        {"name": "plan", "items": [("bot", "mysql-plan", "2 minutes ago")]},
        {"name": "apply", "items": [
            ("human", "schemabot apply -e staging", "1 minute ago", ["👀"]),
            ("bot", "schema-change-apply-automatic", "just now"),
        ]},
        {"name": "status", "items": [("bot", "single-table-running", "just now")]},
        {"name": "applied", "items": [("bot", "summary-completed", "just now")]},
        {"name": "checks", "panel": "checks"},
        {"name": "merged", "items": [("event", "octocat merged 1 commit into main")], "panel": "merged"},
    ],
}

CSS = """
*{box-sizing:border-box}
body{margin:0;padding:24px;background:#fff;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI","Noto Sans",Helvetica,Arial,sans-serif;font-size:14px;line-height:1.5;color:#1f2328;width:860px}
.tl{position:relative;padding-left:56px}
.tl:before{content:"";position:absolute;left:19px;top:0;bottom:0;width:2px;background:#d1d9e0}
.c{position:relative;margin-bottom:16px;border:1px solid #d1d9e0;border-radius:6px;background:#fff}
.av{position:absolute;left:-56px;top:0;width:40px;height:40px;border-radius:8px;object-fit:cover;background:#fff;border:1px solid #d1d9e0}
.av.h{border-radius:50%;background:linear-gradient(135deg,#6e40c9,#2da44e)}
.hd{background:#f6f8fa;border-bottom:1px solid #d1d9e0;padding:8px 16px;border-radius:6px 6px 0 0;display:flex;align-items:center;gap:6px;color:#59636e}
.hd b{color:#1f2328;font-weight:600}
.hd .lbl{margin-left:auto;font-size:12px;border:1px solid #d1d9e0;border-radius:2em;padding:0 7px;line-height:18px;color:#59636e}
.hd .lbl.bot{color:#0969da;border-color:#54aeff66;background:#ddf4ff}
.bd{padding:16px}
.bd>h2{font-size:1.5em;margin:0 0 16px;padding-bottom:.3em;border-bottom:1px solid #d1d9e0;font-weight:600}
.bd p,.bd ul,.bd blockquote,.bd pre,.bd details,.bd table{margin:0 0 16px}
.bd p:last-child,.bd pre:last-child,.bd details:last-child{margin-bottom:0}
.bd code{font-family:ui-monospace,SFMono-Regular,"SF Mono",Menlo,Consolas,monospace;font-size:85%;background:#f0f1f3;padding:.2em .4em;border-radius:6px}
.bd pre{background:#f6f8fa;padding:16px;border-radius:6px;overflow:auto;font-size:85%;line-height:1.45}
.bd pre code{background:none;padding:0;font-size:100%}
.bd hr{height:.25em;border:0;background:#d1d9e0;margin:24px 0}
.bd blockquote{border-left:.25em solid #d1d9e0;padding:0 1em;color:#59636e}
.bd details summary{cursor:pointer}
.bd em{color:#59636e}
.bd ul{padding-left:2em}
.bd a{color:#0969da;text-decoration:none}
.bd table{border-collapse:collapse}
.bd th,.bd td{border:1px solid #d1d9e0;padding:6px 13px}
.bd th{font-weight:600;background:#f6f8fa}
.rx{display:flex;gap:6px;padding:0 16px 12px}
.rx span{border:1px solid #54aeff66;background:#ddf4ff;color:#0969da;border-radius:2em;padding:1px 8px;font-size:12px;line-height:20px}
.ev{position:relative;margin:0 0 16px 0;color:#59636e;display:flex;align-items:center;gap:8px;height:40px}
.ev .dot{position:absolute;left:-44px;width:16px;height:16px;border-radius:50%;background:#1a7f37;border:2px solid #1a7f37}
.ev .dot.m{background:#8250df;border-color:#8250df}
.box{border:1px solid #d1d9e0;border-radius:6px;margin-top:8px;overflow:hidden;margin-left:56px}
.box .row{display:flex;align-items:center;gap:10px;padding:12px 16px;border-top:1px solid #d1d9e0;font-size:14px}
.box .row:first-child{border-top:0}
.box .row.ok{background:#dafbe1;color:#1a7f37;font-weight:600}
.box .pill{margin-left:auto;color:#59636e;font-weight:400}
.box .btn{background:#1f883d;color:#fff;font-weight:600;padding:6px 16px;border-radius:6px;font-size:14px}
.box.merged{border-color:#8250df}
.box.merged .row{background:#fbefff;color:#1f2328}
.i{width:16px;height:16px;border-radius:50%;display:inline-block;flex:none}
.i.ok{background:#1a7f37}.i.m{background:#8250df}
.pl-c{color:#59636e}.pl-c1,.pl-s .pl-v{color:#0550ae}.pl-e,.pl-en{color:#6639ba}
.pl-smi,.pl-s .pl-s1{color:#1f2328}.pl-ent{color:#0a3069}.pl-k{color:#cf222e}
.pl-s,.pl-pds,.pl-s .pl-pse .pl-s1,.pl-sr,.pl-sr .pl-cce,.pl-sr .pl-sre,.pl-sr .pl-sra{color:#0a3069}
.pl-v,.pl-smw{color:#953800}.pl-bu{color:#82071e}
.pl-mh,.pl-mh .pl-en,.pl-ms{font-weight:bold;color:#0550ae}
.pl-mi{font-style:italic;color:#1f2328}.pl-mb{font-weight:bold;color:#1f2328}
.sc{border:1px solid #d1d9e0;border-radius:6px;overflow:hidden}.sc-head{display:flex;align-items:center;gap:12px;padding:20px;background:white;border-bottom:1px solid #d1d9e0}.sc-head b{font-size:18px}.sc-muted{color:#59636e}.sc-ring{width:34px;height:34px;border:4px solid #1a7f37;border-radius:50%;flex:none}.sc-right{margin-left:auto;color:#59636e}.sc-details{background:#f6f8fa;padding:16px 20px}.sc-label{font-size:12px;color:#59636e;font-weight:600;margin-bottom:12px}.sc-entry{display:flex;align-items:center;gap:8px;min-height:36px}.sc-entry img{width:20px;height:20px;border:1px solid #d1d9e0;background:white;border-radius:4px}.sc-tick{color:#1a7f37}.sc-required{margin-left:auto;border:1px solid #d1d9e0;border-radius:20px;font-size:12px;padding:0 6px;line-height:20px}.sc-foot{display:flex;align-items:center;gap:10px;border-top:1px solid #d1d9e0;padding:16px 20px;font-size:12px}.sc-button{margin-left:auto;border:1px solid #1f232826;background:#1f883d;color:white;padding:6px 12px;border-radius:6px;font-size:14px;font-weight:500}
"""

PANELS = {
    "checks": (
        '<div class="sc"><div class="sc-head"><span class="sc-ring"></span><div><b>All checks have passed</b>'
        '<div class="sc-muted">3 successful checks</div></div><span class="sc-right">⌄</span></div>'
        '<div class="sc-details"><div class="sc-label">3 successful checks ⌄</div>'
        '<div class="sc-entry"><span class="sc-tick">✓</span><span>ci / test</span><span class="sc-muted">— Successful</span></div>'
        '<div class="sc-entry"><span class="sc-tick">✓</span><img src="bot.png"><span>SchemaBot (staging)</span>'
        '<span class="sc-muted">— Successful</span><span class="sc-required">Required</span><span class="sc-muted">···</span></div>'
        '<div class="sc-entry"><span class="sc-tick">✓</span><img src="bot.png"><span>SchemaBot (production)</span>'
        '<span class="sc-muted">— Successful</span><span class="sc-required">Required</span><span class="sc-muted">···</span></div></div>'
        '<div class="sc-foot"><span class="sc-tick">✓</span>This branch has no conflicts with the base branch'
        '<span class="sc-button">Merge pull request</span></div></div>'
    ),
    "merged": (
        '<div class="box merged"><div class="row"><span class="i m"></span><b>Pull request successfully merged and closed</b>'
        '<span class="pill">You’re all set. The branch has been merged.</span></div></div>'
    ),
}


def fail(msg):
    print(f"error: {msg}", file=sys.stderr)
    sys.exit(1)


def load_sections():
    """Map every TEMPLATES.md anchor to its markdown body."""
    if not os.path.exists(TEMPLATES):
        fail(f"{TEMPLATES} not found; run 'make templates' first")
    lines = open(TEMPLATES, encoding="utf-8").read().split("\n")
    sections = {}
    i = 0
    while i < len(lines):
        m = re.match(r'<summary><a name="([^"]+)"></a><strong>.*?</strong></summary>', lines[i])
        if m:
            depth, body, j = 1, [], i + 1
            while j < len(lines):
                depth += lines[j].count("<details") - lines[j].count("</details>")
                if depth <= 0:
                    break
                body.append(lines[j])
                j += 1
            sections[m.group(1)] = "\n".join(body).strip()
            i = j
        i += 1
    return sections


def prepare(body):
    for old, new in SUBSTITUTIONS.items():
        body = body.replace(old, new)
    # <relative-time> is a GitHub web component; the API leaves it as-is.
    return re.sub(r"<relative-time[^>]*>(.*?)</relative-time>", r"\1", body)


def render_markdown(body, cache):
    if body in cache:
        return cache[body]
    with tempfile.NamedTemporaryFile("w", suffix=".md", delete=False, encoding="utf-8") as f:
        f.write(body)
        path = f.name
    try:
        out = subprocess.run(
            ["gh", "api", "/markdown", "-f", "mode=gfm", "-f", "context=block/schemabot", "-F", f"text=@{path}"],
            check=True, capture_output=True, text=True,
        ).stdout
    except FileNotFoundError:
        fail("gh not found; install GitHub CLI and run 'gh auth login'")
    except subprocess.CalledProcessError as e:
        fail(f"gh api /markdown failed: {e.stderr.strip()}")
    finally:
        os.unlink(path)
    cache[body] = out
    return out


def comment(who, body_html, bot, when, reactions=()):
    avatar = '<img class="av" src="bot.png">' if bot else '<span class="av h"></span>'
    label = '<span class="lbl bot">bot</span>' if bot else '<span class="lbl">Author</span>'
    rx = ""
    if reactions:
        rx = '<div class="rx">' + "".join(f"<span>{r} 1</span>" for r in reactions) + "</div>"
    return (
        f'<div class="c">{avatar}<div class="hd"><b>{who}</b> commented {when}{label}</div>'
        f'<div class="bd">{body_html}</div>{rx}</div>'
    )


def build_frame(frame, sections, cache):
    parts = []
    for item in frame.get("items", []):
        kind = item[0]
        if kind == "bot":
            _, anchor, when = item
            if anchor not in sections:
                fail(f"anchor {anchor!r} not found in TEMPLATES.md")
            parts.append(comment("schemabot", render_markdown(prepare(sections[anchor]), cache), True, when))
        elif kind == "human":
            _, text, when, reactions = item
            parts.append(comment("octocat", f"<p>{html.escape(text)}</p>", False, when, reactions))
        elif kind == "event":
            dot = "m" if "merged" in item[1] else ""
            parts.append(f'<div class="ev"><span class="dot {dot}"></span>{html.escape(item[1])}</div>')
        else:
            fail(f"unknown item kind {kind!r}")
    timeline = f'<div class="tl">{"".join(parts)}</div>' if parts else ""
    panel = PANELS[frame["panel"]] if frame.get("panel") else ""
    return (
        f'<!doctype html><html><head><meta charset="utf-8"><style>{CSS}</style></head>'
        f'<body>{timeline}{panel}</body></html>'
    )


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


def screenshot(chrome, html_path, png_path):
    subprocess.run(
        [chrome, "--headless=new", "--disable-gpu", "--hide-scrollbars", "--force-device-scale-factor=2",
         "--window-size=908,4000", f"--screenshot={png_path}", f"file://{html_path}"],
        check=True, capture_output=True,
    )
    magick = shutil.which("magick")
    if not magick:
        fail("ImageMagick 'magick' not found; needed to trim the screenshot")
    subprocess.run([magick, png_path, "-trim", "+repage", "-bordercolor", "white", "-border", "24", png_path], check=True)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--scenario", help="render only this scenario")
    ap.add_argument("--out", default=DEFAULT_OUT, help="output directory")
    ap.add_argument("--list", action="store_true", help="list scenarios and frames")
    args = ap.parse_args()

    if args.list:
        for name, frames in SCENARIOS.items():
            print(name)
            for n, f in enumerate(frames, 1):
                print(f"  {n}-{f['name']}")
        return

    scenarios = SCENARIOS
    if args.scenario:
        if args.scenario not in SCENARIOS:
            fail(f"unknown scenario {args.scenario!r}; see --list")
        scenarios = {args.scenario: SCENARIOS[args.scenario]}

    sections = load_sections()
    chrome = find_chrome()
    os.makedirs(args.out, exist_ok=True)
    cache = {}
    with tempfile.TemporaryDirectory() as work:
        shutil.copy(BOT_AVATAR, os.path.join(work, "bot.png"))
        for name, frames in scenarios.items():
            for n, frame in enumerate(frames, 1):
                stem = f"{name}-{n}-{frame['name']}"
                html_path = os.path.join(work, stem + ".html")
                with open(html_path, "w", encoding="utf-8") as f:
                    f.write(build_frame(frame, sections, cache))
                png_path = os.path.join(args.out, stem + ".png")
                screenshot(chrome, html_path, png_path)
                print(os.path.relpath(png_path, REPO_ROOT))


if __name__ == "__main__":
    main()
