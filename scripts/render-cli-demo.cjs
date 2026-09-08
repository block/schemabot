#!/usr/bin/env node
// Requires Playwright and ImageMagick. See assets/src/README-cli-demo.md.
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { execFileSync } = require('node:child_process');
const { pathToFileURL } = require('node:url');
const { chromium } = require('playwright');

async function main() {
  const root = path.resolve(__dirname, '..');
  const demos = JSON.parse(fs.readFileSync(path.join(root, 'assets/src/cli-demo.json')));
  const chrome = process.env.CHROME || '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome';
  const browser = await chromium.launch({
    headless: true,
    ...(fs.existsSync(chrome) ? { executablePath: chrome } : {}),
  });

  try {
    for (const demo of demos) {
      const tmp = fs.mkdtempSync(path.join(os.tmpdir(), demo.name + '-'));
      const page = await browser.newPage({
        viewport: { width: 1100, height: 670 },
        deviceScaleFactor: 1,
      });
      await page.goto(pathToFileURL(path.join(root, 'assets/src/cli-demo.html')).href);
      const duration = await page.evaluate(d => window.setDemo(d), demo);
      const frames = [];
      let previous;
      const fps = 8;

      for (let i = 0; i <= Math.ceil(duration * fps); i++) {
        const check = await page.evaluate(t => window.renderFrame(t), i / fps);
        if (check.overflow) throw Error(demo.name + ' content clipped at ' + i / fps);
        const png = await page.screenshot();
        if (previous && png.equals(previous)) {
          frames.at(-1).delay += 12.5;
        } else {
          const file = path.join(tmp, String(i).padStart(4, '0') + '.png');
          fs.writeFileSync(file, png);
          frames.push({ file, delay: 12.5 });
          previous = png;
        }
      }

      // One shared palette keeps terminal colors stable throughout the loop.
      const palette = path.join(tmp, 'palette.png');
      execFileSync('magick', [
        ...frames.filter((_, i) => i % 8 === 0).map(f => f.file),
        '-append', '-colors', '256', '-unique-colors', palette,
      ]);
      const output = path.join(root, 'assets', demo.name + '.gif');
      execFileSync('magick', [
        '-loop', '0',
        ...frames.flatMap(f => ['-delay', String(Math.round(f.delay)), f.file]),
        '+dither', '-remap', palette, '-layers', 'OptimizePlus', output,
      ]);
      console.log(demo.name, Math.round(duration) + 's', fs.statSync(output).size + ' bytes', 'frames:', tmp);
      await page.close();
    }
  } finally {
    await browser.close();
  }
}

main().catch(error => {
  console.error(error);
  process.exitCode = 1;
});
