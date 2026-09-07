"""Regression checks for per-document TOC depth. Run with python3 -B -m unittest discover -s scripts -p 'test_gen_doc_toc.py'."""
import runpy
import subprocess
import tempfile
import sys
import unittest
from pathlib import Path

TOC = runpy.run_path(str(Path(__file__).with_name('gen-doc-toc.py')))


class DepthTests(unittest.TestCase):
    def document(self, depth=None):
        marker = TOC['BEGIN']
        if depth is not None:
            marker = marker.replace(') -->', f'; max-depth={depth}) -->')
        return f"# Guide\n\n{marker}\n\n{TOC['END']}\n\n## Start\n### Detail\n#### Deep\n"

    def test_default_lists_only_main_sections(self):
        result = TOC['regenerate'](self.document())
        self.assertNotIn('[Detail]', result)
        self.assertNotIn('[Deep]', result)

    def test_depth_survives_regeneration(self):
        result = TOC['regenerate'](self.document(2))
        self.assertIn('- [Start](#start)', result)
        self.assertNotIn('[Detail]', result)
        self.assertIn('### Detail', result)
        self.assertIn('max-depth=2', result)
        self.assertEqual(result, TOC['regenerate'](result))

    def test_subsections_opt_in(self):
        result = TOC['regenerate'](self.document(3))
        self.assertIn('  - [Detail](#detail)', result)
        self.assertEqual(result, TOC['regenerate'](result))

    def test_deeper_opt_in(self):
        result = TOC['regenerate'](self.document(4))
        self.assertIn('    - [Deep](#deep)', result)

    def test_excluded_headings_still_count_for_anchors(self):
        result = TOC['regenerate'](self.document(2) + '\n### Repeat\n## Repeat\n')
        self.assertIn('[Repeat](#repeat-1)', result)

    def test_invalid_depth_is_rejected(self):
        for depth in ('1', '7', '20', 'two'):
            with self.subTest(depth=depth), self.assertRaises(ValueError):
                TOC['regenerate'](self.document(depth))

    def test_case_typo_is_rejected(self):
        for key in ('MAX-DEPTH', 'Max-Depth'):
            with self.subTest(key=key), self.assertRaises(ValueError):
                TOC['regenerate'](self.document(3).replace('max-depth', key))

    def test_invalid_sweep_does_not_write(self):
        with tempfile.TemporaryDirectory() as tmp:
            docs = Path(tmp) / 'docs'
            docs.mkdir()
            good = docs / 'a.md'
            good.write_text(self.document(2))
            bad = docs / 'b.md'
            bad.write_text(self.document('two'))
            before = good.read_bytes()
            script = Path(__file__).with_name('gen-doc-toc.py').resolve()
            result = subprocess.run([sys.executable, str(script)], cwd=tmp,
                                    capture_output=True, text=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn('b.md', result.stderr)
            self.assertEqual(before, good.read_bytes())
            self.assertFalse((docs / '.toc-manifest').exists())

    def test_explicit_default_passes_check(self):
        with tempfile.TemporaryDirectory() as tmp:
            docs = Path(tmp) / 'docs'
            docs.mkdir()
            (docs / 'a.md').write_text(TOC['regenerate'](self.document(2)))
            (docs / '.toc-manifest').write_text('docs/a.md\n')
            script = Path(__file__).with_name('gen-doc-toc.py').resolve()
            result = subprocess.run([sys.executable, str(script), '--check'],
                                    cwd=tmp, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_duplicate_depth_is_rejected(self):
        doc = self.document(3).replace('max-depth=3', 'max-depth=3; MAX-DEPTH=4')
        with self.assertRaisesRegex(ValueError, 'only once'):
            TOC['regenerate'](doc)

    def test_specific_setting_errors(self):
        for value, message in [('7', 'choose a heading level'),
                               ('3x', 'no spaces and an integer'),
                               ('', 'no spaces and an integer')]:
            with self.subTest(value=value), self.assertRaisesRegex(ValueError, message):
                TOC['regenerate'](self.document(value))
        with self.assertRaisesRegex(ValueError, 'must be lowercase'):
            TOC['regenerate'](self.document(3).replace('max-depth', 'MAX-DEPTH'))

    def test_broken_toc_guidance_matches_depth(self):
        script = Path(__file__).with_name('gen-doc-toc.py').resolve()
        for depth, headings in [(2, 'H2'), (4, 'H2–H4'), (6, 'H2–H6')]:
            with self.subTest(depth=depth), tempfile.TemporaryDirectory() as tmp:
                path = Path(tmp) / 'guide.md'
                doc = self.document(depth).split('## Start')[0]
                if depth < 6:
                    doc += '#' * (depth + 1) + ' Deeper section\n'
                path.write_text(doc)
                result = subprocess.run([sys.executable, str(script), str(path)],
                                        cwd=tmp, capture_output=True, text=True)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn(f'no {headings} headings', result.stdout)
                self.assertNotIn('H2–H2', result.stdout)
                if depth < 6:
                    self.assertIn(f'increase max-depth above {depth}', result.stdout)
                else:
                    self.assertNotIn('increase max-depth', result.stdout)


if __name__ == '__main__':
    unittest.main()
