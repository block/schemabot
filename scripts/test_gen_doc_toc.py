"""Regression checks for per-document TOC depth. Run with python3 -m unittest discover -s scripts -p 'test_gen_doc_toc.py'."""
import runpy
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


if __name__ == '__main__':
    unittest.main()
