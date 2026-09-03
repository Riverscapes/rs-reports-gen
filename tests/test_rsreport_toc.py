"""Tests for RSReport dynamic table-of-contents generation."""

import tempfile
import unittest
from pathlib import Path

from util.html.RSReport import TOC_PLACEHOLDER, RSReport


def make_report(tmpdir: str) -> RSReport:
    return RSReport(
        report_name="Test",
        report_type="Test",
        report_dir=tmpdir,
    )


class TestTocExtraction(unittest.TestCase):
    def test_extracts_sections_with_h2(self):
        body = """
        <section id="alpha"><h2>Alpha Section</h2><p>x</p></section>
        <section id="beta"><h2>Beta Section</h2><p>y</p></section>
        """
        report = make_report(tempfile.mkdtemp())
        entries = report._extract_toc_entries(body)
        self.assertEqual([e["id"] for e in entries], ["alpha", "beta"])
        self.assertEqual(entries[0]["title"], "Alpha Section")

    def test_skips_toc_section_itself(self):
        body = """
        <section id="toc"><h2>Table of Contents</h2></section>
        <section id="real"><h2>Real Section</h2></section>
        """
        report = make_report(tempfile.mkdtemp())
        entries = report._extract_toc_entries(body)
        self.assertEqual([e["id"] for e in entries], ["real"])

    def test_skips_sections_without_heading(self):
        body = """
        <section id="highlight-cards"><ul><li>card</li></ul></section>
        <section id="real"><h2>Real Section</h2></section>
        """
        report = make_report(tempfile.mkdtemp())
        entries = report._extract_toc_entries(body)
        self.assertEqual([e["id"] for e in entries], ["real"])

    def test_skips_h3_subsections_but_includes_h1(self):
        body = """
        <section id="top"><h2>Top Level</h2></section>
        <section id="sub"><h3>Subsection</h3></section>
        <section id="appendices"><h1>Appendices</h1><h2>Appendix 1</h2></section>
        """
        report = make_report(tempfile.mkdtemp())
        entries = report._extract_toc_entries(body)
        self.assertEqual([e["id"] for e in entries], ["top", "appendices"])
        self.assertEqual(entries[1]["title"], "Appendices")

    def test_strips_inner_tags_and_keeps_entities(self):
        body = '<section id="elev"><h2>Elevation &amp; Drainage <sup>2</sup></h2></section>'
        report = make_report(tempfile.mkdtemp())
        entries = report._extract_toc_entries(body)
        # Inner tags removed, their text content and HTML entities preserved.
        self.assertEqual(entries[0]["title"], "Elevation &amp; Drainage 2")

    def test_commented_out_sections_are_ignored(self):
        body = """
        <section id="real"><h2>Real Section</h2></section>
        <!-- <section id="dead"><h2>Commented Out</h2></section> -->
        """
        report = make_report(tempfile.mkdtemp())
        entries = report._extract_toc_entries(body)
        self.assertEqual([e["id"] for e in entries], ["real"])

    def test_single_quotes_and_class_attrs(self):
        body = "<section id='real' class=\"appendix\">\n  <h2>Real</h2>\n</section>"
        report = make_report(tempfile.mkdtemp())
        entries = report._extract_toc_entries(body)
        self.assertEqual(entries[0]["id"], "real")

    def test_nests_h3_and_h4_under_section(self):
        body = """
        <section id="bio"><h2>Biophysical Settings</h2>
          <h3 id="hydro">Hydro Geomorphic</h3>
          <h4 id="conf">Confinement</h4>
          <h3 id="ecol">Ecological</h3>
          <h4 id="beaver">Beaver</h4>
        </section>
        """
        report = make_report(tempfile.mkdtemp())
        entries = report._extract_toc_entries(body)
        self.assertEqual(len(entries), 1)
        bio = entries[0]
        self.assertEqual(bio["id"], "bio")
        self.assertEqual([c["id"] for c in bio["children"]], ["hydro", "ecol"])
        self.assertEqual([c["id"] for c in bio["children"][0]["children"]], ["conf"])
        self.assertEqual([c["id"] for c in bio["children"][1]["children"]], ["beaver"])

    def test_h3_sibling_does_not_nest_under_previous_h3(self):
        """A later h3 is a sibling of the previous h3, never its child.

        The old manual inventory TOC got this wrong (stream-order and
        riparian-conditions were nested under unrelated h3s); the automatic TOC
        must reflect the real heading levels.
        """
        body = """
        <section id="bio"><h2>Biophysical Settings</h2>
          <h3 id="hydro">Hydro Geomorphic</h3>
          <h4 id="conf">Confinement</h4>
          <h3 id="stream-order">Stream Order</h3>
          <h3 id="ecol">Ecological</h3>
          <h4 id="beaver">Beaver</h4>
          <h2>Conditions</h2>
          <h3 id="riparian">Riparian Conditions</h3>
        </section>
        """
        report = make_report(tempfile.mkdtemp())
        entries = report._extract_toc_entries(body)
        bio = entries[0]
        top_level_ids = [c["id"] for c in bio["children"]]
        self.assertEqual(top_level_ids, ["hydro", "stream-order", "ecol", "riparian"])
        # confinement stays nested under hydro only
        self.assertEqual([c["id"] for c in bio["children"][0]["children"]], ["conf"])
        # beaver nests under ecol only
        self.assertEqual([c["id"] for c in bio["children"][2]["children"]], ["beaver"])

    def test_heading_without_id_is_skipped(self):
        """Headings with no id have no anchor to link to and are not listed."""
        body = """
        <section id="condition"><h2>Condition</h2>
          <h2>Recovery Potential</h2>
        </section>
        <section id="nid"><h2>National Inventory of Dams</h2></section>
        """
        report = make_report(tempfile.mkdtemp())
        entries = report._extract_toc_entries(body)
        self.assertEqual([e["id"] for e in entries], ["condition", "nid"])
        self.assertEqual(entries[0]["children"], [])

    def test_h1_section_nests_id_d_h2_appendices(self):
        body = """
        <section id="appendices" class="appendix"><h1>Appendices</h1>
          <h2 id="Appendix1">Appendix 1 - Projects</h2>
          <h2>Appendix 2 - Source Data</h2>
        </section>
        """
        report = make_report(tempfile.mkdtemp())
        entries = report._extract_toc_entries(body)
        self.assertEqual(len(entries), 1)
        appx = entries[0]
        self.assertEqual(appx["title"], "Appendices")
        self.assertEqual([c["id"] for c in appx["children"]], ["Appendix1"])


class TestTocRendering(unittest.TestCase):
    def test_auto_toc_html(self):
        body = '<section id="one"><h2>One</h2></section><section id="two"><h2>Two</h2></section>'
        report = make_report(tempfile.mkdtemp())
        html = report.build_toc_html(body)
        self.assertIn('id="toc"', html)
        self.assertIn('class="toc-list"', html)
        self.assertIn('<a href="#one">One</a>', html)
        self.assertIn('<a href="#two">Two</a>', html)

    def test_registered_items_win_over_extraction(self):
        body = '<section id="one"><h2>One</h2></section>'
        report = make_report(tempfile.mkdtemp())
        report.add_toc_item("one", "Custom Label")
        html = report.build_toc_html(body)
        self.assertIn('<a href="#one">Custom Label</a>', html)
        self.assertNotIn(">One</a>", html)

    def test_registered_nested_children(self):
        report = make_report(tempfile.mkdtemp())
        report.add_toc_item("parent", "Parent", children=[
            ("child-a", "Child A"),
            ("child-b", "Child B", [("grandchild", "Grandchild")]),
        ])
        html = report.build_toc_html("")
        self.assertIn('<a href="#child-a">Child A</a>', html)
        self.assertIn('<a href="#grandchild">Grandchild</a>', html)
        # two nested <ol> levels beyond the root list
        self.assertEqual(html.count("<ol"), 3)

    def test_no_entries_no_toc(self):
        report = make_report(tempfile.mkdtemp())
        self.assertEqual(report.build_toc_html("<p>no sections</p>"), "")

    def test_titles_are_escaped(self):
        report = make_report(tempfile.mkdtemp())
        report.add_toc_item("x", 'Bad <script>alert(1)</script>')
        html = report.build_toc_html("")
        self.assertNotIn("<script>", html)


class TestTocIntegration(unittest.TestCase):
    def test_render_replaces_placeholder(self):
        tmpdir = tempfile.mkdtemp()
        template_dir = Path(tmpdir) / "tpl"
        template_dir.mkdir()
        (template_dir / "body.html").write_text(
            "{{ toc }}\n<section id='a'><h2>A Section</h2></section>",
            encoding="utf-8",
        )
        report = RSReport(
            report_name="Test",
            report_type="Test",
            report_dir=tmpdir,
            body_template_path=template_dir / "body.html",
        )
        out_path = report.render(fig_mode="interactive")
        html = Path(out_path).read_text(encoding="utf-8")
        self.assertNotIn(TOC_PLACEHOLDER, html)
        self.assertIn('<a href="#a">A Section</a>', html)
        self.assertIn('class="toc-list"', html)

    def test_render_without_toc_placeholder_is_unchanged(self):
        tmpdir = tempfile.mkdtemp()
        template_dir = Path(tmpdir) / "tpl"
        template_dir.mkdir()
        (template_dir / "body.html").write_text(
            "<section id='a'><h2>A Section</h2></section>",
            encoding="utf-8",
        )
        report = RSReport(
            report_name="Test",
            report_type="Test",
            report_dir=tmpdir,
            body_template_path=template_dir / "body.html",
        )
        out_path = report.render(fig_mode="interactive")
        html = Path(out_path).read_text(encoding="utf-8")
        self.assertNotIn('class="toc-list"', html)
        self.assertIn("A Section", html)


if __name__ == "__main__":
    unittest.main()
