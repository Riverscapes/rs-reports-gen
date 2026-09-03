"""Smoke tests for the DEMO style-guide report (maps section)."""

import importlib
import re
from pathlib import Path

from util.html.demo.build_demo import DEMO_BODY, DEMO_CSS, build_demo
from util.html.demo.sample_data import sample_figures
from util.html.RSReport import RSReport

# ``import util.html.RSReport as ...`` would resolve to the class (shadowed by
# util.html.__init__), so grab the *module* explicitly for monkeypatching.
RSReport_module = importlib.import_module("util.html.RSReport")


def test_make_sample_aoi_map_is_plotly_map_like_rivers_need_space():
    """The demo map goes through util.figures.make_map_with_aoi (same as RNS)."""
    from util.html.demo.sample_data import make_sample_aoi_map

    fig = make_sample_aoi_map()
    trace_types = [t.type for t in fig.data]
    assert "choroplethmap" in trace_types  # DGO polygons colored by flow type
    assert "scattermap" in trace_types  # AOI outline trace
    assert fig.layout.map.style == "open-street-map"
    # Brand template attached (navy colorway baked into the figure).
    assert "#003166" in fig.to_json()


def test_interactive_demo_embeds_plotly_map(tmp_path):
    outputs = build_demo(tmp_path, html_only=True)
    html = Path(outputs[0]).read_text(encoding="utf-8")

    # The new Maps section exists and is TOC-visible (it has an id + h2).
    assert 'id="maps"' in html
    assert "<h2>Maps</h2>" in html
    assert 'href="#maps"' in html

    # The map is a normal Plotly figure embed (choroplethmap + scattermap),
    # with brand navy baked in — not a folium iframe.
    assert "choroplethmap" in html
    assert "scattermap" in html
    assert "open-street-map" in html
    assert "#003166" in html
    assert "leaflet" not in html
    assert "map-static-art" not in html


def test_static_demo_exports_map_as_static_image(monkeypatch, tmp_path):
    def fake_export(fig, out_dir, name, mode, include_plotlyjs=False, report_dir=None):  # noqa: ARG001
        return f'<img src="{name}.{mode}">'

    # Avoid kaleido/Chrome for this test: only the figure -> export mapping matters.
    monkeypatch.setattr(RSReport_module, "export_figure", fake_export)

    from util.html.demo.sample_data import sample_highlight_cards, sample_metric_cards, sample_tables

    report = RSReport(
        report_name="Static branch check",
        report_type="Test",
        report_dir=tmp_path,
        body_template_path=DEMO_BODY,
        css_paths=[DEMO_CSS],
    )
    for name, fig in sample_figures().items():
        report.add_figure(name, fig)
    report.add_html_elements("tables", sample_tables())
    report.add_html_elements("cards", sample_metric_cards())
    report.add_html_elements("highlight_cards", sample_highlight_cards())

    out = report.render(fig_mode="svg", suffix="_static")
    html = Path(out).read_text(encoding="utf-8")

    # Static/PDF builds swap the map for a static image like every other figure.
    assert '<img src="aoi_map.svg">' in html
    assert "choroplethmap" not in html


def test_interactive_demo_has_grid_and_float_sections(tmp_path):
    """The Grids & Floats sections exist and are TOC-visible (id + h2)."""
    outputs = build_demo(tmp_path, html_only=True)
    html = Path(outputs[0]).read_text(encoding="utf-8")

    for section_id, heading in (("grids", "Grids: Side-by-Side Content"), ("floats", "Floats: Text Wrapping Around Figures")):
        assert f'id="{section_id}"' in html
        assert f"<h2>{heading}</h2>" in html
        assert f'href="#{section_id}"' in html

    # Grid uses Pico's .grid class; floats use the base.css utilities.
    assert 'class="grid"' in html
    assert 'class="grid grid-2-1"' in html
    assert 'class="report-figure float-right"' in html
    assert 'class="report-figure float-left"' in html
    assert 'class="clearfix"' in html


def test_sample_figures_includes_aoi_map():
    figures = sample_figures()
    assert "aoi_map" in figures


def test_reused_figures_get_unique_plot_div_ids(tmp_path):
    """A figure embedded more than once must render each copy, not just the first.

    pio.to_html derives the container id from a hash of the figure, so reusing
    one exported fragment (e.g. the bar chart in the Figures section AND the
    "Figure next to text" grid) collided: both scripts targeted the first div
    and the later embeds rendered empty. render_figure stamps a fresh id per
    embed; the rendered demo must therefore have one unique div id per embed.
    """
    outputs = build_demo(tmp_path, html_only=True)
    html = Path(outputs[0]).read_text(encoding="utf-8")

    ids = re.findall(r'<div id="([^"]+)" class="plotly-graph-div"', html)
    # 3 figures in the Figures section + bar reused in Grids + pie reused in
    # Floats + the map = 6 embeds, all with distinct ids.
    assert len(ids) == 6
    assert len(set(ids)) == len(ids), f"duplicate plotly div ids: {ids}"

    # Each embed's script must target its own div (not a shared first one).
    targets = re.findall(r'Plotly\.newPlot\(\s*"([^"]+)"', html)
    assert len(targets) == 6
    assert len(set(targets)) == len(targets)
    assert set(targets) == set(ids)


def test_unique_plot_fragment_rewrites_id_and_passes_static_through():
    from util.plotly.export_figure import unique_plot_fragment

    # Real plotly container ids are 36-char uuids; the rewriter is strict about
    # that shape so it never rewrites unrelated ids from the figure json.
    old_id = "39a98783-ddad-48af-95f4-40bb0fbdddd6"
    fragment = f'<div id="{old_id}" class="plotly-graph-div"></div><script>if (document.getElementById("{old_id}")) {{ Plotly.newPlot("{old_id}", []) }}</script>'
    out = unique_plot_fragment(fragment)
    assert old_id not in out
    new_id = re.search(r'<div id="([^"]+)" class="plotly-graph-div"', out).group(1)
    # Id, getElementById and newPlot references all point at the new id.
    assert out.count(f'"{new_id}"') == 3
    # Two rewrites of the same source never collide.
    assert unique_plot_fragment(fragment) != out

    # Static export fragments (SVG/PNG <img>) pass through untouched.
    static = '<img src="bar.svg">'
    assert unique_plot_fragment(static) == static

    # A non-plotly fragment is left alone too.
    plain = '<p>hello</p>'
    assert unique_plot_fragment(plain) == plain
