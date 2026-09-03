# DEMO Style-Guide Harness

The demo is the **style sandbox** for Riverscapes reports. It renders a
catalog of every shared visual pattern (metric cards, highlight cards,
figures, tables, maps, error messages, page breaks) through the exact same
`RSReport` pipeline the production reports use — but with deterministic
sample data, so it runs in seconds with **no Athena access and no AWS
credentials**.

Use it for:

- **Iterating on `base.css`** — tweak shared styles and re-render in seconds
- **Trying out new Jinja macros** before adding them to `macros.html`
- **Previewing template ideas** without a real report run
- **Checking PDF behavior** (page breaks, print rules) against a known layout

## Quickstart

```bash
# via the installed script entry point (recommended)
uv run rs-report-demo

# or the repo-local wrapper
scripts/build_demo.sh

# fast iteration while tweaking CSS (skips kaleido/Chrome and WeasyPrint)
uv run rs-report-demo --html-only

# choose where the output lands (default: ./demo_output)
uv run rs-report-demo --output-dir /tmp/rs-demo
```

Outputs (written to the output dir):

- `report.html` — interactive (live Plotly charts + map)
- `report_static.html` — static (SVG figures swapped in; needs `kaleido` + Chrome)
- `report_static.pdf` — PDF from the static HTML (needs `weasyprint`)

Open `report.html` in a browser to inspect layout; open `report_static.pdf`
to check pagination and print styles.

## The iteration loop

### Live reload (recommended)

```bash
uv run rs-report-demo-live
```

This renders the demo, starts a local server (default
<http://localhost:8877>), opens your browser, and **watches every shared
input** — `base.css`, `macros.html`, `template.html`, `highlight_cards.css`,
plus the demo's own `body.html` / `demo.css` / `sample_data.py`. Save a file
and the browser reloads itself within ~1 second. If a template error sneaks
in, the traceback prints to the terminal and the server keeps serving the
last good render.

Useful flags: `--port 9000`, `--no-browser`, `--output-dir /tmp/rs-demo`.

### Manual (one-shot)

1. Edit `src/util/html/templates/base.css` (shared) or
   `src/util/html/demo/templates/demo.css` (demo-only).
2. Run `uv run rs-report-demo --html-only`.
3. Refresh the browser (or `open demo_output/report.html`).

Either way, that's the whole loop — there is nothing else to build.

## What the demo exercises

| Pattern | Where it lives | Demo section |
| --- | --- | --- |
| Header / nav / footer shell | `src/util/html/templates/template.html` | every page |
| Metric cards | `render_metric_grid` (`macros.html`) | *Metric Cards* |
| Highlight cards | `render_highlight_cards` (`macros.html`, pico + `highlight_cards.css`) | *Highlight Cards* |
| Figures (pie/bar/line) | `report.add_figure()` → `figures['name']` | *Figures* |
| Maps (Plotly, like Rivers Need Space) | `make_map_with_aoi` (`util/figures.py`) → `figures['name']` | *Maps* |
| Tables | `RSGeoDataFrame.to_html()` / pandas `to_html()` | *Tables* |
| Error messages | `.error-message` in `base.css` | *Error Messages* |
| Page breaks | `.page-break*`, `@page` print rules in `base.css` | *Page Breaks* |
| Table of contents | `{{ toc }}` placeholder (see below) | top of every demo page |
| Per-template CSS hook | `css_paths=[...]` on `RSReport` | `demo.css` in this package |

## Table of contents (auto-generated)

Never hand-write a TOC. Put the placeholder where you want it in your
`body.html`:

```html
{{ toc }}
```

`RSReport.render()` replaces it with a `<section id="toc">` built from every
`<section id="...">` whose first heading is an `h1`/`h2` — so the TOC can never
drift from the actual sections. Sections without an id, without a top-level
heading, or inside HTML comments are skipped.

For custom labels or nested TOCs, register entries in Python instead:

```python
report.add_toc_item("key-indicators", "Key Indicators")
report.add_toc_item("biophysical-settings", "Biophysical Settings", children=[
    ("hydro-geomorphic", "Hydro Geomorphic", [
        ("confinement", "Confinement"),
        ("stream-order", "Stream Order"),
    ]),
])
```

As soon as any item is registered, registration wins over auto-extraction
(see `rpt_riverscapes_inventory/main.py` for a working example).

## How the pieces fit

```text
util/html/demo/
├── build_demo.py        # rs-report-demo entry point
├── sample_data.py       # deterministic figures/tables/cards (no network)
└── templates/
    ├── body.html        # the catalog: one section per shared pattern
    └── demo.css         # demo-only styles (shows the css_paths hook)
```

`build_demo.py` constructs an `RSReport` exactly the way the real reports do:

```python
report = RSReport(
    report_name="DEMO Style Guide",
    report_type="Template Sandbox",
    report_dir=output_dir,
    body_template_path=TEMPLATE_DIR / "body.html",
    css_paths=[TEMPLATE_DIR / "demo.css"],
)
report.add_figure("ownership_pie", sample_figures()["ownership_pie"])
report.add_html_elements("tables", sample_tables())
report.add_html_elements("cards", sample_metric_cards())
report.add_html_elements("highlight_cards", sample_highlight_cards())
report.render(fig_mode="interactive")
```

If you are writing a new report, copy `build_demo.py` logic — it is the most
minimal complete example of the pipeline in the repo.

## Adding a new shared pattern

1. Add the CSS to `demo.css` (or `base.css` if it should be global).
2. Add a section for it in `templates/body.html`. Give the `<section>` an
   `id` and an `h2` heading and it appears in the demo TOC automatically.
3. If it is a macro, add it to `macros.html` and reference it here.
4. Re-run `rs-report-demo --html-only` (or watch it reload via
   `rs-report-demo-live`) and confirm it looks right in HTML **and** in the
   PDF before using it in a real report.

## Maps

The demo map uses the same shared helper as the Rivers Need Space report —
`make_map_with_aoi` in `util/figures.py` — so it inherits the brand Plotly
template and, being a normal Plotly figure, renders in **every** mode
(interactive HTML plus static SVG/PNG via kaleido for the PDF). No extra
work is needed in `body.html`; just `report.add_figure('map', fig)`.

The map needs field metadata for its columns first (real reports load it with
`define_fields()` from Athena; the demo registers a tiny stand-in and restores
the shared `RSFieldMeta` state afterwards so it never pollutes anything).

```python
from util.figures import make_map_with_aoi

fig = make_map_with_aoi(gdf, aoi_gdf)   # DGOs colored by fcode + red AOI outline
report.add_figure('map', fig)
```

Other shared map helpers in `util/figures.py`: `make_aoi_outline_map` (fast
fallback for huge datasets) and `make_point_map_with_aoi` (point features).
If a report needs a Leaflet map instead, the `util/folium/riverscapes`
helpers apply the same brand defaults to `folium.Map()` — but unlike Plotly
figures those are interactive-only and would need a static fallback.

## Troubleshooting

- **`Static render failed (need kaleido + Chrome)`** — the SVG/PNG export of
  figures needs Chrome. Run once: `uv run kaleido get_chrome`.
- **`WeasyPrint could not import some external libraries`** — you need the
  native `pango` + `gobject` libs. Install with `brew install pango` and
  re-run. (The demo handles this gracefully and skips just the PDF output.)
- **PDF fonts look wrong** — WeasyPrint needs local fonts; the Google Fonts
  fetch happens at render time. Run with a network connection once so fonts
  cache locally.
