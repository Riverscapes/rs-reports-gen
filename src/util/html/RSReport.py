# System imports
import os
import re
from datetime import UTC, datetime
from html import escape
from pathlib import Path
from shutil import copytree
from typing import Any

import plotly.graph_objects as go
from jinja2 import Environment, FileSystemLoader
from rsxml import Logger

from util.plotly.export_figure import export_figure

#: Placeholder substituted with the generated table of contents after the body
#: template renders. Writers put ``{{ toc }}`` in their body template where the
#: table of contents should appear.
TOC_PLACEHOLDER = "<!--RS_TOC-->"

#: Section ids that are never included in an auto-generated table of contents.
_TOC_EXCLUDED_SECTION_IDS = frozenset({"toc"})

#: Heading levels considered for TOC extraction. h1/h2 mark major sections;
#: h3/h4 headings that carry their own ``id`` nest beneath their section entry.
_TOC_HEADING_RE = re.compile(r"<h([1-4])\b([^>]*)>(.*?)</h\1>", re.IGNORECASE | re.DOTALL)
_SECTION_OPEN_RE = re.compile(r"<section\b[^>]*?\bid\s*=\s*['\"]([^'\"]+)['\"]", re.IGNORECASE)
_HTML_ID_ATTR_RE = re.compile(r"\bid\s*=\s*['\"]([^'\"]+)['\"]", re.IGNORECASE)
_TAG_RE = re.compile(r"<[^>]+>")
_HTML_COMMENT_RE = re.compile(r"<!--.*?-->", re.DOTALL)


class RSReport:
    """Class to build an HTML report using Jinja2 templates and Plotly figures.
    Figures are output to `report_dir / 'figures'`
    Assets are copied from `body_template_path / 'assets'` to `report_dir / 'assets'`
    """

    def __init__(
        self,
        report_name: str,
        report_type: str,
        report_dir: Path | str,
        figure_dir: Path | str | None = None,
        body_template_path: Path | str | None = None,
        css_paths: list[Path | str] | None = None,
        report_version: str = "1.0",
        report_subtitle: str | None = None,
    ):
        """_summary_

        Args:
            report_name (str): report name / area-of-interest label, used as the page
                title (``<h1>``) when no ``report_subtitle`` is given, or as the
                document ``<title>`` element when one is.
            report_type (str): report type label, shown above the title heading.
            report_subtitle (str | None): optional subtitle (``<h2>``) rendered below
                the main heading — typically the area-of-interest name, e.g.
                ``"Missouri"`` or ``"Royal Gorge District"``. Defaults to None.
            report_dir (Path | str): path to where the report files should be put
            figure_dir (str): DEPRECATED; it will always be 'figures' under report_dir
            body_template_path (str, optional): _description_. Defaults to None.
            css_paths (list[str], optional): _description_. Defaults to None.
            version (str): report version, passed straight to the template
        """
        self.report_name = report_name
        self.report_subtitle = report_subtitle
        self.report_type = report_type
        self.report_dir = Path(report_dir)
        self.figure_dir = self.report_dir / "figures"
        self.table_dir = self.report_dir / "data"
        self.assets_dir = self.report_dir / "assets"
        self.figures = {}
        self.html_elements = {}
        self.tables: dict[str, str] = {}
        self.header_svg_path: str | None = None  # optional SVG shown in the page header

        # Optional explicitly-registered table-of-contents entries. When empty,
        # the TOC is auto-extracted from the rendered body HTML (top-level
        # sections with an h1/h2 heading, plus any id'd h3/h4 headings nested
        # beneath their section). Each item is
        # {'id': str, 'title': str, 'children': list}.
        self.toc_items: list[dict[str, Any]] = []

        self.body_template_path = body_template_path
        self.css_paths = css_paths if css_paths else []
        self._log = Logger("HTML template_builder")
        self.report_version = report_version
        self.report_dir.mkdir(parents=True, exist_ok=True)
        self.figure_dir.mkdir(parents=True, exist_ok=True)

    def add_figure(self, name: str, fig: go.Figure) -> None:
        """Add a Plotly figure to the report.

        Args:
            name (str): _description_
            fig (go.Figure): _description_
        """
        self.figures[name] = fig

    def add_html_elements(self, key: str, el: Any) -> None:
        """Add HTML elements to the report.

        Args:
            key (str): The key for the HTML element (you can reference this in the template).
            el (Any): The HTML element or data (can be str, list, dict, anything that can be represented with __str__)
        """
        self.html_elements[key] = el

    def set_header_svg(self, svg_path: Path | str) -> None:
        """Set an SVG file to display in the report page header.

        The path is stored relative to the report directory so it works
        regardless of where the report is served from.

        Args:
            svg_path (Path | str): Absolute path to the SVG file.
        """
        self.header_svg_path = os.path.relpath(svg_path, start=self.report_dir)

    def add_toc_item(self, section_id: str, title: str, children: list | None = None) -> None:
        """Register a table-of-contents entry explicitly.

        When any items are registered, they are used verbatim (in registration
        order) instead of auto-extracting sections from the body template. Use
        this for custom labels or nested TOCs.

        Args:
            section_id (str): the ``id`` of the section/element to link to (no '#').
            title (str): display text for the link.
            children (list | None): optional nested entries. Each child is either
                a ``(section_id, title)`` tuple or a ``(section_id, title, grandchildren)``
                tuple, recursively.
        """
        self.toc_items.append({"id": section_id, "title": title, "children": children or []})

    @staticmethod
    def _normalize_toc_child(child: Any) -> dict[str, Any]:
        if isinstance(child, dict):
            return {"id": child["id"], "title": child["title"], "children": child.get("children", [])}
        section_id, title, *rest = child
        return {"id": section_id, "title": title, "children": rest[0] if rest else []}

    @classmethod
    def _render_toc_list(cls, items: list[dict[str, Any]], list_class: str | None = None) -> str:
        """Render TOC items. Titles are expected to already be HTML-safe (auto-
        extracted titles are stripped tags with entities intact; registered
        titles are escaped in :meth:`build_toc_html`)."""
        cls_attr = f' class="{list_class}"' if list_class else ""
        parts = [f"<ol{cls_attr}>"]
        for item in items:
            parts.append(f'<li><a href="#{escape(item["id"], quote=True)}">{item["title"]}</a>')
            children = [cls._normalize_toc_child(c) for c in item.get("children", [])]
            if children:
                parts.append(cls._render_toc_list(children))
            parts.append("</li>")
        parts.append("</ol>")
        return "".join(parts)

    @staticmethod
    def _extract_toc_entries(body_html: str) -> list[dict[str, Any]]:
        """Auto-extract TOC entries from rendered body HTML.

        Every ``<section>`` with an ``id`` (other than ``toc``) whose first
        heading is an h1 or h2 becomes a top-level entry, linked by the section
        id and titled with that heading's text. Any later h3/h4 headings in the
        section that carry their own ``id`` nest beneath it, mirroring the
        heading hierarchy: h3 under the section, h4 under the most recent h3.
        Headings without an id (no anchor to link to) and headings at or above
        the section's title level are skipped, so the TOC never emits a dead
        link. This keeps the TOC fully automatic down to h4.
        """
        entries: list[dict[str, Any]] = []
        # Strip HTML comments first so commented-out sections are not listed.
        body_html = _HTML_COMMENT_RE.sub("", body_html)
        matches = list(_SECTION_OPEN_RE.finditer(body_html))
        for i, m in enumerate(matches):
            section_id = m.group(1)
            if section_id in _TOC_EXCLUDED_SECTION_IDS:
                continue
            # Only look for headings before the next section starts.
            boundary = matches[i + 1].start() if i + 1 < len(matches) else len(body_html)
            segment = body_html[m.end():boundary]

            headings: list[tuple[int, str | None, str]] = []
            for heading in _TOC_HEADING_RE.finditer(segment):
                level = int(heading.group(1))
                id_attr = _HTML_ID_ATTR_RE.search(heading.group(2))
                heading_id = id_attr.group(1) if id_attr else None
                # Strip any inner tags (sup/sub/span) but keep entities intact.
                title = _TAG_RE.sub("", heading.group(3)).strip()
                if title:
                    headings.append((level, heading_id, title))
            if not headings:
                continue
            top_level, _top_id, top_title = headings[0]
            if top_level not in (1, 2):
                # Section's first heading isn't a major-section heading; skip.
                continue
            entry: dict[str, Any] = {"id": section_id, "title": top_title, "children": []}
            # Stack of open nodes; the section entry is the root at top_level.
            stack: list[tuple[int, dict[str, Any]]] = [(top_level, entry)]
            for level, heading_id, title in headings[1:]:
                # Only id'd headings can be linked; ignore headings at or above
                # the section title level (e.g. a stray h2 in an h2-led section).
                if heading_id is None or level <= top_level:
                    continue
                # Pop back to the nearest ancestor whose level is below this heading.
                while len(stack) > 1 and stack[-1][0] >= level:
                    stack.pop()
                node: dict[str, Any] = {"id": heading_id, "title": title, "children": []}
                stack[-1][1]["children"].append(node)
                stack.append((level, node))
            entries.append(entry)
        return entries

    def build_toc_html(self, body_html: str) -> str:
        """Build the table-of-contents section HTML for a rendered body.

        Uses ``self.toc_items`` when any were registered via
        :meth:`add_toc_item`; otherwise auto-extracts from the body HTML.
        """
        if self.toc_items:
            # Registered titles are plain text from report code; escape them.
            def with_escaped_titles(item: dict[str, Any]) -> dict[str, Any]:
                normalized = self._normalize_toc_child(item)
                normalized["title"] = escape(normalized["title"])
                normalized["children"] = [with_escaped_titles(c) for c in normalized["children"]]
                return normalized

            items = [with_escaped_titles(i) for i in self.toc_items]
        else:
            items = self._extract_toc_entries(body_html)
        if not items:
            return ""
        return (
            '<section id="toc">\n'
            "  <h2>Table of Contents</h2>\n"
            f"  {self._render_toc_list(items, list_class='toc-list')}\n"
            "</section>"
        )

    def add_table(self, name: str, table_pl: str) -> None:
        """Add table dataframe (RSGeoDataFrame) to the report"""
        self.tables[name] = table_pl

    def set_body_template(self, template_path: str) -> None:
        """Add a body template to the report.

        Args:
            template_path (str): Path to the Jinja2 template file.
        """
        self.body_template_path = template_path

    def render(self, suffix="", fig_mode: str = "static") -> str:
        """Generate the HTML report.

        Args:
            suffix (str, optional): string to append to base report name. Defaults to "".

        Returns:
            str: output path of the report
        """
        log = Logger("template_builder")
        figure_exports = {}

        for name, fig in self.figures.items():
            figure_exports[name] = export_figure(
                fig,
                self.figure_dir,
                name,
                mode=fig_mode,
                include_plotlyjs=False,
                report_dir=self.report_dir,
            )
            # If the fig_mode is svg we also write a .png file since that's more useful for
            # people making powerpoints etc (but we are still rendering the svg in the PDF and the static HTML page)
            if fig_mode == 'svg':
                export_figure(
                    fig,
                    self.figure_dir,
                    name,
                    mode='png',
                    include_plotlyjs=False,
                    report_dir=self.report_dir,
                )

        # Use Path(__file__) for robust path resolution instead of importlib.resources
        # which can be flaky depending on how the package is run/installed
        util_templates_dir = str(Path(__file__).parent / 'templates')

        css = ""
        # Load base.css from utilities
        base_css_path = Path(__file__).parent / 'templates' / 'base.css'
        if base_css_path.exists():
            css = base_css_path.read_text(encoding='utf-8')

        for css_path in self.css_paths:
            if os.path.exists(css_path):
                css += "\n" + open(css_path, encoding="utf-8").read()
            else:
                log.warning(f"CSS path {css_path} does not exist and will be skipped.")
        style_tag = f"<style>{css}</style>"
        now = datetime.now(UTC)
        stage = os.getenv('STAGE', 'Unknown')
        # This is what is passed to each template
        report_context = {
            'report': {
                'head': style_tag,
                'title': self.report_name,
                'subtitle': self.report_subtitle,
                'header_svg': self.header_svg_path,
                'date': now.strftime('%B %d, %Y - %I:%M%p %Z'),
                'date_iso': now.isoformat(),
                'ReportType': self.report_type,
                'version': self.report_version,
                'stage': stage,
            },
            'figures': figure_exports,
            # Replaced with the generated table of contents after the body
            # renders (we need the rendered HTML to extract section headings).
            'toc': TOC_PLACEHOLDER,
            **self.html_elements,
        }

        # Prepare valid paths for loader
        search_paths = [util_templates_dir]
        if self.body_template_path and os.path.exists(self.body_template_path):
            body_template_dir = str(Path(self.body_template_path).parent)
            if body_template_dir not in search_paths:
                search_paths.insert(0, body_template_dir)  # priority to report dir

        env = Environment(loader=FileSystemLoader(search_paths))

        body = ""
        if self.body_template_path:
            body_template_path = Path(self.body_template_path)
            if body_template_path.exists():
                t_name = body_template_path.name
                body = env.get_template(t_name).render(report_context)
                if TOC_PLACEHOLDER in body:
                    # Extract from the rendered body, then substitute into the
                    # original body (the placeholder itself is an HTML comment,
                    # which extraction ignores).
                    toc_html = self.build_toc_html(body)
                    body = body.replace(TOC_PLACEHOLDER, toc_html)

                # Copy assets
                template_assets_dir = body_template_path.parent / "assets"
                if template_assets_dir.exists():
                    copytree(template_assets_dir, self.assets_dir, dirs_exist_ok=True)

        # Here is the final render. Note that we add in the body separately
        html = env.get_template('template.html').render(**report_context, body=body)

        out_path = os.path.join(self.report_dir, f"report{suffix}.html")
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(html)
        log.info(f"Report written to {out_path}")
        return out_path
