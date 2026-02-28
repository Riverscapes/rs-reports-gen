# System imports
import os
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
import plotly.graph_objects as go
from jinja2 import Environment, FileSystemLoader
from rsxml import Logger

from util.plotly.export_figure import export_figure


@dataclass
class TablePayload:
    df: pd.DataFrame
    caption: str | None = None
    limit: int | None = 20
    sort_by: str | Sequence[str] | None = None
    sort_ascending: bool | Sequence[bool] | None = None


class RSReport:
    """Class to build an HTML report using Jinja2 templates and Plotly figures."""

    def __init__(
        self,
        report_name: str,
        report_type: str,
        report_dir: Path | str,
        figure_dir: Path | str,
        body_template_path: Path | str | None = None,
        css_paths: list[Path | str] | None = None,
        report_version: str = "1.0",
    ):
        """_summary_

        Args:
            report_name (str): _description_
            report_type (str): _description_
            report_dir (Path | str): path to where the report files should be put
            figure_dir (str): TODO: remove this, it will always be 'figures' under report_dir
            version (str): _description_
            body_template_path (str, optional): _description_. Defaults to None.
            css_paths (list[str], optional): _description_. Defaults to None.
        """
        self.report_name = report_name
        self.report_type = report_type
        self.report_dir = Path(report_dir)
        self.figure_dir = figure_dir
        self.table_dir = self.report_dir / "data"
        self.figures = {}
        self.html_elements = {}
        self.tables: dict[str, TablePayload] = {}
        self.body_template_path = body_template_path
        self.css_paths = css_paths if css_paths else []
        self._log = Logger("HTML template_builder")
        self.report_version = report_version
        os.makedirs(report_dir, exist_ok=True)
        os.makedirs(figure_dir, exist_ok=True)

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

    def add_table(self, name: str, table_pl: TablePayload) -> None:
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
            suffix (str, optional): _description_. Defaults to "".

        Returns:
            _type_: _description_
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

        # TODO: rethink
        for name, table in self.tables.items():
            rsdf = table.df
            table_html = rsdf.to_html(index=False, escape=False)
            self.add_html_elements(f'table-{name}', table_html)
            # add caption
            self.add_html_elements(f'table-{name}-caption', "table caption")

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
                css += "\n" + open(css_path, "r", encoding="utf-8").read()
            else:
                log.warning(f"CSS path {css_path} does not exist and will be skipped.")
        style_tag = f"<style>{css}</style>"
        now = datetime.now()

        # This is what is passed to each template
        report_context = {
            'report': {
                'head': style_tag,
                'title': self.report_name,
                'date': now.strftime('%B %d, %Y - %I:%M%p'),
                'ReportType': self.report_type,
                'version': self.report_version,
            },
            'figures': figure_exports,
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
        if self.body_template_path and os.path.exists(self.body_template_path):
            t_name = Path(self.body_template_path).name
            body = env.get_template(t_name).render(report_context)

        # Here is the final render. Note that we add in the body separately
        html = env.get_template('template.html').render(**report_context, body=body)

        out_path = os.path.join(self.report_dir, f"report{suffix}.html")
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(html)
        log.info(f"Report written to {out_path}")
        return out_path
