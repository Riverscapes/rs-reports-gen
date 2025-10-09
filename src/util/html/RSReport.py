# System imports
import os
from datetime import datetime
from importlib import resources
from typing import Any

import plotly.graph_objects as go
from rsxml import Logger
from jinja2 import Template
from util.plotly.export_figure import export_figure


class RSReport:
    """ Class to build an HTML report using Jinja2 templates and Plotly figures.
    """

    def __init__(self,
                 report_name: str,
                 report_type: str,
                 report_dir: str,
                 figure_dir: str,
                 body_template_path: str = None,
                 css_paths: list[str] = None,
                 report_version: str = "1.0"):
        """_summary_

        Args:
            report_name (str): _description_
            report_type (str): _description_
            report_dir (str): _description_
            figure_dir (str): _description_
            version (str): _description_
            body_template_path (str, optional): _description_. Defaults to None.
            css_paths (list[str], optional): _description_. Defaults to None.
        """
        self.report_name = report_name
        self.report_type = report_type
        self.report_dir = report_dir
        self.figure_dir = figure_dir
        self.figures = {}
        self.html_elements = {}
        self.body_template_path = body_template_path
        self.css_paths = css_paths if css_paths else []
        self._log = Logger("HTML template_builder")
        self.report_version = report_version
        os.makedirs(report_dir, exist_ok=True)
        os.makedirs(figure_dir, exist_ok=True)

    def add_figure(self, name: str, fig: go.Figure):
        """ Add a PLotly figure to the report.

        Args:
            name (str): _description_
            fig (go.Figure): _description_
        """
        self.figures[name] = fig

    def add_html_elements(self, key: str, el: Any) -> None:
        """ Add HTML elements to the report.

        Args:
            key (str): The key for the HTML element (you can reference this in the template).
            el (Any): The HTML element or data (can be str, list, dict, anything that can be represented with __str__)
        """
        self.html_elements[key] = el

    def set_body_template(self, template_path: str) -> str:
        """ Add a body template to the report.

        Args:
            template_path (str): Path to the Jinja2 template file.

        Returns:
            str: Rendered HTML string from the template.
        """
        self.body_template_path = template_path

    def render(self, suffix="", fig_mode: str = "static") -> str:
        """ Generate the HTML report.

        Args:
            suffix (str, optional): _description_. Defaults to "".

        Returns:
            _type_: _description_
        """
        log = Logger("template_builder")
        figure_exports = {}

        for (name, fig) in self.figures.items():
            figure_exports[name] = export_figure(
                fig,
                self.figure_dir,
                name,
                mode=fig_mode,
                include_plotlyjs=False,
                report_dir=self.report_dir
            )

        templates_pkg = resources.files(__package__).joinpath('templates')
        template = Template(templates_pkg.joinpath('template.html').read_text(encoding='utf-8'))
        css = templates_pkg.joinpath('base.css').read_text(encoding='utf-8')
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
                'version': self.report_version
            },
            'figures': figure_exports,
            **self.html_elements
        }

        body = ""
        if self.body_template_path and os.path.exists(self.body_template_path):
            body_template = Template(open(self.body_template_path, "r", encoding="utf-8").read())
            body = body_template.render(report_context)

        # Here is the final render. Note that we add in the body separately
        html = template.render(**report_context, body=body)

        out_path = os.path.join(self.report_dir, f"report{suffix}.html")
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(html)
        log.info(f"Report written to {out_path}")
        return out_path
