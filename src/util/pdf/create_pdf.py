import os
from typing import Optional
import weasyprint


def make_pdf_from_html(
    html_path: str,
    pdf_path: Optional[str] = None,
    page_margin: str = "0.1in",
    zoom: float = 1.0,
    extra_styles: Optional[list[weasyprint.CSS]] = None,
) -> str:
    """Generate a PDF from an HTML file using WeasyPrint with layout controls.

    Args:
        html_path: Path to the source HTML document.
        page_margin: CSS margin value injected into the `@page` rule.
        zoom: Zoom factor passed to WeasyPrint's renderer (1.0 = 100%).

    Returns:
        Path to the generated PDF file.
    """
    pdf_path_final = pdf_path if pdf_path else os.path.splitext(html_path)[0] + ".pdf"
    margin_css = weasyprint.CSS(
        string=(
            "@page { margin: %s; } "
            "body { margin: 0 !important; padding: 0 !important; }"
        ) % page_margin,
        media_type="print",
    )

    stylesheets = [margin_css]

    if extra_styles:
        stylesheets.extend(extra_styles)

    weasyprint.HTML(filename=html_path, base_url=os.path.dirname(html_path)).write_pdf(
        pdf_path_final,
        stylesheets=stylesheets,
        zoom=zoom,
        presentational_hints=True,
    )
    return pdf_path_final
