import os
from rsxml import Logger
from util.pdf.create_pdf import make_pdf_from_html


def main():
    """Create a demo PDF report for Rivers Need Space."""
    log = Logger('create demo pdf')
    log.info("Creating demo PDF report...")
    # the html file is called DEMO.html and it's right next to this file
    make_pdf_from_html(
        html_path=os.path.join(os.path.dirname(__file__), 'DEMO.html'),
    )
    log.info("Demo PDF report created successfully.")


if __name__ == "__main__":
    main()
