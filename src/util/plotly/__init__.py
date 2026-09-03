"""Plotly helpers.

Importing :mod:`util.plotly` (or any of its submodules) registers the
Riverscapes brand template and makes it the default for every new figure —
see :mod:`util.plotly.riverscapes` for the details and how reports can
override it.
"""

from . import riverscapes  # noqa: F401  (side effect: sets the brand default template)

__all__ = ["riverscapes"]