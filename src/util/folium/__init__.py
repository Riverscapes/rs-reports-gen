"""Folium map helpers.

Importing :mod:`util.folium` (or any of its submodules) applies the
Riverscapes brand defaults to every :class:`folium.Map` created afterwards —
see :mod:`util.folium.riverscapes` for the details and how reports can
override them.
"""

from . import riverscapes  # noqa: F401  (side effect: brand defaults for folium maps)

__all__ = ["riverscapes"]