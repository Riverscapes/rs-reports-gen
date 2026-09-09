"""Riverscapes basemap styles shared by all reports.

Report maps use the shared Plotly map helpers in :mod:`util.figures`
(``make_map_with_aoi``, ``make_aoi_outline_map``, ``make_point_map_with_aoi``)
and each accepts a ``basemap=`` argument so reports can choose which style to
draw under their data. Use a :class:`BasemapStyle` member — the values are
URLs served by the Riverscapes tile service, which Plotly's MapLibre subplot
fetches at render time (interactive HTML *and* static/PDF export), so styles
can evolve on the server without a report re-export.

Example::

    from util.basemaps import BasemapStyle
    from util.figures import make_map_with_aoi

    fig = make_map_with_aoi(gdf, aoi_gdf, basemap=BasemapStyle.TOPO)

The ``basemap=`` argument also accepts:

* a :class:`BasemapStyle` member (recommended),
* any other style URL string,
* a Plotly preset name (e.g. ``"open-street-map"``, the production default),
* a full inline MapLibre style dict.

Without a ``basemap=`` argument, production reports keep Plotly's
``"open-street-map"`` preset (unchanged behavior). The DEMO Style Guide opts
into :attr:`BasemapStyle.TOPO`.
"""

from __future__ import annotations

from enum import StrEnum


class BasemapStyle(StrEnum):
    """MapLibre vector styles hosted on the Riverscapes tile service.

    Pass a member as the ``basemap=`` argument to the shared map helpers in
    :mod:`util.figures`:

        make_map_with_aoi(gdf, aoi_gdf, basemap=BasemapStyle.TOPO)
        make_aoi_outline_map(aoi_gdf, basemap=BasemapStyle.SATELLITE)
    """

    #: Hillshaded terrain with landcover, roads and labels. The DEMO Style
    #: Guide shows this by default.
    TOPO = "https://tiles.riverscapes.net/mapStyles/topo.json"
    #: Road-focused style with labels.
    ROADS = "https://tiles.riverscapes.net/mapStyles/roads.json"
    #: Satellite / aerial imagery.
    SATELLITE = "https://tiles.riverscapes.net/mapStyles/satellite.json"


#: Default basemap for the shared map helpers when a report doesn't pass one.
#: Kept as Plotly's built-in preset so existing reports render unchanged.
DEFAULT_BASEMAP = BasemapStyle.TOPO 