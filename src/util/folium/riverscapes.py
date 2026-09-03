"""Riverscapes-branded folium map defaults.

Folium has no global template mechanism like Plotly's ``pio.templates``, so
brand consistency comes from two cooperating layers:

1. **:func:`make_map`** — a thin factory over :class:`folium.Map` that applies
   the brand defaults unless the caller supplies a value. This is the
   recommended way to create report maps, and :func:`make_map_for_gdf` also
   frames the map to a GeoDataFrame's extent for consistent page presentation.

2. **Import-time defaults** — importing ``util.folium.riverscapes`` wraps
   ``folium.Map.__init__`` so even *direct* ``folium.Map(...)`` calls
   (including in report code that never imports this helper) pick up the same
   defaults — the folium analogue of setting Plotly's default template once.

The brand defaults are deliberately conservative:

* base tiles: ``OpenStreetMap`` with folium's own (correct) attribution,
* ``control_scale=True`` — a scale bar on every map, which report consumers
  expect.

Overriding / opting out (reports stay in control):

* Any argument passed explicitly to ``folium.Map(...)`` or ``make_map(...)``
  wins, e.g. ``make_map(tiles="CartoDB positron")`` or
  ``folium.Map(..., control_scale=False)``.
* Set ``RS_FOLIUM_DEFAULTS=0`` in the environment *before* importing this
  module to skip the import-time patch entirely.
* Or call :func:`restore_riverscapes_defaults()` to undo the patch at runtime.
"""

from __future__ import annotations

import os
from itertools import cycle

import folium

from util.brand import COLORWAY, HEADER_COLOR

#: Base tile layer used for report maps.
DEFAULT_TILES = "OpenStreetMap"
#: Attribution override. ``None`` means "let folium attach the proper
#: attribution for ``DEFAULT_TILES``"; set this if the base tiles ever change.
DEFAULT_ATTR = None  # type: str | None
#: Show the Leaflet scale control (km/mi) on every branded map.
DEFAULT_CONTROL_SCALE = True

_ORIGINAL_MAP_INIT = None  # set once by apply_riverscapes_defaults()
_PATCHED = False


def make_map(
    location=None,
    tiles=DEFAULT_TILES,
    attr=DEFAULT_ATTR,
    control_scale=DEFAULT_CONTROL_SCALE,
    **kwargs,
) -> folium.Map:
    """Create a :class:`folium.Map` with Riverscapes brand defaults applied.

    Args:
        location: ``[lat, lon]`` center, like :class:`folium.Map`.
        tiles: Base tile layer; ``DEFAULT_TILES`` unless overridden.
        attr: Tile attribution; ``DEFAULT_ATTR`` (folium auto-attribution)
            unless overridden.
        control_scale: Show the scale bar; ``DEFAULT_CONTROL_SCALE`` unless
            overridden.
        **kwargs: Everything else is passed straight to :class:`folium.Map`
            (``zoom_start``, ``width``, ``height``, ``zoom_control``, ...).

    Returns:
        A branded :class:`folium.Map`.
    """
    return folium.Map(
        location=location,
        tiles=tiles,
        attr=attr,
        control_scale=control_scale,
        **kwargs,
    )


def make_map_for_gdf(gdf, *, padding=(35, 35), **kwargs) -> folium.Map:
    """Create a branded map framed to fit the extent of *gdf*.

    Uses :class:`folium.Map`'s ``fit_bounds`` so every report map is framed
    consistently (a padding buffer around the data), regardless of how the
    caller obtained the geometry. Any reprojection needed to WGS84 is handled
    automatically.

    Args:
        gdf: GeoDataFrame (or anything with ``total_bounds`` / ``crs`` and a
            ``to_crs`` method) to frame the map around.
        padding: ``(lat_pad, lon_pad)`` padding for ``fit_bounds``.
        **kwargs: Forwarded to :func:`make_map` (``zoom_start``, ``width``,
            ``height``, ...).

    Returns:
        A branded :class:`folium.Map`.
    """
    m = make_map(**kwargs)
    if gdf is None:
        return m

    try:
        crs = getattr(gdf, "crs", None)
        if crs is not None and crs.to_epsg() != 4326:
            # Reproject to WGS84 so total_bounds are in lon/lat.
            gdf = gdf.to_crs(epsg=4326)
    except Exception:  # pragma: no cover - defensive; don't crash map creation
        pass

    minx, miny, maxx, maxy = gdf.total_bounds
    m.fit_bounds([[miny, minx], [maxy, maxx]], padding=padding)
    return m


def make_style_function(
    fill_color=HEADER_COLOR,
    edge_color=HEADER_COLOR,
    weight=2,
    fill_opacity=0.35,
):
    """Return a folium ``style_function`` that paints polygons in brand colors.

    Usable directly as the ``style_function`` argument of ``folium.GeoJson``.
    """

    def _style(_feature):
        return {
            "fillColor": fill_color,
            "color": edge_color,
            "weight": weight,
            "fillOpacity": fill_opacity,
        }

    return _style


def brand_color_cycle():
    """Iterator that repeats the Riverscapes data-series palette.

    For per-feature choropleth / marker styling across layers, e.g.:

    .. code-block:: python

        colors = brand_color_cycle()
        for name, gdf in layers.items():
            folium.GeoJson(gdf, style_function=make_style_function(
                fill_color=next(colors), fill_opacity=0.5)).add_to(m)
    """
    return cycle(COLORWAY)


def is_map_defaults_applied() -> bool:
    """True if the import-time :func:`folium.Map` default patch is active."""
    return _PATCHED


def apply_riverscapes_defaults() -> None:
    """Wrap :func:`folium.Map.__init__` with brand defaults. Idempotent.

    Only parameters the caller *omitted* are touched, so explicit arguments
    always win. Parameter names passed positionally are detected too; the one
    documented corner is a positionally-supplied ``control_scale=False`` right
    after a location tuple, which is indistinguishable from the default.
    """
    global _ORIGINAL_MAP_INIT, _PATCHED

    if _PATCHED:
        return

    _ORIGINAL_MAP_INIT = folium.Map.__init__
    try:
        import inspect

        sig = inspect.signature(folium.Map.__init__)
        param_names = list(sig.parameters)
    except (ValueError, TypeError):  # pragma: no cover - folium version drift
        return

    def _branded_map_init(self, *args, **kwargs):
        try:
            bound = sig.bind(self, *args, **kwargs)
            bound.apply_defaults()

            # Which parameters did the caller explicitly provide?
            positional = set(param_names[1 : 1 + len(args)])
            explicit = set(kwargs) | positional

            if "control_scale" not in explicit and bound.arguments.get("control_scale") is False:
                bound.arguments["control_scale"] = DEFAULT_CONTROL_SCALE
            if "tiles" not in explicit:
                tiles = bound.arguments.get("tiles")
                # Only re-point string defaults; leave TileLayer objects alone.
                if isinstance(tiles, str) and tiles != DEFAULT_TILES:
                    bound.arguments["tiles"] = DEFAULT_TILES
            if "attr" not in explicit and bound.arguments.get("attr") is None:
                bound.arguments["attr"] = DEFAULT_ATTR

            return _ORIGINAL_MAP_INIT(**dict(bound.arguments))
        except Exception:
            # Mismatched signature or bad args: behave exactly like folium would.
            return _ORIGINAL_MAP_INIT(self, *args, **kwargs)

    folium.Map.__init__ = _branded_map_init
    _PATCHED = True


def restore_riverscapes_defaults() -> None:
    """Undo :func:`apply_riverscapes_defaults` (back to stock folium)."""
    global _ORIGINAL_MAP_INIT, _PATCHED
    if _PATCHED and _ORIGINAL_MAP_INIT is not None:
        folium.Map.__init__ = _ORIGINAL_MAP_INIT
    _PATCHED = False


# ── One-shot, import-time setup ──────────────────────────────────────────────
# Mirrors the Plotly approach ("set the brand default once at import") so every
# map created after this module is imported is branded unless the caller opts
# out. Disable with RS_FOLIUM_DEFAULTS=0 in the environment.
_RS_FOLIUM_DEFAULTS_ENABLED = os.environ.get("RS_FOLIUM_DEFAULTS", "1").strip().lower() not in ("0", "false", "no", "off")
if _RS_FOLIUM_DEFAULTS_ENABLED:
    apply_riverscapes_defaults()