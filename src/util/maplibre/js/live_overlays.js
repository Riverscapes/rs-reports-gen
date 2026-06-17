/**
 * Add MapLibre IControl overlays (scale bar, title, statistics legend) to the map.
 * Called inside map.on('load', ...).
 * @param {maplibregl.Map} map
 * @param {{ title: string|null, legend: [string,string][], unitSystem: string }} cfg
 */
function setupLiveOverlays(map, cfg) {
  const titleText  = cfg.title;
  const legend     = cfg.legend || [];
  const unitSystem = cfg.unitSystem || 'metric';

  // Scale bar (MapLibre built-in)
  map.addControl(
    new maplibregl.ScaleControl({
      maxWidth: 150,
      unit: unitSystem === 'imperial' ? 'imperial' : 'metric',
    }),
    'bottom-left'
  );

  // Title (top-left custom control)
  if (titleText) {
    const el = document.createElement('div');
    el.style.cssText = [
      'background: rgba(255,255,255,0.85)',
      'border: 1px solid rgba(0,0,0,0.3)',
      'border-radius: 4px',
      'padding: 6px 10px',
      'font: bold 14px/1.4 Arial, sans-serif',
      'pointer-events: none',
      'max-width: 280px',
    ].join(';');
    el.textContent = titleText;
    map.addControl({ onAdd: () => el, onRemove: () => el.remove() }, 'top-left');
  }

  // Statistics legend (bottom-right custom control)
  if (legend.length > 0) {
    const container = document.createElement('div');
    container.style.cssText = [
      'background: rgba(255,255,255,0.85)',
      'border: 1px solid rgba(0,0,0,0.3)',
      'border-radius: 4px',
      'padding: 8px 12px',
      'font: 12px/1.5 Arial, sans-serif',
      'pointer-events: none',
      'max-width: 220px',
    ].join(';');

    const header = document.createElement('div');
    header.style.cssText = 'font-weight:bold;margin-bottom:4px';
    header.textContent = 'Statistics';
    container.appendChild(header);

    for (const [lbl, val] of legend) {
      const row = document.createElement('div');
      const lblNode = document.createTextNode(lbl + ': ');
      const valSpan = document.createElement('strong');
      valSpan.textContent = val;
      row.appendChild(lblNode);
      row.appendChild(valSpan);
      container.appendChild(row);
    }

    map.addControl({ onAdd: () => container, onRemove: () => container.remove() }, 'bottom-right');
  }
}
