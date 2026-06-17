/**
 * Build a function that composites the MapLibre WebGL canvas with overlays
 * (title, legend, scale bar) into a PNG data URL.
 *
 * Supports high-DPI export: reads window.devicePixelRatio so that when
 * Playwright is launched with device_scale_factor > 1, the output PNG is
 * rendered at physical-pixel resolution rather than logical-pixel resolution.
 * All overlay drawing (text, boxes, scale bar) uses ctx.scale(dpr, dpr) so
 * positions and font sizes remain visually correct at any DPI.
 *
 * @param {object} cfg
 * @param {number} cfg.width  - Logical map width in CSS pixels
 * @param {number} cfg.height - Logical map height in CSS pixels
 * @param {string|null} cfg.title
 * @param {Array} cfg.legend  - Array of [label, value] pairs
 * @param {object|null} cfg.scalebar
 * @param {string} [cfg.containerId] - CSS selector for the map container
 * @returns {function} Zero-arg function that returns a PNG data URL
 */
function buildGetMapScreenshot(cfg) {
  var logW        = cfg.width;
  var logH        = cfg.height;
  var titleText   = cfg.title;
  var legend      = cfg.legend || [];
  var sb          = cfg.scalebar;
  var containerId = cfg.containerId || '#map';

  return function getMapScreenshot() {
    var dpr   = window.devicePixelRatio || 1;
    var physW = Math.round(logW * dpr);
    var physH = Math.round(logH * dpr);

    var mapCanvas = document.querySelector(containerId + ' canvas');
    var offscreen = document.createElement('canvas');
    offscreen.width  = physW;
    offscreen.height = physH;
    var ctx = offscreen.getContext('2d');

    // Scale context so all drawing commands use logical pixels
    ctx.scale(dpr, dpr);

    // 1. Draw map tiles (source canvas is already at physical resolution)
    ctx.drawImage(mapCanvas, 0, 0, logW, logH);

    // 2. Title box (top-left)
    if (titleText) {
      ctx.font = 'bold 14px Arial';
      var tw  = ctx.measureText(titleText).width;
      var pad = 8;
      ctx.fillStyle   = 'rgba(255,255,255,0.85)';
      ctx.strokeStyle = 'rgba(0,0,0,0.3)';
      ctx.lineWidth   = 1;
      roundRect(ctx, 10, 10, tw + pad * 2, 28, 4);
      ctx.fill();
      ctx.stroke();
      ctx.fillStyle = '#000';
      ctx.fillText(titleText, 10 + pad, 10 + 20);
    }

    // 3. Statistics legend (bottom-right)
    if (legend.length > 0) {
      var headerText = 'Statistics';
      ctx.font = 'bold 12px Arial';
      var maxW = ctx.measureText(headerText).width;
      ctx.font = '12px Arial';
      for (var i = 0; i < legend.length; i++) {
        var line = legend[i][0] + ': ' + legend[i][1];
        if (ctx.measureText(line).width > maxW) {
          maxW = ctx.measureText(line).width;
        }
      }
      var pad2  = 10;
      var lineH = 18;
      var boxW  = maxW + pad2 * 2;
      var boxH  = (legend.length + 1) * lineH + pad2 * 2;
      var bx    = logW - boxW - 10;
      var by    = logH - boxH - 10;
      ctx.fillStyle   = 'rgba(255,255,255,0.85)';
      ctx.strokeStyle = 'rgba(0,0,0,0.3)';
      ctx.lineWidth   = 1;
      roundRect(ctx, bx, by, boxW, boxH, 4);
      ctx.fill();
      ctx.stroke();
      ctx.fillStyle = '#000';
      ctx.font      = 'bold 12px Arial';
      ctx.fillText(headerText, bx + pad2, by + pad2 + 12);
      ctx.font = '12px Arial';
      var ly = by + pad2 + 12 + lineH;
      for (var j = 0; j < legend.length; j++) {
        var lbl = legend[j][0];
        var val = legend[j][1];
        ctx.fillText(lbl + ': ', bx + pad2, ly);
        ctx.font = 'bold 12px Arial';
        ctx.fillText(val, bx + pad2 + ctx.measureText(lbl + ': ').width, ly);
        ctx.font = '12px Arial';
        ly += lineH;
      }
    }

    // 4. Scale bar (bottom-left)
    if (sb) {
      var barPx = sb.bar_px;
      var label = sb.label;
      var x     = sb.x;
      var y     = sb.y;
      var tickH = 8;
      ctx.strokeStyle = 'black';
      ctx.lineWidth   = 2.5;
      ctx.beginPath();
      ctx.moveTo(x, y - tickH);
      ctx.lineTo(x, y);
      ctx.lineTo(x + barPx, y);
      ctx.lineTo(x + barPx, y - tickH);
      ctx.stroke();
      ctx.fillStyle  = '#000';
      ctx.font       = '11px Arial';
      ctx.textAlign  = 'center';
      ctx.fillText(label, x + barPx / 2, y - tickH - 3);
      ctx.textAlign  = 'left';
    }

    return offscreen.toDataURL('image/png');
  }
}
