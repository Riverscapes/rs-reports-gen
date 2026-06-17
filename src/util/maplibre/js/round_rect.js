/**
 * Draw a rounded rectangle path on a canvas context.
 * @param {CanvasRenderingContext2D} ctx
 * @param {number} x @param {number} y @param {number} w @param {number} h @param {number} r
 */
function roundRect(ctx, x, y, w, h, r) {
  ctx.beginPath();
  ctx.moveTo(x + r, y);
  ctx.arcTo(x + w, y,  x + w, y + h, r);
  ctx.arcTo(x + w, y + h, x,  y + h, r);
  ctx.arcTo(x,     y + h, x,  y,     r);
  ctx.arcTo(x,     y,     x + w, y,  r);
  ctx.closePath();
}
