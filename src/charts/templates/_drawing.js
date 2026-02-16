// ============================================================================
// DRAWING TOOLS ENGINE for TradingView Lightweight Charts v4.1.1
// ============================================================================

// Tool definitions: how many points each tool needs
var TOOL_POINTS = {
  trendline:2, ray:2, extended:2, hline:1, vline:1, crossline:1,
  fib_retrace:2, fib_extension:2, parallel_channel:3
};

var DRAWING_COLORS = [
  '#58a6ff','#79c0ff','#3fb950','#f0883e',
  '#f85149','#e5c07b','#c792ea','#a371f7'
];

var FIB_LEVELS = [0, 0.236, 0.382, 0.5, 0.618, 0.786, 1.0];
var FIB_EXT_LEVELS = [0, 0.236, 0.382, 0.5, 0.618, 0.786, 1.0, 1.272, 1.618];
var FIB_ZONE_COLORS = [
  'rgba(88,166,255,0.06)','rgba(88,166,255,0.04)','rgba(88,166,255,0.06)',
  'rgba(88,166,255,0.04)','rgba(88,166,255,0.06)','rgba(88,166,255,0.04)',
  'rgba(88,166,255,0.06)','rgba(88,166,255,0.04)'
];

// ============================================================================
// Drawing data model
// ============================================================================
function Drawing(type, points, style) {
  this.id = 'drw_' + Date.now() + '_' + Math.random().toString(36).substr(2,6);
  this.type = type;
  this.points = points;
  this.style = style || {};
}
Drawing.prototype.toJSON = function() {
  return {id:this.id, type:this.type, points:this.points, style:this.style};
};
Drawing.fromJSON = function(obj) {
  var d = new Drawing(obj.type, obj.points, obj.style);
  d.id = obj.id;
  return d;
};

// ============================================================================
// DrawingManager — one per chart panel
// ============================================================================
function DrawingManager(chart, candleSeries, container, symbolGetter, barsGetter, layoutIdGetter) {
  this.chart = chart;
  this.candleSeries = candleSeries;
  this.container = container;
  this.getSymbol = symbolGetter;
  this.getBars = barsGetter;
  this.getLayoutId = layoutIdGetter || function(){ return 'default'; };
  this.drawings = [];
  this.activeTool = null;
  this.state = 'IDLE';
  this.currentPoints = [];
  this.selectedDrawing = null;
  this.magnetMode = false;
  this.canvas = null;
  this.ctx = null;
  this.toolbar = null;
  this._previewPoint = null;
  this._subs = [];
  this._dragging = null; // {drawing, pointIndex, startX, startY}

  this._initCanvas();
  this._createToolbar();
  this._bindEvents();
}

// ============================================================================
// Canvas overlay
// ============================================================================
DrawingManager.prototype._initCanvas = function() {
  this.canvas = document.createElement('canvas');
  this.canvas.className = 'drawing-canvas';
  this.container.style.position = 'relative';
  this.container.appendChild(this.canvas);
  this.ctx = this.canvas.getContext('2d');
  this._resizeCanvas();
  var self = this;
  this._canvasResizeObs = new ResizeObserver(function() { self._resizeCanvas(); self.render(); });
  this._canvasResizeObs.observe(this.container);
};

DrawingManager.prototype._resizeCanvas = function() {
  var rect = this.container.getBoundingClientRect();
  var dpr = window.devicePixelRatio || 1;
  this.canvas.width = Math.round(rect.width * dpr);
  this.canvas.height = Math.round(rect.height * dpr);
  this.canvas.style.width = rect.width + 'px';
  this.canvas.style.height = rect.height + 'px';
  this.ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
};

// ============================================================================
// Coordinate conversion
// ============================================================================
DrawingManager.prototype.priceToY = function(price) {
  if (!this.candleSeries) return null;
  return this.candleSeries.priceToCoordinate(price);
};
DrawingManager.prototype.yToPrice = function(y) {
  if (!this.candleSeries) return null;
  return this.candleSeries.coordinateToPrice(y);
};
DrawingManager.prototype.timeToX = function(time) {
  if (!this.chart) return null;
  return this.chart.timeScale().timeToCoordinate(time);
};
DrawingManager.prototype.xToTime = function(x) {
  if (!this.chart) return null;
  return this.chart.timeScale().coordinateToTime(x);
};

// ============================================================================
// Magnet snap to OHLC
// ============================================================================
DrawingManager.prototype.snapToOHLC = function(time, rawPrice) {
  if (!this.magnetMode) return rawPrice;
  var bar = this._findBarNearTime(time);
  if (!bar) return rawPrice;
  var candidates = [bar.open, bar.high, bar.low, bar.close];
  var closest = rawPrice, minDist = Infinity;
  for (var i = 0; i < candidates.length; i++) {
    var d = Math.abs(candidates[i] - rawPrice);
    if (d < minDist) { minDist = d; closest = candidates[i]; }
  }
  return closest;
};

DrawingManager.prototype._findBarNearTime = function(time) {
  var bars = this.getBars ? this.getBars() : null;
  if (!bars || !bars.length) return null;
  var lo = 0, hi = bars.length - 1;
  while (lo <= hi) {
    var mid = (lo + hi) >> 1;
    if (bars[mid].time === time) return bars[mid];
    if (bars[mid].time < time) lo = mid + 1;
    else hi = mid - 1;
  }
  if (lo >= bars.length) return bars[bars.length - 1];
  if (lo === 0) return bars[0];
  return Math.abs(bars[lo].time - time) < Math.abs(bars[lo-1].time - time) ? bars[lo] : bars[lo-1];
};

// ============================================================================
// Event binding
// ============================================================================
DrawingManager.prototype._bindEvents = function() {
  var self = this;

  // Chart click — used for placing drawing points
  var clickSub = function(param) {
    if (self.state === 'IDLE') {
      // Hit test for selection
      if (param.point) {
        var hit = self._hitTest(param.point.x, param.point.y);
        self.selectedDrawing = hit;
        self.render();
      }
      return;
    }
    if (!param.time || !param.point) return;
    var price = self.yToPrice(param.point.y);
    if (price === null) return;
    if (self.magnetMode) price = self.snapToOHLC(param.time, price);
    self._handleClick(param.time, price);
  };
  this.chart.subscribeClick(clickSub);
  this._subs.push({type:'click', fn:clickSub});

  // Crosshair move — for preview during placement
  var moveSub = function(param) {
    if (self.state === 'IDLE') return;
    if (!param.time || !param.point) { self._previewPoint = null; return; }
    var price = self.yToPrice(param.point.y);
    if (price === null) return;
    if (self.magnetMode) price = self.snapToOHLC(param.time, price);
    self._previewPoint = {time: param.time, price: price};
    self.render();
  };
  this.chart.subscribeCrosshairMove(moveSub);
  this._subs.push({type:'move', fn:moveSub});

  // Re-render on scroll/zoom
  var rangeSub = function() { self.render(); };
  this.chart.timeScale().subscribeVisibleLogicalRangeChange(rangeSub);
  this._subs.push({type:'range', fn:rangeSub});

  // Double-click to edit drawing properties
  this._dblClickHandler = function(e) {
    if (self.state !== 'IDLE' || !self.selectedDrawing) return;
    self._showPropertiesPopup(self.selectedDrawing, e.clientX, e.clientY);
  };
  this.container.addEventListener('dblclick', this._dblClickHandler);

  // Drag anchor points to reposition drawings
  this._dragMouseDown = function(e) {
    if (self.state !== 'IDLE' || !self.selectedDrawing) return;
    var rect = self.canvas.getBoundingClientRect();
    var px = e.clientX - rect.left, py = e.clientY - rect.top;
    var pts = self.selectedDrawing.points;
    var hitIdx = -1, bestDist = 10; // 10px grab radius
    for (var i = 0; i < pts.length; i++) {
      var ax = self.timeToX(pts[i].time), ay = self.priceToY(pts[i].price);
      if (ax === null || ay === null) continue;
      var dist = Math.sqrt((px - ax) * (px - ax) + (py - ay) * (py - ay));
      if (dist < bestDist) { bestDist = dist; hitIdx = i; }
    }
    if (hitIdx >= 0) {
      e.preventDefault();
      e.stopPropagation();
      self._dragging = {drawing: self.selectedDrawing, pointIndex: hitIdx};
      self.container.style.cursor = 'grabbing';
      self._closePropertiesPopup();
      // Disable chart scroll/scale while dragging
      if (self.chart) self.chart.applyOptions({handleScroll:{mouseWheel:false,pressedMouseMove:false,horzTouchDrag:false,vertTouchDrag:false},handleScale:{mouseWheel:false,pinch:false,axisPressedMouseMove:false,axisDoubleClickReset:false}});
    }
  };
  this._dragMouseMove = function(e) {
    if (!self._dragging) {
      // Show grab cursor when hovering over anchor points
      if (self.state === 'IDLE' && self.selectedDrawing && self.canvas) {
        var rect = self.canvas.getBoundingClientRect();
        var px = e.clientX - rect.left, py = e.clientY - rect.top;
        var pts = self.selectedDrawing.points;
        var nearAnchor = false;
        for (var i = 0; i < pts.length; i++) {
          var ax = self.timeToX(pts[i].time), ay = self.priceToY(pts[i].price);
          if (ax !== null && ay !== null) {
            var dist = Math.sqrt((px - ax) * (px - ax) + (py - ay) * (py - ay));
            if (dist < 10) { nearAnchor = true; break; }
          }
        }
        self.container.style.cursor = nearAnchor ? 'grab' : '';
      }
      return;
    }
    e.preventDefault();
    var rect = self.canvas.getBoundingClientRect();
    var px = e.clientX - rect.left, py = e.clientY - rect.top;
    var newTime = self.xToTime(px);
    var newPrice = self.yToPrice(py);
    if (newTime === null || newPrice === null) return;
    if (self.magnetMode) newPrice = self.snapToOHLC(newTime, newPrice);
    var pt = self._dragging.drawing.points[self._dragging.pointIndex];
    pt.time = newTime;
    pt.price = newPrice;
    self.render();
  };
  this._dragMouseUp = function(e) {
    if (!self._dragging) return;
    self._dragging = null;
    self.container.style.cursor = '';
    // Re-enable chart scroll/scale
    if (self.chart) self.chart.applyOptions({handleScroll:{mouseWheel:true,pressedMouseMove:true,horzTouchDrag:true,vertTouchDrag:false},handleScale:{mouseWheel:true,pinch:true,axisPressedMouseMove:{time:false,price:true},axisDoubleClickReset:{time:true,price:true}}});
    self.save();
  };
  this.container.addEventListener('mousedown', this._dragMouseDown);
  document.addEventListener('mousemove', this._dragMouseMove);
  document.addEventListener('mouseup', this._dragMouseUp);

  // Keyboard: Escape cancels, Delete removes selected
  this._keyHandler = function(e) {
    if (e.target.tagName === 'INPUT' || e.target.tagName === 'SELECT' || e.target.tagName === 'TEXTAREA') return;
    if (e.key === 'Escape') {
      self.cancelPlacement();
      self._closePropertiesPopup();
    }
    if ((e.key === 'Delete' || e.key === 'Backspace') && self.selectedDrawing) {
      e.preventDefault();
      self._closePropertiesPopup();
      self.removeDrawing(self.selectedDrawing);
      self.selectedDrawing = null;
      self.render();
      self.save();
    }
  };
  document.addEventListener('keydown', this._keyHandler);
};

// ============================================================================
// State machine
// ============================================================================
DrawingManager.prototype._handleClick = function(time, price) {
  this.currentPoints.push({time: time, price: price});
  var needed = TOOL_POINTS[this.activeTool] || 2;
  if (this.currentPoints.length >= needed) {
    var style = this._getDefaultStyle(this.activeTool);
    var drawing = new Drawing(this.activeTool, this.currentPoints.slice(), style);
    this.drawings.push(drawing);
    this.currentPoints = [];
    this._previewPoint = null;
    this.state = 'IDLE';
    this.activeTool = null;
    this.container.style.cursor = '';
    this._updateToolbarActive();
    this.render();
    this.save();
  } else {
    this.render();
  }
};

DrawingManager.prototype.setTool = function(toolId) {
  this.activeTool = toolId;
  this.state = 'PLACING';
  this.currentPoints = [];
  this._previewPoint = null;
  this.selectedDrawing = null;
  this.container.style.cursor = 'crosshair';
  this._updateToolbarActive();
  this._closeFlyouts();
};

DrawingManager.prototype.cancelPlacement = function() {
  if (this.state !== 'IDLE') {
    this.activeTool = null;
    this.state = 'IDLE';
    this.currentPoints = [];
    this._previewPoint = null;
    this.container.style.cursor = '';
    this._updateToolbarActive();
    this.render();
  }
  this.selectedDrawing = null;
  this.render();
};

DrawingManager.prototype.removeDrawing = function(drawing) {
  var idx = this.drawings.indexOf(drawing);
  if (idx >= 0) this.drawings.splice(idx, 1);
};

DrawingManager.prototype.clearAll = function() {
  this.drawings = [];
  this.selectedDrawing = null;
  this.render();
  this.save();
};

DrawingManager.prototype._getDefaultStyle = function(tool) {
  if (tool === 'fib_retrace' || tool === 'fib_extension') return {color:'#58a6ff', lineWidth:1, lineStyle:2};
  if (tool === 'parallel_channel') return {color:'#c792ea', lineWidth:1, lineStyle:0};
  if (tool === 'hline') return {color:'#e5c07b', lineWidth:1, lineStyle:2};
  if (tool === 'vline') return {color:'#e5c07b', lineWidth:1, lineStyle:2};
  return {color:'#58a6ff', lineWidth:1, lineStyle:0};
};

// ============================================================================
// Render
// ============================================================================
DrawingManager.prototype.render = function() {
  if (!this.ctx || !this.canvas) return;
  var dpr = window.devicePixelRatio || 1;
  var w = this.canvas.width / dpr;
  var h = this.canvas.height / dpr;
  this.ctx.clearRect(0, 0, w, h);
  var self = this;

  // Render all completed drawings
  this.drawings.forEach(function(d) {
    self._renderDrawing(d, w, h, d === self.selectedDrawing);
  });

  // Render in-progress preview
  if (this.currentPoints.length > 0 && this._previewPoint) {
    var allPts = this.currentPoints.concat([this._previewPoint]);
    var preview = new Drawing(this.activeTool, allPts, this._getDefaultStyle(this.activeTool));
    this._renderDrawing(preview, w, h, false);
  }
};

DrawingManager.prototype._renderDrawing = function(d, w, h, selected) {
  var ctx = this.ctx;
  var pts = d.points;
  var s = d.style;

  ctx.save();
  if (selected) {
    ctx.shadowColor = '#58a6ff';
    ctx.shadowBlur = 4;
  }

  switch(d.type) {
    case 'trendline': this._drawTrendLine(ctx, pts, s, w, h); break;
    case 'ray': this._drawRay(ctx, pts, s, w, h); break;
    case 'extended': this._drawExtended(ctx, pts, s, w, h); break;
    case 'hline': this._drawHLine(ctx, pts, s, w, h); break;
    case 'vline': this._drawVLine(ctx, pts, s, w, h); break;
    case 'crossline': this._drawHLine(ctx, pts, s, w, h); this._drawVLine(ctx, pts, s, w, h); break;
    case 'fib_retrace': this._drawFibRetrace(ctx, pts, s, w, h); break;
    case 'fib_extension': this._drawFibExtension(ctx, pts, s, w, h); break;
    case 'parallel_channel': this._drawParallelChannel(ctx, pts, s, w, h); break;
  }

  // Draw anchor handles for selected drawing
  if (selected) {
    ctx.shadowBlur = 0;
    pts.forEach(function(pt) {
      var x = this.timeToX(pt.time), y = this.priceToY(pt.price);
      if (x !== null && y !== null) {
        ctx.fillStyle = '#58a6ff';
        ctx.strokeStyle = '#fff';
        ctx.lineWidth = 1.5;
        ctx.setLineDash([]);
        ctx.beginPath();
        ctx.arc(x, y, 5, 0, Math.PI * 2);
        ctx.fill();
        ctx.stroke();
      }
    }.bind(this));
  }

  ctx.restore();
};

// ============================================================================
// Line style helper
// ============================================================================
DrawingManager.prototype._applyStyle = function(ctx, s) {
  ctx.strokeStyle = s.color || '#58a6ff';
  ctx.lineWidth = s.lineWidth || 1;
  var dash = [];
  switch(s.lineStyle) {
    case 1: dash = [2,3]; break;
    case 2: dash = [6,4]; break;
    case 3: dash = [10,4]; break;
    default: dash = [];
  }
  ctx.setLineDash(dash);
};

// ============================================================================
// Line drawing tools
// ============================================================================
DrawingManager.prototype._drawTrendLine = function(ctx, pts, s, w, h) {
  if (pts.length < 2) return;
  var x1 = this.timeToX(pts[0].time), y1 = this.priceToY(pts[0].price);
  var x2 = this.timeToX(pts[1].time), y2 = this.priceToY(pts[1].price);
  if (x1 === null || x2 === null || y1 === null || y2 === null) return;
  ctx.beginPath();
  this._applyStyle(ctx, s);
  ctx.moveTo(x1, y1);
  ctx.lineTo(x2, y2);
  ctx.stroke();
};

DrawingManager.prototype._drawRay = function(ctx, pts, s, w, h) {
  if (pts.length < 2) return;
  var x1 = this.timeToX(pts[0].time), y1 = this.priceToY(pts[0].price);
  var x2 = this.timeToX(pts[1].time), y2 = this.priceToY(pts[1].price);
  if (x1 === null || x2 === null || y1 === null || y2 === null) return;
  var dx = x2 - x1, dy = y2 - y1;
  var len = Math.sqrt(dx*dx + dy*dy);
  if (len === 0) return;
  // Extend to canvas edge
  var maxDist = Math.max(w, h) * 2;
  var ex = x2 + (dx / len) * maxDist;
  var ey = y2 + (dy / len) * maxDist;
  ctx.beginPath();
  this._applyStyle(ctx, s);
  ctx.moveTo(x1, y1);
  ctx.lineTo(ex, ey);
  ctx.stroke();
};

DrawingManager.prototype._drawExtended = function(ctx, pts, s, w, h) {
  if (pts.length < 2) return;
  var x1 = this.timeToX(pts[0].time), y1 = this.priceToY(pts[0].price);
  var x2 = this.timeToX(pts[1].time), y2 = this.priceToY(pts[1].price);
  if (x1 === null || x2 === null || y1 === null || y2 === null) return;
  var dx = x2 - x1, dy = y2 - y1;
  var len = Math.sqrt(dx*dx + dy*dy);
  if (len === 0) return;
  var maxDist = Math.max(w, h) * 2;
  var sx = x1 - (dx / len) * maxDist, sy = y1 - (dy / len) * maxDist;
  var ex = x2 + (dx / len) * maxDist, ey = y2 + (dy / len) * maxDist;
  ctx.beginPath();
  this._applyStyle(ctx, s);
  ctx.moveTo(sx, sy);
  ctx.lineTo(ex, ey);
  ctx.stroke();
};

DrawingManager.prototype._drawHLine = function(ctx, pts, s, w, h) {
  if (pts.length < 1) return;
  var y = this.priceToY(pts[0].price);
  if (y === null) return;
  ctx.beginPath();
  this._applyStyle(ctx, s);
  ctx.moveTo(0, y);
  ctx.lineTo(w, y);
  ctx.stroke();
  // Price label
  ctx.setLineDash([]);
  ctx.font = '10px sans-serif';
  ctx.fillStyle = s.color || '#58a6ff';
  ctx.textAlign = 'left';
  ctx.fillText(pts[0].price.toFixed(2), 4, y - 3);
};

DrawingManager.prototype._drawVLine = function(ctx, pts, s, w, h) {
  if (pts.length < 1) return;
  var x = this.timeToX(pts[0].time);
  if (x === null) return;
  ctx.beginPath();
  this._applyStyle(ctx, s);
  ctx.moveTo(x, 0);
  ctx.lineTo(x, h);
  ctx.stroke();
};

// ============================================================================
// Fibonacci tools
// ============================================================================
DrawingManager.prototype._drawFibRetrace = function(ctx, pts, s, w, h) {
  if (pts.length < 2) return;
  this._drawFibLevels(ctx, pts, s, w, h, FIB_LEVELS);
};

DrawingManager.prototype._drawFibExtension = function(ctx, pts, s, w, h) {
  if (pts.length < 2) return;
  this._drawFibLevels(ctx, pts, s, w, h, FIB_EXT_LEVELS);
};

DrawingManager.prototype._drawFibLevels = function(ctx, pts, s, w, h, levels) {
  var p1 = pts[0].price, p2 = pts[1].price;
  var range = p2 - p1;
  var x1 = this.timeToX(pts[0].time), x2 = this.timeToX(pts[1].time);
  var lx = Math.min(x1 || 0, x2 || 0), rx = Math.max(x1 || w, x2 || w);
  if (rx - lx < 20) { lx = 0; rx = w; }

  ctx.font = '10px sans-serif';
  for (var i = 0; i < levels.length; i++) {
    var price = p1 + range * (1 - levels[i]);
    var y = this.priceToY(price);
    if (y === null) continue;

    // Fill zone to next level
    if (i < levels.length - 1) {
      var nextPrice = p1 + range * (1 - levels[i + 1]);
      var yNext = this.priceToY(nextPrice);
      if (yNext !== null) {
        ctx.fillStyle = FIB_ZONE_COLORS[i] || 'rgba(88,166,255,0.04)';
        ctx.fillRect(lx, Math.min(y, yNext), rx - lx, Math.abs(yNext - y));
      }
    }

    // Level line
    ctx.beginPath();
    ctx.strokeStyle = s.color || '#58a6ff';
    ctx.lineWidth = 1;
    ctx.setLineDash([4, 3]);
    ctx.moveTo(lx, y);
    ctx.lineTo(rx, y);
    ctx.stroke();

    // Label
    ctx.setLineDash([]);
    ctx.fillStyle = s.color || '#58a6ff';
    ctx.textAlign = 'left';
    ctx.fillText(levels[i].toFixed(3) + ' (' + price.toFixed(2) + ')', lx + 4, y - 3);
  }

  // Connecting line between the two anchor points
  if (x1 !== null && x2 !== null) {
    var y1 = this.priceToY(p1), y2 = this.priceToY(p2);
    if (y1 !== null && y2 !== null) {
      ctx.beginPath();
      ctx.strokeStyle = s.color || '#58a6ff';
      ctx.lineWidth = 1;
      ctx.setLineDash([2, 2]);
      ctx.moveTo(x1, y1);
      ctx.lineTo(x2, y2);
      ctx.stroke();
    }
  }
};

// ============================================================================
// Parallel channel
// ============================================================================
DrawingManager.prototype._drawParallelChannel = function(ctx, pts, s, w, h) {
  if (pts.length < 2) return;
  var x1 = this.timeToX(pts[0].time), y1 = this.priceToY(pts[0].price);
  var x2 = this.timeToX(pts[1].time), y2 = this.priceToY(pts[1].price);
  if (x1 === null || x2 === null || y1 === null || y2 === null) return;

  // Baseline
  this._applyStyle(ctx, s);
  ctx.beginPath();
  ctx.moveTo(x1, y1);
  ctx.lineTo(x2, y2);
  ctx.stroke();

  if (pts.length < 3) return;
  var x3 = this.timeToX(pts[2].time), y3 = this.priceToY(pts[2].price);
  if (x3 === null || y3 === null) return;

  // Calculate perpendicular offset
  var dx = x2 - x1, dy = y2 - y1;
  var len = Math.sqrt(dx * dx + dy * dy);
  if (len === 0) return;
  var nx = -dy / len, ny = dx / len;
  var proj = (x3 - x1) * nx + (y3 - y1) * ny;

  // Parallel line
  ctx.beginPath();
  this._applyStyle(ctx, s);
  ctx.moveTo(x1 + nx * proj, y1 + ny * proj);
  ctx.lineTo(x2 + nx * proj, y2 + ny * proj);
  ctx.stroke();

  // Fill between
  ctx.fillStyle = (s.color || '#c792ea').replace('rgb(', 'rgba(').replace(')', ',0.08)');
  if (ctx.fillStyle.indexOf('rgba') === -1) ctx.fillStyle = 'rgba(199,146,234,0.08)';
  ctx.beginPath();
  ctx.moveTo(x1, y1);
  ctx.lineTo(x2, y2);
  ctx.lineTo(x2 + nx * proj, y2 + ny * proj);
  ctx.lineTo(x1 + nx * proj, y1 + ny * proj);
  ctx.closePath();
  ctx.fill();
};

// ============================================================================
// Hit testing for selection
// ============================================================================
DrawingManager.prototype._hitTest = function(px, py) {
  for (var i = this.drawings.length - 1; i >= 0; i--) {
    if (this._isNearDrawing(this.drawings[i], px, py, 6)) return this.drawings[i];
  }
  return null;
};

DrawingManager.prototype._isNearDrawing = function(d, px, py, threshold) {
  var pts = d.points;
  switch(d.type) {
    case 'trendline':
    case 'ray':
    case 'extended':
      if (pts.length < 2) return false;
      var x1=this.timeToX(pts[0].time),y1=this.priceToY(pts[0].price);
      var x2=this.timeToX(pts[1].time),y2=this.priceToY(pts[1].price);
      if (x1===null||x2===null||y1===null||y2===null) return false;
      return this._distToSegment(px,py,x1,y1,x2,y2) < threshold;
    case 'hline':
      if (pts.length < 1) return false;
      var hy = this.priceToY(pts[0].price);
      return hy !== null && Math.abs(py - hy) < threshold;
    case 'vline':
      if (pts.length < 1) return false;
      var vx = this.timeToX(pts[0].time);
      return vx !== null && Math.abs(px - vx) < threshold;
    case 'crossline':
      if (pts.length < 1) return false;
      var cy=this.priceToY(pts[0].price), cx=this.timeToX(pts[0].time);
      return (cy!==null&&Math.abs(py-cy)<threshold)||(cx!==null&&Math.abs(px-cx)<threshold);
    case 'fib_retrace':
    case 'fib_extension':
      if (pts.length < 2) return false;
      var levels = d.type==='fib_extension'?FIB_EXT_LEVELS:FIB_LEVELS;
      var p1=pts[0].price, range=pts[1].price-p1;
      for(var j=0;j<levels.length;j++){
        var fy=this.priceToY(p1+range*(1-levels[j]));
        if(fy!==null&&Math.abs(py-fy)<threshold)return true;
      }
      return false;
    case 'parallel_channel':
      if(pts.length<2)return false;
      var cx1=this.timeToX(pts[0].time),cy1=this.priceToY(pts[0].price);
      var cx2=this.timeToX(pts[1].time),cy2=this.priceToY(pts[1].price);
      if(cx1===null||cx2===null||cy1===null||cy2===null)return false;
      if(this._distToSegment(px,py,cx1,cy1,cx2,cy2)<threshold)return true;
      if(pts.length>=3){
        var dx=cx2-cx1,dy=cy2-cy1,len=Math.sqrt(dx*dx+dy*dy);
        if(len>0){
          var nx=-dy/len,ny=dx/len;
          var cx3=this.timeToX(pts[2].time),cy3=this.priceToY(pts[2].price);
          if(cx3!==null&&cy3!==null){
            var proj=(cx3-cx1)*nx+(cy3-cy1)*ny;
            if(this._distToSegment(px,py,cx1+nx*proj,cy1+ny*proj,cx2+nx*proj,cy2+ny*proj)<threshold)return true;
          }
        }
      }
      return false;
  }
  return false;
};

DrawingManager.prototype._distToSegment = function(px, py, x1, y1, x2, y2) {
  var dx = x2 - x1, dy = y2 - y1;
  var lenSq = dx * dx + dy * dy;
  if (lenSq === 0) return Math.sqrt((px-x1)*(px-x1)+(py-y1)*(py-y1));
  var t = Math.max(0, Math.min(1, ((px-x1)*dx+(py-y1)*dy)/lenSq));
  var projX = x1 + t * dx, projY = y1 + t * dy;
  return Math.sqrt((px-projX)*(px-projX)+(py-projY)*(py-projY));
};

// ============================================================================
// Save / Load
// ============================================================================
DrawingManager.prototype._drawingKey = function() {
  var sym = this.getSymbol ? this.getSymbol() : null;
  if (!sym) return null;
  var lyId = this.getLayoutId ? this.getLayoutId() : 'default';
  return 'orb_drawings_' + lyId + '_' + sym;
};

DrawingManager.prototype.save = function() {
  var key = this._drawingKey();
  if (!key) return;
  saveToStorage(key, this.drawings.map(function(d) { return d.toJSON(); }));
};

DrawingManager.prototype.load = function() {
  var key = this._drawingKey();
  if (!key) return;
  var saved = loadFromStorage(key);
  if (saved && Array.isArray(saved)) {
    this.drawings = saved.map(function(obj) { return Drawing.fromJSON(obj); });
  }
  this.render();
};

// ============================================================================
// Properties popup for editing drawings
// ============================================================================
DrawingManager.prototype._showPropertiesPopup = function(drawing, mx, my) {
  this._closePropertiesPopup();
  var self = this;
  var d = drawing;
  var popup = document.createElement('div');
  popup.className = 'drawing-props-popup';
  popup.id = 'drawing-props-popup';

  var html = '<div class="dpp-title">' + d.type.replace(/_/g,' ') + '</div>';

  // Color row
  html += '<div class="dpp-row"><span class="dpp-label">Color</span><div class="dpp-colors">';
  DRAWING_COLORS.forEach(function(c) {
    var active = d.style.color === c ? ' active' : '';
    html += '<div class="dpp-swatch' + active + '" data-color="' + c + '" style="background:' + c + '"></div>';
  });
  html += '</div></div>';

  // Width row
  html += '<div class="dpp-row"><span class="dpp-label">Width</span><div class="dpp-widths">';
  [1,2,3].forEach(function(w) {
    var active = d.style.lineWidth === w ? ' active' : '';
    html += '<div class="dpp-opt' + active + '" data-width="' + w + '">' + w + 'px</div>';
  });
  html += '</div></div>';

  // Style row
  html += '<div class="dpp-row"><span class="dpp-label">Style</span><div class="dpp-styles">';
  var styles = [{v:0,label:'━'},{v:2,label:'╌'},{v:1,label:'···'}];
  styles.forEach(function(s) {
    var active = d.style.lineStyle === s.v ? ' active' : '';
    html += '<div class="dpp-opt' + active + '" data-style="' + s.v + '">' + s.label + '</div>';
  });
  html += '</div></div>';

  // Delete button
  html += '<button class="dpp-delete" data-action="delete">Delete Drawing</button>';

  popup.innerHTML = html;

  // Event delegation
  popup.addEventListener('click', function(e) {
    var el = e.target;
    if (el.dataset.color) {
      d.style.color = el.dataset.color;
      popup.querySelectorAll('.dpp-swatch').forEach(function(s){s.classList.remove('active');});
      el.classList.add('active');
      self.render(); self.save();
    }
    if (el.dataset.width) {
      d.style.lineWidth = parseInt(el.dataset.width);
      popup.querySelectorAll('.dpp-widths .dpp-opt').forEach(function(s){s.classList.remove('active');});
      el.classList.add('active');
      self.render(); self.save();
    }
    if (el.dataset.style !== undefined) {
      d.style.lineStyle = parseInt(el.dataset.style);
      popup.querySelectorAll('.dpp-styles .dpp-opt').forEach(function(s){s.classList.remove('active');});
      el.classList.add('active');
      self.render(); self.save();
    }
    if (el.dataset.action === 'delete') {
      self._closePropertiesPopup();
      self.removeDrawing(d);
      self.selectedDrawing = null;
      self.render(); self.save();
    }
    e.stopPropagation();
  });

  document.body.appendChild(popup);
  // Position — keep on screen
  var pw = 220, ph = 200;
  var left = Math.min(mx + 10, window.innerWidth - pw - 10);
  var top = Math.min(my - 20, window.innerHeight - ph - 10);
  popup.style.left = left + 'px';
  popup.style.top = Math.max(10, top) + 'px';
  this._propsPopup = popup;

  // Close on outside click (delayed to avoid immediate close)
  var self2 = this;
  setTimeout(function() {
    self2._propsCloseHandler = function(e) {
      if (self2._propsPopup && !self2._propsPopup.contains(e.target)) {
        self2._closePropertiesPopup();
      }
    };
    document.addEventListener('mousedown', self2._propsCloseHandler);
  }, 50);
};

DrawingManager.prototype._closePropertiesPopup = function() {
  if (this._propsPopup && this._propsPopup.parentNode) {
    this._propsPopup.parentNode.removeChild(this._propsPopup);
  }
  this._propsPopup = null;
  if (this._propsCloseHandler) {
    document.removeEventListener('mousedown', this._propsCloseHandler);
    this._propsCloseHandler = null;
  }
};

// ============================================================================
// Re-attach after chart recreation
// ============================================================================
DrawingManager.prototype.attach = function(chart, candleSeries, container) {
  // Unsub old events
  this._cleanup();
  this.chart = chart;
  this.candleSeries = candleSeries;
  this.container = container;
  this._initCanvas();
  this._createToolbar();
  this._bindEvents();
  this.load();
};

DrawingManager.prototype._cleanup = function() {
  this._closePropertiesPopup();
  this._dragging = null;
  if (this._canvasResizeObs) { this._canvasResizeObs.disconnect(); this._canvasResizeObs = null; }
  if (this.canvas && this.canvas.parentNode) this.canvas.parentNode.removeChild(this.canvas);
  if (this.toolbar && this.toolbar.parentNode) this.toolbar.parentNode.removeChild(this.toolbar);
  if (this._keyHandler) document.removeEventListener('keydown', this._keyHandler);
  if (this._dblClickHandler && this.container) this.container.removeEventListener('dblclick', this._dblClickHandler);
  if (this._dragMouseDown && this.container) this.container.removeEventListener('mousedown', this._dragMouseDown);
  if (this._dragMouseMove) document.removeEventListener('mousemove', this._dragMouseMove);
  if (this._dragMouseUp) document.removeEventListener('mouseup', this._dragMouseUp);
  this.canvas = null;
  this.ctx = null;
  this.toolbar = null;
};

// ============================================================================
// Toolbar UI
// ============================================================================
DrawingManager.prototype._createToolbar = function() {
  var tb = document.createElement('div');
  tb.className = 'drawing-toolbar';
  var self = this;

  var categories = [
    {id:'lines', icon:'\u2571', label:'Lines', tools:[
      {id:'trendline', label:'Trend Line', icon:'\u2571'},
      {id:'ray', label:'Ray', icon:'\u2192'},
      {id:'extended', label:'Extended Line', icon:'\u2194'},
      {id:'hline', label:'Horizontal Line', icon:'\u2500'},
      {id:'vline', label:'Vertical Line', icon:'\u2502'},
      {id:'crossline', label:'Cross Line', icon:'\u253C'},
    ]},
    {id:'fib', icon:'F', label:'Fibonacci', tools:[
      {id:'fib_retrace', label:'Fib Retracement', icon:'FR'},
      {id:'fib_extension', label:'Fib Extension', icon:'FE'},
    ]},
    {id:'channels', icon:'\u2225', label:'Channels', tools:[
      {id:'parallel_channel', label:'Parallel Channel', icon:'\u2225'},
    ]},
  ];

  categories.forEach(function(cat) {
    var btn = document.createElement('button');
    btn.className = 'dt-btn';
    btn.title = cat.label;
    btn.textContent = cat.icon;
    btn.setAttribute('data-cat', cat.id);

    var flyout = document.createElement('div');
    flyout.className = 'dt-flyout';

    cat.tools.forEach(function(tool) {
      var item = document.createElement('div');
      item.className = 'dt-flyout-item';
      item.innerHTML = '<span class="dti-icon">' + tool.icon + '</span>' + tool.label;
      item.addEventListener('click', function(e) {
        e.stopPropagation();
        self.setTool(tool.id);
      });
      flyout.appendChild(item);
    });

    btn.addEventListener('click', function(e) {
      e.stopPropagation();
      // Close other flyouts
      tb.querySelectorAll('.dt-flyout').forEach(function(f) { if (f !== flyout) f.classList.remove('show'); });
      flyout.classList.toggle('show');
    });

    btn.appendChild(flyout);
    tb.appendChild(btn);
  });

  // Separator
  var sep = document.createElement('div');
  sep.className = 'dt-sep';
  tb.appendChild(sep);

  // Magnet toggle
  var magBtn = document.createElement('button');
  magBtn.className = 'dt-btn' + (this.magnetMode ? ' magnet-on' : '');
  magBtn.title = 'Magnet Mode (snap to OHLC)';
  magBtn.innerHTML = '<svg width="14" height="14" viewBox="0 0 16 16"><path d="M1 8a7 7 0 1 1 14 0A7 7 0 0 1 1 8zm7-5a.5.5 0 0 0-.5.5v4a.5.5 0 0 0 .5.5h3a.5.5 0 0 0 0-1H8.5V3.5A.5.5 0 0 0 8 3z" fill="currentColor"/></svg>';
  magBtn.addEventListener('click', function(e) {
    e.stopPropagation();
    self.magnetMode = !self.magnetMode;
    magBtn.classList.toggle('magnet-on', self.magnetMode);
  });
  this._magBtn = magBtn;
  tb.appendChild(magBtn);

  // Delete all
  var delBtn = document.createElement('button');
  delBtn.className = 'dt-btn';
  delBtn.title = 'Delete All Drawings';
  delBtn.innerHTML = '<svg width="14" height="14" viewBox="0 0 16 16"><path d="M5.5 5.5A.5.5 0 0 1 6 6v6a.5.5 0 0 1-1 0V6a.5.5 0 0 1 .5-.5zm2.5 0a.5.5 0 0 1 .5.5v6a.5.5 0 0 1-1 0V6a.5.5 0 0 1 .5-.5zm3 .5a.5.5 0 0 0-1 0v6a.5.5 0 0 0 1 0V6z" fill="currentColor"/><path fill-rule="evenodd" d="M14.5 3a1 1 0 0 1-1 1H13v9a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V4h-.5a1 1 0 0 1-1-1V2a1 1 0 0 1 1-1H6a1 1 0 0 1 1-1h2a1 1 0 0 1 1 1h3.5a1 1 0 0 1 1 1v1zM4.118 4L4 4.059V13a1 1 0 0 0 1 1h6a1 1 0 0 0 1-1V4.059L11.882 4H4.118zM2.5 3V2h11v1h-11z" fill="currentColor"/></svg>';
  delBtn.style.color = '#f85149';
  delBtn.addEventListener('click', function(e) {
    e.stopPropagation();
    self.clearAll();
  });
  tb.appendChild(delBtn);

  this.toolbar = tb;

  // Insert toolbar at the beginning of the chart area parent
  var chartArea = this.container.parentNode;
  if (chartArea) {
    chartArea.style.position = 'relative';
    chartArea.insertBefore(tb, chartArea.firstChild);
    this.container.classList.add('has-drawing-toolbar');
  }
};

DrawingManager.prototype._updateToolbarActive = function() {
  if (!this.toolbar) return;
  this.toolbar.querySelectorAll('.dt-btn').forEach(function(btn) {
    btn.classList.remove('active');
  });
  // Active state is handled by canvas cursor change
};

DrawingManager.prototype._closeFlyouts = function() {
  if (!this.toolbar) return;
  this.toolbar.querySelectorAll('.dt-flyout').forEach(function(f) { f.classList.remove('show'); });
};

// Close flyouts on outside click
document.addEventListener('click', function() {
  document.querySelectorAll('.dt-flyout.show').forEach(function(f) { f.classList.remove('show'); });
});
