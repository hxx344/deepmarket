/**
 * Dashboard 工具函数
 */

/** 数字格式化: $1,234.56 */
function formatUSD(val, decimals = 2) {
  return '$' + Number(val).toLocaleString('en-US', {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals,
  });
}

/** PnL 格式: +$1,234.56 / -$789.00 */
function formatPnl(val, decimals = 2) {
  const sign = val >= 0 ? '+' : '';
  return sign + formatUSD(val, decimals);
}

/** 百分比: +12.34% */
function formatPct(val, decimals = 2) {
  const sign = val >= 0 ? '+' : '';
  return sign + val.toFixed(decimals) + '%';
}

/** Unix timestamp → HH:MM:SS */
function formatTime(ts) {
  const d = new Date(ts * 1000);
  return d.toLocaleTimeString('zh-CN', { hour12: false });
}

/** Unix timestamp → YYYY-MM-DD HH:MM:SS */
function formatDateTime(ts) {
  const d = new Date(ts * 1000);
  return d.toLocaleString('zh-CN', { hour12: false });
}

/** 秒 → 00:12:34 */
function formatDuration(seconds) {
  const h = Math.floor(seconds / 3600);
  const m = Math.floor((seconds % 3600) / 60);
  const s = Math.floor(seconds % 60);
  return [h, m, s].map(v => v.toString().padStart(2, '0')).join(':');
}

/** 阶梯颜色: 正值绿色, 负值红色 */
function pnlColor(val) {
  return val >= 0 ? 'text-up' : 'text-down';
}

/** 简单节流 */
function throttle(fn, ms) {
  let last = 0;
  return function (...args) {
    const now = Date.now();
    if (now - last >= ms) {
      last = now;
      return fn.apply(this, args);
    }
  };
}

/** 创建 TradingView Lightweight Charts K 线图 */
function createCandlestickChart(container, options = {}) {
  const chart = LightweightCharts.createChart(container, {
    layout: {
      background: { type: 'solid', color: '#1f2937' },
      textColor: '#9ca3af',
    },
    grid: {
      vertLines: { color: '#374151' },
      horzLines: { color: '#374151' },
    },
    crosshair: {
      mode: LightweightCharts.CrosshairMode.Normal,
    },
    rightPriceScale: {
      borderColor: '#374151',
    },
    timeScale: {
      borderColor: '#374151',
      timeVisible: true,
      secondsVisible: false,
    },
    ...options,
  });

  const candleSeries = chart.addCandlestickSeries({
    upColor: '#22c55e',
    downColor: '#ef4444',
    borderDownColor: '#ef4444',
    borderUpColor: '#22c55e',
    wickDownColor: '#ef4444',
    wickUpColor: '#22c55e',
  });

  return { chart, candleSeries };
}

/** 创建面积图 (用于 PnL 曲线等) */
function createAreaChart(container, options = {}) {
  const chart = LightweightCharts.createChart(container, {
    layout: {
      background: { type: 'solid', color: '#1f2937' },
      textColor: '#9ca3af',
    },
    grid: {
      vertLines: { color: '#374151' },
      horzLines: { color: '#374151' },
    },
    rightPriceScale: { borderColor: '#374151' },
    timeScale: { borderColor: '#374151', timeVisible: true },
    ...options,
  });

  const areaSeries = chart.addAreaSeries({
    lineColor: '#6366f1',
    topColor: 'rgba(99, 102, 241, 0.4)',
    bottomColor: 'rgba(99, 102, 241, 0.04)',
    lineWidth: 2,
  });

  return { chart, areaSeries };
}

/** 创建实时价格线图 */
function createLineChart(container, options = {}) {
  const chart = LightweightCharts.createChart(container, {
    layout: {
      background: { type: 'solid', color: '#1f2937' },
      textColor: '#9ca3af',
    },
    grid: {
      vertLines: { color: '#374151' },
      horzLines: { color: '#374151' },
    },
    rightPriceScale: { borderColor: '#374151' },
    timeScale: { borderColor: '#374151', timeVisible: true, secondsVisible: true },
    ...options,
  });

  const lineSeries = chart.addLineSeries({
    color: '#6366f1',
    lineWidth: 2,
  });

  return { chart, lineSeries };
}
