/**
 * 周频 BLM 结果可视化共用：权重曲线、新闻条数条、摘要卡片。
 * 供 weekly-agent.js 与 pipeline-full.js 复用。
 */
function getApiBase() {
  const m = document.querySelector('meta[name="emotion-bl-api-base"]');
  const c = m && m.getAttribute("content");
  if (c != null && String(c).trim() !== "") {
    return String(c).trim().replace(/\/$/, "");
  }
  return window.location.origin;
}

function escapeHtml(s) {
  return String(s)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function pct(x) {
  if (typeof x !== "number" || Number.isNaN(x)) return "—";
  return (x * 100).toFixed(2) + "%";
}

function tickerColors(tickers) {
  return tickers.map((_, i) => `hsl(${(i * 37) % 360} 68% 55%)`);
}

/** @type {string | null} */
let waFocusTicker = null;

function drawWeightChart(canvas, weeks, tickers, colors) {
  const ctx = canvas.getContext("2d");
  if (!ctx) return;
  const cssW = canvas.clientWidth || 900;
  const cssH = canvas.clientHeight || 320;
  const dpr = Math.min(window.devicePixelRatio || 1, 2);
  canvas.width = Math.floor(cssW * dpr);
  canvas.height = Math.floor(cssH * dpr);
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  ctx.clearRect(0, 0, cssW, cssH);

  const pad = { l: 44, r: 12, t: 10, b: 36 };
  const n = weeks.length;
  const innerW = cssW - pad.l - pad.r;
  const innerH = cssH - pad.t - pad.b;

  ctx.strokeStyle = "#2d3a4f";
  ctx.lineWidth = 1;
  ctx.beginPath();
  ctx.moveTo(pad.l, pad.t);
  ctx.lineTo(pad.l, pad.t + innerH);
  ctx.lineTo(pad.l + innerW, pad.t + innerH);
  ctx.stroke();

  ctx.fillStyle = "#8b9bb4";
  ctx.font = "11px Segoe UI, PingFang SC, Microsoft YaHei, sans-serif";
  for (let g = 0; g <= 4; g += 1) {
    const yv = g / 4;
    const y = pad.t + innerH * (1 - yv);
    ctx.strokeStyle = "rgba(45, 58, 79, 0.45)";
    ctx.beginPath();
    ctx.moveTo(pad.l, y);
    ctx.lineTo(pad.l + innerW, y);
    ctx.stroke();
    ctx.fillText(`${(yv * 100).toFixed(0)}%`, 4, y + 4);
  }

  if (n === 0) {
    ctx.fillStyle = "#8b9bb4";
    ctx.fillText("无周数据", pad.l, pad.t + innerH / 2);
    return;
  }

  function xAt(i) {
    if (n <= 1) return pad.l + innerW / 2;
    return pad.l + (i / (n - 1)) * innerW;
  }

  tickers.forEach((t, ti) => {
    const dim = waFocusTicker != null && waFocusTicker !== t ? 0.18 : 1;
    ctx.globalAlpha = dim;
    ctx.strokeStyle = colors[ti];
    ctx.lineWidth = waFocusTicker === t ? 2.6 : 1.7;
    ctx.beginPath();
    weeks.forEach((wk, i) => {
      const wt = wk.weights && typeof wk.weights[t] === "number" ? wk.weights[t] : 0;
      const y = pad.t + innerH * (1 - wt);
      if (i === 0) ctx.moveTo(xAt(i), y);
      else ctx.lineTo(xAt(i), y);
    });
    ctx.stroke();
  });
  ctx.globalAlpha = 1;

  const ix = [0, Math.floor((n - 1) / 2), n - 1].filter((v, j, a) => a.indexOf(v) === j);
  ctx.fillStyle = "#8b9bb4";
  ix.forEach((i) => {
    const lab = (weeks[i] && weeks[i].week_end) || String(i);
    const x = xAt(i);
    ctx.fillText(lab, x - 18, pad.t + innerH + 22);
  });
}

/**
 * @param {HTMLElement} container
 * @param {string[]} tickers
 * @param {string[]} colors
 * @param {string} canvasId 用于悬停重绘的 canvas 元素 id
 */
function buildLegend(container, tickers, colors, canvasId) {
  const cid = canvasId || "waWeightCanvas";
  container.innerHTML = "";
  tickers.forEach((t, i) => {
    const el = document.createElement("span");
    el.className = "wa-legend-item";
    el.dataset.ticker = t;
    el.innerHTML = `<i class="wa-legend-swatch" style="background:${colors[i]}"></i><span>${escapeHtml(
      t
    )}</span>`;
    el.addEventListener("mouseenter", () => {
      waFocusTicker = t;
      el.classList.add("wa-focus");
      document.querySelectorAll(".wa-legend-item").forEach((o) => {
        if (o !== el) o.classList.remove("wa-focus");
      });
      const c = document.getElementById(cid);
      if (c && c._waRedraw) c._waRedraw();
    });
    el.addEventListener("mouseleave", () => {
      waFocusTicker = null;
      el.classList.remove("wa-focus");
      const c = document.getElementById(cid);
      if (c && c._waRedraw) c._waRedraw();
    });
    container.appendChild(el);
  });
}

/**
 * 填充摘要、每周新闻条数、权重图（不含明细表）。
 * @param {object} data — weekly_blm_agent_result.json 结构
 * @param {{ metaId: string, newsBarsId: string, canvasId: string, legendId: string }} ids
 */
function renderWeeklyResultPanels(data, ids) {
  if (!data || !Array.isArray(data.weeks)) {
    throw new Error("JSON 缺少 weeks 数组");
  }
  let tickers =
    Array.isArray(data.universe) && data.universe.length ? data.universe.slice() : [];
  if (!tickers.length && data.weeks.length) {
    tickers = Object.keys(data.weeks[0].weights || {});
  }

  const weeks = [...data.weeks].sort((a, b) =>
    String(a.week_end || "").localeCompare(String(b.week_end || ""))
  );

  const maxNews = Math.max(1, ...weeks.map((w) => Number(w.n_news) || 0));

  const meta = document.getElementById(ids.metaId);
  const newsEl = document.getElementById(ids.newsBarsId);
  if (meta) {
    const tags = tickers.map((t) => `<span class="wa-tag">${escapeHtml(t)}</span>`).join("");
    meta.innerHTML = `
    <p><strong>年份</strong>：${escapeHtml(String(data.year ?? "—"))}
    · <strong>生成时间（UTC）</strong>：<code>${escapeHtml(data.generated_at || "—")}</code></p>
    <p><strong>行情数据源</strong>：<code>${escapeHtml(data.market_data_source || "—")}</code>
    · <strong>structures.DATA_SOURCE</strong>：<code>${escapeHtml(
      data.structures_data_source_used || "—"
    )}</code>
    · <strong>τ</strong>：${data.tau != null ? escapeHtml(String(data.tau)) : "—"}</p>
    <p><strong>说明</strong>：市值隐含均衡收益与协方差构成 Black-Litterman 先验；<strong>每周情感观点</strong>以视图矩阵 (P,Q,Ω) 形式并入模型，得到后验权重（非直接替换 π）。</p>
    <p><strong>周数</strong>：${weeks.length} · <strong>universe</strong>：</p>
    <div class="wa-tag-row">${tags}</div>
  `;
  }

  if (newsEl) {
    newsEl.innerHTML = "";
    weeks.forEach((w) => {
      const n = Number(w.n_news) || 0;
      const row = document.createElement("div");
      row.className = "wa-news-row";
      const pctW = (n / maxNews) * 100;
      const skip = w.skipped_no_news ? " wa-skip" : "";
      row.innerHTML = `
      <span class="wa-news-label">${escapeHtml(w.week_end || "—")}</span>
      <div class="wa-news-bar-wrap"><div class="wa-news-bar${skip}" style="width:${pctW}%"></div></div>
      <span class="wa-news-label" style="flex:0 0 2rem;text-align:right">${n}</span>
    `;
      newsEl.appendChild(row);
    });
  }

  const colors = tickerColors(tickers);
  const canvas = document.getElementById(ids.canvasId);
  const legend = document.getElementById(ids.legendId);
  if (canvas && legend) {
    buildLegend(legend, tickers, colors, ids.canvasId);
    const redraw = () => drawWeightChart(canvas, weeks, tickers, colors);
    canvas._waRedraw = redraw;
    redraw();
    if (!canvas._waResizeBound) {
      canvas._waResizeBound = true;
      let resizeTimer = null;
      window.addEventListener("resize", () => {
        if (resizeTimer) clearTimeout(resizeTimer);
        resizeTimer = setTimeout(() => redraw(), 120);
      });
    }
  }

  return { weeks, tickers };
}

/** 从项目根相对路径解析 weekly-blm API 查询参数（文件须在 data/ 下） */
function weeklyResultFetchUrl(resultRelative) {
  const u = new URL("/api/weekly-blm-agent-result", getApiBase() + "/");
  const rr = String(resultRelative || "")
    .replace(/\\/g, "/")
    .replace(/^data\//, "");
  if (rr && rr !== "weekly_blm_agent_result.json") {
    u.searchParams.set("relative_path", rr);
  }
  return u.href;
}
