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
    const dim =
      waFocusTicker != null && waFocusTicker !== t ? 0.18 : 1;
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

function buildLegend(container, tickers, colors) {
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
      const c = document.getElementById("waWeightCanvas");
      if (c && c._waRedraw) c._waRedraw();
    });
    el.addEventListener("mouseleave", () => {
      waFocusTicker = null;
      el.classList.remove("wa-focus");
      const c = document.getElementById("waWeightCanvas");
      if (c && c._waRedraw) c._waRedraw();
    });
    container.appendChild(el);
  });
}

function renderPayload(data) {
  const status = document.getElementById("waStatus");
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

  document.getElementById("waMetaSection").classList.remove("hidden");
  document.getElementById("waNewsSection").classList.remove("hidden");
  document.getElementById("waChartSection").classList.remove("hidden");
  document.getElementById("waTableSection").classList.remove("hidden");

  const meta = document.getElementById("waMeta");
  const tags = tickers.map((t) => `<span class="wa-tag">${escapeHtml(t)}</span>`).join("");
  meta.innerHTML = `
    <p><strong>年份</strong>：${escapeHtml(String(data.year ?? "—"))}
    · <strong>生成时间（UTC）</strong>：<code>${escapeHtml(data.generated_at || "—")}</code></p>
    <p><strong>行情数据源</strong>：<code>${escapeHtml(data.market_data_source || "—")}</code>
    · <strong>structures.DATA_SOURCE</strong>：<code>${escapeHtml(
      data.structures_data_source_used || "—"
    )}</code>
    · <strong>τ</strong>：${data.tau != null ? escapeHtml(String(data.tau)) : "—"}</p>
    <p><strong>周数</strong>：${weeks.length} · <strong>universe</strong>：</p>
    <div class="wa-tag-row">${tags}</div>
  `;

  const newsEl = document.getElementById("waNewsBars");
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

  const colors = tickerColors(tickers);
  const canvas = document.getElementById("waWeightCanvas");
  const legend = document.getElementById("waLegend");
  buildLegend(legend, tickers, colors);

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

  const thead = document.getElementById("waThead");
  const tbody = document.getElementById("waTbody");
  const thCells = [
    "<th>week_end</th>",
    "<th>n_news</th>",
    "<th>视图</th>",
    ...tickers.map((t) => `<th>${escapeHtml(t)}</th>`),
  ];
  thead.innerHTML = `<tr>${thCells.join("")}</tr>`;
  tbody.innerHTML = "";
  weeks.forEach((w) => {
    const tr = document.createElement("tr");
    const skip = w.skipped_no_news;
    const viewCell = skip
      ? `<span class="wa-badge">无新闻</span>`
      : `<span class="wa-badge ok">有观点</span>`;
    const wcells = tickers
      .map((t) => `<td>${pct(w.weights && w.weights[t])}</td>`)
      .join("");
    tr.innerHTML = `
      <td><strong>${escapeHtml(w.week_end || "—")}</strong></td>
      <td>${w.n_news != null ? escapeHtml(String(w.n_news)) : "—"}</td>
      <td>${viewCell}</td>
      ${wcells}
    `;
    tbody.appendChild(tr);
  });

  status.textContent = `已加载 ${weeks.length} 周 · ${tickers.length} 只标的`;
  status.classList.remove("error");
}

async function loadFromServer() {
  const status = document.getElementById("waStatus");
  status.textContent = "正在请求…";
  status.classList.remove("error");
  const url = new URL("/api/weekly-blm-agent-result", getApiBase() + "/");
  const r = await fetch(url.href, { method: "GET" });
  if (!r.ok) {
    const t = await r.text();
    let d = t;
    try {
      const j = JSON.parse(t);
      d = j.detail || t;
    } catch (_) {
      /* ignore */
    }
    throw new Error(d || `HTTP ${r.status}`);
  }
  const data = await r.json();
  renderPayload(data);
}

function main() {
  const status = document.getElementById("waStatus");
  document.getElementById("waReload").addEventListener("click", () => {
    loadFromServer().catch((e) => {
      status.textContent = String(e.message || e);
      status.classList.add("error");
    });
  });
  document.getElementById("waFile").addEventListener("change", (ev) => {
    const f = ev.target.files && ev.target.files[0];
    if (!f) return;
    const reader = new FileReader();
    reader.onload = () => {
      try {
        const data = JSON.parse(String(reader.result || "{}"));
        renderPayload(data);
      } catch (e) {
        status.textContent = "解析上传文件失败: " + (e.message || e);
        status.classList.add("error");
      }
    };
    reader.readAsText(f, "UTF-8");
  });

  loadFromServer().catch((e) => {
    status.textContent =
      (e && e.message) ||
      String(e) ||
      "加载失败。若用 Live Server 打开本页，请填 meta emotion-bl-api-base 或使用「上传 JSON」。";
    status.classList.add("error");
  });
}

document.addEventListener("DOMContentLoaded", main);
