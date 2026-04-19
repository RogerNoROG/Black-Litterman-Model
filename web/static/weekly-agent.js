function renderPayload(data) {
  const status = document.getElementById("waStatus");
  const { weeks, tickers } = renderWeeklyResultPanels(data, {
    metaId: "waMeta",
    newsBarsId: "waNewsBars",
    canvasId: "waWeightCanvas",
    legendId: "waLegend",
  });

  document.getElementById("waMetaSection").classList.remove("hidden");
  document.getElementById("waNewsSection").classList.remove("hidden");
  document.getElementById("waChartSection").classList.remove("hidden");
  document.getElementById("waTableSection").classList.remove("hidden");

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
