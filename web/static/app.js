function simpleMarkdown(md) {
  if (!md) return "";
  return md
    .split(/\n\n+/)
    .map((block) => {
      const line = block.replace(/\*\*(.+?)\*\*/g, "<strong>$1</strong>");
      return `<p>${line}</p>`;
    })
    .join("");
}

function parseTickers(raw) {
  const s = (raw || "").trim();
  if (!s) return null;
  return s
    .split(/[,，\s]+/)
    .map((t) => t.trim())
    .filter(Boolean);
}

/** 0 = 前端不中断请求（LLM 批量分析可能数分钟）。若需限制可改为毫秒，如 120000 */
const RUN_TIMEOUT_MS = 0;

function getApiBase() {
  const m = document.querySelector('meta[name="emotion-bl-api-base"]');
  const c = m && m.getAttribute("content");
  if (c != null && String(c).trim() !== "") {
    return String(c).trim().replace(/\/$/, "");
  }
  return window.location.origin;
}

const STREAM_FALLBACK_URLS = () => {
  const base = getApiBase() + "/";
  return [
    new URL("/stream/ndjson", base).href,
    new URL("/api/run/stream", base).href,
    new URL("/api/run-stream", base).href,
  ];
};

async function discoverStreamUrls() {
  const base = getApiBase() + "/";
  try {
    const r = await fetch(new URL("/api/meta", base).href, { method: "GET" });
    if (r.ok) {
      const j = await r.json();
      if (Array.isArray(j.stream_post_paths) && j.stream_post_paths.length) {
        return {
          urls: j.stream_post_paths.map((p) => new URL(p, base).href),
          meta: j,
        };
      }
    }
  } catch (_) {
    /* ignore */
  }
  return { urls: STREAM_FALLBACK_URLS(), meta: null };
}

function appendRunLog(text) {
  const ta = document.getElementById("runLog");
  if (!ta) return;
  const t = new Date().toLocaleTimeString();
  ta.value += `[${t}] ${text}\n`;
  ta.scrollTop = ta.scrollHeight;
}

function clearRunLog() {
  const ta = document.getElementById("runLog");
  if (ta) ta.value = "";
}

function openFlowPanelStatic() {
  const panel = document.getElementById("flowPanel");
  const btn = document.getElementById("flowBtn");
  if (!panel || !btn) return;
  panel.classList.remove("hidden");
  panel.setAttribute("aria-hidden", "false");
  btn.setAttribute("aria-expanded", "true");
  btn.textContent = "收起全流程";
}

function setFlowStepLive(index) {
  clearFlowAnimation();
  document.querySelectorAll(".flow-step").forEach((el) => {
    const n = Number(el.dataset.step);
    const on = index !== null && index !== undefined && n === index;
    el.classList.toggle("active", on);
  });
}

function renderDashboardResult(data) {
  const summaryEl = document.getElementById("summary");
  const tbody = document.querySelector("#viewsTable tbody");
  const emptyHint = document.getElementById("emptyViews");
  const recentEl = document.getElementById("recent");
  const resultSection = document.getElementById("resultSection");

  const dash = data.dashboard;
  const meta = `<p class="meta">生成时间（UTC）：${escapeHtml(
    dash.generated_at || "—"
  )} · 分析条数：${dash.articles_analyzed ?? "—"}</p>`;
  summaryEl.innerHTML = meta + simpleMarkdown(dash.summary_markdown || "");

  const views = dash.preset_views || [];
  tbody.innerHTML = "";
  if (views.length === 0) {
    emptyHint.classList.remove("hidden");
  } else {
    emptyHint.classList.add("hidden");
    for (const v of views) {
      const tr = document.createElement("tr");
      const dirClass =
        v.direction === "偏多" ? "dir-long" : v.direction === "偏空" ? "dir-short" : "";
      tr.innerHTML = `
          <td><strong>${escapeHtml(v.ticker)}</strong></td>
          <td class="${dirClass}">${escapeHtml(v.direction)}</td>
          <td>${v.sentiment_mean}</td>
          <td>${v.view_q_bps}</td>
          <td>${v.omega}</td>
          <td>
            <div class="cred-bar" title="Ω 越小，批次内相对可信度越高">
              <span>${v.credibility_percent}%（${escapeHtml(v.credibility_label)}）</span>
              <i><b style="width:${v.credibility_percent}%"></b></i>
            </div>
          </td>
          <td>${v.supporting_articles}</td>
        `;
      tbody.appendChild(tr);
    }
  }

  recentEl.innerHTML = "";
  (data.recent_articles || []).forEach((a) => {
    const li = document.createElement("li");
    li.innerHTML = `${escapeHtml(a.title || "")} <span class="score">${escapeHtml(
      a.label || ""
    )} · ${typeof a.score === "number" ? a.score.toFixed(3) : ""}</span>`;
    recentEl.appendChild(li);
  });

  resultSection.classList.remove("hidden");
}

async function run() {
  const btn = document.getElementById("runBtn");
  const status = document.getElementById("status");

  btn.disabled = true;
  status.textContent =
    RUN_TIMEOUT_MS > 0
      ? `运行中…（前端最长 ${RUN_TIMEOUT_MS / 1000} 秒）`
      : "运行中…（无前端超时；LLM 模式按条数 × 单次耗时，请看日志）";
  status.classList.remove("error");

  clearRunLog();
  openFlowPanelStatic();
  setFlowStepLive(null);
  appendRunLog("开始请求流式分析 …");
  const { urls: streamUrlCandidates, meta: apiMeta } = await discoverStreamUrls();
  if (apiMeta && apiMeta.main_file) {
    appendRunLog(`(自检) 当前 API 模块文件: ${apiMeta.main_file}`);
    if (apiMeta.analysis_deadline_sec != null) {
      const readS = apiMeta.llm_per_article_read_sec ?? "—";
      const retries = apiMeta.llm_per_article_max_retries ?? "—";
      appendRunLog(
        `(配置) ANALYSIS_DEADLINE_SEC=${apiMeta.analysis_deadline_sec} · LLM 单条读超时=${readS}s · 单条最多重试=${retries} · LLM_MAX_CONCURRENT=${apiMeta.llm_max_concurrent ?? "—"} · 情感=${apiMeta.sentiment_backend}`
      );
    }
  } else {
    appendRunLog("(自检) GET /api/meta 不可用，将依次尝试流式路径（多为旧进程或非本项目）。");
  }

  const body = {
    crawl: document.getElementById("crawl").checked,
    project_root: ".",
    max_articles: Number(document.getElementById("maxArticles").value) || 40,
  };

  const feeds = document.getElementById("feeds").value.trim();
  if (feeds) {
    body.feed_urls = feeds.split(/[,，\s]+/).map((u) => u.trim()).filter(Boolean);
  }

  const jp = document.getElementById("jsonlPath").value.trim();
  if (jp) body.jsonl_path = jp;

  const tickers = parseTickers(document.getElementById("tickers").value);
  if (tickers && tickers.length) body.tickers = tickers;

  const ac = new AbortController();
  const timeoutId =
    RUN_TIMEOUT_MS > 0 ? setTimeout(() => ac.abort(), RUN_TIMEOUT_MS) : null;

  try {
    let res = null;
    let lastStatus = 0;
    const fetchOpts = {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    };
    if (RUN_TIMEOUT_MS > 0) fetchOpts.signal = ac.signal;

    for (const streamUrl of streamUrlCandidates) {
      appendRunLog(`POST ${streamUrl}`);
      res = await fetch(streamUrl, { ...fetchOpts });
      lastStatus = res.status;
      if (res.ok) break;
      if (res.status === 404) {
        appendRunLog(`→ 404，换下一候选地址`);
        continue;
      }
      break;
    }

    if (!res || !res.ok) {
      const errText = res ? await res.text() : "";
      let detail = errText;
      try {
        const j = JSON.parse(errText);
        detail = j.detail || errText;
      } catch (_) {
        /* ignore */
      }
      throw new Error(
        detail ||
          (res && res.statusText) ||
          `HTTP ${lastStatus || "?"} — 全部流式地址均失败。请在本项目根目录执行: python run_api.py（先关闭占用 8000 的旧进程），并打开 http://127.0.0.1:8000/ 。浏览器访问 http://127.0.0.1:8000/api/meta 应返回 JSON 且含 stream_post_paths。`
      );
    }

    const reader = res.body.getReader();
    const dec = new TextDecoder();
    let buffer = "";
    let finished = false;

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += dec.decode(value, { stream: true });
      let nl;
      while ((nl = buffer.indexOf("\n")) >= 0) {
        const line = buffer.slice(0, nl).trim();
        buffer = buffer.slice(nl + 1);
        if (!line) continue;
        let obj;
        try {
          obj = JSON.parse(line);
        } catch (e) {
          appendRunLog("(解析行失败) " + line.slice(0, 200));
          continue;
        }
        if (obj.type === "step" && typeof obj.index === "number") {
          setFlowStepLive(obj.index);
          appendRunLog(`>>> 阶段 ${obj.index + 1}/7 进行中`);
        } else if (
          obj.type === "progress" &&
          obj.kind === "llm_articles" &&
          typeof obj.current === "number" &&
          typeof obj.total === "number"
        ) {
          setFlowStepLive(2);
          status.textContent = `运行中… 情感 LLM ${obj.current}/${obj.total} 条（按完成计数）`;
          appendRunLog(`[进度] LLM 已完成 ${obj.current}/${obj.total} 条`);
        } else if (obj.type === "log" && obj.message) {
          appendRunLog(obj.message);
        } else if (obj.type === "error") {
          appendRunLog("错误: " + (obj.message || ""));
          status.textContent = obj.message || "出错";
          status.classList.add("error");
          finished = true;
          break;
        } else if (obj.type === "done") {
          renderDashboardResult(obj);
          setFlowStepLive(6);
          appendRunLog(">>> 全部完成");
          status.textContent = "完成。";
          finished = true;
          break;
        }
      }
      if (finished) break;
    }

    if (!finished) {
      appendRunLog("流结束但未收到 done 事件。");
      status.textContent = "未正常结束";
      status.classList.add("error");
    }
  } catch (e) {
    if (e.name === "AbortError" && RUN_TIMEOUT_MS > 0) {
      appendRunLog(`已中止：前端等待超过 ${RUN_TIMEOUT_MS / 1000} 秒。`);
      status.textContent = `已超时（${RUN_TIMEOUT_MS / 1000}s），可减少「最多分析条数」或把 RUN_TIMEOUT_MS 改为 0。`;
    } else {
      appendRunLog("异常: " + (e.message || e));
      status.textContent = String(e.message || e);
    }
    status.classList.add("error");
  } finally {
    if (timeoutId) clearTimeout(timeoutId);
    btn.disabled = false;
  }
}

function escapeHtml(s) {
  return String(s)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

document.getElementById("runBtn").addEventListener("click", run);

const FLOW_STEP_MS = 520;
let flowAnimTimer = null;

function clearFlowAnimation() {
  if (flowAnimTimer) {
    clearTimeout(flowAnimTimer);
    flowAnimTimer = null;
  }
}

function playFlowSequence() {
  const steps = document.querySelectorAll(".flow-step");
  clearFlowAnimation();
  steps.forEach((el) => el.classList.remove("active"));
  let i = 0;
  function tick() {
    if (i > 0) steps[i - 1].classList.remove("active");
    if (i < steps.length) {
      steps[i].classList.add("active");
      steps[i].scrollIntoView({ behavior: "smooth", block: "nearest" });
      i += 1;
      flowAnimTimer = setTimeout(tick, FLOW_STEP_MS);
    } else {
      flowAnimTimer = null;
      steps.forEach((el) => el.classList.add("active"));
    }
  }
  tick();
}

document.getElementById("flowBtn").addEventListener("click", function () {
  const panel = document.getElementById("flowPanel");
  const btn = document.getElementById("flowBtn");
  const open = panel.classList.contains("hidden");
  if (open) {
    panel.classList.remove("hidden");
    panel.setAttribute("aria-hidden", "false");
    btn.setAttribute("aria-expanded", "true");
    btn.textContent = "收起全流程";
    panel.scrollIntoView({ behavior: "smooth", block: "start" });
    playFlowSequence();
  } else {
    panel.classList.add("hidden");
    panel.setAttribute("aria-hidden", "true");
    btn.setAttribute("aria-expanded", "false");
    btn.textContent = "展示全流程：从采集到观点";
    clearFlowAnimation();
    document.querySelectorAll(".flow-step").forEach((el) => el.classList.remove("active"));
  }
});
