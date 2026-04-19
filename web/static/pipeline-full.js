const PF_STREAM_URL = () => new URL("/api/weekly-pipeline/stream", getApiBase() + "/").href;
const PF_TIMEOUT_MS = 0;

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

function appendPfLog(text) {
  const ta = document.getElementById("pfLog");
  if (!ta) return;
  const t = new Date().toLocaleTimeString();
  ta.value += `[${t}] ${text}\n`;
  ta.scrollTop = ta.scrollHeight;
}

function clearPfLog() {
  const ta = document.getElementById("pfLog");
  if (ta) ta.value = "";
}

function setPfStep(index, labelFromServer) {
  document.querySelectorAll("#pfFlowSteps .flow-step").forEach((el) => {
    const n = Number(el.dataset.step);
    const on = index !== null && index !== undefined && n === index;
    el.classList.toggle("active", on);
    if (on && labelFromServer) {
      const cap = el.querySelector("h3");
      if (cap) cap.setAttribute("title", labelFromServer);
    }
  });
}

function clearPfSteps() {
  document.querySelectorAll("#pfFlowSteps .flow-step").forEach((el) => {
    el.classList.remove("active");
  });
}

function buildRequestBody() {
  const body = {
    project_root: document.getElementById("pfRoot").value.trim() || ".",
    skip_crawl: document.getElementById("pfSkipCrawl").checked,
    truncate_jsonl_before_crawl: document.getElementById("pfTruncate").checked,
    max_weeks: Number(document.getElementById("pfMaxWeeks").value) || 0,
  };
  const y = document.getElementById("pfYear").value.trim();
  if (y) body.year = Number(y);
  const ms = document.getElementById("pfMarket").value.trim();
  if (ms) body.market_source = ms;
  const feeds = document.getElementById("pfFeeds").value.trim();
  if (feeds) {
    body.feed_urls = feeds.split(/[,，\s]+/).map((u) => u.trim()).filter(Boolean);
  }
  return body;
}

async function runPipelineFull() {
  const btn = document.getElementById("pfRunBtn");
  const status = document.getElementById("pfStatus");
  const doneSec = document.getElementById("pfDoneSection");
  const doneCard = document.getElementById("pfDoneCard");

  btn.disabled = true;
  doneSec.classList.add("hidden");
  status.textContent =
    PF_TIMEOUT_MS > 0
      ? `运行中…（前端最长 ${PF_TIMEOUT_MS / 1000}s；周频+LLM 可能很久）`
      : "运行中…（无前端超时；AkShare/LLM 较慢时请耐心等待）";
  status.classList.remove("error");
  clearPfLog();
  clearPfSteps();
  appendPfLog(`POST ${PF_STREAM_URL()}`);

  const ac = new AbortController();
  const tid = PF_TIMEOUT_MS > 0 ? setTimeout(() => ac.abort(), PF_TIMEOUT_MS) : null;

  try {
    const res = await fetch(PF_STREAM_URL(), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(buildRequestBody()),
      signal: ac.signal,
    });
    if (!res.ok) {
      const t = await res.text();
      throw new Error(t || `HTTP ${res.status}`);
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
          appendPfLog("(解析行失败) " + line.slice(0, 200));
          continue;
        }
        if (obj.type === "step" && typeof obj.index === "number") {
          setPfStep(obj.index, obj.label || "");
          appendPfLog(`>>> 阶段 ${obj.index}: ${obj.label || ""}`);
          document
            .querySelector(`#pfFlowSteps [data-step="${obj.index}"]`)
            ?.scrollIntoView({ behavior: "smooth", block: "nearest" });
        } else if (obj.type === "log" && obj.message) {
          appendPfLog(obj.message);
        } else if (
          obj.type === "progress" &&
          obj.kind === "blm_week" &&
          typeof obj.current === "number" &&
          typeof obj.total === "number"
        ) {
          const we = obj.week_end || "";
          status.textContent = `逐周 BLM：${obj.current}/${obj.total}（${we}）`;
        } else if (
          obj.type === "progress" &&
          obj.kind === "llm_articles" &&
          typeof obj.current === "number" &&
          typeof obj.total === "number"
        ) {
          status.textContent = `本周情感 LLM：${obj.current}/${obj.total} 条已完成`;
        } else if (obj.type === "error") {
          appendPfLog("错误: " + (obj.message || ""));
          status.textContent = obj.message || "出错";
          status.classList.add("error");
          finished = true;
          break;
        } else if (obj.type === "done") {
          setPfStep(8, "落盘");
          appendPfLog(">>> 全部完成");
          status.textContent = "完成。";
          const u = obj.universe || [];
          doneCard.innerHTML = `
            <p><strong>生成时间</strong>：<code>${escapeHtml(obj.generated_at || "—")}</code></p>
            <p><strong>年</strong>：${escapeHtml(String(obj.year ?? "—"))}
            · <strong>周数</strong>：${escapeHtml(String(obj.weeks_count ?? "—"))}
            · <strong>行情源</strong>：<code>${escapeHtml(obj.market_data_source || "—")}</code></p>
            <p><strong>输出文件</strong>：<code>${escapeHtml(obj.result_relative || "—")}</code></p>
            <p><strong>Universe</strong>：${u.map((t) => `<code>${escapeHtml(t)}</code>`).join(" ")}</p>
            <p><a class="btn secondary" href="${escapeHtml(
              obj.dashboard_url || "/weekly-agent"
            )}">打开周频看板</a></p>
          `;
          doneSec.classList.remove("hidden");
          finished = true;
          break;
        }
      }
      if (finished) break;
    }

    if (!finished) {
      appendPfLog("流结束但未收到 done/error。");
      status.textContent = "未正常结束";
      status.classList.add("error");
    }
  } catch (e) {
    if (e.name === "AbortError") {
      appendPfLog("已中止（前端超时）。");
      status.textContent = "已中止";
    } else {
      appendPfLog("异常: " + (e.message || e));
      status.textContent = String(e.message || e);
    }
    status.classList.add("error");
  } finally {
    if (tid) clearTimeout(tid);
    btn.disabled = false;
  }
}

document.getElementById("pfRunBtn").addEventListener("click", runPipelineFull);
