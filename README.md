# Black-Litterman-Model

Python 实现的 **Black-Litterman** 周频回测：默认 **AkShare** 拉取 **沪深300、上证综指、深证成指** 及 **沪深300 权重前十成分股**，与 **等权组合** 对比累计对数收益。

## 快速运行

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
cd code
MPLBACKEND=Agg ../.venv/bin/python main.py
```

- 配置见 **`code/structures.py`**（`DATA_SOURCE`、`DATA_SOURCE_FALLBACK`、`BACK_TEST_YEAR`、`VIEW_TYPE` 等）。

### 情绪量化 Agent（原 emotion-quanty-agent 已并入本仓库）

仓库根目录提供 **FastAPI**：默认 **Istero 中文新闻 API**（或 RSS）→ 情感分析 → 生成 Black-Litterman 观点矩阵并与市值先验/均衡收益融合（实现见 **`emotion_bl/`**；周频管线用 **`code/black_litterman.py`** 的 `BlackLitterman`，与 **`emotion_bl/bl/black_litterman.py`** 的 `BlackLittermanEngine` 为不同入口）。

```bash
# 仓库根目录；先复制 .env.example 为 .env 并按需填写（尤其 LLM 相关）
./restart_server.sh
# 或: .venv/bin/python run_api.py
```

- 静态页与前端脚本：**`web/`**；爬虫工程：**`news_crawler/`**，配置 **`scrapy.cfg`**。
- 环境变量说明：根目录 **`.env.example`** 中「情绪量化 Agent」一节；`emotion_bl.config.Settings` 会读取 `.env`。

**Web 路由（`run_api.py` 启动后，默认 `http://127.0.0.1:8000`）**

| 路径 | 说明 |
|------|------|
| **`/`** | 时效性情绪 → 预设观点（流式 NDJSON：`/api/run/stream` 等） |
| **`/weekly-agent`** | 周频 Agent 结果看板（读 `data/weekly_blm_agent_result.json`，`GET /api/weekly-blm-agent-result`） |
| **`/pipeline-full`** | 全流程：默认 **Istero 中文新闻 API** → 周线分桶 → 行情 → 逐周情感视图并入 BL → 落盘并在页内展示权重图（`POST /api/weekly-pipeline/stream`） |

一键打开全流程页（会尝试后台启动 API）：**`./scripts/one_click_weekly_pipeline_web.sh`**。

### 终端输出与配图路径

- **拉数阶段可能较长时间无新行**：主源（如 AkShare）会重试；若 **`DATA_SOURCE_FALLBACK`** 非空，失败后会依次换源。`main.py` 会先打印加载提示；换源成功时 **`data_providers`** 可打印 **`[data] 主源 … 已改用 …`**；随后打印 **`Data source:`**、**`数据就绪: price …, mv …`**。关键 `print` 使用 **`flush=True`**，并尽量 **`sys.stdout.reconfigure(line_buffering=True)`** 减轻块缓冲。
- **标准输出缓冲**：若仍觉得「迟迟不出字」，可在命令前加 **`PYTHONUNBUFFERED=1`**，或改用终端直接运行。
- **配图目录固定为 `code/Plot/`**：与你在哪个目录执行 `python` **无关**（`back_test.py` 按脚本位置解析路径）。运行结束会打印一行 **`图已保存: <绝对路径>`**。
- 使用 **`MPLBACKEND=Agg`** 时**不会弹出窗口**，只写入 PNG。回测结束另有 **`完成。`**。仓库根目录下若另有小写 **`plot/`** 等旧路径，与当前程序默认输出**无关**，请以终端里的 **`图已保存`** 为准。

### 数据源 `DATA_SOURCE`（`code/data_providers.py` 统一入口）

| 取值 | 说明 |
|------|------|
| **`akshare`** | 东财/新浪等接口拉取（默认）；可选 `AKSHARE_SAVE_JSON_AFTER_FETCH` 落盘 JSON |
| **`json`** | 读 `PRICE_DATA_PATH`、`MV_DATA_PATH`（见下） |
| **`csv`** | 读 `PRICE_CSV_PATH`、`MV_CSV_PATH`，列与 JSON 导出表一致，建议 UTF-8（Excel 另存可选 utf-8-sig） |
| **`baostock`** | [Baostock](http://baostock.com) 周 K，**无需 token**；区间与 `AKSHARE_START_DATE` / `AKSHARE_END_DATE` 共用；市值列为价×1 的代理 |
| **`tushare`** | [Tushare Pro](https://tushare.pro)：`index_daily` + `daily`/`pro_bar`（与 `AKSHARE_ADJUST` 对齐）周频；市值由 `daily_basic.total_mv`（万元）与日线收盘推算股本；需 **`TUSHARE_TOKEN`**（环境变量或 `structures.TUSHARE_TOKEN`），可选 `TUSHARE_REQUEST_PAUSE_SEC` 限频 |

**主数据源备用列表**：`DATA_SOURCE_FALLBACK` 为主源失败后的**依次尝试**列表。仓库当前默认 **`[]`**（仅用 `DATA_SOURCE`，例如固定 AkShare）；可自行改为 `["baostock", "tushare", "csv", "json"]` 等以启用自动换源。

**AkShare 个股日 K（仅影响成分股，指数另走东财 EM + 新浪备用）**：`AKSHARE_STOCK_HIST_SOURCES` 为优先级元组，同一次 `fetch_bl_tables` 内会**粘性**使用先成功的源（东财 `stock_zh_a_hist` / 腾讯 `stock_zh_a_hist_tx`），该源整段失败后再按列表重探测。见 `code/structures.py`、`code/data_akshare.py`。

## 周频情绪 × Black-Litterman（沪深 300 前十 + 周线）

- **CLI**：**`scripts/run_weekly_blm_agent.py`** — 默认 **Istero 中文新闻 API**（`--news-source api`，需 **`ISTEREO_API_TOKEN`**）或 **`--news-source rss`**（Scrapy）→ `data/news_items.jsonl` → 按 **W-FRI** 分桶 → 情感/观点 `pipeline_analyze` → 与市值先验融合（**`get_post_weight_with_sentiment_views`**）→ **`data/weekly_blm_agent_result.json`**。
- **共用逻辑**：**`emotion_bl/weekly_pipeline.py`**（`run_weekly_pipeline`），供 CLI 与 **`POST /api/weekly-pipeline/stream`** 复用。
- **注意**：新闻 `published` 所在自然年应与 **`--year`** 及行情区间大致一致，否则多数周线桶会「无新闻」；详见管线日志中的桶日期范围提示。
- 常用参数：`--year`、`--skip-crawl`、`--market-source json|akshare|…`、`--jsonl`、`--out`。

## Istero 央视要闻（可选）

- `.env` 中 **`ISTEREO_API_TOKEN`**（Bearer，不要写 `Bearer ` 前缀）。
- **拉取并写 JSONL**：`.venv/bin/python scripts/fetch_istero_cctv_news.py`（默认覆盖写入；**`--append`** 追加）。详见 **`emotion_bl/istero_news.py`**。

## LLM-BLM 桥接（可选）

- 子模块：**`third_party/LLM-BLM`**（美股实验向，自有 README）。
- 与本仓库 BL 矩阵引擎对齐的示例：**`extras/llm_blm/`**、**`scripts/demo_llm_blm_bridge.py`**。

## 研究设计自检

- 根目录 **`Todo.md`**：论文/实证方法链条待补项（与代码功能非一一对应）；末附「与仓库代码的对照」表。

## 数据（JSON）

UTF-8，根对象含 `version`、`kind`、`records`；每条记录含 `Date`（`YYYY-MM-DD`）。  
价格表：除 `Date` 外为各指数与股票的周收盘价列（与 `data_akshare` 列名一致）。  
市值表：各股票市值列 + **`Total`**（兼容列名 `TOTAL`）。

## 成分与列名

- 股指：`CSI300.GI`、`SSE.GI`、`SZCI.GI`（新浪：`sh000300`、`sh000001`、`sz399001`）。
- 股票（与 `data_akshare.STOCKS` 及 `P` 矩阵下标一致）：  
  `300750`、`600519`、`300308`、`601318`、`601899`、`600036`、`300502`、`000333`、`600900`、`601166`。

## 观点类型（`VIEW_TYPE` 0–3）

在 **`black_litterman.get_views_P_Q_matrix`** 中定义；类型 0 在 **`get_post_weight`** 中直接使用市值权重。

## 仓库布局

| 路径 | 作用 |
|------|------|
| `code/main.py` | 入口 |
| `code/black_litterman.py` | 模型核心 |
| `code/back_test.py` | 回测与作图 |
| `code/structures.py` | 参数 |
| `code/data_providers.py` | 多数据源调度；`fetch_price_market_pair()` 返回 **`(price_df, mv_df, 实际源名)`** |
| `code/data_akshare.py` | AkShare 实现 |
| `code/data_json.py` | JSON 读写 |
| `code/Data/` | 本地 JSON 数据目录（可空，含 `.gitkeep`） |
| **`emotion_bl/`** | **情绪 Agent**：API、情感分析、BL 观点与 `BlackLittermanEngine` |
| **`run_api.py`** | 启动 FastAPI（`uvicorn emotion_bl.api.main:app`） |
| **`news_crawler/`**、`scrapy.cfg` | Scrapy 新闻爬虫 |
| **`web/`** | Agent 前端静态资源 |
| **`data/`** | 新闻 JSONL 等示例数据（Agent 默认 `NEWS_JSONL_PATH`） |
| **`scripts/`** | `run_weekly_blm_agent.py` 周频 Agent；`fetch_istero_cctv_news.py` Istero；`one_click_weekly_pipeline_web.sh`；`demo_llm_blm_bridge.py`；`verify_api.py` |
| **`emotion_bl/weekly_pipeline.py`** | 周频管线核心（CLI / API 流式共用） |
| **`extras/`** | **非核心**：旧 Wind 迁移、`llm_blm` 桥接示例、历史配图（见 `extras/README.md`） |
| **`third_party/LLM-BLM`** | 外部 LLM-BLM 子模块（可选 `git submodule`） |
| **`Todo.md`** | 方法论文档向自检 + 与代码对照附录 |

## 旧版美股 Excel → JSON

将 `Price_Data.xlsx`、`Market_Value.xlsx` 放入 **`extras/legacy_data/wind_us_xlsx/`**（或 `code/Data/`），在仓库根目录执行：

```bash
.venv/bin/python extras/migrate_xlsx_to_json.py
```

需安装 **`openpyxl`**。生成文件写入 **`code/Data/`** 下路径（由 `structures` 指定）。

## 其他

- 环境变量示例：根目录 **`.env.example`**（回测相关：`TUSHARE_TOKEN`、`DATA_SOURCE` 等；**`code/main.py` 默认不自动加载 `.env`**；情绪 Agent 通过 **`emotion_bl.config`** 读取 `.env`）。
- **`.gitignore`**：忽略 `.venv`、`code/Plot/*.png`、`api_server.log`、`.env` 等。

若出现 **`RemoteDisconnected` / `ConnectionError`**：AkShare 访问东财时较常见。已在 **`data_akshare`** 中对**指数与个股**做退避重试；**个股**还可按 **`AKSHARE_STOCK_HIST_SOURCES`** 在东财与腾讯之间自动切换。整表数据源仍可通过非空的 **`DATA_SOURCE_FALLBACK`** 在 akshare / baostock / tushare / csv / json 间切换；当前默认 **`DATA_SOURCE_FALLBACK = []`** 时则仅主源。也可配置 HTTP(S) 代理。

从仓库根目录也可执行：`MPLBACKEND=Agg .venv/bin/python code/main.py`（配图仍写入 **`code/Plot/`**）。
