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

### 终端输出与配图路径

- **拉数阶段可能较长时间无新行**：主源（如 AkShare）会重试，失败后再按 **`DATA_SOURCE_FALLBACK`** 换源。`main.py` 会先打印「正在加载行情（主源可能较慢或重试；失败将按 DATA_SOURCE_FALLBACK 切换）…」，成功换源时 **`data_providers`** 会打印 **`[data] 主源 … 已改用 …`**；随后打印 **`Data source:`**、**`数据就绪: price …, mv …`**，全程对关键 `print` 使用 **`flush=True`**，并尽量 **`sys.stdout.reconfigure(line_buffering=True)`** 减轻块缓冲。
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

**自动切换备用源**：`DATA_SOURCE_FALLBACK` 为主源失败（网络超时、断连、缺本地文件等）后的**依次尝试**列表，默认 `["baostock", "tushare", "csv", "json"]`。设为 **`[]`** 则只使用 `DATA_SOURCE`，行为与旧版一致。

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
| **`extras/`** | **非核心**：旧 Wind 迁移脚本、示例 xlsx/json、历史配图与结构图（见 `extras/README.md`） |

## 旧版美股 Excel → JSON

将 `Price_Data.xlsx`、`Market_Value.xlsx` 放入 **`extras/legacy_data/wind_us_xlsx/`**（或 `code/Data/`），在仓库根目录执行：

```bash
.venv/bin/python extras/migrate_xlsx_to_json.py
```

需安装 **`openpyxl`**。生成文件写入 **`code/Data/`** 下路径（由 `structures` 指定）。

## 其他

- 环境变量示例：根目录 **`.env.example`**（含 `TUSHARE_TOKEN`、`DATA_SOURCE` / 回退列表等说明；**当前代码默认不自动加载 `.env`**，需自行在入口 `load_dotenv` 或导出环境变量）。
- **`.gitignore`**：忽略 `.venv`、`code/Plot/*.png`、`.env` 等。

若出现 **`RemoteDisconnected` / `ConnectionError`**：AkShare 访问东财时较常见。已在 **`data_akshare`** 中做**指数退避重试**；仍失败时默认会按 **`DATA_SOURCE_FALLBACK`** 自动换源（如 **baostock**）。也可手动改 **`DATA_SOURCE`**、将 **`DATA_SOURCE_FALLBACK = []`** 关闭自动切换，或配置代理。

从仓库根目录也可执行：`MPLBACKEND=Agg .venv/bin/python code/main.py`（配图仍写入 **`code/Plot/`**）。
