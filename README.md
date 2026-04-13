# Black-Litterman-Model

Python 实现的 **Black-Litterman** 周频回测：默认 **AkShare** 拉取 **沪深300、上证综指、深证成指** 及 **沪深300 权重前十成分股**，与 **等权组合** 对比累计对数收益。

## 快速运行

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
cd code
MPLBACKEND=Agg ../.venv/bin/python main.py
```

- 配置见 **`code/structures.py`**（`DATA_SOURCE`、`BACK_TEST_YEAR`、`VIEW_TYPE` 等）。
- `DATA_SOURCE="json"` 时需在 **`code/Data/`** 放置 `price_data.json`、`market_value_data.json`（格式见下）。
- 输出图：**`code/Plot/*.png`**（运行前会自动创建 `Plot`）。

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
| `code/data_akshare.py` | AkShare 拉数 |
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

- 环境变量示例：根目录 **`.env.example`**。
- **`.gitignore`**：忽略 `.venv`、`code/Plot/*.png`、`.env` 等。
