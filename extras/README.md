# 非核心 / 归档内容

与 **Black-Litterman 主流程**（`code/main.py` → `black_litterman` → `back_test`）无直接依赖，便于精简仓库根目录。

| 路径 | 说明 |
|------|------|
| `migrate_xlsx_to_json.py` | 将旧美股 Wind Excel 转为 `code/Data/` 下 JSON（需 `openpyxl`） |
| `legacy_data/wind_us_xlsx/` | 示例：Wind 导出的 `Price_Data.xlsx`、`Market_Value.xlsx` |
| `legacy_data/us_json_samples/` | 历史美股样本 JSON（若曾放入版本库） |
| `report/Black-Litterman-Model.png` | 原项目结构示意图 |
| `plot_samples/` | 历史回测生成的 PNG 示例（运行 `main.py` 会在 `code/Plot/` 重新生成） |
| `llm_blm/` | 与上游 [youngandbin/LLM-BLM](https://github.com/youngandbin/LLM-BLM) 对齐的桥接（见 `bridge.py`）；上游代码以 **git submodule** 置于 `third_party/LLM-BLM` |

迁移脚本用法见 `migrate_xlsx_to_json.py` 文件头注释。拉取子模块：`git submodule update --init --recursive`。
