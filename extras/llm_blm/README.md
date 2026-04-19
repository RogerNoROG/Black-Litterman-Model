# LLM-BLM 与本仓库对接

上游仓库：[youngandbin/LLM-BLM](https://github.com/youngandbin/LLM-BLM)（论文：[ICLR 2025 Workshop](https://arxiv.org/abs/2504.14345)）。

本仓库通过 **git submodule** 将其置于 `third_party/LLM-BLM`，并在 `bridge.py` 中复现其 `evaluate_multiple.py` 里的观点定义（`P=I`、`Q`/`Ω` 由 LLM 多次采样统计）与后验矩阵公式，**后验计算统一走** `emotion_bl.bl.black_litterman.BlackLittermanEngine`，便于与情绪管线数值对照。

首次克隆本仓库后请执行：

```bash
git submodule update --init --recursive
```

使用已附带的 `yfinance/`、`responses/`、`market_caps.json` 快速验证：

```bash
.venv/bin/python scripts/demo_llm_blm_bridge.py
```

自行跑上游 `run.py` 生成新观点时，需配置其 OpenAI 兼容端点与 API Key（上游 `run.py` 内为占位符）；生成的新 JSON 仍可通过 `load_llm_blm_response_json` + `run_llm_blm_period` 接入。
