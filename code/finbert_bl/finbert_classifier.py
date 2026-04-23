"""Hugging Face ``yiyanghkust/finbert-tone-chinese``：三分类 0 中 / 1 正 / 2 负。"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, List, Sequence

# 与模型卡一致: https://huggingface.co/yiyanghkust/finbert-tone-chinese
DEFAULT_MODEL_ID = "yiyanghkust/finbert-tone-chinese"


@dataclass
class ScoredText:
    """单条文本的情绪分解。"""

    e: float
    """离散文方向：-1 / 0 / +1（由 argmax 标签映射）。"""

    w: float
    """与 ``e`` 同向时的 softmax 分数（作置信度）。"""

    label_id: int
    """0=Neutral,1=Positive,2=Negative。"""

    scores: list[dict[str, Any]]
    """``return_all_scores`` 的原始项（LABEL_0/1/2）。"""


def label_to_sentiment(label_id: int) -> float:
    if label_id == 1:
        return 1.0
    if label_id == 2:
        return -1.0
    return 0.0


def _parse_pipeline_result(
    one: list[dict[str, Any]] | dict[str, Any],
) -> ScoredText:
    if isinstance(one, dict):
        one = [one]  # 少数版本可能返回单 dict
    best = max(
        [x for x in one if "score" in x and "label" in x],
        key=lambda d: d["score"],
    )
    lab = str(best["label"])
    # LABEL_0 / LABEL_1 / LABEL_2
    if "LABEL_0" in lab:
        i = 0
    elif "LABEL_1" in lab:
        i = 1
    elif "LABEL_2" in lab:
        i = 2
    else:
        # 兜底：取最后一个数字
        for j in (2, 1, 0):
            if str(j) in lab:
                i = j
                break
        else:
            i = 0
    w = float(best["score"])
    e = label_to_sentiment(i)
    return ScoredText(e=e, w=w, label_id=i, scores=[dict(x) for x in one])


class FinBertSentiment:
    """薄封装，支持批量推理与 CPU/CUDA。"""

    def __init__(
        self,
        model_id: str = DEFAULT_MODEL_ID,
        *,
        device: int = -1,
        batch_size: int = 8,
    ) -> None:
        self._model_id = model_id
        self._device = device
        self._batch_size = max(1, int(batch_size))
        self._pipe: Any = None

    def _ensure_pipeline(self) -> Any:
        if self._pipe is not None:
            return self._pipe
        from transformers import pipeline

        self._pipe = pipeline(
            "text-classification",
            model=self._model_id,
            tokenizer=self._model_id,
            device=self._device,
        )
        return self._pipe

    def score_texts(
        self,
        texts: Sequence[str],
        *,
        max_length: int = 512,
    ) -> list[ScoredText]:
        """对非空文本列表批处理；长文本在 tokenizer 中截断。"""
        pipe = self._ensure_pipeline()
        out: list[ScoredText] = []
        # transformers pipeline 的 truncation 在 tokenizer_kwargs 中传递
        batch_size = self._batch_size
        tlist = [str(t) for t in texts]
        for i in range(0, len(tlist), batch_size):
            chunk = tlist[i : i + batch_size]
            raw: Any = pipe(
                chunk,
                return_all_scores=True,
                batch_size=len(chunk),
                truncation=True,
                max_length=max_length,
            )
            if not isinstance(raw, list):
                raw = [raw]
            if raw and isinstance(raw[0], dict):
                raws: list = [raw]  # 单条：3 个 label 的 list[dict]
            else:
                raws = raw  # 多条：list[ list[dict] ]
            for one in raws:
                if one is None:
                    out.append(ScoredText(e=0.0, w=0.0, label_id=0, scores=[]))  # pragma: no cover
                else:
                    out.append(_parse_pipeline_result(one))
        return out
