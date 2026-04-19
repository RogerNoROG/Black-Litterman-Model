from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

from emotion_bl.config import settings


def _resolve_backend(instance_use_bert: bool) -> str:
    b = (settings.sentiment_backend or "snownlp").strip().lower()
    if b == "llm":
        return "llm"
    if b == "bert":
        return "bert"
    if b == "snownlp" and (instance_use_bert or settings.use_bert):
        return "bert"
    return "snownlp"


@dataclass
class SentimentResult:
    """单条文本情绪：score ∈ [-1, 1]，label 为三分类。"""

    score: float
    label: Literal["negative", "neutral", "positive"]
    backend: str
    # LLM 在 TICKER_MAPPING_MODE=llm_json 时填充；其它后端为空列表
    mentioned_tickers: list[str] = field(default_factory=list)


class SentimentAnalyzer:
    """
    SnowNLP：中文轻量默认；可选 Transformers BERT（USE_BERT=true）。
    score 统一映射到 [-1, 1] 作为「情绪价值」强度。
    """

    def __init__(self, use_bert: bool | None = None, bert_model_id: str | None = None):
        self.use_bert = use_bert if use_bert is not None else settings.use_bert
        self.bert_model_id = bert_model_id or settings.bert_model_id
        self._bert_pipe = None

    def _ensure_bert(self):
        if self._bert_pipe is not None:
            return
        try:
            from transformers import pipeline
        except ImportError as e:
            raise RuntimeError(
                "USE_BERT=true 需要安装 transformers 与 torch，见 requirements 注释。"
            ) from e
        self._bert_pipe = pipeline(
            "sentiment-analysis",
            model=self.bert_model_id,
            device_map="auto",
        )

    def analyze(self, text: str) -> SentimentResult:
        t = (text or "").strip()
        if not t:
            return SentimentResult(0.0, "neutral", "empty")

        backend = _resolve_backend(self.use_bert)
        if backend == "llm":
            from emotion_bl.sentiment.llm_backend import analyze_text_llm

            return analyze_text_llm(t)
        if backend == "bert":
            return self._analyze_bert(t)
        return self._analyze_snownlp(t)

    def _analyze_snownlp(self, text: str) -> SentimentResult:
        from snownlp import SnowNLP

        s = float(SnowNLP(text).sentiments)
        score = (s - 0.5) * 2.0
        if score > 0.15:
            label: Literal["negative", "neutral", "positive"] = "positive"
        elif score < -0.15:
            label = "negative"
        else:
            label = "neutral"
        return SentimentResult(score=score, label=label, backend="snownlp")

    def _analyze_bert(self, text: str) -> SentimentResult:
        self._ensure_bert()
        assert self._bert_pipe is not None
        out = self._bert_pipe(text[:512])[0]
        lab = str(out["label"]).lower()
        conf = float(out["score"])
        if "pos" in lab or lab == "label_1":
            score = conf
            label: Literal["negative", "neutral", "positive"] = "positive"
        elif "neg" in lab or lab == "label_0":
            score = -conf
            label = "negative"
        else:
            score = 0.0
            label = "neutral"
        return SentimentResult(score=score, label=label, backend=f"bert:{self.bert_model_id}")
