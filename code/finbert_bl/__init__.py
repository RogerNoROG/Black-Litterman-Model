"""FinBERT 中文情绪 + 行业相对观点、He–Litterman 型 Omega、BL 后验与周频滞后可对接 ``rolling_bl_expost_loop``。"""

from .finbert_classifier import FinBertSentiment, label_to_sentiment
from .name_matcher import load_csi300_code_names, match_codes_in_text
from .aggregate import aggregate_by_stock, industry_bar
from .pq_omega import build_top_bottom_pair_PQ, he_litterman_omega
from .bl_posterior import (
    implied_excess_equilibrium_return,
    bl_posterior_combined_return,
)
from .lagged_align import previous_iso_week_file_key, find_news_jsonl_path
from .pipeline import build_mu_post_dataframe, posterior_from_week_jsonl

__all__ = [
    "FinBertSentiment",
    "label_to_sentiment",
    "load_csi300_code_names",
    "match_codes_in_text",
    "aggregate_by_stock",
    "industry_bar",
    "build_top_bottom_pair_PQ",
    "he_litterman_omega",
    "implied_excess_equilibrium_return",
    "bl_posterior_combined_return",
    "previous_iso_week_file_key",
    "find_news_jsonl_path",
    "build_mu_post_dataframe",
    "posterior_from_week_jsonl",
]
