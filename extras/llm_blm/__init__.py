"""与 GitHub youngandbin/LLM-BLM 工作流对齐的桥接（``bridge`` 模块）。"""

from .bridge import (
    capm_equilibrium_returns,
    llm_blm_absolute_views,
    load_llm_blm_response_json,
    long_only_weights_min_variance,
    run_llm_blm_period,
    solve_posterior_mu_with_engine,
)

__all__ = [
    "capm_equilibrium_returns",
    "llm_blm_absolute_views",
    "load_llm_blm_response_json",
    "long_only_weights_min_variance",
    "run_llm_blm_period",
    "solve_posterior_mu_with_engine",
]
