from pathlib import Path

from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# 通义千问（阿里云百炼）OpenAI 兼容模式 BASE_URL，按地域选用其一
DASHSCOPE_COMPAT_BASE_URLS = {
    "cn": "https://dashscope.aliyuncs.com/compatible-mode/v1",
    "intl": "https://dashscope-intl.aliyuncs.com/compatible-mode/v1",
    "us": "https://dashscope-us.aliyuncs.com/compatible-mode/v1",
    "hk": "https://cn-hongkong.dashscope.aliyuncs.com/compatible-mode/v1",
}


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    news_jsonl_path: Path = Path("data/news_items.jsonl")
    # 情感后端：snownlp | bert | llm（大模型 API，OpenAI 兼容）
    sentiment_backend: str = "snownlp"
    # 新闻 → ticker：keyword_map=字符串关键词表（默认）；llm_json=由 LLM 在每条响应里输出 tickers（需 SENTIMENT_BACKEND=llm）
    ticker_mapping_mode: str = "keyword_map"
    # llm_json 下若某条 tickers 为空，是否对该条回退到关键词表（避免全空）
    llm_ticker_json_fallback_keyword: bool = True
    use_bert: bool = False  # 兼容旧配置：为 True 且 backend 未指定 llm 时等同 bert
    bert_model_id: str = "ProsusAI/finbert"  # 英文金融情绪；中文可换 bert-base-chinese + 自训头
    # 大模型：默认按「通义千问 · 百炼 OpenAI 兼容」配置；OpenAI/DeepSeek 等可改 LLM_BASE_URL + LLM_MODEL
    llm_api_key: str = ""  # 百炼控制台 API-Key，请求头 Authorization: Bearer <key>
    llm_base_url: str = DASHSCOPE_COMPAT_BASE_URLS["cn"]
    # 中国内地常用：qwen-turbo-latest、qwen-plus、qwen-flash 等（以控制台文档为准）
    llm_model: str = "qwen-turbo-latest"
    # 单条新闻单次 HTTP 的读超时（秒）；超时则终止该次请求并换下一次重试
    llm_per_article_read_sec: float = 60.0
    # 单条新闻读超时后的额外重试次数（不含首次），默认 3 即最多共 4 次请求
    llm_per_article_max_retries: int = 3
    # LLM 情感分析并发数（仅 sentiment_backend=llm）；过大易排队/限流，过小则墙钟时间长
    llm_max_concurrent: int = 8
    # 设为 cn/intl/us/hk 时覆盖 llm_base_url 为对应 DashScope 兼容端点（便于切换地域）
    llm_dashscope_region: str | None = None

    @model_validator(mode="after")
    def _apply_dashscope_region(self):
        reg = (self.llm_dashscope_region or "").strip().lower()
        if reg in DASHSCOPE_COMPAT_BASE_URLS:
            self.llm_base_url = DASHSCOPE_COMPAT_BASE_URLS[reg]
        return self
    default_tau: float = 0.05
    default_view_scale: float = 0.02
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    # 流式「整次分析」总时限（秒）。0 = 不限制（LLM 逐条调用时可能需数分钟）。
    analysis_deadline_sec: float = 0

    # Istero 开放接口：央视国内要闻（Bearer Token，见开发者中心《开发文档》）
    istero_api_token: str = ""
    istero_api_url: str = (
        "https://api.istero.com/resource/v1/cctv/china/latest/news"
    )
    # 若服务商要求 X-Signature，请按其《开发文档》在 emotion_bl/istero_news.py 中实现后再启用
    istero_sign_secret: str = ""
    istero_use_signature_headers: bool = False


settings = Settings()
