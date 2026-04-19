from __future__ import annotations

BOT_NAME = "news_crawler"
SPIDER_MODULES = ["news_crawler.spiders"]
NEWSPIDER_MODULE = "news_crawler.spiders"

ROBOTSTXT_OBEY = True
CONCURRENT_REQUESTS = 8
DOWNLOAD_DELAY = 0.5
AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_START_DELAY = 0.5
AUTOTHROTTLE_MAX_DELAY = 10

DEFAULT_REQUEST_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
}

ITEM_PIPELINES = {
    "news_crawler.pipelines.JsonLinesExportPipeline": 300,
}

# 输出路径（可被环境变量覆盖，见 emotion_bl.config）
NEWS_JSONL_PATH = "data/news_items.jsonl"
RSS_FEED_URLS: list[str] = []
