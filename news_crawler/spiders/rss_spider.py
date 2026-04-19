"""
时效性内容：RSS 源（可配置）。可扩展为搜索引擎 Spider 或 API 回调。
"""
from datetime import datetime, timezone

import feedparser
import scrapy

from news_crawler.items import NewsItem


class RssSpider(scrapy.Spider):
    name = "rss"
    custom_settings = {
        "ROBOTSTXT_OBEY": False,
    }

    def __init__(self, feeds=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # feeds: 逗号分隔 URL，或通过 settings RSS_FEED_URLS 列表注入
        if feeds:
            self.start_urls = [u.strip() for u in feeds.split(",") if u.strip()]
        else:
            self.start_urls = []

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super().from_crawler(crawler, *args, **kwargs)
        urls = crawler.settings.getlist("RSS_FEED_URLS")
        if urls and not spider.start_urls:
            spider.start_urls = list(urls)
        if not spider.start_urls:
            spider.start_urls = [
                "https://feeds.bbci.co.uk/news/business/rss.xml",
            ]
        return spider

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse_feed, meta={"feed_url": url})

    def parse_feed(self, response):
        feed_url = response.meta["feed_url"]
        parsed = feedparser.parse(response.body)
        for entry in parsed.entries[:50]:
            title = getattr(entry, "title", "") or ""
            link = getattr(entry, "link", "") or ""
            summary = ""
            if hasattr(entry, "summary"):
                summary = entry.summary
            elif hasattr(entry, "description"):
                summary = entry.description
            published = ""
            if hasattr(entry, "published_parsed") and entry.published_parsed:
                try:
                    published = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc).isoformat()
                except (TypeError, ValueError):
                    published = getattr(entry, "published", "") or ""
            else:
                published = getattr(entry, "published", "") or ""

            item = NewsItem()
            item["title"] = title.strip()
            item["link"] = link.strip()
            item["summary"] = self._strip_html(summary)[:2000]
            item["published"] = published
            item["source"] = feed_url
            item["fetched_at"] = datetime.now(timezone.utc).isoformat()
            yield item

    @staticmethod
    def _strip_html(raw: str) -> str:
        if not raw:
            return ""
        import re

        return re.sub(r"<[^>]+>", " ", raw).strip()
