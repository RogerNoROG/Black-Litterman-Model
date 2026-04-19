"""
可扩展：基于 httpx 的搜索占位 Spider。
配置 SEARCH_QUERY、SEARCH_PROVIDER_URL 后实现具体解析逻辑。
"""
from datetime import datetime, timezone

import scrapy

from news_crawler.items import NewsItem


class SearchSpider(scrapy.Spider):
    name = "search"
    custom_settings = {"ROBOTSTXT_OBEY": False}

    def start_requests(self):
        # 子类或 settings 注入 query；默认生成占位说明项
        query = getattr(self, "query", None) or self.settings.get("SEARCH_QUERY", "")
        if not query:
            item = NewsItem()
            item["title"] = "configure SEARCH_QUERY or pass -a query=..."
            item["link"] = ""
            item["summary"] = ""
            item["published"] = ""
            item["source"] = "search_spider"
            item["fetched_at"] = datetime.now(timezone.utc).isoformat()
            yield item
            return
        url = self.settings.get("SEARCH_START_URL", "")
        if url:
            yield scrapy.Request(url, callback=self.parse_results, meta={"query": query})

    def parse_results(self, response):
        """覆盖此方法以解析具体搜索 HTML/JSON。"""
        pass
