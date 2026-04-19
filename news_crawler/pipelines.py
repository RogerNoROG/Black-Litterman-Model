import json
from datetime import datetime, timezone
from pathlib import Path

from scrapy import Item


class JsonLinesExportPipeline:
    """将条目追加写入 JSON Lines，供 FastAPI / 情感模块读取。"""

    def __init__(self, output_path: str):
        self.output_path = Path(output_path)
        self._fh = None

    @classmethod
    def from_crawler(cls, crawler):
        path = crawler.settings.get(
            "NEWS_JSONL_PATH",
            "data/news_items.jsonl",
        )
        return cls(output_path=path)

    def open_spider(self, spider):
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = self.output_path.open("a", encoding="utf-8")

    def close_spider(self, spider):
        if self._fh:
            self._fh.close()
            self._fh = None

    def process_item(self, item: Item, spider):
        row = dict(item)
        if "fetched_at" not in row or not row["fetched_at"]:
            row["fetched_at"] = datetime.now(timezone.utc).isoformat()
        self._fh.write(json.dumps(row, ensure_ascii=False) + "\n")
        self._fh.flush()
        return item
