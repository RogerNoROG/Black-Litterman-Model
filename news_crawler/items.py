import scrapy


class NewsItem(scrapy.Item):
    title = scrapy.Field()
    link = scrapy.Field()
    summary = scrapy.Field()
    published = scrapy.Field()
    source = scrapy.Field()
    fetched_at = scrapy.Field()
