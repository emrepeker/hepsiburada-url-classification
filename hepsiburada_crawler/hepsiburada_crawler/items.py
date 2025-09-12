# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class HepsiburadaCrawlerItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass
#acts the crawler schema
class PageItem(scrapy.Item):
    url = scrapy.Field()
    status_code = scrapy.Field()
    title = scrapy.Field()
    meta_description = scrapy.Field()
    html_content = scrapy.Field()
    content_length = scrapy.Field()
    link_depth = scrapy.Field()
    fetched_at = scrapy.Field() # pipeline now()
    content_hash = scrapy.Field() # pipeline SHA-256 Dedup
    
class LinkItem(scrapy.Item):
    url = scrapy.Field()
    normalized_url = scrapy.Field()
    anchor_text = scrapy.Field()
    rel = scrapy.Field()
    is_external = scrapy.Field()    
    
    
