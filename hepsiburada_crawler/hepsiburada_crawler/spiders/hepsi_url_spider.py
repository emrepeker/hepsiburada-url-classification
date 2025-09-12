import os,json
import scrapy
from ..items import LinkItem
from ..pipelines import normalize_url, is_external

def _load_cfg(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

class HepsiSpider(scrapy.Spider):
    cfg = _load_cfg(os.getenv("SPIDER_CONFIG", "hepsi_config.json"))["spider"]
    name = cfg["name"]
    allowed_domains = cfg["domain"]
    start_urls = cfg["start_urls"]
    handle_httpstatus_list = [403] 
   
   
    seen_urls = set() # Unique URL's
    
    def start_requests(self):
        yield scrapy.Request(
            "https://www.hepsiburada.com/",
            callback=self.parse,
            dont_filter=True,
            meta={
                "playwright": True,
                "playwright_page_methods": [
                    # Cookie consent / waiting for popout
                    ("wait_for_selector", {"selector": "a", "timeout": 15000}),
                ],
                "playwright_context_kwargs": {
                    "locale": "tr-TR",
                    "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                                  "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
                },
            },
        )


   
    def parse(self, response):
        base_url = response.url
        seen_local = set()  # Unique URL's

        for a in response.xpath("//a"):
            href = a.xpath(".//@href").get()
            norm = normalize_url(base_url, href)
            if not norm:
                continue
            if norm in seen_local:
                continue
            seen_local.add(norm)

            item = LinkItem()
            item["url"] = href
            item["normalized_url"] = norm
            item["anchor_text"] = a.xpath("normalize-space(string(.))").get() or None
            rel = a.xpath(".//@rel").getall()
            item["rel"] = " ".join(rel) if rel else None
            item["is_external"] = is_external(norm)
            yield item

        # Note : Only HomePage we are not entering inside of the URL
        
    