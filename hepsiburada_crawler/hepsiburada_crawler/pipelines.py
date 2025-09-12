# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import psycopg2
from psycopg2 import sql
import os, json, re
import hashlib
from dotenv import load_dotenv
from urllib.parse import urljoin, urldefrag, urlparse, parse_qsl, urlencode, urlunparse
from psycopg2.extras import execute_values
from datetime import datetime, timezone 
load_dotenv()
# Hepsiburada   
TRACKING_PARAM_PREFIXES = ("utm_", "gclid", "fbclid", "gclsrc", "mc_", "sc_")
ROOT_DOMAIN = "hepsiburada.com"
# URL Funcs
# URL normlize
def normalize_url(base_url: str, href: str) -> str | None:
    if not href:
        return None
    href = href.strip()
    if href.startswith(("#", "mailto:", "tel:", "javascript:", "data:")):
        return None
    abs_url = urljoin(base_url, href)
    abs_url, _ = urldefrag(abs_url)
    p = urlparse(abs_url)
    if p.scheme not in ("http", "https"):
        return None
    netloc = p.netloc.lower()
    q_pairs = [(k, v) for (k, v) in parse_qsl(p.query, keep_blank_values=True)
               if not k.startswith(TRACKING_PARAM_PREFIXES)]
    query = urlencode(q_pairs, doseq=True)
    path = re.sub(r"/{2,}", "/", p.path) or "/"
    return urlunparse((p.scheme, netloc, path, "", query, ""))
# understand given link is populated from domain url
def is_external(normalized_url: str) -> bool:
    host = urlparse(normalized_url).netloc
    return not host.endswith(ROOT_DOMAIN)



class HepsiburadaCrawlerPipeline:
    def process_item(self, item, spider):
        return item
class PostgresPipeline:
    print(os.getenv("PGHOST")) 
    print("POSTGRES_PIPILINE ACTIVE _______________________________________________________________")
    def open_spider(self, spider):
        self.conn = psycopg2.connect(
            host = os.getenv("PGHOST"),
            port = os.getenv("PGPORT"),
            dbname = os.getenv("PGDATABASE"),
            user = os.getenv("PGUSER"),
            password = os.getenv("PGPASSWORD")
        )
        
        self.conn.autocommit = False
        self.cur = self.conn.cursor()
        #Create Table
        self.cur.execute("""
        CREATE TABLE IF NOT EXISTS pages (
          id SERIAL PRIMARY KEY,
          url TEXT UNIQUE,
          status_code INT,
          title TEXT,
          meta_description TEXT,
          html_content TEXT,
          content_length INT,
          link_depth INT,
          fetched_at TIMESTAMP WITH TIME ZONE,
          content_hash CHAR(64)
        )
        """)
        self.conn.commit()
        
    def close_spider(self, spider):
            try:
                self.conn.commit()
            finally:
                self.cur.close()
                self.conn.close()
                
    def process_item(self, item, spider):
            # hash: dedup and change follow
            
            html = item.get("html_content") or ""
            content_hash = hashlib.sha256(html.encode("utf-8","ignore")).hexdigest()
            
            # fetched_at set
            fetched_at = datetime.now(timezone.utc)
            
                # UPSERT (url unique)
            self.cur.execute("""
                INSERT INTO pages (
                    url, status_code, title, html_content,
                    content_length, link_depth, fetched_at, content_hash
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (url) DO UPDATE SET
                    status_code = EXCLUDED.status_code,
                    title = EXCLUDED.title,
                    html_content = EXCLUDED.html_content,
                    content_length = EXCLUDED.content_length,
                    link_depth = EXCLUDED.link_depth,
                    fetched_at = EXCLUDED.fetched_at,
                    content_hash = EXCLUDED.content_hash
            """, (
                item.get("url"),
                item.get("status_code"),
                item.get("title"),
                html,
                item.get("content_length"),
                item.get("link_depth"),
                fetched_at,
                content_hash
            ))
            self.conn.commit()
            return item

class HepsiPostgresPipeline:
    def open_spider(self, spider):
        # Establish Database connection
        self.conn = psycopg2.connect(  
            host=os.getenv("PGHOST"), # os library to use env
            port=int(os.getenv("PGPORT")),
            dbname=os.getenv("PGDATABASE"),
            user=os.getenv("PGUSER"),
            password=os.getenv("PGPASSWORD"),
        )
        # Cursor object for making changes on database
        self.cur = self.conn.cursor()
        # Create The label with given columns
        self.cur.execute("""
        CREATE TABLE IF NOT EXISTS links (
            id SERIAL PRIMARY KEY,
            url TEXT,
            normalized_url TEXT UNIQUE,
            anchor_text TEXT,
            rel TEXT,
            is_external BOOLEAN NOT NULL DEFAULT FALSE,
            discovered_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """)
        self.conn.commit()
        self.buffer = []
        self.seen = set()  # normalized_url  (spider-run general)

    def process_item(self, item, spider):
        norm = item.get("normalized_url")
        # Duplicate Check
        if not norm:
            return item
        if norm in self.seen:
            return item
        # Add to seen set
        self.seen.add(norm)
        # Create row from spider -> Item
        row = (
            item.get("url"),
            norm,
            item.get("anchor_text"),
            item.get("rel"),
            item.get("is_external", False),
            datetime.now(timezone.utc), # Take current time
        )
        self.buffer.append(row) # Later insert this buffer rows to database
        if len(self.buffer) >= 200: # when buffer reach 200 
            self.flush(spider) # commit rows and flush
        return item

    def flush(self, spider=None):
        if not self.buffer:
            return
        # When buffer has data -> insert to DB
        try:
            execute_values(
                self.cur,
                """
                INSERT INTO links (url, normalized_url, anchor_text, rel, is_external, discovered_at)
                VALUES %s
                ON CONFLICT (normalized_url) DO NOTHING;
                """,
                self.buffer,
                page_size=200,
            )
            self.conn.commit() # Save changes on database
        except Exception as e: # incase of error go back to latest version od DB
            self.conn.rollback()
            # Logged error:
            if spider:
                spider.logger.error(f"DB flush failed; rolled back. {e}", exc_info=True)
            else:
                print("DB flush failed; rolled back:", e)
        finally:
            self.buffer.clear() # Free memmory

#   After Termination
    def close_spider(self, spider):
        self.flush(spider) # Insert Buffer rows
        self.cur.close()   # Close the cursor
        self.conn.close()  # Close  the DB connection
        
# Load Config
def _load_pipeline_cfg():
    path = os.getenv("PIPELINE_CONFIG", "hepsi_config.json")
    # In case no config default
    if not os.path.exists(path):
        # Minimal sensible defaults if no config is present
        return {
            "db": {
                "table": "links",
                "unique_key": "normalized_url",
                "buffer_size": 200,
                "create_table": True
            },
            "columns": {
                "url": "url",
                "normalized_url": "normalized_url",
                "anchor_text": "anchor_text",
                "rel": "rel",
                "is_external": "is_external",
                "discovered_at": "discovered_at"
            }
        }
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)
class GenericPipeline:
    #   Generic, config-driven pipeline for saving discovered links to PostgreSQL.
    # - Reads table/columns/buffer settings from JSON config (PIPELINE_CONFIG).
    # - Uses ON CONFLICT on the configured unique key to dedupe at DB level.
    # - Also keeps an in-process 'seen' set on normalized_url to drop duplicates early.
    

    def __init__(self):
        self.cfg = _load_pipeline_cfg()
        self.table = self.cfg["db"]["table"]
        self.unique_key = self.cfg["db"]["unique_key"]
        self.buffer_size = int(self.cfg["db"].get("buffer_size", 200))
        self.create_table_flag = bool(self.cfg["db"].get("create_table", True))

        # Map of DB column -> item key (what we read off the Item)
        self.colmap = self.cfg["columns"]
        # Build a stable list of DB columns/order for INSERT
        self.db_columns = list(self.colmap.keys())

        self.conn = None
        self.cur = None
        self.buffer = []
        self.seen = set()  # normalized_url-based dedupe within one spider run

    @classmethod
    def from_crawler(cls, crawler):
        # allows Scrapy to instantiate with settings if needed later
        return cls()

    def open_spider(self, spider):
        # Connect using env vars (or you can add optional DSN in config if you want)
        self.conn = psycopg2.connect(
            host=os.getenv("PGHOST"),
            port=int(os.getenv("PGPORT", "5432")),
            dbname=os.getenv("PGDATABASE"),
            user=os.getenv("PGUSER"),
            password=os.getenv("PGPASSWORD"),
        )
        self.cur = self.conn.cursor()

        if self.create_table_flag:
            self._ensure_table(spider)

    def _ensure_table(self, spider):    
        # Create table if missing, with a reasonable default schema.
        # If you need a custom schema, you can extend config to provide DDL.
        ddl = sql.SQL("""
            CREATE TABLE IF NOT EXISTS {table} (
                id SERIAL PRIMARY KEY,
                url TEXT,
                normalized_url TEXT UNIQUE,
                anchor_text TEXT,
                rel TEXT,
                is_external BOOLEAN NOT NULL DEFAULT FALSE,
                discovered_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        """).format(table=sql.Identifier(self.table))
        self.cur.execute(ddl)
        self.conn.commit()

        # Ensure a unique index on the configured unique_key (safety)
        idx_sql = sql.SQL("""
            CREATE UNIQUE INDEX IF NOT EXISTS {idx}
            ON {table} ({key});
        """).format(
            idx=sql.Identifier(f"uq_{self.table}_{self.unique_key}"),
            table=sql.Identifier(self.table),
            key=sql.Identifier(self.unique_key),
        )
        self.cur.execute(idx_sql)
        self.conn.commit()

    def process_item(self, item, spider):
        # In-memory dedupe on normalized_url to avoid growing buffer with dups
        norm_key = self.colmap.get("normalized_url", "normalized_url")
        norm_val = item.get(norm_key)
        if not norm_val:
            return item
        if norm_val in self.seen:
            return item
        self.seen.add(norm_val)

        # Build row tuple ordered as self.db_columns
        row = []
        for db_col in self.db_columns:
            item_key = self.colmap[db_col]
            if db_col == "discovered_at":
                # always set server time if not provided by item
                row.append(item.get(item_key) or datetime.now(timezone.utc))
            else:
                row.append(item.get(item_key))
        self.buffer.append(tuple(row))

        if len(self.buffer) >= self.buffer_size:
            self.flush(spider)
        return item

    def flush(self, spider=None):
        if not self.buffer:
            return
        try:
            # Compose INSERT with protected identifiers
            insert_sql = sql.SQL("""
                INSERT INTO {table} ({cols})
                VALUES %s
                ON CONFLICT ({unique_key}) DO NOTHING;
            """).format(
                table=sql.Identifier(self.table),
                cols=sql.SQL(", ").join(sql.Identifier(c) for c in self.db_columns),
                unique_key=sql.Identifier(self.unique_key),
            )
            execute_values(self.cur, insert_sql.as_string(self.cur), self.buffer, page_size=self.buffer_size)
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            if spider:
                spider.logger.error(f"DB flush failed; rolled back. {e}", exc_info=True)
            else:
                print("DB flush failed; rolled back:", e)
        finally:
            self.buffer.clear()

    def close_spider(self, spider):
        self.flush(spider)
        try:
            if self.cur:
                self.cur.close()
        finally:
            if self.conn:
                self.conn.close()  

            