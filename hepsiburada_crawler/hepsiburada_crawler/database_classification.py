# run_pg_classification_from_patterns.py
import os
import psycopg2
import pandas as pd
import re
from dotenv import load_dotenv

# ---- CONFIG ----
# If your file is named regexes.py and contains PATTERNS = [(label, re.compile(...)), ...]
FROM_MODULE = "regexes"          # e.g., "regexes" or "url_classifier"
TABLE = "links"                  # your table name
URL_COL = "url"                  # url column name
ONLY_UPDATE_NULLS = True         # set False to overwrite all rows
CSV_OUT = "urls_with_categories_from_db.csv"  # optional sample export
# .env
load_dotenv()
# ---- IMPORT REGEX PATTERNS ----
PATTERNS = __import__(FROM_MODULE).PATTERNS

# ---- HELPERS ----
def pg_case_from_patterns(patterns, url_col: str) -> str:
    """
    Build a Postgres CASE WHEN string from compiled Python regex patterns.
    Respects re.IGNORECASE:
      - uses ~* (case-insensitive) if IGNORECASE set
      - uses ~  (case-sensitive)   otherwise
    Escapes single quotes for SQL literals.
    """
    parts = []
    for label, regex_obj in patterns:
        pat = regex_obj.pattern
        flags = regex_obj.flags

        # Choose ~* for IGNORECASE; otherwise ~
        op = "~*" if (flags & re.IGNORECASE) else "~"

        # Escape single quotes in pattern
        pat_sql = pat.replace("'", "''")
        label_sql = label.replace("'", "''")

        parts.append(f"WHEN {url_col} ~* '{pat_sql}' THEN '{label_sql}'")

    return "CASE " + " ".join(parts) + " ELSE 'other' END"

def main():
    # db connection same as pipeline 
    conn = psycopg2.connect( 
        host=os.getenv("PGHOST"),
        port=os.getenv("PGPORT"),
        dbname=os.getenv("PGDATABASE"),
        user=os.getenv("PGUSER"),
        password=os.getenv("PGPASSWORD"),
    )
    try:
        with conn:
            with conn.cursor() as cur:
                # 1) Ensure output column exists
                cur.execute(f"ALTER TABLE {TABLE} ADD COLUMN IF NOT EXISTS pred_category TEXT;")

                # 2) Build CASE from your Python patterns
                case_sql = pg_case_from_patterns(PATTERNS, URL_COL)

                # 3) UPDATE
                where_clause = f"WHERE {URL_COL} IS NOT NULL"
                if ONLY_UPDATE_NULLS:
                    where_clause += " AND pred_category IS NULL"

                sql_update = f"""
                    UPDATE {TABLE}
                    SET pred_category = {case_sql}
                    {where_clause};
                """
                cur.execute(sql_update)

                # (Optional) Helpful index to filter or group by category later
                cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{TABLE}_pred_category ON {TABLE}(pred_category);")

         # 4) Export with COPY (no pandas)
        with conn.cursor() as cur, open(CSV_OUT, "w", encoding="utf-8", newline="") as f:
            copy_sql = f"""
                COPY (
                    SELECT {URL_COL} AS url, pred_category
                    FROM {TABLE}
                    ORDER BY 1
                ) TO STDOUT WITH CSV HEADER
            """
            cur.copy_expert(copy_sql, f)

        print(f"Updated table '{TABLE}'. Exported CSV: {CSV_OUT}")

    finally:
        conn.close()

if __name__ == "__main__":
    main()
