import os, json, re, psycopg2
from dotenv import load_dotenv
load_dotenv()

def _load_cfg(path: str):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def _compile_flags(flag_list):
    f = 0
    for name in flag_list or []:
        if name.upper() == "IGNORECASE":
            f |= re.IGNORECASE
        elif name.upper() == "MULTILINE":
            f |= re.MULTILINE
        elif name.upper() == "DOTALL":
            f |= re.DOTALL
    return f

def pg_case_from_patterns(patterns, url_col: str) -> str:
    parts = []
    for p in patterns:
        label = p["label"]
        pat = p["pattern"].replace("'", "''")
        # We’ll always use case-insensitive operator if IGNORECASE in flags, else case-sensitive
        flags = _compile_flags(p.get("flags", []))
        op = "~*" if (flags & re.IGNORECASE) else "~"
        label_sql = label.replace("'", "''")
        parts.append(f"WHEN {url_col} {op} '{pat}' THEN '{label_sql}'")
    return "CASE " + " ".join(parts) + " ELSE 'other' END"

def main():
    cfg_path = os.getenv("PIPELINE_CONFIG", "hepsi_config.json")
    cfg = _load_cfg(cfg_path)

    table = cfg["classification"]["table"]
    url_col = cfg["classification"]["url_col"]
    only_nulls = bool(cfg["classification"].get("only_update_nulls", True))
    csv_out = cfg["classification"].get("csv_out", "urls_with_categories_from_db.csv")
    patterns = cfg["classification"]["patterns"]

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
                cur.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS pred_category TEXT;")
                case_sql = pg_case_from_patterns(patterns, url_col)
                where_clause = f"WHERE {url_col} IS NOT NULL"
                if only_nulls:
                    where_clause += " AND pred_category IS NULL"

                cur.execute(f"UPDATE {table} SET pred_category = {case_sql} {where_clause};")
                cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_pred_category ON {table}(pred_category);")

        with conn.cursor() as cur, open(csv_out, "w", encoding="utf-8", newline="") as f:
            copy_sql = f"""
                COPY (
                    SELECT {url_col} AS url, pred_category
                    FROM {table}
                    ORDER BY 1
                ) TO STDOUT WITH CSV HEADER
            """
            cur.copy_expert(copy_sql, f)

        print(f"Updated table '{table}'. Exported CSV: {csv_out}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
