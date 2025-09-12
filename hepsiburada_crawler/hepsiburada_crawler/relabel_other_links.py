# relabel_other_links.py
import os, json, time, argparse, sys
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

# OpenAI (compatible) client
from openai import OpenAI

load_dotenv()

# ---------- Config helpers ----------
def resolve_config_path() -> str:
    """
    Priority:
      1) PIPELINE_CONFIG env (absolute or relative to CWD)
      2) ../hepsi_config.json relative to THIS file
      3) hepsi_config.json in CWD
    """
    env_path = os.getenv("PIPELINE_CONFIG")
    if env_path:
        return os.path.abspath(env_path) if not os.path.isabs(env_path) else env_path

    this_dir = os.path.dirname(os.path.abspath(__file__))
    candidate = os.path.abspath(os.path.join(this_dir, "..", "hepsi_config.json"))
    if os.path.exists(candidate):
        return candidate

    fallback = os.path.abspath("hepsi_config.json")
    return fallback

def load_cfg() -> dict:
    cfg_path = resolve_config_path()
    if not os.path.exists(cfg_path):
        raise FileNotFoundError(
            f"Config not found. Tried: {cfg_path}\n"
            "Hint: set PIPELINE_CONFIG to an absolute path, or place hepsi_config.json "
            "one level above this script."
        )
    with open(cfg_path, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    print(f"[relabler] Using config: {cfg_path}")
    return cfg

# ---------- LLM ----------
def build_system_prompt(taxonomy: list[str]) -> str:
    return (
        "You are a strict classifier for web URLs/pages (often e-commerce).\n"
        f"Pick exactly ONE label from: {taxonomy}.\n"
        "Return JSON only:\n"
        '{"label":"<one_of_taxonomy>"}\n'
        'If unsure, use "other".\n'
    )

USER_TEMPLATE = (
    "URL: {url}\n"
    "Anchor: {anchor}\n"
    "Rel: {rel}\n"
    "External: {is_external}\n"
)

def make_client() -> OpenAI:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is not set.")
    return OpenAI(api_key=api_key)

def extract_json_label(raw: str, taxonomy: list[str]) -> str:
    """
    Tries to parse model output as JSON and returns a valid taxonomy label, else 'other'.
    Handles accidental code fences or extra text.
    """
    raw = (raw or "").strip()
    # strip code fences if the model returned ```json ... ```
    if raw.startswith("```"):
        # remove first fence
        first = raw.find("\n")
        if first != -1:
            raw = raw[first+1:]
        # remove trailing ```
        if raw.endswith("```"):
            raw = raw[:-3].strip()

    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        s, e = raw.find("{"), raw.rfind("}")
        if s != -1 and e != -1 and e > s:
            try:
                data = json.loads(raw[s:e+1])
            except json.JSONDecodeError:
                return "other"
        else:
            return "other"

    label = data.get("label", "other")
    return label if label in taxonomy else "other"

def call_llm(client: OpenAI, model: str, system_prompt: str, user_prompt: str) -> str:
    resp = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        temperature=0,
    )
    content = resp.choices[0].message.content
    return content

# ---------- DB ----------
def connect_db():
    missing = [k for k in ["PGHOST","PGPORT","PGDATABASE","PGUSER","PGPASSWORD"] if not os.getenv(k)]
    if missing:
        raise RuntimeError("Missing DB env vars: " + ", ".join(missing))
    return psycopg2.connect(
        host=os.getenv("PGHOST"),
        port=int(os.getenv("PGPORT", "5432")),
        dbname=os.getenv("PGDATABASE"),
        user=os.getenv("PGUSER"),
        password=os.getenv("PGPASSWORD"),
    )

# ---------- Main ----------
def relabel_loop(table: str,
                 where_other_sql: str,
                 batch_size: int,
                 max_batches: int,
                 taxonomy: list[str],
                 model: str):
    client = make_client()
    system_prompt = build_system_prompt(taxonomy)

    conn = connect_db()
    conn.autocommit = False
    cur = conn.cursor(cursor_factory=RealDictCursor)

    batches = 0
    total_updated = 0

    print(f"[relabler] table={table} model={model} batch_size={batch_size} max_batches={max_batches or '∞'}")
    print(f"[relabler] taxonomy={taxonomy}")

    try:
        while True:
            cur.execute(
                f"""
                SELECT id, url, anchor_text, rel, is_external
                FROM {table}
                WHERE {where_other_sql}
                ORDER BY id
                LIMIT %s
                """,
                (batch_size,),
            )
            rows = cur.fetchall()
            if not rows:
                print("[relabler] No more 'other' rows. Done.")
                break

            updated_this_batch = 0
            for r in rows:
                user_prompt = USER_TEMPLATE.format(
                    url=r["url"],
                    anchor=r.get("anchor_text") or "",
                    rel=r.get("rel") or "",
                    is_external=bool(r.get("is_external")),
                )
                label = "other"

                # Robust retry/backoff
                backoff = 1.0
                for attempt in range(6):
                    try:
                        raw = call_llm(client, model, system_prompt, user_prompt)
                        label = extract_json_label(raw, taxonomy)
                        break
                    except Exception as e:
                        # network/transient error
                        sleep_dur = min(backoff, 30.0)
                        print(f"[relabler] LLM call failed (attempt {attempt+1}): {e} -> retry in {sleep_dur:.1f}s")
                        time.sleep(sleep_dur)
                        backoff *= 2.0

                cur.execute(
                    f"UPDATE {table} SET pred_category = %s WHERE id = %s",
                    (label, r["id"])
                )
                updated_this_batch += 1

            conn.commit()
            total_updated += updated_this_batch
            batches += 1
            print(f"[relabler] Batch {batches}: updated {updated_this_batch} rows (total {total_updated}).")

            if max_batches and batches >= max_batches:
                print("[relabler] Reached max_batches limit. Stopping.")
                break

    except KeyboardInterrupt:
        print("\n[relabler] Interrupted. Rolling back current batch...")
        conn.rollback()
        raise
    finally:
        try:
            cur.close()
        finally:
            conn.close()

    print(f"[relabler] Done. Total updated: {total_updated}")

def main():
    cfg = load_cfg()
    rel = cfg.get("relabel", {})

    # Defaults if not provided in config
    table = rel.get("table", "linkstesttest")
    where_other = rel.get("select_where_other_sql", "COALESCE(pred_category, 'other') = 'other'")
    batch_size = int(rel.get("batch_size", 100))
    max_batches = int(rel.get("max_batches", 0))   # 0 => unlimited
    taxonomy = rel.get("taxonomy") or [
        "product","campaign","brand","seller_store","policy","search_results",
        "blog_article","help_center","login","cart_or_checkout","homepage",
        "about_or_contact","other"
    ]
    model = rel.get("openai_model", os.getenv("LLM_MODEL", "gpt-4o-mini"))

    # CLI overrides
    parser = argparse.ArgumentParser(description="Relabel 'other' links via LLM (config-driven).")
    parser.add_argument("--table", default=table, help="DB table (default from config)")
    parser.add_argument("--where", default=where_other, help="SQL WHERE for selecting 'other'")
    parser.add_argument("--batch-size", type=int, default=batch_size, help="Batch size per SELECT")
    parser.add_argument("--max-batches", type=int, default=max_batches, help="Stop after N batches (0 = unlimited)")
    parser.add_argument("--model", default=model, help="LLM model name (overrides config)")
    args = parser.parse_args()

    relabel_loop(
        table=args.table,
        where_other_sql=args.where,
        batch_size=args.batch_size,
        max_batches=args.max_batches,
        taxonomy=taxonomy,
        model=args.model,
    )

if __name__ == "__main__":
    main()
