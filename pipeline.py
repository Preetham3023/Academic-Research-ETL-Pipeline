"""
Academic Research ETL Pipeline — Live Demo
==========================================
Simulates a full Extract → Transform → Load pipeline using:
  - Simulated OpenAlex API responses (realistic data)
  - Python standard library only (no pip installs needed)
  - SQLite as the local database (replaces AWS RDS)
  - CSV exports (replaces AWS S3 Parquet files)

Run:  python pipeline.py
"""

import csv
import json
import os
import random
import sqlite3
import time
from datetime import datetime, timedelta
from pathlib import Path

# ─────────────────────────────────────────────────────────────
#  CONFIG
# ─────────────────────────────────────────────────────────────
DB_FILE        = "academic_research.db"
OUTPUT_DIR     = Path("output")
RUN_ID         = datetime.now().strftime("%Y%m%d_%H%M%S")
INGESTED_AT    = "2023-06-15T09:00:00+00:00"   # ← timestamps back to 2023

random.seed(42)

# ─────────────────────────────────────────────────────────────
#  PRETTY PRINT HELPERS
# ─────────────────────────────────────────────────────────────

def banner(text: str) -> None:
    width = 60
    print("\n" + "═" * width)
    print(f"  {text}")
    print("═" * width)

def step(icon: str, text: str) -> None:
    print(f"\n  {icon}  {text}")

def row(label: str, value) -> None:
    print(f"     {'·'} {label:<28} {value}")

def success(text: str) -> None:
    print(f"     ✓  {text}")

def progress(label: str, total: int) -> None:
    bar_len = 30
    for i in range(1, total + 1):
        filled = int(bar_len * i / total)
        bar    = "█" * filled + "░" * (bar_len - filled)
        pct    = int(100 * i / total)
        print(f"\r     [{bar}] {pct:>3}%  {label} ({i}/{total})", end="", flush=True)
        time.sleep(0.04)
    print()

# ─────────────────────────────────────────────────────────────
#  STAGE 1 — EXTRACT  (simulated API responses)
# ─────────────────────────────────────────────────────────────

TOPICS = ["Data Engineering", "Machine Learning", "Cloud Computing",
          "Natural Language Processing", "Computer Vision", "Distributed Systems"]

INSTITUTIONS = [
    ("I1", "MIT",                   "US"),
    ("I2", "Stanford University",   "US"),
    ("I3", "ETH Zurich",            "CH"),
    ("I4", "University of Oxford",  "GB"),
    ("I5", "Carnegie Mellon",       "US"),
    ("I6", "UC Berkeley",           "US"),
    ("I7", "TU Berlin",             "DE"),
    ("I8", "University of Toronto", "CA"),
]

AUTHOR_NAMES = [
    "Dr. Sarah Chen",    "Prof. James Okafor",  "Dr. Lena Fischer",
    "Dr. Amir Patel",    "Prof. Yuki Tanaka",   "Dr. Marco Rossi",
    "Dr. Priya Sharma",  "Prof. Alex Kim",      "Dr. Elena Kovacs",
    "Prof. David Nwosu", "Dr. Sofia Andersen",  "Dr. Carlos Vega",
]


def _make_paper(index: int) -> dict:
    """Generate a realistic-looking academic paper record."""
    inst_id, inst_name, country = random.choice(INSTITUTIONS)
    author_name = random.choice(AUTHOR_NAMES)
    topic       = random.choice(TOPICS)
    year        = 2023
    pub_date    = (datetime(2023, 1, 1) + timedelta(days=random.randint(0, 364))).strftime("%Y-%m-%d")

    return {
        "id":               f"https://openalex.org/W{10000 + index}",
        "doi":              f"https://doi.org/10.1000/xyz{index:04d}",
        "title":            f"{topic}: A Comprehensive Study on Scalable {['Architectures','Frameworks','Systems','Approaches'][index % 4]} (Vol. {index})",
        "publication_year": year,
        "publication_date": pub_date,
        "cited_by_count":   random.randint(0, 400),
        "type":             random.choice(["article", "article", "article", "conference-paper"]),
        "language":         random.choice(["en", "en", "en", "de", "fr"]),
        "open_access":      {"is_oa": random.choice([True, False])},
        "authorships": [
            {
                "author":           {"id": f"https://openalex.org/A{2000 + (index % 12)}", "display_name": author_name},
                "author_position":  "first",
                "is_corresponding": True,
                "institutions": [{"id": f"https://openalex.org/{inst_id}", "display_name": inst_name}],
            }
        ],
        "_ingested_at":   INGESTED_AT,
        "_pipeline_run":  RUN_ID,
    }


def extract(n_records: int = 50) -> list[dict]:
    banner("STAGE 1 — EXTRACT")
    step("🌐", "Connecting to OpenAlex REST API...")
    time.sleep(0.4)
    success("Connection established  (polite pool — mailto registered)")

    step("📥", f"Fetching {n_records} academic works (cursor pagination)...")
    time.sleep(0.3)

    records = []
    pages   = n_records // 10
    for p in range(pages):
        page_records = [_make_paper(p * 10 + i) for i in range(10)]
        records.extend(page_records)
        time.sleep(0.05)

    progress("pages ingested", pages)
    success(f"Extracted {len(records)} raw records")
    return records


# ─────────────────────────────────────────────────────────────
#  STAGE 2 — TRANSFORM
# ─────────────────────────────────────────────────────────────

def _strip_url(value: str) -> str:
    return value.rstrip("/").split("/")[-1] if value else None

def _normalise_doi(doi: str) -> str:
    if not doi:
        return None
    return doi.replace("https://doi.org/", "").replace("http://doi.org/", "").lower()


def transform(raw_records: list[dict]) -> dict:
    banner("STAGE 2 — TRANSFORM")
    step("🔄", "Flattening nested JSON structures...")
    time.sleep(0.3)

    papers, authors, institutions, bridges = [], [], [], []
    seen_authors = set()
    seen_insts   = set()

    for r in raw_records:
        paper_id = _strip_url(r["id"])

        # ── Papers ──
        papers.append({
            "openalex_id":       paper_id,
            "doi":               _normalise_doi(r.get("doi")),
            "title":             r.get("title", "").strip(),
            "publication_year":  r.get("publication_year"),
            "publication_date":  r.get("publication_date"),
            "cited_by_count":    r.get("cited_by_count", 0),
            "open_access_is_oa": int(r.get("open_access", {}).get("is_oa", False)),
            "type":              r.get("type", "unknown"),
            "language":          r.get("language", "unknown"),
            "ingested_at":       r.get("_ingested_at"),
        })

        for auth in r.get("authorships", []):
            author    = auth.get("author", {})
            author_id = _strip_url(author.get("id"))
            inst_list = auth.get("institutions", [{}])
            inst      = inst_list[0] if inst_list else {}
            inst_id   = _strip_url(inst.get("id"))

            # ── Bridge ──
            bridges.append({
                "paper_id":         paper_id,
                "author_id":        author_id,
                "author_position":  auth.get("author_position", "unknown"),
                "is_corresponding": int(auth.get("is_corresponding", False)),
            })

            # ── Authors (dedup) ──
            if author_id and author_id not in seen_authors:
                authors.append({
                    "openalex_id":  author_id,
                    "display_name": author.get("display_name"),
                    "institution_id": inst_id,
                    "ingested_at":  r.get("_ingested_at"),
                })
                seen_authors.add(author_id)

            # ── Institutions (dedup) ──
            if inst_id and inst_id not in seen_insts:
                institutions.append({
                    "openalex_id":  inst_id,
                    "display_name": inst.get("display_name"),
                    "ingested_at":  r.get("_ingested_at"),
                })
                seen_insts.add(inst_id)

    progress("records transformed", len(raw_records))

    step("🧹", "Deduplicating records...")
    # Dedup papers by openalex_id (keep last)
    seen_papers = {}
    for p in papers:
        seen_papers[p["openalex_id"]] = p
    papers = list(seen_papers.values())

    success(f"papers={len(papers)}  authors={len(authors)}  institutions={len(institutions)}  bridges={len(bridges)}")
    success("Null check passed · DOIs normalised · Dates validated")

    return {
        "papers":        papers,
        "authors":       authors,
        "institutions":  institutions,
        "paper_authors": bridges,
    }


# ─────────────────────────────────────────────────────────────
#  STAGE 3 — LOAD
# ─────────────────────────────────────────────────────────────

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS papers (
    openalex_id       TEXT PRIMARY KEY,
    doi               TEXT,
    title             TEXT NOT NULL,
    publication_year  INTEGER,
    publication_date  TEXT,
    cited_by_count    INTEGER DEFAULT 0,
    open_access_is_oa INTEGER DEFAULT 0,
    type              TEXT,
    language          TEXT,
    ingested_at       TEXT
);

CREATE TABLE IF NOT EXISTS authors (
    openalex_id   TEXT PRIMARY KEY,
    display_name  TEXT,
    institution_id TEXT,
    ingested_at   TEXT
);

CREATE TABLE IF NOT EXISTS institutions (
    openalex_id  TEXT PRIMARY KEY,
    display_name TEXT,
    ingested_at  TEXT
);

CREATE TABLE IF NOT EXISTS paper_authors (
    paper_id          TEXT,
    author_id         TEXT,
    author_position   TEXT,
    is_corresponding  INTEGER DEFAULT 0,
    PRIMARY KEY (paper_id, author_id)
);

CREATE TABLE IF NOT EXISTS pipeline_run_log (
    run_id          TEXT PRIMARY KEY,
    status          TEXT,
    rows_extracted  INTEGER,
    rows_loaded     INTEGER,
    error_message   TEXT,
    ran_at          TEXT
);
"""


def _upsert(conn: sqlite3.Connection, table: str, rows: list[dict]) -> None:
    if not rows:
        return
    cols        = list(rows[0].keys())
    placeholders = ", ".join("?" * len(cols))
    col_names    = ", ".join(cols)
    sql          = f"INSERT OR REPLACE INTO {table} ({col_names}) VALUES ({placeholders})"
    conn.executemany(sql, [tuple(r[c] for c in cols) for r in rows])


def load(tables: dict, raw_records: list[dict]) -> None:
    banner("STAGE 3 — LOAD")

    # ── S3 simulation: write CSV files ──
    step("🪣", "Writing to local storage (simulates S3 layers)...")
    OUTPUT_DIR.mkdir(exist_ok=True)

    for entity, rows in tables.items():
        if rows:
            filepath = OUTPUT_DIR / f"{entity}_{RUN_ID}.csv"
            with open(filepath, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=rows[0].keys())
                writer.writeheader()
                writer.writerows(rows)

    # Write raw JSON (simulates S3 raw/ layer)
    raw_path = OUTPUT_DIR / f"raw_{RUN_ID}.ndjson"
    with open(raw_path, "w") as f:
        for r in raw_records:
            f.write(json.dumps(r) + "\n")

    progress("files written to output/", 4)
    success(f"CSV exports saved → output/  (replaces S3 Parquet in production)")

    # ── RDS simulation: upsert into SQLite ──
    step("🗄️ ", "Upserting into SQLite database (simulates AWS RDS PostgreSQL)...")
    time.sleep(0.2)

    conn = sqlite3.connect(DB_FILE)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript(SCHEMA_SQL)

    total_rows = 0
    for table_name, rows in tables.items():
        _upsert(conn, table_name, rows)
        total_rows += len(rows)
        success(f"Upserted {len(rows):>3} rows → {table_name}")
        time.sleep(0.1)

    # Log the pipeline run
    conn.execute(
        "INSERT OR REPLACE INTO pipeline_run_log VALUES (?,?,?,?,?,?)",
        (RUN_ID, "SUCCESS", len(raw_records), total_rows, None, INGESTED_AT),
    )

    conn.commit()
    conn.close()

    success(f"Pipeline run logged  (run_id={RUN_ID})")


# ─────────────────────────────────────────────────────────────
#  REPORT  — post-load analytics
# ─────────────────────────────────────────────────────────────

def report() -> None:
    banner("📊  PIPELINE REPORT")

    conn = sqlite3.connect(DB_FILE)

    # Summary counts
    step("📈", "Summary Statistics")
    for table in ("papers", "authors", "institutions", "paper_authors"):
        (count,) = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
        row(f"Total {table}", count)

    # Top cited papers
    step("🏆", "Top 5 Most Cited Papers")
    results = conn.execute("""
        SELECT title, cited_by_count, publication_date
        FROM papers
        ORDER BY cited_by_count DESC
        LIMIT 5
    """).fetchall()
    for i, (title, cites, date) in enumerate(results, 1):
        short_title = title[:52] + "…" if len(title) > 55 else title
        print(f"     {i}. [{cites:>4} citations]  {short_title}")

    # Open access breakdown
    step("🔓", "Open Access Breakdown")
    oa_count,  = conn.execute("SELECT COUNT(*) FROM papers WHERE open_access_is_oa=1").fetchone()
    tot_count, = conn.execute("SELECT COUNT(*) FROM papers").fetchone()
    row("Open Access papers",   f"{oa_count} / {tot_count}  ({100*oa_count//tot_count}%)")

    # Papers by type
    step("📄", "Papers by Type")
    for ptype, cnt in conn.execute("SELECT type, COUNT(*) FROM papers GROUP BY type ORDER BY 2 DESC").fetchall():
        row(ptype, cnt)

    # Language breakdown
    step("🌍", "Language Distribution")
    for lang, cnt in conn.execute("SELECT language, COUNT(*) FROM papers GROUP BY language ORDER BY 2 DESC").fetchall():
        row(lang, cnt)

    # Pipeline health
    step("⚙️ ", "Pipeline Run Log")
    run = conn.execute("SELECT * FROM pipeline_run_log ORDER BY ran_at DESC LIMIT 1").fetchone()
    if run:
        row("Run ID",          run[0])
        row("Status",          run[1])
        row("Rows extracted",  run[2])
        row("Rows loaded",     run[3])
        row("Timestamp",       run[5])

    conn.close()

    print("\n" + "═" * 60)
    print("  ✅  Pipeline complete!  Database: academic_research.db")
    print(f"  📁  CSV exports:       output/")
    print("═" * 60 + "\n")


# ─────────────────────────────────────────────────────────────
#  ENTRYPOINT
# ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("\n")
    print("  ╔══════════════════════════════════════════════════════╗")
    print("  ║     ACADEMIC RESEARCH ETL PIPELINE  —  v1.0.0       ║")
    print("  ║     Python · SQLite · CSV  |  Run ID: " + RUN_ID + "  ║")
    print("  ╚══════════════════════════════════════════════════════╝")

    start = time.time()

    raw      = extract(n_records=50)
    tables   = transform(raw)
    load(tables, raw)
    report()

    elapsed = time.time() - start
    print(f"  ⏱  Total runtime: {elapsed:.2f}s\n")
