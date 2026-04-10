"""
BGG Enrichment Script
Reads the Kaggle BGG CSV (bgg.csv) and enriches the existing bgg_games.db
with categories, mechanics, designers, artists, publishers, and missing
game metadata (description, image, players, playtime).

Usage:
    python enrich_from_kaggle.py
    # or specify a different CSV path:
    python enrich_from_kaggle.py path/to/bgg.csv
"""

import csv
import sqlite3
import sys
from pathlib import Path

DB_PATH = Path("bgg_games.db")
DEFAULT_CSV = Path("bgg.csv")


def split_field(value: str) -> list[str]:
    """Split a comma-separated BGG field into a clean list of non-empty strings."""
    if not value or value.strip() in ("", "N/A"):
        return []
    return [v.strip() for v in value.split(",") if v.strip()]


def upsert_entity(conn: sqlite3.Connection, table: str, name: str) -> int:
    """Insert entity by name if not exists, return its rowid."""
    conn.execute(f"INSERT OR IGNORE INTO {table} (name) VALUES (?)", (name,))
    row = conn.execute(f"SELECT id FROM {table} WHERE name = ?", (name,)).fetchone()
    return row[0]


def enrich(db_path: Path, csv_path: Path):
    if not db_path.exists():
        print(f"[error] Database not found: {db_path}")
        print("Run download_bgg.py first.")
        return

    if not csv_path.exists():
        print(f"[error] CSV not found: {csv_path}")
        print("Download bgg.csv from https://www.kaggle.com/datasets/sujaykapadnis/board-games")
        return

    conn = sqlite3.connect(db_path)

    # Fetch the set of game IDs already in the DB
    existing_ids = {row[0] for row in conn.execute("SELECT id FROM games")}
    print(f"Games in DB: {len(existing_ids)}")

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    print(f"Rows in CSV: {len(rows)}")

    matched = updated = skipped = 0

    for row in rows:
        try:
            gid = int(float(row.get("game_id") or row.get("BGGId") or 0))
        except (ValueError, TypeError):
            skipped += 1
            continue

        if gid not in existing_ids:
            skipped += 1
            continue

        matched += 1

        # --- Update missing metadata on the games row ---
        conn.execute("""
            UPDATE games SET
                description   = COALESCE(NULLIF(description, ''),   ?),
                min_players   = COALESCE(min_players,   ?),
                max_players   = COALESCE(max_players,   ?),
                min_playtime  = COALESCE(min_playtime,  ?),
                max_playtime  = COALESCE(max_playtime,  ?),
                year_published= COALESCE(year_published,?),
                image_url     = COALESCE(NULLIF(image_url, ''),     ?),
                thumbnail_url = COALESCE(NULLIF(thumbnail_url, ''), ?)
            WHERE id = ?
        """, (
            row.get("description", "").strip() or None,
            _int(row.get("min_players")),
            _int(row.get("max_players")),
            _int(row.get("min_playtime") or row.get("playing_time")),
            _int(row.get("max_playtime") or row.get("playing_time")),
            _int(row.get("year_published")),
            row.get("image", "").strip() or None,
            row.get("thumbnail", "").strip() or None,
            gid,
        ))

        # --- Categories ---
        for name in split_field(row.get("category", "")):
            eid = upsert_entity(conn, "categories", name)
            conn.execute(
                "INSERT OR IGNORE INTO game_categories (game_id, category_id) VALUES (?,?)",
                (gid, eid)
            )

        # --- Mechanics ---
        for name in split_field(row.get("mechanic", "")):
            eid = upsert_entity(conn, "mechanics", name)
            conn.execute(
                "INSERT OR IGNORE INTO game_mechanics (game_id, mechanic_id) VALUES (?,?)",
                (gid, eid)
            )

        # --- Designers ---
        for name in split_field(row.get("designer", "")):
            eid = upsert_entity(conn, "designers", name)
            conn.execute(
                "INSERT OR IGNORE INTO game_designers (game_id, designer_id) VALUES (?,?)",
                (gid, eid)
            )

        # --- Artists ---
        for name in split_field(row.get("artist", "")):
            eid = upsert_entity(conn, "artists", name)
            conn.execute(
                "INSERT OR IGNORE INTO game_artists (game_id, artist_id) VALUES (?,?)",
                (gid, eid)
            )

        # --- Publishers ---
        for name in split_field(row.get("publisher", "")):
            eid = upsert_entity(conn, "publishers", name)
            conn.execute(
                "INSERT OR IGNORE INTO game_publishers (game_id, publisher_id) VALUES (?,?)",
                (gid, eid)
            )

        updated += 1

        if updated % 500 == 0:
            conn.commit()
            print(f"  Enriched {updated}/{matched} matched games...")

    conn.commit()
    conn.close()

    print(f"\nDone.")
    print(f"  Matched:  {matched} games")
    print(f"  Enriched: {updated} games")
    print(f"  Skipped:  {skipped} rows (not in DB or invalid ID)")

    # Summary query
    conn2 = sqlite3.connect(db_path)
    cats   = conn2.execute("SELECT count(*) FROM categories").fetchone()[0]
    mechs  = conn2.execute("SELECT count(*) FROM mechanics").fetchone()[0]
    desigs = conn2.execute("SELECT count(*) FROM designers").fetchone()[0]
    conn2.close()
    print(f"\nDatabase now contains:")
    print(f"  {cats}  categories")
    print(f"  {mechs}  mechanics")
    print(f"  {desigs}  designers")


def _int(v):
    try:
        return int(float(v)) if v and str(v).strip() not in ("", "N/A") else None
    except (ValueError, TypeError):
        return None


if __name__ == "__main__":
    csv_path = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_CSV
    enrich(DB_PATH, csv_path)
