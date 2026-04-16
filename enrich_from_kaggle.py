"""
BGG Enrichment Script
Enriches bgg_games.db with categories, mechanics, designers, artists, publishers
by combining multiple public datasets:
  1. Kaggle CSV (board_games.csv) - sujaykapadnis dataset, Oct 2023
  2. inaimathi/all-boardgames-ever medium.json - downloaded from GitHub

Usage:
    python enrich_from_kaggle.py
"""

import csv
import io
import json
import sqlite3
import tarfile
from pathlib import Path

import requests

DB_PATH = Path("bgg_games.db")
KAGGLE_CSV = Path("board_games.csv")
INAIMATHI_URL = "https://github.com/inaimathi/all-boardgames-ever/raw/refs/heads/master/medium.json.tar.gz"


def upsert_entity(conn, table, name):
    conn.execute(f"INSERT OR IGNORE INTO {table} (name) VALUES (?)", (name,))
    return conn.execute(f"SELECT id FROM {table} WHERE name = ?", (name,)).fetchone()[0]


def link(conn, link_table, fk_col, game_id, entity_id):
    conn.execute(
        f"INSERT OR IGNORE INTO {link_table} (game_id, {fk_col}) VALUES (?,?)",
        (game_id, entity_id)
    )


def enrich_game(conn, gid, data):
    """Apply one game's enrichment data dict to the DB."""
    # Update missing metadata
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
        data.get("description") or None,
        data.get("min_players"),
        data.get("max_players"),
        data.get("min_playtime"),
        data.get("max_playtime"),
        data.get("year_published"),
        data.get("image_url") or None,
        data.get("thumbnail_url") or None,
        gid,
    ))

    for name in data.get("categories", []):
        link(conn, "game_categories", "category_id", gid, upsert_entity(conn, "categories", name))
    for name in data.get("mechanics", []):
        link(conn, "game_mechanics", "mechanic_id", gid, upsert_entity(conn, "mechanics", name))
    for name in data.get("designers", []):
        link(conn, "game_designers", "designer_id", gid, upsert_entity(conn, "designers", name))
    for name in data.get("artists", []):
        link(conn, "game_artists", "artist_id", gid, upsert_entity(conn, "artists", name))
    for name in data.get("publishers", []):
        link(conn, "game_publishers", "publisher_id", gid, upsert_entity(conn, "publishers", name))


def _split(value):
    if not value or str(value).strip() in ("", "N/A"):
        return []
    return [v.strip() for v in str(value).split(",") if v.strip()]


def _int(v):
    try:
        return int(float(v)) if v and str(v).strip() not in ("", "N/A") else None
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# Source 1: Kaggle CSV
# ---------------------------------------------------------------------------
def load_kaggle(csv_path):
    if not csv_path.exists():
        print(f"  [skip] {csv_path} not found.")
        return {}
    with open(csv_path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    result = {}
    for row in rows:
        try:
            gid = int(float(row.get("game_id") or 0))
        except (ValueError, TypeError):
            continue
        if not gid:
            continue
        result[gid] = {
            "description":  (row.get("description") or "").strip(),
            "min_players":  _int(row.get("min_players")),
            "max_players":  _int(row.get("max_players")),
            "min_playtime": _int(row.get("min_playtime") or row.get("playing_time")),
            "max_playtime": _int(row.get("max_playtime") or row.get("playing_time")),
            "year_published": _int(row.get("year_published")),
            "image_url":    (row.get("image") or "").strip(),
            "thumbnail_url":(row.get("thumbnail") or "").strip(),
            "categories":   _split(row.get("category")),
            "mechanics":    _split(row.get("mechanic")),
            "designers":    _split(row.get("designer")),
            "artists":      _split(row.get("artist")),
            "publishers":   _split(row.get("publisher")),
        }
    print(f"  Loaded {len(result):,} games from Kaggle CSV.")
    return result


# ---------------------------------------------------------------------------
# Source 2: inaimathi medium.json
# ---------------------------------------------------------------------------
def load_inaimathi():
    print(f"  Downloading inaimathi dataset from GitHub...")
    resp = requests.get(INAIMATHI_URL, timeout=120)
    with tarfile.open(fileobj=io.BytesIO(resp.content)) as tf:
        raw = tf.extractfile("medium.json").read().decode("utf-8")
    lines = [l.strip() for l in raw.strip().split("\n") if l.strip()]
    result = {}
    for line in lines:
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        gid = obj.get("bgg-id")
        if not gid:
            continue
        def first_or_none(lst):
            return lst[0] if lst else None
        result[gid] = {
            "description":   (obj.get("description") or [""])[0][:5000] if isinstance(obj.get("description"), list) else str(obj.get("description") or "")[:5000],
            "min_players":   _int(obj.get("minimum-players")),
            "max_players":   _int(obj.get("maximum-players")),
            "min_playtime":  _int(obj.get("playing-time")),
            "max_playtime":  _int(obj.get("playing-time")),
            "year_published":_int(obj.get("year-published")),
            "image_url":     first_or_none(obj.get("image") or []),
            "thumbnail_url": first_or_none(obj.get("thumbnail") or []),
            "categories":    obj.get("category") or [],
            "mechanics":     obj.get("mechanic") or [],
            "designers":     [],
            "artists":       [],
            "publishers":    obj.get("publisher") or [],
        }
    print(f"  Loaded {len(result):,} games from inaimathi dataset.")
    return result


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def enrich(db_path):
    if not db_path.exists():
        print(f"[error] Database not found: {db_path}. Run download_bgg.py first.")
        return

    conn = sqlite3.connect(db_path)
    existing_ids = {r[0] for r in conn.execute("SELECT id FROM games")}
    print(f"Games in DB: {len(existing_ids):,}\n")

    # Load both sources and merge (Kaggle takes priority for overlapping games)
    print("Loading Kaggle CSV...")
    kaggle = load_kaggle(KAGGLE_CSV)
    print("Loading inaimathi dataset...")
    inaimathi = load_inaimathi()

    # Merge: start with inaimathi, overwrite with Kaggle (more recent)
    merged = {}
    for gid, data in inaimathi.items():
        if gid in existing_ids:
            merged[gid] = data
    for gid, data in kaggle.items():
        if gid in existing_ids:
            if gid in merged:
                # Kaggle overrides metadata; union the lists
                base = merged[gid]
                merged[gid] = {
                    "description":   data["description"] or base["description"],
                    "min_players":   data["min_players"] or base["min_players"],
                    "max_players":   data["max_players"] or base["max_players"],
                    "min_playtime":  data["min_playtime"] or base["min_playtime"],
                    "max_playtime":  data["max_playtime"] or base["max_playtime"],
                    "year_published":data["year_published"] or base["year_published"],
                    "image_url":     data["image_url"] or base["image_url"],
                    "thumbnail_url": data["thumbnail_url"] or base["thumbnail_url"],
                    "categories":    list({*data["categories"], *base["categories"]}),
                    "mechanics":     list({*data["mechanics"], *base["mechanics"]}),
                    "designers":     list({*data["designers"], *base["designers"]}),
                    "artists":       list({*data["artists"], *base["artists"]}),
                    "publishers":    list({*data["publishers"], *base["publishers"]}),
                }
            else:
                merged[gid] = data

    print(f"\nTotal games to enrich: {len(merged):,} / {len(existing_ids):,}")
    print(f"Still uncovered:       {len(existing_ids) - len(merged):,} (rank+rating only)\n")

    for i, (gid, data) in enumerate(merged.items(), 1):
        enrich_game(conn, gid, data)
        if i % 500 == 0:
            conn.commit()
            print(f"  Enriched {i:,}/{len(merged):,}...")

    conn.commit()

    # Summary
    cats  = conn.execute("SELECT count(*) FROM categories").fetchone()[0]
    mechs = conn.execute("SELECT count(*) FROM mechanics").fetchone()[0]
    desig = conn.execute("SELECT count(*) FROM designers").fetchone()[0]
    with_mech = conn.execute("""
        SELECT count(DISTINCT game_id) FROM game_mechanics
    """).fetchone()[0]
    with_cat = conn.execute("""
        SELECT count(DISTINCT game_id) FROM game_categories
    """).fetchone()[0]
    conn.close()

    print(f"\nDone. Database summary:")
    print(f"  {cats:>5}  categories  (unique)")
    print(f"  {mechs:>5}  mechanics   (unique)")
    print(f"  {desig:>5}  designers   (unique)")
    print(f"  {with_cat:>5}  games with categories")
    print(f"  {with_mech:>5}  games with mechanics")


if __name__ == "__main__":
    enrich(DB_PATH)
