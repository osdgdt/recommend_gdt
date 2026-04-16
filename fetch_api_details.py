"""
BGG API Detail Fetcher
Uses the registered BGG application token to fetch full details
(categories, mechanics, designers, artists, publishers, description, images)
for all games already in bgg_games.db.

Usage:
    python fetch_api_details.py

Resume: re-run at any time — skips already-fetched IDs.
Reset:  delete api_progress.json to restart from scratch.
"""

import json
import sqlite3
import time
import xml.etree.ElementTree as ET
from pathlib import Path

import requests

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
DB_PATH       = Path("bgg_games.db")
PROGRESS_PATH = Path("api_progress.json")
API_URL       = "https://boardgamegeek.com/xmlapi2/thing"
BGG_TOKEN     = "63ce9599-482c-4fc2-85b0-12acd0f8282e"
BATCH_SIZE    = 20
API_DELAY     = 2.0
BACKOFF_STEPS = [30, 60, 120, 300]   # longer waits for 429s
CHECKPOINT_EVERY = 200

SESSION = requests.Session()
SESSION.headers.update({
    "Authorization": f"Bearer {BGG_TOKEN}",
    "User-Agent": "BGGRecommender/1.0",
    "Accept-Encoding": "gzip, deflate",
})


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------
def get_with_backoff(url, params=None):
    for attempt, wait in enumerate([0] + BACKOFF_STEPS):
        if wait:
            print(f"  [backoff] waiting {wait}s (attempt {attempt + 1})...")
            time.sleep(wait)
        try:
            resp = SESSION.get(url, params=params, timeout=30)
            if resp.status_code == 200:
                return resp
            if resp.status_code == 202:
                print("  [202] BGG queued request, retrying in 5s...")
                time.sleep(5)
                continue
            if resp.status_code == 429:
                retry_after = int(resp.headers.get("Retry-After", wait if wait else 30))
                print(f"  [429] Rate limited — waiting {retry_after}s...")
                time.sleep(retry_after)
                continue
            print(f"  [HTTP {resp.status_code}] {resp.text[:120]}")
        except requests.RequestException as e:
            print(f"  [error] {e}")
    raise RuntimeError(f"Failed after {len(BACKOFF_STEPS)+1} attempts")


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------
def safe_int(v):
    try:
        return int(float(v)) if v else None
    except (ValueError, TypeError):
        return None


def safe_float(v):
    try:
        return float(v) if v else None
    except (ValueError, TypeError):
        return None


def parse_item(item_el):
    def first(tag):
        return item_el.find(tag)

    def links(link_type):
        return item_el.findall(f'link[@type="{link_type}"]')

    name_el = item_el.find('name[@type="primary"]')
    name = name_el.get("value", "") if name_el is not None else ""

    desc_el = first("description")
    description = (desc_el.text or "").strip() if desc_el is not None else ""

    stats = item_el.find("statistics/ratings")
    bgg_rank = avg_rating = bayes_rating = num_ratings = num_comments = complexity = None
    def sv(el):
        return el.get("value") if el is not None else None

    if stats is not None:
        rank_el = stats.find('ranks/rank[@name="boardgame"]')
        if rank_el is not None:
            rv = rank_el.get("value", "")
            bgg_rank = int(rv) if rv.isdigit() else None
        avg_rating   = safe_float(sv(stats.find("average")))
        bayes_rating = safe_float(sv(stats.find("bayesaverage")))
        num_ratings  = safe_int(  sv(stats.find("usersrated")))
        num_comments = safe_int(  sv(stats.find("numcomments")))
        complexity   = safe_float(sv(stats.find("averageweight")))

    img_el   = first("image")
    thumb_el = first("thumbnail")

    def getval(el):
        return el.get("value") if el is not None else None

    return {
        "id":               int(item_el.get("id")),
        "name":             name,
        "year_published":   safe_int(getval(first("yearpublished"))),
        "description":      description,
        "min_players":      safe_int(getval(first("minplayers"))),
        "max_players":      safe_int(getval(first("maxplayers"))),
        "min_playtime":     safe_int(getval(first("minplaytime"))),
        "max_playtime":     safe_int(getval(first("maxplaytime"))),
        "complexity_weight":complexity,
        "bgg_rank":         bgg_rank,
        "avg_rating":       avg_rating,
        "bayes_rating":     bayes_rating,
        "num_ratings":      num_ratings,
        "num_comments":     num_comments,
        "image_url":        (img_el.text   or "").strip() if img_el   is not None else "",
        "thumbnail_url":    (thumb_el.text or "").strip() if thumb_el is not None else "",
        "categories":  [(int(l.get("id")), l.get("value")) for l in links("boardgamecategory")],
        "mechanics":   [(int(l.get("id")), l.get("value")) for l in links("boardgamemechanic")],
        "designers":   [(int(l.get("id")), l.get("value")) for l in links("boardgamedesigner")],
        "publishers":  [(int(l.get("id")), l.get("value")) for l in links("boardgamepublisher")],
        "artists":     [(int(l.get("id")), l.get("value")) for l in links("boardgameartist")],
    }


def fetch_batch(id_batch):
    resp = get_with_backoff(API_URL, params={"id": ",".join(map(str, id_batch)), "stats": 1})
    try:
        root = ET.fromstring(resp.content)
    except ET.ParseError:
        return []
    games = []
    for item in root.findall("item"):
        try:
            games.append(parse_item(item))
        except Exception as e:
            print(f"  [parse error] id={item.get('id')}: {e}")
    return games


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------
def save_games(conn, games):
    for g in games:
        conn.execute("""
            INSERT OR REPLACE INTO games
            (id, name, year_published, description, min_players, max_players,
             min_playtime, max_playtime, complexity_weight, bgg_rank,
             avg_rating, bayes_rating, num_ratings, num_comments,
             image_url, thumbnail_url)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            g["id"], g["name"], g["year_published"], g["description"],
            g["min_players"], g["max_players"], g["min_playtime"], g["max_playtime"],
            g["complexity_weight"], g["bgg_rank"], g["avg_rating"], g["bayes_rating"],
            g["num_ratings"], g["num_comments"], g["image_url"], g["thumbnail_url"],
        ))

        for kind, link_table, entity_table, fk in [
            ("categories", "game_categories", "categories",  "category_id"),
            ("mechanics",  "game_mechanics",  "mechanics",   "mechanic_id"),
            ("designers",  "game_designers",  "designers",   "designer_id"),
            ("publishers", "game_publishers", "publishers",  "publisher_id"),
            ("artists",    "game_artists",    "artists",     "artist_id"),
        ]:
            for (eid, ename) in g[kind]:
                conn.execute(
                    f"INSERT OR IGNORE INTO {entity_table} (id, name) VALUES (?,?)",
                    (eid, ename)
                )
                conn.execute(
                    f"INSERT OR IGNORE INTO {link_table} (game_id, {fk}) VALUES (?,?)",
                    (g["id"], eid)
                )
    conn.commit()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    if not DB_PATH.exists():
        print("bgg_games.db not found. Run download_bgg.py first.")
        return

    conn = sqlite3.connect(DB_PATH)

    # Load all game IDs from DB
    all_ids = [r[0] for r in conn.execute("SELECT id FROM games ORDER BY bgg_rank")]
    print(f"=== BGG API Detail Fetcher ===")
    print(f"Games in DB: {len(all_ids):,}")

    # Load progress
    if PROGRESS_PATH.exists():
        with open(PROGRESS_PATH) as f:
            done_ids = set(json.load(f)["done_ids"])
    else:
        done_ids = set()

    remaining = [gid for gid in all_ids if gid not in done_ids]
    print(f"Already fetched: {len(done_ids):,}  |  Remaining: {len(remaining):,}\n")

    if not remaining:
        print("All games already fetched.")
        conn.close()
        return

    # Quick API test
    test = SESSION.get(API_URL, params={"id": "174430", "stats": "1"}, timeout=30)
    print(f"API test (Gloomhaven): HTTP {test.status_code}")
    if test.status_code != 200:
        print(f"Token not working: {test.text[:200]}")
        conn.close()
        return
    print("Token OK — starting fetch...\n")

    fetched = 0
    for i in range(0, len(remaining), BATCH_SIZE):
        batch = remaining[i: i + BATCH_SIZE]

        try:
            games = fetch_batch(batch)
        except RuntimeError as e:
            print(f"\n[fatal] {e} — saving progress and stopping.")
            break

        if games:
            save_games(conn, games)

        for gid in batch:
            done_ids.add(gid)
        fetched += len(games)

        if (i // BATCH_SIZE + 1) % (CHECKPOINT_EVERY // BATCH_SIZE) == 0:
            with open(PROGRESS_PATH, "w") as f:
                json.dump({"done_ids": list(done_ids)}, f)
            total_done = len(done_ids)
            pct = total_done / len(all_ids) * 100
            print(f"  [checkpoint] {total_done:,}/{len(all_ids):,} ({pct:.1f}%) — {fetched} saved this run")

        if i + BATCH_SIZE < len(remaining):
            time.sleep(API_DELAY)

    with open(PROGRESS_PATH, "w") as f:
        json.dump({"done_ids": list(done_ids)}, f)

    # Final summary
    cats  = conn.execute("SELECT count(*) FROM categories").fetchone()[0]
    mechs = conn.execute("SELECT count(*) FROM mechanics").fetchone()[0]
    desig = conn.execute("SELECT count(*) FROM designers").fetchone()[0]
    with_mech = conn.execute("SELECT count(DISTINCT game_id) FROM game_mechanics").fetchone()[0]
    conn.close()

    print(f"\nDone. Fetched {fetched} games this run.")
    print(f"  {cats}  categories | {mechs}  mechanics | {desig}  designers")
    print(f"  {with_mech}/10000 games have mechanics data")


if __name__ == "__main__":
    main()
