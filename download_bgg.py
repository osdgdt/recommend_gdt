"""
BGG Dataset Downloader
Downloads the top 10,000 ranked board games from BoardGameGeek
and stores them in a SQLite database suitable for a recommendation web app.

Strategy: scan BGG item IDs sequentially via the official XML API2.
BGG game IDs are not sequential by rank, so we scan IDs 1..MAX_GAME_ID,
fetch full details for each batch, and keep only boardgames that have a rank.
We stop once TARGET_RANKED_GAMES ranked games have been saved.

Usage:
    pip install -r requirements.txt
    python download_bgg.py

Resume: re-run at any time — picks up from last scanned ID.
Reset:  delete progress.json and bgg_games.db to start from scratch.
"""

import csv
import gzip
import io
import json
import zipfile
import sqlite3
import time
import xml.etree.ElementTree as ET
from pathlib import Path

import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
DB_PATH = Path("bgg_games.db")
PROGRESS_PATH = Path("progress.json")
API_URL = "https://boardgamegeek.com/xmlapi2/thing"
MAX_GAME_ID = 410_000       # BGG IDs currently go up to ~400k
TARGET_RANKED_GAMES = 10_000
BATCH_SIZE = 20             # BGG API: up to 20 IDs per request
API_DELAY = 0.6             # seconds between requests (~100 req/min, well within limits)
CHECKPOINT_EVERY = 200      # save progress every N ranked games found
BACKOFF_STEPS = [15, 30, 60, 120]

BGG_USERNAME = "Dadapatata"
BGG_PASSWORD = "B14ef08vs1994ic"
BGG_LOGIN_URL = "https://boardgamegeek.com/login/api/v1"

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Encoding": "gzip, deflate",  # no brotli — requests can't decompress it natively
    "Accept-Language": "en-US,en;q=0.9",
})


def bgg_login():
    """Log in to BGG, then visit the homepage to collect all session tokens."""
    print("Logging in to BGG...", end=" ", flush=True)
    resp = SESSION.post(
        BGG_LOGIN_URL,
        json={"credentials": {"username": BGG_USERNAME, "password": BGG_PASSWORD}},
        timeout=30,
    )
    if resp.status_code not in (200, 204):
        raise RuntimeError(f"BGG login failed (HTTP {resp.status_code}): {resp.text[:200]}")
    print("OK")

    # Visit homepage so BGG sets any additional session/CSRF cookies
    print("Fetching homepage to initialize session...", end=" ", flush=True)
    SESSION.get("https://boardgamegeek.com/", timeout=30)
    print("OK")
    print(f"  Cookies: {list(SESSION.cookies.keys())}")

    # Add Referer so the API accepts requests as coming from the site
    SESSION.headers.update({"Referer": "https://boardgamegeek.com/"})

    # Sanity check
    test = SESSION.get(API_URL, params={"id": "174430", "stats": "1"}, timeout=30)
    print(f"  API test (Gloomhaven): HTTP {test.status_code}")
    if test.status_code != 200:
        print(f"  Response: {test.text[:400]}")
        print("\n  BGG XML API is blocking requests. Switching to data dump download...")
        return False
    return True


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------
SCHEMA = """
CREATE TABLE IF NOT EXISTS games (
    id                INTEGER PRIMARY KEY,
    name              TEXT,
    year_published    INTEGER,
    description       TEXT,
    min_players       INTEGER,
    max_players       INTEGER,
    min_playtime      INTEGER,
    max_playtime      INTEGER,
    complexity_weight REAL,
    bgg_rank          INTEGER,
    avg_rating        REAL,
    bayes_rating      REAL,
    num_ratings       INTEGER,
    num_comments      INTEGER,
    image_url         TEXT,
    thumbnail_url     TEXT
);
CREATE TABLE IF NOT EXISTS categories  (id INTEGER PRIMARY KEY, name TEXT UNIQUE);
CREATE TABLE IF NOT EXISTS mechanics   (id INTEGER PRIMARY KEY, name TEXT UNIQUE);
CREATE TABLE IF NOT EXISTS designers   (id INTEGER PRIMARY KEY, name TEXT UNIQUE);
CREATE TABLE IF NOT EXISTS publishers  (id INTEGER PRIMARY KEY, name TEXT UNIQUE);
CREATE TABLE IF NOT EXISTS artists     (id INTEGER PRIMARY KEY, name TEXT UNIQUE);
CREATE TABLE IF NOT EXISTS game_categories (game_id INTEGER, category_id INTEGER, PRIMARY KEY (game_id, category_id));
CREATE TABLE IF NOT EXISTS game_mechanics  (game_id INTEGER, mechanic_id  INTEGER, PRIMARY KEY (game_id, mechanic_id));
CREATE TABLE IF NOT EXISTS game_designers  (game_id INTEGER, designer_id  INTEGER, PRIMARY KEY (game_id, designer_id));
CREATE TABLE IF NOT EXISTS game_publishers (game_id INTEGER, publisher_id INTEGER, PRIMARY KEY (game_id, publisher_id));
CREATE TABLE IF NOT EXISTS game_artists    (game_id INTEGER, artist_id    INTEGER, PRIMARY KEY (game_id, artist_id));
"""


def init_db(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.executescript(SCHEMA)
    conn.commit()
    return conn


# ---------------------------------------------------------------------------
# Checkpoint
# ---------------------------------------------------------------------------
def load_progress():
    if PROGRESS_PATH.exists():
        with open(PROGRESS_PATH) as f:
            return json.load(f)
    return {"last_id": 0, "ranked_count": 0}


def save_progress(progress: dict):
    with open(PROGRESS_PATH, "w") as f:
        json.dump(progress, f)


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------
def get_with_backoff(url: str, params: dict = None) -> requests.Response:
    for attempt, wait in enumerate([0] + BACKOFF_STEPS):
        if wait:
            print(f"  [backoff] waiting {wait}s (attempt {attempt + 1})...")
            time.sleep(wait)
        try:
            resp = SESSION.get(url, params=params, timeout=30)
            if resp.status_code == 200:
                return resp
            if resp.status_code == 202:
                print("  [202] BGG queued the request — retrying in 5s...")
                time.sleep(5)
                continue
            if resp.status_code == 429:
                print("  [429] Rate limited.")
                continue
            # 404 means none of the IDs in this batch exist — return empty XML
            if resp.status_code == 404:
                return resp
            print(f"  [HTTP {resp.status_code}]")
        except requests.RequestException as e:
            print(f"  [error] {e}")
    raise RuntimeError(f"Failed to fetch after {len(BACKOFF_STEPS) + 1} attempts")


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------
def safe_int(v):
    try:
        return int(v) if v else None
    except (ValueError, TypeError):
        return None


def safe_float(v):
    try:
        return float(v) if v else None
    except (ValueError, TypeError):
        return None


def parse_item(item_el):
    """Parse one <item> element. Returns None if not a ranked boardgame."""
    if item_el.get("type") != "boardgame":
        return None

    stats = item_el.find("statistics/ratings")
    bgg_rank = None
    if stats is not None:
        rank_el = stats.find('ranks/rank[@name="boardgame"]')
        if rank_el is not None:
            rv = rank_el.get("value", "")
            bgg_rank = int(rv) if rv.isdigit() else None

    if bgg_rank is None:
        return None  # unranked — skip

    def first(tag):
        return item_el.find(tag)

    def all_links(link_type):
        return item_el.findall(f'link[@type="{link_type}"]')

    name_el = item_el.find('name[@type="primary"]')
    name = name_el.get("value", "") if name_el is not None else ""

    desc_el = first("description")
    description = desc_el.text or "" if desc_el is not None else ""

    return {
        "id": int(item_el.get("id")),
        "name": name,
        "year_published": safe_int((first("yearpublished") or {}).get("value")),
        "description": description,
        "min_players": safe_int((first("minplayers") or {}).get("value")),
        "max_players": safe_int((first("maxplayers") or {}).get("value")),
        "min_playtime": safe_int((first("minplaytime") or {}).get("value")),
        "max_playtime": safe_int((first("maxplaytime") or {}).get("value")),
        "complexity_weight": safe_float((stats.find("averageweight") or {}).get("value")) if stats is not None else None,
        "bgg_rank": bgg_rank,
        "avg_rating": safe_float((stats.find("average") or {}).get("value")) if stats is not None else None,
        "bayes_rating": safe_float((stats.find("bayesaverage") or {}).get("value")) if stats is not None else None,
        "num_ratings": safe_int((stats.find("usersrated") or {}).get("value")) if stats is not None else None,
        "num_comments": safe_int((stats.find("numcomments") or {}).get("value")) if stats is not None else None,
        "image_url": (first("image").text or "").strip() if first("image") is not None else "",
        "thumbnail_url": (first("thumbnail").text or "").strip() if first("thumbnail") is not None else "",
        "categories": [(int(l.get("id")), l.get("value")) for l in all_links("boardgamecategory")],
        "mechanics":  [(int(l.get("id")), l.get("value")) for l in all_links("boardgamemechanic")],
        "designers":  [(int(l.get("id")), l.get("value")) for l in all_links("boardgamedesigner")],
        "publishers": [(int(l.get("id")), l.get("value")) for l in all_links("boardgamepublisher")],
        "artists":    [(int(l.get("id")), l.get("value")) for l in all_links("boardgameartist")],
    }


def fetch_batch(id_batch):
    resp = get_with_backoff(API_URL, params={"id": ",".join(map(str, id_batch)), "stats": 1})
    if resp.status_code == 404:
        return []
    try:
        root = ET.fromstring(resp.content)
    except ET.ParseError:
        return []
    games = []
    for item in root.findall("item"):
        try:
            g = parse_item(item)
            if g is not None:
                games.append(g)
        except Exception as e:
            print(f"  [parse error] id={item.get('id')}: {e}")
    return games


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------
def save_games(conn: sqlite3.Connection, games):
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
                conn.execute(f"INSERT OR IGNORE INTO {entity_table} (id, name) VALUES (?,?)", (eid, ename))
                conn.execute(f"INSERT OR IGNORE INTO {link_table} (game_id, {fk}) VALUES (?,?)", (g["id"], eid))
    conn.commit()


# ---------------------------------------------------------------------------
# Data dump fallback
# ---------------------------------------------------------------------------
BGG_DUMP_URL = "https://boardgamegeek.com/data_dumps/bg_ranks"

def _resolve_dump_url(page_url):
    """
    GET the BGG data-dumps page and return the direct CSV/file download URL
    found in the first <a href> that looks like a file download.
    """
    resp = SESSION.get(page_url, timeout=30)
    soup = BeautifulSoup(resp.text, "lxml")
    # Look for links that point to actual file downloads (csv, gz, zip, json)
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if any(ext in href.lower() for ext in (".csv", ".gz", ".zip", ".json", "download", "export")):
            if href.startswith("http"):
                return href
            return "https://boardgamegeek.com" + href
    # Fallback: print all links so we can debug
    print("  Could not auto-detect download link. All <a> hrefs on page:")
    for a in soup.find_all("a", href=True)[:30]:
        print(f"    {a['href']}")
    raise RuntimeError("No download link found on the data dump page.")


def fetch_from_data_dump(conn: sqlite3.Connection) -> int:
    """
    Download BGG's official ranked-games CSV dump and save basic game data.
    Returns the number of games saved.
    """
    print(f"\nFetching data dump page: {BGG_DUMP_URL} ...")
    download_url = _resolve_dump_url(BGG_DUMP_URL)
    print(f"  Download URL: {download_url}")

    print("  Downloading file...", end=" ", flush=True)
    resp = SESSION.get(download_url, timeout=120)
    print(f"HTTP {resp.status_code}")
    if resp.status_code != 200:
        print(f"  Body: {resp.text[:300]}")
        raise RuntimeError("Could not download BGG data dump.")

    content = resp.content
    print(f"  Content-Type: {resp.headers.get('Content-Type', 'unknown')}")
    print(f"  Size: {len(content):,} bytes  First bytes: {content[:8]!r}")

    # Detect and decompress
    magic = content[:4]
    if magic[:2] == b'PK':
        # ZIP archive
        with zipfile.ZipFile(io.BytesIO(content)) as zf:
            names = zf.namelist()
            print(f"  ZIP contents: {names}")
            csv_name = next((n for n in names if n.endswith(".csv")), names[0])
            raw = zf.read(csv_name).decode("utf-8")
        print(f"  Extracted '{csv_name}' from ZIP.")
    elif magic[:2] == b'\x1f\x8b':
        with gzip.open(io.BytesIO(content)) as f:
            raw = f.read().decode("utf-8")
        print("  Decoded as gzip CSV.")
    else:
        for enc in ("utf-8", "latin-1"):
            try:
                raw = content.decode(enc)
                print(f"  Decoded as plain text ({enc}).")
                break
            except UnicodeDecodeError:
                continue
        else:
            raise RuntimeError("Could not decode dump content — unknown format.")

    reader = csv.DictReader(io.StringIO(raw))
    rows = list(reader)
    print(f"  Downloaded {len(rows)} rows. Columns: {reader.fieldnames}")

    # Normalise column names (BGG dump columns vary slightly between exports)
    def col(row, *candidates):
        for c in candidates:
            if c in row and row[c] not in ("", "N/A", None):
                return row[c]
        return None

    saved = 0
    for row in rows:
        # Skip expansions — not useful for standalone recommendations
        if col(row, "is_expansion") == "1":
            continue
        rank_val = col(row, "rank", "boardgamerank")
        if not rank_val:
            continue
        try:
            rank = int(float(rank_val))  # handles "1234" and "1234.0"
        except (ValueError, TypeError):
            continue
        if rank < 1 or rank > TARGET_RANKED_GAMES:  # 0 = unranked
            continue
        gid = safe_int(col(row, "game_id", "objectid", "id"))
        if gid is None:
            continue
        conn.execute("""
            INSERT OR REPLACE INTO games
            (id, name, year_published, bgg_rank, avg_rating, bayes_rating, num_ratings)
            VALUES (?,?,?,?,?,?,?)
        """, (
            gid,
            col(row, "name", "primary"),
            safe_int(col(row, "yearpublished", "year")),
            rank,
            safe_float(col(row, "average", "avgrating")),
            safe_float(col(row, "bayesaverage", "bayesavgrating")),
            safe_int(col(row, "usersrated", "numvoters")),
        ))
        saved += 1

    conn.commit()
    print(f"  Saved {saved} ranked games from data dump.")
    return saved


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    print("=== BGG Dataset Downloader ===")
    print(f"Target: {TARGET_RANKED_GAMES} ranked boardgames -> {DB_PATH}\n")

    api_ok = bgg_login()
    progress = load_progress()
    conn = init_db(DB_PATH)

    if not api_ok:
        # XML API is blocked — use data dump for basic data
        ranked_count = fetch_from_data_dump(conn)
        save_progress({"last_id": MAX_GAME_ID, "ranked_count": ranked_count})
        conn.close()
        print(f"\nDone (data dump mode). {ranked_count} games in {DB_PATH}.")
        print("Note: categories/mechanics/designers are unavailable without API access.")
        return

    start_id = progress["last_id"] + 1
    ranked_count = progress["ranked_count"]

    if ranked_count >= TARGET_RANKED_GAMES:
        print(f"Already have {ranked_count} ranked games. Done.")
        conn.close()
        return

    if start_id > 1:
        print(f"Resuming from ID {start_id} ({ranked_count} ranked games found so far).\n")

    batch_num = 0
    for id_start in range(start_id, MAX_GAME_ID + 1, BATCH_SIZE):
        id_batch = list(range(id_start, min(id_start + BATCH_SIZE, MAX_GAME_ID + 1)))
        batch_num += 1

        try:
            games = fetch_batch(id_batch)
        except RuntimeError as e:
            print(f"\n[fatal] {e} — saving progress and stopping.")
            progress["last_id"] = id_start - 1
            save_progress(progress)
            break

        if games:
            save_games(conn, games)
            ranked_count += len(games)
            names = ", ".join(f"{g['name']} (#{g['bgg_rank']})" for g in games)
            print(f"IDs {id_batch[0]}-{id_batch[-1]} -> {len(games)} ranked: {names[:120]}")

        progress["last_id"] = id_batch[-1]
        progress["ranked_count"] = ranked_count

        if batch_num % (CHECKPOINT_EVERY // BATCH_SIZE) == 0:
            save_progress(progress)
            pct = id_batch[-1] / MAX_GAME_ID * 100
            print(f"  [checkpoint] {ranked_count}/{TARGET_RANKED_GAMES} ranked games"
                  f" | scanned up to ID {id_batch[-1]} ({pct:.1f}%)")

        if ranked_count >= TARGET_RANKED_GAMES:
            print(f"\nReached {ranked_count} ranked games. Stopping.")
            break

        time.sleep(API_DELAY)

    save_progress(progress)
    conn.close()
    print(f"\nDone. {ranked_count} ranked games stored in {DB_PATH}.")


if __name__ == "__main__":
    main()
