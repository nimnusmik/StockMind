"""Yahoo Finance community post collector (standard library only).

In early 2026 Yahoo replaced its OpenWeb iframe comments with its own community, and the old
Playwright crawler (src/) started collecting zero posts. The new community loads posts through
GraphQL (GetContentByAssociatedContentId) and requires no login.

How it works: for each ticker, page from the newest posts back in time and store into SQLite.
- Stop when a page contains only already-stored posts (incremental collection).
- Exception: if the oldest stored post is newer than BACKFILL_UNTIL, keep paging to fill the
  past. If interrupted, the next run picks up where this one left off.

Run:  python3 collect.py            (all 15 tickers)
      python3 collect.py AAPL TSLA  (subset)
      python3 collect.py --summary  (counts per ticker only)
"""
import json
import re
import socket
import sqlite3
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

TICKERS = ["AAPL", "GOOG", "META", "TSLA", "MSFT", "AMZN", "NVDA", "NFLX",  # mega-cap tech
           "GME", "AMC", "PLTR", "SOFI", "RIVN", "COIN", "HOOD"]  # retail favorites (added 2026-09-29)
BACKFILL_UNTIL = "2026-07-01"  # backfill history down to this date (ISO string comparison)
MAX_PAGES = 5000               # safety cap per ticker per run (10 posts/page); NVDA exceeds 2000 pages in 3 months
DELAY = 0.7                    # seconds between requests; unofficial API, so go slowly

HERE = Path(__file__).parent
DB = HERE / "data" / "community.db"
QUERY = (HERE / "feed_query.graphql").read_text()
API = "https://yfc-server-query.finance.yahoo.com/"
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/128 Safari/537.36"
HEADERS = {"content-type": "application/json", "user-agent": UA,
           "origin": "https://finance.yahoo.com", "referer": "https://finance.yahoo.com/"}


def log(msg):
    print(f"{datetime.now():%Y-%m-%d %H:%M:%S} {msg}", flush=True)


def content_id(symbol):
    """Find the board ID (finmb_<digits>) embedded in the ticker's community page HTML."""
    req = urllib.request.Request(f"https://finance.yahoo.com/quote/{symbol}/community/", headers={"user-agent": UA})
    html = urllib.request.urlopen(req, timeout=30).read().decode("utf-8", "ignore")
    ids = set(re.findall(r"finmb_\d+", html))
    if len(ids) != 1:
        raise RuntimeError(f"{symbol}: could not pin down a single board ID: {ids}")
    return ids.pop()


def fetch_page(cid, after):
    variables = {"contentId": cid, "first": 10, "includePost": False, "sortOrder": "TIME_DESC",
                 "postUuid": "00000000-0000-0000-0000-000000000000"}
    if after:
        variables["after"] = after
    body = json.dumps({"operationName": "GetContentByAssociatedContentId", "variables": variables, "query": QUERY})
    for attempt in range(3):
        try:
            res = urllib.request.urlopen(urllib.request.Request(API, body.encode(), HEADERS), timeout=30)
            data = json.loads(res.read())
            feed = data["data"]["getContentByAssociatedContentId"]["newFeed"]
            return [e["node"] for e in feed["edges"] if e.get("node")], feed["pageInfo"]
        except Exception as e:  # retry network hiccups; after 3 failures leave this ticker to the next run
            log(f"  retry {attempt + 1}/3: {e}")
            time.sleep(5 * (attempt + 1))
    raise RuntimeError("request failed 3 times")


def open_db():
    DB.parent.mkdir(exist_ok=True)
    con = sqlite3.connect(DB)
    con.execute("""CREATE TABLE IF NOT EXISTS posts (
        uuid TEXT PRIMARY KEY, symbol TEXT NOT NULL, created_at TEXT NOT NULL, body TEXT,
        upvotes INTEGER, reply_count INTEGER, username TEXT, investor_identity TEXT,
        collected_at TEXT NOT NULL)""")
    con.execute("CREATE INDEX IF NOT EXISTS posts_symbol_time ON posts(symbol, created_at)")
    return con


def collect(con, symbol):
    cid = content_id(symbol)
    oldest = con.execute("SELECT MIN(created_at) FROM posts WHERE symbol=?", (symbol,)).fetchone()[0]
    need_backfill = oldest is None or oldest > BACKFILL_UNTIL
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    after, new, pages = None, 0, 0
    while pages < MAX_PAGES:
        nodes, info = fetch_page(cid, after)
        pages += 1
        rows = [(n["uuid"], symbol, n["createdAt"], n.get("body"), (n.get("votes") or {}).get("upvoteCount"),
                 (n.get("comments") or {}).get("count"),
                 ((n.get("user") or {}).get("profile") or {}).get("username"),
                 ((n.get("user") or {}).get("profile") or {}).get("investorIdentity"), now) for n in nodes]
        before = con.total_changes
        con.executemany("INSERT OR IGNORE INTO posts VALUES (?,?,?,?,?,?,?,?,?)", rows)
        con.commit()
        added = con.total_changes - before
        new += added
        page_oldest = min((r[2] for r in rows), default="")
        if not info.get("hasNextPage") or (page_oldest and page_oldest < BACKFILL_UNTIL):
            break
        if added == 0 and not need_backfill:
            break  # only known posts left -> caught up
        after = info.get("endCursor")
        time.sleep(DELAY)
    log(f"{symbol} ({cid}): {new} new posts, {pages} pages")


def summary(con):
    print("sym   total    first                last")
    for sym, n, lo, hi in con.execute(
            "SELECT symbol, COUNT(*), MIN(created_at), MAX(created_at) FROM posts GROUP BY symbol ORDER BY symbol"):
        print(f"{sym:5} {n:6}  {lo}  {hi}")


if __name__ == "__main__":
    # Right after the Mac wakes, Wi-Fi may not be up yet and every ticker fails with DNS errors -> wait up to 3 min
    for _ in range(18):
        try:
            socket.getaddrinfo("yfc-server-query.finance.yahoo.com", 443)
            break
        except OSError:
            time.sleep(10)
    con = open_db()
    if "--summary" in sys.argv:
        summary(con)
        sys.exit()
    failed = []
    for sym in [a.upper() for a in sys.argv[1:]] or TICKERS:
        try:
            collect(con, sym)
        except Exception as e:
            failed.append(sym)
            log(f"{sym} failed: {e}")
    summary(con)
    sys.exit(1 if failed else 0)
