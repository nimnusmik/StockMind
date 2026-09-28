"""Yahoo Finance 커뮤니티 글 수집기 (표준 라이브러리만 사용).

Yahoo가 2026년 초 OpenWeb iframe 댓글을 자체 커뮤니티로 바꾸면서 옛 Playwright 크롤러(src/)가 0개를 수집했다.
새 커뮤니티는 GraphQL(GetContentByAssociatedContentId)로 글을 불러오고, 로그인이 필요 없다.

동작: 종목마다 최신 글부터 과거로 페이지를 넘기며 SQLite에 저장한다.
- 이미 저장된 글로만 채워진 페이지를 만나면 멈춘다 (증분 수집).
- 단, 가장 오래된 저장 글이 BACKFILL_UNTIL보다 최근이면 계속 내려가서 과거를 채운다.
  중간에 끊겨도 다음 실행이 이어받는다.

실행: python3 collect.py            (8종목)
      python3 collect.py AAPL TSLA  (일부만)
      python3 collect.py --summary  (종목·날짜별 개수만 출력)
"""
import json
import re
import sqlite3
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

TICKERS = ["AAPL", "GOOG", "META", "TSLA", "MSFT", "AMZN", "NVDA", "NFLX"]
BACKFILL_UNTIL = "2026-07-01"  # 이 날짜까지 과거 글을 채운다 (ISO 문자열 비교)
MAX_PAGES = 5000               # 한 종목 한 번 실행의 안전 상한 (10개/페이지). NVDA는 3개월에 2000페이지를 넘음
DELAY = 0.7                    # 요청 간격(초). 비공식 API라 천천히

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
    """종목 커뮤니티 페이지 HTML에 박힌 게시판 ID(finmb_숫자)를 찾는다."""
    req = urllib.request.Request(f"https://finance.yahoo.com/quote/{symbol}/community/", headers={"user-agent": UA})
    html = urllib.request.urlopen(req, timeout=30).read().decode("utf-8", "ignore")
    ids = set(re.findall(r"finmb_\d+", html))
    if len(ids) != 1:
        raise RuntimeError(f"{symbol} 게시판 ID를 하나로 특정 못함: {ids}")
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
        except Exception as e:  # 네트워크 흔들림은 재시도, 3번 실패면 이 종목은 다음 실행으로
            log(f"  재시도 {attempt + 1}/3: {e}")
            time.sleep(5 * (attempt + 1))
    raise RuntimeError("요청 3회 실패")


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
            break  # 이미 아는 글만 나옴 → 따라잡기 끝
        after = info.get("endCursor")
        time.sleep(DELAY)
    log(f"{symbol} ({cid}): 새 글 {new}개, {pages}페이지")


def summary(con):
    print("종목  전체     최초                 최근")
    for sym, n, lo, hi in con.execute(
            "SELECT symbol, COUNT(*), MIN(created_at), MAX(created_at) FROM posts GROUP BY symbol ORDER BY symbol"):
        print(f"{sym:5} {n:6}  {lo}  {hi}")


if __name__ == "__main__":
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
            log(f"{sym} 실패: {e}")
    summary(con)
    sys.exit(1 if failed else 0)
