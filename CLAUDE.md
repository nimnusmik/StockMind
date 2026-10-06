# CLAUDE.md

## Project overview

A research project that honestly tests whether Yahoo Finance community posts (a human crowd)
predict next-day stock behavior (volatility, direction).
Long-term goal: compare the human crowd against an AI crowd (multiple LLM agents).

## Structure

- **community/** — current code and data (launchd references this path; do not move it)
  - `collect.py` — community collector. Yahoo's own community GraphQL
    (`GetContentByAssociatedContentId`, no login) -> SQLite `data/community.db`. Backfilled to Jul 1, then incremental.
    launchd `com.sunmin.stockmind` runs it at :23 every hour; `data/.collect.lock` prevents overlapping runs. Log: `data/collect.log`.
  - `feed_query.graphql` — the GraphQL query the collector uses
  - `prices.py` — daily prices via yfinance (collected tickers + SPY) -> `data/prices.csv`
  - `minute_bars.py` — accumulates 1-minute bars -> `data/prices_1m.csv.gz` (yfinance serves only 30 days, so fetched daily).
    launchd `com.sunmin.stockmind.minute`, daily 14:47, log `data/minute.log`
  - `sentiment.py` — per-post sentiment (cardiffnlp twitter-roberta) -> `data/sentiment.csv`.
    torch is heavy: `uv run --no-project --with transformers --with torch --with pandas python sentiment.py`
  - `stance.py` — per-post bull/bear/neutral stance (FinTwitBERT) -> `data/stance.csv` (78% vs 59% for the mood model
    on a hand-labelled 50-post key in `data/stance/key50.tsv`)
  - `toxicity.py` — per-post toxicity (Detoxify) -> `data/toxicity.csv`
  - `exp1_*` .. `exp11_*`, `plot_*.py` — experiments (stock versions of Morstatter 2026 "The Conversation Turns First",
    plus contrarian/capitulation tests). Performance claims use time-split results.
  - `data/export/` — CSV snapshots for spreadsheets
- **docs/** — internal notes (Korean; NEVER commit — contains contact info and email drafts)
- **legacy/** — the old StockMind app (FastAPI, Next.js, news pipeline, Playwright crawler) and July 2025 data.
  Unused, kept for reference. The old performance numbers there (75% accuracy etc.) were invalidated by look-ahead leakage.

## Running

```bash
cd community
python3 collect.py              # incremental collection, 15 tickers
python3 collect.py GME AMC     # subset
python3 collect.py --summary   # per-ticker counts
python3 prices.py              # refresh prices
```

## Rules

- Tickers: 8 mega-cap tech (AAPL GOOG META TSLA MSFT AMZN NVDA NFLX) + 7 retail favorites (GME AMC PLTR SOFI RIVN COIN HOOD)
- DB timestamps (`created_at`) are UTC. Day boundaries are cut at the New York close (16:00 ET) to prevent look-ahead leakage.
- Performance claims only after a time split, t -> t+1 targets, and baseline comparisons ("always HOLD", random).
- Unofficial API: keep the request interval (`DELAY`). Never publish raw posts (`data/` is git-ignored).
- Code comments and README in English (public repo); internal docs (docs/) in Korean.
- Pre-registered re-tests pending (judge on data collected after 2026-10-05 only):
  the overnight comment effect (exp 1) and the NVDA 5-day post-signal bounce (exp 9).
