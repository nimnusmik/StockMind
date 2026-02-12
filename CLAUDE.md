# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

StockMind is a stock trading signal platform that combines community sentiment analysis from Yahoo Finance and financial news NLP to generate AI-based buy/hold/sell signals. It targets 8 tech stocks: AAPL, GOOG, META, TSLA, MSFT, AMZN, NVDA, NFLX.

**Current Status**: MVP complete with FastAPI backend, PostgreSQL database (12,969 comments), Redis caching, and 8 trained RandomForest ML models.

## Architecture

Three main modules:

- **api/** — FastAPI REST API backend serving trading signals, price predictions, sentiment analysis, and community buzz indicators. Uses Redis for caching and PostgreSQL for data storage.
- **community/** — Yahoo Finance comment crawler. Playwright-based automation scrapes comments from SpotIM iframes and stores directly into PostgreSQL.
- **news/** — Financial news analysis pipeline. 6-step process: fetch price data (TwelveData API) → scrape news links → extract content → NLP analysis (DistilBART summarization, FinBERT sentiment, KeyBERT keywords) → build metadata → train RandomForest model.

## Running Services

```bash
# Start all services (PostgreSQL, Redis, FastAPI)
docker-compose up -d

# Access API documentation
open http://localhost:8001/docs

# Run community crawler
cd community && python3 src/main.py

# Run CSV-to-DB migration
cd community && python3 src/migrate_csv_to_db.py

# Train ML models
cd news/code && python3 train_model.py
```

## Key Configuration

**Community Crawler:**
- Stock list, cutoff dates, user agents: `community/src/config.py`
- DB schema: `community/init_db.sql`

**API:**
- Environment variables: `api/.env`
- Settings: `api/config.py`
- Trading signal thresholds: BUY (>+2% & sentiment >0.3), SELL (<-2% & sentiment <-0.3)

**Docker:**
- PostgreSQL: `localhost:5433` (mapped from container 5432)
- Redis: `localhost:6379`
- FastAPI: `localhost:8001`
- DB credentials: database=`stockmind`, user=`user`, password=`password`, host=`db` (inside container) or `localhost` (outside)

## API Endpoints

- `GET /api/v1/predictions/{symbol}/price` — ML price prediction
- `GET /api/v1/signals/{symbol}` — Trading signals (BUY/HOLD/SELL)
- `GET /api/v1/sentiment/{symbol}/community` — Community sentiment
- `GET /api/v1/sentiment/{symbol}/news` — News sentiment
- `GET /api/v1/sentiment/{symbol}/combined` — Combined sentiment (60% news + 40% community)
- `GET /api/v1/buzz/{symbol}` — Community activity metrics
- `GET /api/v1/historical/{symbol}/comments` — Historical comments

## News Pipeline Setup

```bash
python3 -m venv venv
source venv/bin/activate
pip install pandas bs4 selenium webdriver_manager keybert matplotlib scikit-learn
pip install torch transformers sentence-transformers
```

Run scripts sequentially: `1st_stock_graph.py` → `2nd_create_csv_with_link.py` → `3rd_add_content_in_csv.py` → `4th_analysis.py` → `5th_make_metadata.py` → `train_model.py`

Models are saved to `api/models/{SYMBOL}_rf_model.pkl` for API use.

## Data Flow

1. **Community Crawler** → PostgreSQL `comments` table (12,969 rows)
2. **News Pipeline** → Feature CSVs (392 dimensions) → Trained ML models
3. **API** loads models → Combines sentiment → Generates trading signals
4. **Redis** caches results (5min-24hr TTL) for performance

## Conventions

- Code comments and log messages are in **Korean**
- Logging uses emoji indicators (✅ ❌ 🔄 📊 💾)
- Community logs stored per-stock: `community/logs/{SYMBOL}/`
- CSV format: columns `time`, `text`, `stock_symbol`; time format "12 Jul, 2025 11:48 PM"
- Duplicate detection uses MD5 hash of comment text
- ML features: 384D embeddings + 3D sentiment + 5D keywords = 392D total
