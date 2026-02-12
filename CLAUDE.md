# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

StockMind is a stock price movement prediction platform that analyzes community sentiment from Yahoo Finance and financial news. It targets 8 tech stocks: AAPL, GOOG, META, TSLA, MSFT, AMZN, NVDA, NFLX. The project is in early phases (data collection and migration complete; automation and ML pipelines in progress).

## Architecture

Three independent modules, each in its own directory:

- **community/** — Yahoo Finance comment crawling pipeline. Playwright-based browser automation scrapes comments from SpotIM iframes, stores them as CSV or directly into PostgreSQL. Includes an Airflow DAG for daily scheduling (not yet active).
- **news/** — Financial news analysis pipeline. A numbered 6-step process: fetch price data (TwelveData API) → scrape news links → extract article content → NLP analysis (DistilBART summarization, FinBERT sentiment, KeyBERT keywords) → build metadata → train RandomForest model.
- **chart_pattern/** — Synthetic chart pattern generation (cup & handle, flag, pennant, gap) using mplfinance for model training/testing.
 
## Running Services

```bash
# Start PostgreSQL + crawler containers
cd community && docker-compose up -d

# Run crawler locally
python community/src/main.py

# Run CSV-to-DB migration
python community/src/migrate_csv_to_db.py
```

## Key Configuration

- **Stock list, cutoff dates, user agents**: `community/src/config.py`
- **DB schema**: `community/init_db.sql` — `comments` table with unique constraint on (stock_symbol, comment_time, comment_hash)
- **Docker DB credentials**: host=`db`, port=5432, database=`stockmind`, user=`user`, password=`password`

## News Pipeline Setup

```bash
python3.10 -m venv venv
pip install pandas bs4 selenium webdriver_manager keybert matplotlib
pip install "torch==2.2.2" "transformers<4.39" "sentence-transformers<3"
```

Scripts run sequentially: `1st_stock_graph.py` → `2nd_create_csv_with_link.py` → `3rd_add_content_in_csv.py` → `4th_analysis.py` → `5th_make_metadata.py` → `train_model.py`

## Conventions

- Code comments and log messages are in **Korean**
- Logging uses emoji indicators (✅ ❌ 🔄 📊) with per-stock log files under `community/logs/{SYMBOL}/`
- Two crawler variants exist: `crawler.py` (CSV output) and `crawler_dblinked.py` (direct DB insert) — both define `MultiStockYahooFinanceCrawler`
- CSV data format: columns `time`, `text`, `stock_symbol`; time format "12 Jul, 2025 11:48 PM"
- Duplicate detection uses MD5 hash of comment text

## Planned but Incomplete

- Airflow DAG (`community/dags/stock_crawler_dag.py`) — references old import paths, not active
- MLflow experiment tracking — minimal setup only (`mlruns/0/meta.yaml`)
- Phases 3-4 (ML serving, FastAPI, dashboard, Redis, Grafana) not yet implemented
