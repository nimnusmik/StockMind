# Yahoo Finance Community Crawler (TypeScript)

Production-grade TypeScript crawler for Yahoo Finance community comments, implementing best practices from senior engineer feedback.

## Features

- ✅ **HTML Parsing**: Extracts data from JSON-LD/Script variables (window.YAHOO.context) with DOM fallback
- ✅ **Dynamic Spot ID Extraction**: Uses regex to find Spot ID dynamically from HTML
- ✅ **Strict Type Validation**: Price parsing with null handling (never returns 0 for failures)
- ✅ **Enhanced Error Handling**: Distinguishes 404/403/other errors with detailed logging
- ✅ **User-Agent Rotation**: Random User-Agent selection from pool
- ✅ **Retry Logic**: Exponential backoff with configurable attempts
- ✅ **Content Sanitization**: HTML tag removal and entity handling
- ✅ **Dual Output**: Saves to both CSV and PostgreSQL
- ✅ **Batch Insert**: Efficient database operations with duplicate detection

## Architecture

```
ts-crawler/
├── src/
│   ├── types.ts                    # TypeScript interfaces
│   ├── config.ts                   # Configuration & constants
│   ├── database.ts                 # PostgreSQL operations
│   ├── YahooCommunityScraper.ts    # Main scraper class
│   └── index.ts                    # Entry point & orchestrator
├── package.json
├── tsconfig.json
└── .env                            # Environment variables
```

## Setup

### 1. Install Dependencies

```bash
cd community/ts-crawler
npm install
```

### 2. Configure Environment

Create `.env` file (copy from `.env.example`):

```bash
cp .env.example .env
```

Edit `.env`:

```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=stockmind
DB_USER=user
DB_PASSWORD=password
```

### 3. Start PostgreSQL

```bash
cd ../
docker-compose up -d
```

### 4. Build TypeScript

```bash
npm run build
```

## Usage

### Crawl All Stocks

Crawls all 8 configured stocks (AAPL, GOOG, META, TSLA, MSFT, AMZN, NVDA, NFLX):

```bash
npm start
```

### Crawl Single Stock

```bash
npm start -- --ticker AAPL
```

### CSV Only (Skip Database)

```bash
npm start -- --no-db
```

### Development Mode (with ts-node)

```bash
npm run dev
```

### Help

```bash
npm start -- --help
```

## Output

### CSV Files

Saved to `../data/`:

```
AAPL_comments_2026-02-12.csv
GOOG_comments_2026-02-12.csv
...
```

CSV format:
```csv
time,text,stock_symbol,likes,replies_count
"12 Feb, 2026 10:30 AM","Comment text...",AAPL,5,2
```

### PostgreSQL

Inserted into `comments` table with duplicate detection:

```sql
CREATE TABLE comments (
    id SERIAL PRIMARY KEY,
    stock_symbol VARCHAR(10) NOT NULL,
    comment_time TIMESTAMP WITH TIME ZONE NOT NULL,
    comment_text TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    comment_hash TEXT NOT NULL,
    UNIQUE (stock_symbol, comment_time, comment_hash)
);
```

## Configuration

### Stock List

Edit `src/config.ts`:

```typescript
export const STOCKS = ['AAPL', 'GOOG', 'META', 'TSLA', 'MSFT', 'AMZN', 'NVDA', 'NFLX'];
```

### Scraper Settings

```typescript
export const SCRAPER_CONFIG = {
  retryAttempts: 3,           // Number of retries on failure
  retryDelay: 2000,           // Initial retry delay (ms)
  timeout: 30000,             // Request timeout (ms)
  defaultSpotId: 'sp_R2cfqc5q',  // Fallback Spot ID
  commentsPerRequest: 20,     // Comments to fetch per request
};
```

## Implementation Details

### HTML Parsing Strategy

1. **Priority 1**: Extract from `window.YAHOO.context` (JSON-LD/Script variables)
2. **Priority 2**: Parse DOM using `data-testid` selectors (Cheerio)
3. **Validation**: Strict type checking, null handling for missing data

### Spot ID Extraction

```typescript
// Regex pattern to find spotId dynamically
const spotIdMatch = htmlContext?.match(/"spotId"\s*:\s*"([a-zA-Z0-9_]+)"/);
```

### Price Parsing

```typescript
// Returns null (not 0) if parsing fails
const cleanPrice = priceText.trim().replace(/,/g, '');
const price = cleanPrice && !isNaN(Number(cleanPrice)) ? parseFloat(cleanPrice) : null;
```

### Retry Logic

Exponential backoff with User-Agent rotation:
- Attempt 1: 2s delay
- Attempt 2: 4s delay
- Attempt 3: 8s delay

### Error Handling

Distinguishes error types:
- **404**: Invalid symbol or Spot ID
- **403**: WAF blocking (need proxy/Puppeteer)
- **Network errors**: Retry with backoff

### Content Sanitization

Removes HTML tags and handles entities:
```typescript
rawHtml
  .replace(/<[^>]*>?/gm, '')  // Remove tags
  .replace(/&nbsp;/g, ' ')     // Handle entities
  .replace(/\s+/g, ' ')        // Collapse spaces
  .trim();
```

## Comparison with Python Crawler

| Feature | Python (crawler.py) | TypeScript (ts-crawler) |
|---------|---------------------|------------------------|
| Language | Python | TypeScript |
| HTML Parsing | Playwright DOM | Cheerio + JSON-LD |
| HTTP Client | Playwright | Axios |
| Spot ID | iframe detection | Regex extraction |
| Price Parsing | N/A | null vs number |
| Type Safety | None | Full TypeScript |
| Retry Logic | Basic | Exponential backoff |
| User-Agent | Static | Rotation |

## Troubleshooting

### "Database connection failed"

Check Docker container:
```bash
docker-compose ps
docker-compose logs db
```

### "Failed to parse price"

The selector might have changed. Check HTML structure:
```bash
curl https://finance.yahoo.com/quote/AAPL/community/ > test.html
```

### "Access Denied (403)"

WAF is blocking requests. Try:
1. Increase delays between requests
2. Use proxy/VPN
3. Use Puppeteer with real browser

### "Could not find Spot ID"

Spot ID is loaded dynamically. The fallback ID (`sp_R2cfqc5q`) will be used. If API returns 404, the fallback ID is outdated.

## License

MIT
