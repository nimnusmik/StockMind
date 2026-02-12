/**
 * Main entry point for Yahoo Finance Community Crawler
 * Orchestrates crawling all stocks from configuration
 */

import { YahooCommunityScraper } from './YahooCommunityScraper';
import { STOCKS } from './config';
import { testConnection, closeDatabase, getCommentCount } from './database';

/**
 * Crawler Orchestrator
 * Manages sequential crawling of all configured stocks
 */
class CrawlerOrchestrator {
  private scraper: YahooCommunityScraper;
  private successCount: number = 0;
  private failureCount: number = 0;
  private startTime: number = 0;

  constructor() {
    this.scraper = new YahooCommunityScraper();
  }

  /**
   * Crawl all stocks from configuration
   */
  public async crawlAllStocks(saveToDb: boolean = true): Promise<void> {
    this.startTime = Date.now();

    console.log('\n' + '='.repeat(80));
    console.log('🚀 YAHOO FINANCE COMMUNITY CRAWLER - Starting');
    console.log('='.repeat(80));
    console.log(`📋 Target stocks: ${STOCKS.join(', ')}`);
    console.log(`📊 Total stocks to crawl: ${STOCKS.length}`);
    console.log(`💾 Save to database: ${saveToDb ? 'YES' : 'NO'}`);
    console.log('='.repeat(80) + '\n');

    // Test database connection if saving to DB
    if (saveToDb) {
      console.log('🔍 Testing database connection...');
      const connected = await testConnection();
      if (!connected) {
        console.error('❌ Database connection failed. Aborting.');
        return;
      }
      console.log('');
    }

    // Crawl each stock sequentially
    for (let i = 0; i < STOCKS.length; i++) {
      const symbol = STOCKS[i];
      const progress = `[${i + 1}/${STOCKS.length}]`;

      console.log(`\n${progress} Processing ${symbol}...`);

      try {
        await this.scraper.crawlStock(symbol, saveToDb);
        this.successCount++;

        // Add delay between requests to avoid rate limiting
        if (i < STOCKS.length - 1) {
          const delay = 2000 + Math.random() * 2000; // 2-4 seconds
          console.log(`⏳ Waiting ${Math.round(delay / 1000)}s before next request...`);
          await this.sleep(delay);
        }
      } catch (error) {
        console.error(`❌ Failed to crawl ${symbol}:`, error);
        this.failureCount++;

        // Continue with next stock even if one fails
        if (i < STOCKS.length - 1) {
          console.log('⏭️ Continuing with next stock...');
          await this.sleep(3000); // Wait a bit longer after failure
        }
      }
    }

    // Print summary
    await this.printSummary(saveToDb);

    // Close database connection
    if (saveToDb) {
      await closeDatabase();
    }
  }

  /**
   * Crawl a single stock (for CLI argument)
   */
  public async crawlSingleStock(symbol: string, saveToDb: boolean = true): Promise<void> {
    this.startTime = Date.now();

    console.log('\n' + '='.repeat(80));
    console.log('🚀 YAHOO FINANCE COMMUNITY CRAWLER - Single Stock Mode');
    console.log('='.repeat(80));
    console.log(`📋 Target stock: ${symbol}`);
    console.log(`💾 Save to database: ${saveToDb ? 'YES' : 'NO'}`);
    console.log('='.repeat(80) + '\n');

    // Test database connection if saving to DB
    if (saveToDb) {
      console.log('🔍 Testing database connection...');
      const connected = await testConnection();
      if (!connected) {
        console.error('❌ Database connection failed. Aborting.');
        return;
      }
      console.log('');
    }

    try {
      await this.scraper.crawlStock(symbol.toUpperCase(), saveToDb);
      this.successCount = 1;
    } catch (error) {
      console.error(`❌ Failed to crawl ${symbol}:`, error);
      this.failureCount = 1;
    }

    // Print summary
    await this.printSummary(saveToDb);

    // Close database connection
    if (saveToDb) {
      await closeDatabase();
    }
  }

  /**
   * Print crawl summary
   */
  private async printSummary(saveToDb: boolean): Promise<void> {
    const endTime = Date.now();
    const duration = ((endTime - this.startTime) / 1000).toFixed(2);

    console.log('\n' + '='.repeat(80));
    console.log('📊 CRAWL SUMMARY');
    console.log('='.repeat(80));
    console.log(`✅ Successful: ${this.successCount}`);
    console.log(`❌ Failed: ${this.failureCount}`);
    console.log(`⏱️  Total duration: ${duration}s`);

    if (saveToDb && this.successCount > 0) {
      console.log('\n📈 Database Statistics:');
      try {
        const counts = await getCommentCount();
        console.table(counts);
      } catch (error) {
        console.error('❌ Failed to retrieve database statistics');
      }
    }

    console.log('='.repeat(80) + '\n');

    if (this.successCount > 0) {
      console.log('🎉 Crawl completed successfully!');
    } else {
      console.log('⚠️  Crawl completed with errors.');
    }
  }

  /**
   * Sleep utility
   */
  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}

/**
 * Parse command line arguments
 */
function parseArgs(): { ticker?: string; noDB: boolean } {
  const args = process.argv.slice(2);
  let ticker: string | undefined;
  let noDB = false;

  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--ticker' && i + 1 < args.length) {
      ticker = args[i + 1];
      i++;
    } else if (args[i] === '--no-db') {
      noDB = true;
    } else if (args[i] === '--help' || args[i] === '-h') {
      printHelp();
      process.exit(0);
    }
  }

  return { ticker, noDB };
}

/**
 * Print help message
 */
function printHelp(): void {
  console.log(`
Yahoo Finance Community Crawler

Usage:
  npm start                    # Crawl all stocks (AAPL, GOOG, META, etc.)
  npm start -- --ticker AAPL   # Crawl single stock
  npm start -- --no-db         # Skip database insertion (CSV only)
  npm start -- --help          # Show this help message

Options:
  --ticker <SYMBOL>   Crawl a specific stock symbol
  --no-db             Skip database insertion (save to CSV only)
  --help, -h          Show this help message

Examples:
  npm start                           # Crawl all 8 stocks
  npm start -- --ticker TSLA          # Crawl only TSLA
  npm start -- --ticker AAPL --no-db  # Crawl AAPL, CSV only
  `);
}

/**
 * Main execution
 */
async function main() {
  const { ticker, noDB } = parseArgs();
  const orchestrator = new CrawlerOrchestrator();
  const saveToDb = !noDB;

  try {
    if (ticker) {
      // Single stock mode
      await orchestrator.crawlSingleStock(ticker, saveToDb);
    } else {
      // All stocks mode
      await orchestrator.crawlAllStocks(saveToDb);
    }
  } catch (error) {
    console.error('❌ Fatal error:', error);
    process.exit(1);
  }
}

// Run if this is the main module
if (require.main === module) {
  main().catch((error) => {
    console.error('❌ Unhandled error:', error);
    process.exit(1);
  });
}

export { CrawlerOrchestrator };
