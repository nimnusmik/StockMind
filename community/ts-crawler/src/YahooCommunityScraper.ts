/**
 * Yahoo Finance Community Scraper
 * Production-grade implementation following crawl-plan.md specifications
 */

import axios, { AxiosInstance, AxiosError } from 'axios';
import * as cheerio from 'cheerio';
import * as fs from 'fs';
import * as path from 'path';
import { createObjectCsvWriter } from 'csv-writer';
import {
  StockHeader,
  CommunityComment,
  SpotImApiResponse,
  SpotImComment,
  DbComment,
} from './types';
import {
  getRandomUserAgent,
  getYahooFinanceUrl,
  getSpotImApiUrl,
  SCRAPER_CONFIG,
  OUTPUT_DIR,
} from './config';
import { batchInsertComments, generateCommentHash } from './database';

export class YahooCommunityScraper {
  private httpClient: AxiosInstance;
  private readonly DEFAULT_SPOT_ID = SCRAPER_CONFIG.defaultSpotId;

  constructor() {
    // Initialize HTTP client with retry interceptor
    this.httpClient = axios.create({
      timeout: SCRAPER_CONFIG.timeout,
      headers: {
        'User-Agent': getRandomUserAgent(),
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': 'https://finance.yahoo.com/',
      },
    });

    // Add retry interceptor
    this.setupRetryInterceptor();
  }

  /**
   * Setup Axios retry interceptor with exponential backoff
   */
  private setupRetryInterceptor(): void {
    this.httpClient.interceptors.response.use(
      (response) => response,
      async (error: AxiosError) => {
        const config: any = error.config;

        if (!config || !config.retry) {
          config.retry = 0;
        }

        if (config.retry >= SCRAPER_CONFIG.retryAttempts) {
          return Promise.reject(error);
        }

        config.retry += 1;
        const delay = SCRAPER_CONFIG.retryDelay * Math.pow(2, config.retry - 1);

        console.log(`🔄 Retry attempt ${config.retry}/${SCRAPER_CONFIG.retryAttempts} after ${delay}ms`);

        // Rotate User-Agent on retry
        config.headers['User-Agent'] = getRandomUserAgent();

        await this.sleep(delay);
        return this.httpClient.request(config);
      }
    );
  }

  /**
   * Sleep utility for delays
   */
  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  /**
   * Fetch HTML from Yahoo Finance community page
   */
  public async fetchHtml(symbol: string): Promise<string> {
    const url = getYahooFinanceUrl(symbol);

    try {
      console.log(`🌐 Fetching HTML for ${symbol}: ${url}`);
      const { data } = await this.httpClient.get(url);
      console.log(`✅ HTML fetched successfully (${data.length} bytes)`);
      return data;
    } catch (error) {
      if (axios.isAxiosError(error)) {
        console.error(`❌ Failed to fetch HTML: ${error.message}`);
        if (error.response?.status === 404) {
          console.error(`❌ Page not found (404). Symbol might be invalid.`);
        } else if (error.response?.status === 403) {
          console.error(`❌ Access denied (403). WAF might be blocking requests.`);
        }
      }
      throw error;
    }
  }

  /**
   * Parse static HTML to extract stock header information
   * Priority 1: JSON-LD / Script variables (window.YAHOO.context)
   * Priority 2: DOM selectors with data-testid
   */
  public parseStaticHtml(html: string): StockHeader {
    const $ = cheerio.load(html);

    // Priority 1: Extract from Script context (window.YAHOO.context)
    const contextMatch = html.match(/window\.YAHOO\.context\s*=\s*({.*?});/s);
    let contextData: any = {};

    if (contextMatch && contextMatch[1]) {
      try {
        contextData = JSON.parse(contextMatch[1]);
        console.log('✅ Found window.YAHOO.context data');
      } catch (e) {
        console.warn('⚠️ Failed to parse JSON context from script');
      }
    }

    // Priority 2: DOM selectors (fallback)
    const symbol =
      $('div[data-testid="quote-hdr"] .symbol').text().trim() ||
      $('h1').text().split('(')[1]?.replace(')', '').trim() ||
      contextData.dispatcher?.stores?.QuoteSummaryStore?.symbol ||
      'UNKNOWN';

    const name =
      $('section[data-testid="quote-title"] h1').text().split('(')[0].trim() ||
      contextData.dispatcher?.stores?.QuoteSummaryStore?.price?.shortName ||
      'Unknown Company';

    // Price parsing with strict validation
    const priceText = $('span[data-testid="qsp-price"]').first().text().trim();
    const price = this.parsePrice(priceText);

    // Exchange and currency information
    const exchangeText = $('.exchange').first().text();
    const exchangeParts = exchangeText.split('-');
    const exchange = exchangeParts[0]?.trim() || 'Unknown';
    const currency = exchangeText.includes('USD')
      ? 'USD'
      : exchangeText.split('•')[1]?.trim() || 'USD';

    const marketState = contextData.dispatcher?.stores?.QuoteSummaryStore?.marketState;

    if (price === null) {
      console.error(
        `🚨 CRITICAL: Failed to parse price for ${symbol}. Raw text: '${priceText}'`
      );
    }

    const stockHeader: StockHeader = {
      symbol,
      name,
      price,
      currency,
      exchange,
      marketState,
    };

    console.log('📊 Stock Header:', stockHeader);
    return stockHeader;
  }

  /**
   * Parse price with strict validation
   * Returns null if parsing fails (never returns 0 for failed parsing)
   */
  private parsePrice(priceText: string): number | null {
    if (!priceText) {
      return null;
    }

    const cleanPrice = priceText.trim().replace(/,/g, '');

    if (!cleanPrice || isNaN(Number(cleanPrice))) {
      console.error(`🚨 CRITICAL: Failed to parse price '${priceText}'`);
      return null;
    }

    return parseFloat(cleanPrice);
  }

  /**
   * Extract Spot ID dynamically from HTML context
   * Uses regex to find spotId pattern
   */
  private extractSpotId(htmlContext?: string): string {
    if (!htmlContext) {
      console.warn(`⚠️ No HTML context provided. Using fallback: ${this.DEFAULT_SPOT_ID}`);
      return this.DEFAULT_SPOT_ID;
    }

    // Regex pattern to find spotId (e.g., "spotId":"sp_xxxxx" or "spotId": "sp_xxxxx")
    const spotIdMatch = htmlContext.match(/"spotId"\s*:\s*"([a-zA-Z0-9_]+)"/);

    if (spotIdMatch && spotIdMatch[1]) {
      console.log(`✅ Found dynamic Spot ID: ${spotIdMatch[1]}`);
      return spotIdMatch[1];
    } else {
      console.warn(`⚠️ Could not find Spot ID in HTML. Using fallback: ${this.DEFAULT_SPOT_ID}`);
      return this.DEFAULT_SPOT_ID;
    }
  }

  /**
   * Fetch comments from Spot.IM API
   * Includes enhanced error handling for 404/403 responses
   */
  public async fetchComments(
    symbol: string,
    htmlContext?: string
  ): Promise<CommunityComment[]> {
    const spotId = this.extractSpotId(htmlContext);
    const apiUrl = getSpotImApiUrl(spotId, symbol);

    const payload = {
      count: SCRAPER_CONFIG.commentsPerRequest,
      sort_by: 'best',
    };

    try {
      console.log(`📡 Fetching comments from Spot.IM API...`);
      const { data } = await this.httpClient.post<SpotImApiResponse>(apiUrl, payload);

      if (!data || !data.comments) {
        console.warn('⚠️ No comments found in API response');
        return [];
      }

      console.log(`✅ Retrieved ${data.comments.length} comments`);

      return data.comments.map((c: SpotImComment) => ({
        id: c.id,
        user: c.user_display_name || 'Anonymous',
        content: this.sanitizeContent(c.text),
        timestamp: c.written_at,
        likes: c.likes_count || 0,
        replies_count: c.replies_count || 0,
      }));
    } catch (error) {
      if (axios.isAxiosError(error)) {
        if (error.response?.status === 404) {
          console.error(
            `❌ Comments API Not Found (404). SpotID or Symbol might be wrong. SpotID: ${spotId}`
          );
        } else if (error.response?.status === 403) {
          console.error(`❌ Access Denied (403). WAF blocked request. Need Proxy/Puppeteer.`);
        } else {
          console.error(`❌ API Error: ${error.message}`);
        }
      } else {
        console.error(`❌ Unknown error:`, error);
      }
      return [];
    }
  }

  /**
   * Sanitize HTML content
   * Removes HTML tags and cleans up whitespace
   */
  private sanitizeContent(rawHtml: string): string {
    if (!rawHtml) {
      return '';
    }

    return rawHtml
      .replace(/<[^>]*>?/gm, '') // Remove HTML tags
      .replace(/&nbsp;/g, ' ') // Replace &nbsp; with space
      .replace(/&amp;/g, '&') // Replace &amp; with &
      .replace(/&lt;/g, '<') // Replace &lt; with <
      .replace(/&gt;/g, '>') // Replace &gt; with >
      .replace(/&quot;/g, '"') // Replace &quot; with "
      .replace(/&#39;/g, "'") // Replace &#39; with '
      .replace(/\s+/g, ' ') // Collapse multiple spaces
      .trim();
  }

  /**
   * Save comments to CSV file
   */
  public async saveToCsv(
    comments: CommunityComment[],
    stockSymbol: string
  ): Promise<void> {
    if (comments.length === 0) {
      console.log('⚠️ No comments to save to CSV');
      return;
    }

    // Ensure output directory exists
    if (!fs.existsSync(OUTPUT_DIR)) {
      fs.mkdirSync(OUTPUT_DIR, { recursive: true });
    }

    const timestamp = new Date().toISOString().replace(/[:.]/g, '-').split('T')[0];
    const filename = `${stockSymbol}_comments_${timestamp}.csv`;
    const filepath = path.join(OUTPUT_DIR, filename);

    const csvWriter = createObjectCsvWriter({
      path: filepath,
      header: [
        { id: 'time', title: 'time' },
        { id: 'text', title: 'text' },
        { id: 'stock_symbol', title: 'stock_symbol' },
        { id: 'likes', title: 'likes' },
        { id: 'replies_count', title: 'replies_count' },
      ],
    });

    const records = comments.map((c) => ({
      time: new Date(c.timestamp * 1000).toLocaleString('en-US', {
        day: '2-digit',
        month: 'short',
        year: 'numeric',
        hour: '2-digit',
        minute: '2-digit',
        hour12: true,
      }),
      text: c.content,
      stock_symbol: stockSymbol,
      likes: c.likes,
      replies_count: c.replies_count,
    }));

    await csvWriter.writeRecords(records);
    console.log(`✅ Saved ${comments.length} comments to CSV: ${filepath}`);
  }

  /**
   * Save comments to PostgreSQL database
   */
  public async saveToDb(
    comments: CommunityComment[],
    stockSymbol: string
  ): Promise<number> {
    if (comments.length === 0) {
      console.log('⚠️ No comments to save to database');
      return 0;
    }

    const dbComments: DbComment[] = comments.map((c) => ({
      stock_symbol: stockSymbol,
      comment_time: new Date(c.timestamp * 1000).toISOString(),
      comment_text: c.content,
      comment_hash: generateCommentHash(c.content),
    }));

    return await batchInsertComments(dbComments);
  }

  /**
   * Main crawl method: fetch HTML, parse data, fetch comments, save to CSV and DB
   */
  public async crawlStock(symbol: string, saveToDb: boolean = true): Promise<void> {
    console.log(`\n${'='.repeat(60)}`);
    console.log(`🚀 Starting crawl for ${symbol}`);
    console.log(`${'='.repeat(60)}\n`);

    try {
      // Step 1: Fetch HTML
      const html = await this.fetchHtml(symbol);

      // Step 2: Parse stock header
      const stockHeader = this.parseStaticHtml(html);

      if (!stockHeader.symbol || stockHeader.symbol === 'UNKNOWN') {
        console.error('⛔ Failed to extract symbol. Aborting.');
        return;
      }

      // Step 3: Fetch comments
      const comments = await this.fetchComments(stockHeader.symbol, html);

      if (comments.length === 0) {
        console.log(`⚠️ No comments found for ${symbol}`);
        return;
      }

      // Step 4: Save to CSV
      await this.saveToCsv(comments, stockHeader.symbol);

      // Step 5: Save to DB (if enabled)
      if (saveToDb) {
        await this.saveToDb(comments, stockHeader.symbol);
      }

      console.log(`\n✅ Successfully completed crawl for ${symbol}`);
      console.log(`📊 Total comments: ${comments.length}`);
    } catch (error) {
      console.error(`❌ Failed to crawl ${symbol}:`, error);
      throw error;
    }
  }
}
