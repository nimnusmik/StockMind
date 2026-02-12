/**
 * TypeScript type definitions for Yahoo Finance Community Crawler
 * Based on crawl-plan.md specifications
 */

/**
 * Stock header information extracted from Yahoo Finance page
 */
export interface StockHeader {
  symbol: string;
  name: string;
  price: number | null;  // null if parsing failed, never 0
  currency: string;
  exchange: string;
  marketState?: string;
}

/**
 * Community comment from Spot.IM API
 */
export interface CommunityComment {
  id: string;
  user: string;
  content: string;  // Sanitized HTML content
  timestamp: number;
  likes: number;
  replies_count: number;
}

/**
 * Database comment record
 */
export interface DbComment {
  stock_symbol: string;
  comment_time: string;
  comment_text: string;
  comment_hash: string;
}

/**
 * Scraper configuration
 */
export interface ScraperConfig {
  stocks: string[];
  dbConfig: {
    host: string;
    port: number;
    database: string;
    user: string;
    password: string;
  };
  userAgent: string;
  retryAttempts: number;
  retryDelay: number;
}

/**
 * API response types
 */
export interface SpotImApiResponse {
  comments: SpotImComment[];
  conversation_id: string;
  count: number;
}

export interface SpotImComment {
  id: string;
  user_display_name: string;
  text: string;
  written_at: number;
  likes_count: number;
  replies_count: number;
}

/**
 * Yahoo context window object (extracted from HTML)
 */
export interface YahooContext {
  spotId?: string;
  quoteData?: {
    symbol?: string;
    shortName?: string;
    regularMarketPrice?: number;
    currency?: string;
    fullExchangeName?: string;
    marketState?: string;
  };
}
