/**
 * Configuration for Yahoo Finance Community Crawler
 * Loads settings from environment variables and defines constants
 */

import * as dotenv from 'dotenv';
import * as path from 'path';

dotenv.config();

/**
 * Target stock symbols (8 tech stocks)
 */
export const STOCKS = ['AAPL', 'GOOG', 'META', 'TSLA', 'MSFT', 'AMZN', 'NVDA', 'NFLX'];

/**
 * User-Agent pool for rotation
 */
export const USER_AGENTS = [
  // 최신 데스크톱 브라우저
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:112.0) Gecko/20100101 Firefox/112.0",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.3 Safari/605.1.15",
  "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36 Edg/123.0.0.0",

  // 모바일 기기 (Android)
  "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Mobile Safari/537.36",
  "Mozilla/5.0 (Linux; Android 10) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.5414.117 Mobile Safari/537.36",
  "Mozilla/5.0 (Linux; Android 12; SM-G998B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Mobile Safari/537.36",

  // 모바일 기기 (iOS)
  "Mozilla/5.0 (iPhone; CPU iPhone OS 16_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Mobile/15E148 Safari/604.1",
  "Mozilla/5.0 (iPad; CPU OS 16_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) CriOS/112.0.0.0 Mobile/15E148 Safari/604.1",
  "Mozilla/5.0 (iPhone; CPU iPhone OS 15_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.6 Mobile/15E148 Safari/604.1",
];

/**
 * Database configuration from environment variables
 */
export const DB_CONFIG = {
  host: process.env.DB_HOST || 'localhost',
  port: parseInt(process.env.DB_PORT || '5432', 10),
  database: process.env.DB_NAME || 'stockmind',
  user: process.env.DB_USER || 'user',
  password: process.env.DB_PASSWORD || 'password',
};

/**
 * Output directories
 */
export const OUTPUT_DIR = path.resolve(__dirname, '../../data');
export const LOGS_DIR = path.resolve(__dirname, '../../logs');

/**
 * Scraper settings
 */
export const SCRAPER_CONFIG = {
  retryAttempts: 3,
  retryDelay: 2000, // milliseconds
  timeout: 30000, // milliseconds
  defaultSpotId: 'sp_R2cfqc5q',
  commentsPerRequest: 20,
};

/**
 * Get a random User-Agent from the pool
 */
export function getRandomUserAgent(): string {
  return USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)];
}

/**
 * Yahoo Finance URL builder
 */
export function getYahooFinanceUrl(symbol: string): string {
  return `https://finance.yahoo.com/quote/${symbol}/community/`;
}

/**
 * Spot.IM API URL builder
 */
export function getSpotImApiUrl(spotId: string, symbol: string): string {
  return `https://api-2.spot.im/v1/spot/${spotId}/article/${symbol}/conversation`;
}
