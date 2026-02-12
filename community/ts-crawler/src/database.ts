/**
 * Database module for PostgreSQL operations
 * Handles connection and batch insert of comments
 */

import { Pool, PoolClient } from 'pg';
import { DB_CONFIG } from './config';
import { DbComment } from './types';
import * as crypto from 'crypto';

/**
 * PostgreSQL connection pool
 */
let pool: Pool | null = null;

/**
 * Initialize database connection pool
 */
export function initDatabase(): Pool {
  if (!pool) {
    pool = new Pool({
      host: DB_CONFIG.host,
      port: DB_CONFIG.port,
      database: DB_CONFIG.database,
      user: DB_CONFIG.user,
      password: DB_CONFIG.password,
      max: 10,
      idleTimeoutMillis: 30000,
      connectionTimeoutMillis: 10000,
    });

    pool.on('error', (err) => {
      console.error('❌ PostgreSQL pool error:', err);
    });

    console.log(`✅ Database pool initialized: ${DB_CONFIG.host}:${DB_CONFIG.port}/${DB_CONFIG.database}`);
  }

  return pool;
}

/**
 * Close database connection pool
 */
export async function closeDatabase(): Promise<void> {
  if (pool) {
    await pool.end();
    pool = null;
    console.log('✅ Database pool closed');
  }
}

/**
 * Generate MD5 hash for comment deduplication
 */
export function generateCommentHash(text: string): string {
  return crypto.createHash('md5').update(text).digest('hex');
}

/**
 * Batch insert comments into PostgreSQL
 * Uses ON CONFLICT to skip duplicates based on unique constraint
 */
export async function batchInsertComments(comments: DbComment[]): Promise<number> {
  if (comments.length === 0) {
    console.log('⚠️ No comments to insert');
    return 0;
  }

  const db = initDatabase();
  let client: PoolClient | null = null;

  try {
    client = await db.connect();

    // Build parameterized query for batch insert
    const values: any[] = [];
    const placeholders: string[] = [];

    comments.forEach((comment, idx) => {
      const offset = idx * 4;
      placeholders.push(`($${offset + 1}, $${offset + 2}, $${offset + 3}, $${offset + 4})`);
      values.push(
        comment.stock_symbol,
        comment.comment_time,
        comment.comment_text,
        comment.comment_hash
      );
    });

    const query = `
      INSERT INTO comments (stock_symbol, comment_time, comment_text, comment_hash)
      VALUES ${placeholders.join(', ')}
      ON CONFLICT (stock_symbol, comment_time, comment_hash) DO NOTHING
      RETURNING id;
    `;

    const result = await client.query(query, values);
    const insertedCount = result.rowCount || 0;

    console.log(`📊 Inserted ${insertedCount} new comments (${comments.length - insertedCount} duplicates skipped)`);

    return insertedCount;

  } catch (error) {
    console.error('❌ Database insert error:', error);
    throw error;
  } finally {
    if (client) {
      client.release();
    }
  }
}

/**
 * Test database connection
 */
export async function testConnection(): Promise<boolean> {
  const db = initDatabase();
  let client: PoolClient | null = null;

  try {
    client = await db.connect();
    const result = await client.query('SELECT NOW()');
    console.log('✅ Database connection test successful:', result.rows[0].now);
    return true;
  } catch (error) {
    console.error('❌ Database connection test failed:', error);
    return false;
  } finally {
    if (client) {
      client.release();
    }
  }
}

/**
 * Get comment count by stock symbol
 */
export async function getCommentCount(stockSymbol?: string): Promise<any> {
  const db = initDatabase();
  let client: PoolClient | null = null;

  try {
    client = await db.connect();

    let query: string;
    let params: any[] = [];

    if (stockSymbol) {
      query = 'SELECT COUNT(*) as count FROM comments WHERE stock_symbol = $1';
      params = [stockSymbol];
    } else {
      query = 'SELECT stock_symbol, COUNT(*) as count FROM comments GROUP BY stock_symbol ORDER BY stock_symbol';
    }

    const result = await client.query(query, params);
    return result.rows;

  } catch (error) {
    console.error('❌ Failed to get comment count:', error);
    throw error;
  } finally {
    if (client) {
      client.release();
    }
  }
}
