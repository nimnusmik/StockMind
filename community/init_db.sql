CREATE TABLE IF NOT EXISTS comments (
    id SERIAL PRIMARY KEY,
    stock_symbol VARCHAR(10) NOT NULL,
    comment_time TIMESTAMP WITH TIME ZONE NOT NULL,
    comment_text TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    comment_hash TEXT NOT NULL,
    UNIQUE (stock_symbol, comment_time, comment_hash)
);
CREATE INDEX IF NOT EXISTS idx_stock_time ON comments (stock_symbol, comment_time DESC);
