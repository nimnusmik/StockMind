CREATE TABLE IF NOT EXISTS comments (
    id TEXT PRIMARY KEY,
    symbol TEXT,
    timestamp TIMESTAMP,
    user TEXT,
    content TEXT
);