-- airflow 데이터베이스 생성
CREATE DATABASE airflow;
GRANT ALL PRIVILEGES ON DATABASE airflow TO "user";

-- stockmind 데이터베이스 생성
CREATE DATABASE stockmind;
GRANT ALL PRIVILEGES ON DATABASE stockmind TO "user";

-- stockmind 데이터베이스에서 comments 테이블 생성
\c stockmind
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