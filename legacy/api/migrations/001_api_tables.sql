-- API 테이블 생성 마이그레이션
-- API 키 관리 및 신호 이력 테이블

-- API 키 테이블
CREATE TABLE IF NOT EXISTS api_keys (
    id SERIAL PRIMARY KEY,
    key_hash TEXT NOT NULL UNIQUE,
    user_id VARCHAR(100),
    created_at TIMESTAMP DEFAULT NOW(),
    expires_at TIMESTAMP,
    rate_limit INT DEFAULT 100,
    is_active INT DEFAULT 1,
    last_used_at TIMESTAMP,
    usage_count INT DEFAULT 0
);

-- API 키 인덱스
CREATE INDEX IF NOT EXISTS idx_api_keys_hash ON api_keys(key_hash);
CREATE INDEX IF NOT EXISTS idx_api_keys_user ON api_keys(user_id);
CREATE INDEX IF NOT EXISTS idx_api_keys_active ON api_keys(is_active);

-- 신호 이력 테이블 (백테스팅용)
CREATE TABLE IF NOT EXISTS signal_history (
    id SERIAL PRIMARY KEY,
    stock_symbol VARCHAR(10) NOT NULL,
    signal_date TIMESTAMP NOT NULL,
    signal_type VARCHAR(10) NOT NULL,  -- BUY, HOLD, SELL
    signal_strength VARCHAR(10),  -- STRONG, MODERATE, WEAK
    confidence FLOAT,
    predicted_price FLOAT,
    actual_price FLOAT,
    predicted_change_pct FLOAT,
    sentiment_score FLOAT,
    news_sentiment FLOAT,
    community_sentiment FLOAT,
    supporting_factors TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

-- 신호 이력 인덱스
CREATE INDEX IF NOT EXISTS idx_signal_history_symbol ON signal_history(stock_symbol);
CREATE INDEX IF NOT EXISTS idx_signal_history_date ON signal_history(signal_date DESC);
CREATE INDEX IF NOT EXISTS idx_signal_history_symbol_date ON signal_history(stock_symbol, signal_date DESC);
CREATE INDEX IF NOT EXISTS idx_signal_history_type ON signal_history(signal_type);

-- 코멘트 (확인용, 이미 존재하는 경우 무시)
COMMENT ON TABLE api_keys IS 'API 인증 키 관리 테이블';
COMMENT ON TABLE signal_history IS '매매 신호 이력 테이블 (백테스팅 및 성능 추적용)';

-- 초기 데이터: 테스트용 API 키 (개발 환경용)
-- 실제 프로덕션에서는 제거하거나 비활성화해야 함
INSERT INTO api_keys (key_hash, user_id, rate_limit, is_active)
VALUES (
    '$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/LewY5ztNB5K.yKpGu',  -- 해시된 "dev-api-key"
    'developer',
    1000,
    1
) ON CONFLICT (key_hash) DO NOTHING;

-- 마이그레이션 완료 로그
DO $$
BEGIN
    RAISE NOTICE '✅ API 테이블 마이그레이션 완료';
END $$;
