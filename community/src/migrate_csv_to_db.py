# community/src/migrate_csv_to_db.py
import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
import hashlib
from datetime import datetime
from logger import setup_logger  # 기존 logger.py 사용

def migrate_csv_to_db(csv_dir, db_config, logger):
    """
    CSV 파일들을 Postgres DB로 마이그레이션
    :param csv_dir: CSV 파일이 있는 디렉토리 (예: /app/data)
    :param db_config: Postgres 연결 정보
    :param logger: 로깅 객체
    """
    logger.info("🚀 CSV to DB 마이그레이션 시작...")
    
    # DB 연결
    try:
        conn = psycopg2.connect(**db_config)
        cur = conn.cursor()
        logger.info("✅ DB 연결 성공")
    except Exception as e:
        logger.error(f"❌ DB 연결 실패: {e}")
        return
    
    # CSV 파일 목록
    csv_files = [f for f in os.listdir(csv_dir) if f.endswith('202508.csv')]
    logger.info(f"📂 발견된 CSV 파일: {csv_files}")
    
    for csv_file in csv_files:
        try:
            logger.info(f"\n📄 처리 중: {csv_file}")
            filepath = os.path.join(csv_dir, csv_file)
            
            # CSV 읽기
            df = pd.read_csv(filepath, encoding='utf-8')
            logger.info(f"📊 {csv_file}에서 {len(df)} 행 읽음")
            
            # 데이터 준비
            prepared_comments = []
            for _, row in df.iterrows():
                try:
                    # time 파싱 (형식: "20 Aug, 2025 12:49 PM")
                    comment_time = datetime.strptime(row['time'], '%d %b, %Y %I:%M %p')
                    comment_text = str(row['text']).strip()
                    stock_symbol = str(row['stock_symbol']).strip()
                    comment_hash = hashlib.md5(comment_text.encode()).hexdigest()
                    
                    prepared_comments.append((
                        stock_symbol,
                        comment_time,
                        comment_text,
                        comment_hash
                    ))

                except Exception as e:
                    logger.warning(f"⚠️ 행 처리 오류 (스킵): {e}")
                    continue
            
            # Batch UPSERT
            upsert_query = """
            INSERT INTO comments (stock_symbol, comment_time, comment_text, comment_hash)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (stock_symbol, comment_time, comment_hash)
            DO UPDATE SET comment_text = EXCLUDED.comment_text;
            """
            
            execute_batch(cur, upsert_query, prepared_comments, page_size=1000)
            conn.commit()
            logger.info(f"✅ {csv_file}: {len(prepared_comments)} 행 DB에 UPSERT 완료")
            
        except Exception as e:
            logger.error(f"❌ {csv_file} 처리 중 오류: {e}")
            conn.rollback()
            continue
    
    # 정리
    cur.close()
    conn.close()
    logger.info("🎉 모든 CSV 파일 마이그레이션 완료")

if __name__ == "__main__":
    # 로깅 설정
    logger = setup_logger("migrate_csv_to_db", "/app/logs")
    
    # DB 설정 (docker-compose와 일치)
    db_config = {
        "dbname": "stockmind",
        "user": "user",
        "password": "password",
        "host": "db",  # docker-compose 서비스 이름
        "port": "5432"
    }
    
    # CSV 디렉토리
    csv_dir = "/app/data"
    
    # 마이그레이션 실행
    migrate_csv_to_db(csv_dir, db_config, logger)