# community/src/migrate_csv_to_db_local.py
# CSV 파일을 로컬 PostgreSQL DB로 마이그레이션
import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
import hashlib
from datetime import datetime

def migrate_csv_to_db(csv_dir, db_config):
    """
    CSV 파일들을 Postgres DB로 마이그레이션
    """
    print("🚀 CSV to DB 마이그레이션 시작...")

    # DB 연결
    try:
        conn = psycopg2.connect(**db_config)
        cur = conn.cursor()
        print(f"✅ DB 연결 성공: {db_config['host']}:{db_config['port']}/{db_config['dbname']}")
    except Exception as e:
        print(f"❌ DB 연결 실패: {e}")
        return

    # CSV 파일 목록 (temp 파일 제외, 최신 파일만)
    all_files = [f for f in os.listdir(csv_dir) if f.endswith('.csv')]

    # temp 파일이 아닌 최종 파일 우선, 그 다음 가장 큰 temp 파일
    stock_files = {}
    for f in all_files:
        stock = f.split('_')[0]
        if '_temp_' not in f:
            # 최종 파일 (temp 없음)
            if stock not in stock_files or '_temp_' in stock_files[stock]:
                stock_files[stock] = f
        else:
            # temp 파일 중 가장 큰 것
            if stock not in stock_files or '_temp_' in stock_files[stock]:
                # 숫자 추출하여 비교
                try:
                    current_num = int(f.split('_temp_')[1].split('.')[0])
                    if stock in stock_files and '_temp_' in stock_files[stock]:
                        existing_num = int(stock_files[stock].split('_temp_')[1].split('.')[0])
                        if current_num > existing_num:
                            stock_files[stock] = f
                    else:
                        stock_files[stock] = f
                except:
                    pass

    csv_files = list(stock_files.values())
    print(f"📂 마이그레이션할 CSV 파일 ({len(csv_files)}개):")
    for f in sorted(csv_files):
        filepath = os.path.join(csv_dir, f)
        size = os.path.getsize(filepath) / 1024  # KB
        print(f"   - {f} ({size:.1f} KB)")

    total_inserted = 0
    total_skipped = 0

    for csv_file in sorted(csv_files):
        try:
            print(f"\n📄 처리 중: {csv_file}")
            filepath = os.path.join(csv_dir, csv_file)

            # CSV 읽기
            df = pd.read_csv(filepath, encoding='utf-8')
            print(f"📊 {len(df)} 행 읽음")

            # 데이터 준비
            prepared_comments = []
            skipped = 0

            for idx, row in df.iterrows():
                try:
                    # time 파싱 (형식: "20 Aug, 2025 12:49 PM" 또는 "12 Feb, 2026 10:30 AM")
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
                    skipped += 1
                    if skipped <= 3:  # 처음 3개 에러만 출력
                        print(f"⚠️ 행 {idx} 처리 오류 (스킵): {e}")
                    continue

            if skipped > 3:
                print(f"⚠️ 총 {skipped}개 행 스킵됨")

            # Batch INSERT
            insert_query = """
            INSERT INTO comments (stock_symbol, comment_time, comment_text, comment_hash)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (stock_symbol, comment_time, comment_hash) DO NOTHING;
            """

            execute_batch(cur, insert_query, prepared_comments, page_size=1000)
            conn.commit()

            # 결과 확인
            cur.execute("""
                SELECT COUNT(*) FROM comments WHERE stock_symbol = %s
            """, (prepared_comments[0][0],))
            total_in_db = cur.fetchone()[0]

            inserted = len(prepared_comments) - skipped
            print(f"✅ {csv_file}: {inserted}개 행 처리 완료")
            print(f"   DB에 {total_in_db}개 댓글 저장됨 (중복 제외)")

            total_inserted += inserted
            total_skipped += skipped

        except Exception as e:
            print(f"❌ {csv_file} 처리 중 오류: {e}")
            import traceback
            traceback.print_exc()
            conn.rollback()
            continue

    # 최종 통계
    print(f"\n{'='*60}")
    print("📊 마이그레이션 완료")
    print(f"{'='*60}")

    cur.execute("SELECT stock_symbol, COUNT(*) as count FROM comments GROUP BY stock_symbol ORDER BY stock_symbol")
    results = cur.fetchall()

    print("\n종목별 댓글 수:")
    total_comments = 0
    for stock, count in results:
        print(f"   {stock}: {count:,}개")
        total_comments += count

    print(f"\n총 댓글 수: {total_comments:,}개")
    print(f"총 처리 행: {total_inserted:,}개")
    print(f"총 스킵 행: {total_skipped:,}개")

    # 정리
    cur.close()
    conn.close()
    print("\n🎉 모든 CSV 파일 마이그레이션 완료")

if __name__ == "__main__":
    # DB 설정 (로컬 PostgreSQL)
    db_config = {
        "dbname": "stockmind",
        "user": "user",
        "password": "password",
        "host": os.getenv('DB_HOST', 'localhost'),
        "port": "5432"
    }

    # CSV 디렉토리
    csv_dir = "/Users/sunminkim/Desktop/AIStages/StockMind/community/data"

    # 마이그레이션 실행
    migrate_csv_to_db(csv_dir, db_config)
