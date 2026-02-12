#!/usr/bin/env python3
"""
간단한 CSV-to-DB 마이그레이션 스크립트 (Docker 내부용)
"""
import pandas as pd
import psycopg2
import hashlib
from datetime import datetime
import glob

# DB 연결 (Docker 내부에서는 localhost)
conn = psycopg2.connect(
    dbname="stockmind",
    user="user",
    password="password",
    host="db",
    port="5432"
)
cur = conn.cursor()

print("✅ DB 연결 성공")

# CSV 파일 처리
csv_files = glob.glob("/tmp/csvdata/*.csv")
total_inserted = 0

for csv_file in sorted(csv_files):
    print(f"\n📄 처리 중: {csv_file}")
    df = pd.read_csv(csv_file)
    print(f"📊 {len(df)} 행 읽음")

    for _, row in df.iterrows():
        try:
            # 댓글 해시
            comment_hash = hashlib.md5(row['text'].encode()).hexdigest()

            # 날짜 파싱 (여러 형식 시도)
            time_str = str(row['time'])
            try:
                comment_time = datetime.strptime(time_str, "%d %b, %Y %I:%M %p")
            except:
                try:
                    comment_time = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
                except:
                    comment_time = datetime.now()

            # INSERT (중복 무시)
            cur.execute("""
                INSERT INTO comments (stock_symbol, comment_time, comment_text, comment_hash)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (stock_symbol, comment_time, comment_hash) DO NOTHING
            """, (row['stock_symbol'], comment_time, row['text'], comment_hash))

            total_inserted += cur.rowcount

        except Exception as e:
            print(f"⚠️ 에러: {e}")
            continue

    conn.commit()
    print(f"✅ 완료: {csv_file.split('/')[-1]}")

print(f"\n🎉 총 {total_inserted}개 댓글 저장 완료!")

cur.close()
conn.close()
