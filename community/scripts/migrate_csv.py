import os
import pandas as pd
import psycopg2
from psycopg2 import sql

output_dir = "/app/data"
db_params = {
    "dbname": "stockmind",
    "user": "user",
    "password": "password",
    "host": "localhost",  # 로컬에서 테스트 시
    "port": "5432"
}

def connect_db():
    try:
        conn = psycopg2.connect(**db_params)
        print("✅ PostgreSQL 연결 성공")
        return conn
    except Exception as e:
        print(f"❌ PostgreSQL 연결 실패: {e}")
        return None

def load_csv_to_db(csv_file):
    conn = connect_db()
    if not conn:
        return
    
    try:
        cur = conn.cursor()
        df = pd.read_csv(csv_file)
        for _, row in df.iterrows():
            insert_query = sql.SQL("""
                INSERT INTO comments (stock_symbol, time, text)
                VALUES (%s, %s, %s)
                ON CONFLICT DO NOTHING
            """)
            cur.execute(insert_query, (row['stock_symbol'], row['time'], row['text']))
        conn.commit()
        print(f"✅ {csv_file} 데이터를 comments 테이블에 삽입 완료")
    except Exception as e:
        print(f"❌ 데이터 삽입 실패: {e}")
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    for csv_file in os.listdir(output_dir):
        if csv_file.endswith(".csv"):
            csv_path = os.path.join(output_dir, csv_file)
            print(f"📂 처리 중: {csv_path}")
            load_csv_to_db(csv_path)