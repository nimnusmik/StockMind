# CSV → DB 마이그레이션
import pandas as pd
import hashlib
import os
from src.storage import get_db_connection

def hash_comment(timestamp, content, symbol):
    return hashlib.sha256(f"{timestamp}{content}{symbol}".encode()).hexdigest()

def migrate_csv_to_db(csv_file, symbol):
    conn = get_db_connection()
    c = conn.cursor()
    df = pd.read_csv(csv_file)
    
    for _, row in df.iterrows():
        comment_id = hash_comment(row['time'], row['text'], row['stock_symbol'])
        c.execute(
            """
            INSERT INTO comments (id, symbol, timestamp, user, content)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
            """,
            (comment_id, row['stock_symbol'], row['time'], 'unknown', row['text'])
        )
    
    conn.commit()
    conn.close()
    print(f"Migrated {len(df)} comments for {symbol} from {csv_file}")

if __name__ == "__main__":
    symbols = ['AAPL', 'GOOG', 'META', 'TSLA', 'MSFT', 'AMZN', 'NVDA', 'NFLX']
    for symbol in symbols:
        csv_file = f"data/{symbol}_comments_202507.csv"
        if os.path.exists(csv_file):
            migrate_csv_to_db(csv_file, symbol)
        else:
            print(f"CSV file not found for {symbol}: {csv_file}")