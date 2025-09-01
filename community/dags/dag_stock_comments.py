import sys
import os
# src 디렉토리를 Python 경로에 추가
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator
from crawler import MultiStockYahooFinanceCrawler
from config import stocks, logs_base_dir
import psycopg2
from psycopg2.extras import execute_values
from logger import setup_logger

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

db_config = {
    'dbname': 'stockmind',
    'user': 'user',
    'password': 'password',
    'host': 'db',
    'port': '5432'
}

def crawl_stock_comments(stock_symbol, **context):
    logger = setup_logger(f"crawl_{stock_symbol}", logs_base_dir)
    try:
        crawler = MultiStockYahooFinanceCrawler(headless=True, db_config=db_config)
        comments = crawler.crawl_stock_comments(stock_symbol)
        crawler.close()
        logger.info(f"✅ {stock_symbol} 크롤링 완료: {len(comments) if comments else 0} 댓글")
        return len(comments) if comments else 0
    except Exception as e:
        logger.error(f"❌ {stock_symbol} 크롤링 실패: {e}")
        raise

def validate_data(**context):
    logger = setup_logger('validate_data', logs_base_dir)
    try:
        # 현재 실행 날짜 (오늘 크롤링한 데이터를 검증)
        execution_date = context['ds']  # '2025-09-01' 형태
        
        with psycopg2.connect(**db_config) as conn:
            with conn.cursor() as cur:
                # 오늘 수집된 댓글 수 확인
                cur.execute("""
                    SELECT stock_symbol, COUNT(*) as count
                    FROM comments
                    WHERE DATE(created_at) = %s
                    GROUP BY stock_symbol
                """, (execution_date,))
                results = cur.fetchall()
                
                logger.info(f"{execution_date} 데이터 검증 결과:")
                total_comments = 0
                expected_stocks = set(stocks)  # config에서 가져온 주식 리스트
                found_stocks = set()
                
                for stock, count in results:
                    logger.info(f"   - {stock}: {count} 댓글")
                    total_comments += count
                    found_stocks.add(stock)
                
                # 검증 로직
                missing_stocks = expected_stocks - found_stocks
                if missing_stocks:
                    logger.warning(f"데이터가 없는 주식들: {missing_stocks}")
                
                if total_comments == 0:
                    logger.error(f"{execution_date}에 수집된 댓글이 없습니다")
                    raise ValueError(f"No comments collected for {execution_date}")
                
                # 최소 임계값 설정 (선택사항)
                if total_comments < len(stocks) * 10:  # 주식당 최소 10개 댓글
                    logger.warning(f"수집된 댓글 수가 적습니다: {total_comments}")
                
                logger.info(f"총 {total_comments}개 댓글 검증 완료")
                
                # 추가: 전체 누적 데이터도 확인
                cur.execute("SELECT COUNT(*) FROM comments")
                total_all = cur.fetchone()[0]
                logger.info(f"전체 누적 댓글 수: {total_all}")
                
    except Exception as e:
        logger.error(f"데이터 검증 실패: {e}")
        raise

with DAG(
    'stock_comments_pipeline',
    default_args=default_args,
    description='Daily Yahoo Finance comments crawling and DB storage',
    schedule_interval='0 9 * * *',
    start_date=datetime(2025, 8, 28),
    catchup=False,
) as dag:
    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')
    with TaskGroup(group_id='crawl_stocks') as crawl_group:
        for stock in stocks:
            crawl_task = PythonOperator(
                task_id=f'crawl_{stock}',
                python_callable=crawl_stock_comments,
                op_kwargs={'stock_symbol': stock},
            )
    validate = PythonOperator(
        task_id='validate_data',
        python_callable=validate_data,
    )
    start >> crawl_group >> validate >> end