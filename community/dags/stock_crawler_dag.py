from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from community.src.before.crawler import MultiStockYahooFinanceCrawler
from community.src.before.config import STOCKS, LOG_DIR
import logging
import os

def setup_logging(stock_symbol=None):
    log_handlers = [logging.StreamHandler()]
    if stock_symbol:
        stock_log_dir = os.path.join(LOG_DIR, stock_symbol)
        os.makedirs(stock_log_dir, exist_ok=True)
        log_handlers.append(logging.FileHandler(f"{stock_log_dir}/crawler.log"))
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=log_handlers
    )
    return logging.getLogger(__name__)

logger = setup_logging()

default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(seconds=300),
}

with DAG(
    'stock_comments_crawler',
    default_args=default_args,
    schedule='@daily',
    start_date=datetime(2025, 8, 1),
    catchup=False
) as dag:
    crawler = MultiStockYahooFinanceCrawler(headless=True)

    for stock in STOCKS:
        task = PythonOperator(
            task_id=f'crawl_{stock}_comments',
            python_callable=crawler.crawl_stock_comments,
            op_kwargs={'stock_symbol': stock}
        )