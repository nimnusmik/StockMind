import random
import time
from datetime import datetime
import os
import pandas as pd
import hashlib

from playwright.sync_api import sync_playwright
import psycopg2
from psycopg2.extras import execute_batch
from config import stocks, logs_base_dir, user_agents, proxy_list
from logger import setup_logger
from utils import wait_for_element, wait_for_comments_frame, scroll_and_wait, is_after_cutoff

class MultiStockYahooFinanceCrawler:
    def __init__(self, headless=True, db_config=None):
        self.headless = headless
        self.playwright = sync_playwright().start()
        self.stocks = stocks
        self.logs_base_dir = logs_base_dir
        self.db_config = db_config
        os.makedirs(self.logs_base_dir, exist_ok=True)
        self.browser = self.playwright.chromium.launch(
            headless=headless,
            args=[
                '--no-sandbox',
                '--disable-blink-features=AutomationControlled',
                '--disable-dev-shm-usage',
                '--disable-web-security',
                '--disable-features=VizDisplayCompositor'
            ]
        )
        self.context = None
        self.page = None
        self.setup_context()

    
    def setup_context(self):
        if self.context:
            self.context.close()
        
        # 프록시 설정 (proxy_list가 비어있지 않을 때만 사용)
        context_options = {
            "user_agent": random.choice(user_agents),
            "viewport": {"width": 1920, "height": 1080},
            "ignore_https_errors": True,
            "java_script_enabled": True,
        }
        
        # proxy_list가 있을 때만 프록시 사용
        if proxy_list:
            context_options["proxy"] = random.choice(proxy_list)
        
        self.context = self.browser.new_context(**context_options)
        self.page = self.context.new_page()
        self.page.set_default_timeout(30000)

    def get_latest_comment_time(self, stock_symbol, logger):
        try:
            with psycopg2.connect(**self.db_config) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT MAX(comment_time) FROM comments WHERE stock_symbol = %s",
                        (stock_symbol,)
                    )
                    result = cur.fetchone()[0]
                    return result if result else datetime(2025, 8, 1)
        except Exception as e:
            logger.error(f"❌ DB에서 최신 comment_time 조회 실패: {e}")
            return datetime(2025, 8, 1)

    def sort_comments_by_newest(self, target_frame, logger):
        try:
            if not wait_for_element(target_frame, '#spotim-sort-by'):
                logger.info("❌ Sort by 버튼을 찾을 수 없습니다")
                return False
            button = target_frame.query_selector('#spotim-sort-by')
            button.scroll_into_view_if_needed()
            button.click()
            logger.info("🔄 정렬 드롭다운 열기...")
            time.sleep(random.uniform(2, 5))
            newest_button = target_frame.locator('text="Newest"').first
            if newest_button.is_visible():
                newest_button.click()
                logger.info("✅ Newest 버튼 클릭 성공!")
                time.sleep(random.uniform(2, 5))
                return True
            else:
                logger.info("❌ Newest 버튼을 찾을 수 없습니다")
                return False
        except Exception as e:
            logger.info(f"❌ 정렬 오류: {e}")
            return False

    def load_more_comments(self, target_frame, logger, stock_symbol):
        try:
            logger.info("🔍 Show More 버튼 탐색 시작...")
            initial_comment_count = len(target_frame.query_selector_all('li[aria-label="Comment"]'))
            logger.info(f"📊 현재 댓글 수: {initial_comment_count}")
            scroll_and_wait(target_frame, 1000, logger)
            button_patterns = [
                'text="Show More Comments"', 'text="Show more comments"', 
                'text="Show More"', 'text="Load More"', 'text="Load more"',
                'text="More Comments"', 'text="See more comments"',
                '[aria-label*="more"]', '[aria-label*="More"]',
                '[title*="more"]', '[title*="More"]',
            ]

            for attempt in range(3):
                for pattern in button_patterns:
                    try:
                        logger.info(f"🔍 패턴 시도: {pattern} (시도 {attempt+1})")
                        more_button = target_frame.locator(pattern).first
                        if more_button.is_visible():
                            logger.info(f"✅ '{pattern}' 버튼 발견!")
                            more_button.scroll_into_view_if_needed()
                            time.sleep(random.uniform(1, 3))
                            before_click_count = len(target_frame.query_selector_all('li[aria-label="Comment"]'))
                            more_button.click()
                            logger.info("🖱️ 버튼 클릭 완료")
                            time.sleep(random.uniform(3, 7))
                            after_click_count = len(target_frame.query_selector_all('li[aria-label="Comment"]'))
                            if after_click_count > before_click_count:
                                logger.info(f"🎉 버튼 클릭으로 새 댓글 로딩: {before_click_count} -> {after_click_count}")
                                return True
                            else:
                                logger.info(f"⚠️ 버튼 클릭했지만 댓글 수 변화 없음: {before_click_count}")
                    except Exception as e:
                        logger.info(f"⚠️ {pattern} 패턴 실패: {e}")
                        continue
                if attempt < 2:
                    logger.info(f"🔄 버튼 찾기 재시도 {attempt+2}...")
                    time.sleep(random.uniform(5, 10))
            return False
        
        except Exception as e:
            logger.info(f"⚠️ Show More 버튼 찾기 전체 오류: {e}")
            return False

    def save_comments_to_db(self, comments, stock_symbol, logger):
        try:
            with psycopg2.connect(**self.db_config) as conn:
                with conn.cursor() as cur:
                    upsert_query = """
                    INSERT INTO comments (stock_symbol, comment_time, comment_text, comment_hash)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (stock_symbol, comment_time, comment_hash)
                    DO UPDATE SET comment_text = EXCLUDED.comment_text;
                    """
                    prepared_comments = [
                        (
                            stock_symbol,
                            datetime.strptime(comment['time'], '%d %b, %Y %I:%M %p'),
                            comment['text'].strip(),
                            hashlib.md5(comment['text'].encode()).hexdigest()
                        ) for comment in comments
                    ]
                    execute_batch(cur, upsert_query, prepared_comments, page_size=1000)
                    conn.commit()
                    logger.info(f"✅ {stock_symbol}: {len(prepared_comments)} 행 DB에 UPSERT 완료")
        except Exception as e:
            logger.error(f"❌ DB 저장 실패: {e}")
            raise

    def collect_comments_optimized(self, target_frame, stock_symbol, sort_success=True, logger=None):
        collected = []
        seen_ids = set()
        consecutive_old_comments = 0
        max_consecutive_old = 10 if not sort_success else 5
        last_processed_index = 0
        batch_size = 50
        no_new_comments_count = 0
        max_no_new_comments = 10
        rounds = 0
        cutoff_date = self.get_latest_comment_time(stock_symbol, logger)

        logger.info("🚀 최적화된 댓글 수집 시작...")
        logger.info(f"📊 배치 크기: {batch_size}, 최대 연속 오래된 댓글: {max_consecutive_old}, cutoff_date: {cutoff_date}")

        while rounds < 500:
            rounds += 1
            logger.info(f"\n🔄 라운드 {rounds}")
            all_comments = target_frame.query_selector_all('li[aria-label="Comment"]')
            total_comments = len(all_comments)
            logger.info(f"📋 총 댓글 수: {total_comments}")
            new_comments = all_comments[last_processed_index:]
            if not new_comments:
                logger.info("⏳ 새 댓글이 없습니다. Show More 시도...")
                no_new_comments_count += 1
                if no_new_comments_count >= max_no_new_comments:
                    logger.info(f"💀 연속 {no_new_comments_count}번 새 댓글 없음, 수집 종료")
                    break
            else:
                logger.info(f"🆕 새 댓글 {len(new_comments)}개 처리 중...")
                no_new_comments_count = 0
                for i, comment in enumerate(new_comments[:batch_size]):
                    try:
                        time_tag = comment.query_selector('time[data-spot-im-class="message-timestamp"]')
                        text_tag = comment.query_selector('div[data-spot-im-class="message-text"]')
                        if not time_tag or not text_tag:
                            continue
                        time_str = time_tag.get_attribute('title')
                        text_str = text_tag.inner_text().strip()
                        if not time_str or not text_str:
                            continue
                        comment_id = f"{time_str}_{hash(text_str)}"
                        if comment_id in seen_ids:
                            continue
                        seen_ids.add(comment_id)
                        if is_after_cutoff(time_str, cutoff_date):
                            collected.append({"time": time_str, "text": text_str})
                            logger.info(f"✅ 수집 ({len(collected)}): {time_str}")
                            consecutive_old_comments = 0
                        else:
                            consecutive_old_comments += 1
                            logger.info(f"⏰ 오래된 댓글: {time_str} ({consecutive_old_comments})")
                            if consecutive_old_comments >= max_consecutive_old:
                                logger.info("💀 연속으로 오래된 댓글 발견, 수집 중단")
                                return collected
                    except Exception as e:
                        logger.info(f"⚠️ 댓글 처리 오류: {e}")
                        continue
                last_processed_index = min(last_processed_index + batch_size, total_comments)
                logger.info(f"📈 처리 진행률: {last_processed_index}/{total_comments}")
            if not self.load_more_comments(target_frame, logger, stock_symbol):
                logger.info("📄 더 이상 댓글 로딩 불가")
                if no_new_comments_count == 0:
                    continue
                else:
                    break
            if rounds % 10 == 0:
                logger.info("🧹 메모리 정리 중...")
                target_frame.evaluate("if (window.gc) window.gc();")
                time.sleep(random.uniform(1, 3))
        return collected

    def navigate_to_stock_page(self, stock_symbol, logger):
        urls_to_try = [
            f"https://finance.yahoo.com/quote/{stock_symbol}/community/",
            f"https://finance.yahoo.com/quote/{stock_symbol}/",
        ]
        for i, url in enumerate(urls_to_try):
            for attempt in range(3):
                try:
                    logger.info(f"🔄 시도 {i+1}, 재시도 {attempt+1}: {url}")
                    self.page.goto(url, timeout=90000, wait_until="domcontentloaded")
                    wait_for_element(self.page, 'body')
                    logger.info(f"✅ 페이지 로딩 성공: {url}")
                    if "community" not in url:
                        logger.info("🔄 커뮤니티 페이지로 이동...")
                        community_link = self.page.locator('text="Community"').first
                        if community_link.is_visible(timeout=5000):
                            community_link.click()
                            self.page.wait_for_url("**/community/**", timeout=30000)
                        else:
                            self.page.goto(f"https://finance.yahoo.com/quote/{stock_symbol}/community/", 
                                         timeout=60000, wait_until="domcontentloaded")
                    return True
                except Exception as e:
                    logger.info(f"❌ {url} 로딩 실패: {e}")
                    if attempt < 2:
                        logger.info(f"🔄 재시도 {attempt+2}...")
                        time.sleep(random.uniform(5, 10))
                    continue
            if i < len(urls_to_try) - 1:
                logger.info("🔄 다른 URL로 재시도...")
                time.sleep(random.uniform(5, 10))
        return False

    def crawl_stock_comments(self, stock_symbol):
        logger = setup_logger(stock_symbol, self.logs_base_dir)
        logger.info(f"\n{'='*60}")
        logger.info(f"🎯 {stock_symbol} 댓글 크롤링 시작...")
        logger.info(f"{'='*60}")
        self.setup_context()
        
        if not self.navigate_to_stock_page(stock_symbol, logger):
            logger.info(f"❌ {stock_symbol} 페이지 로딩 실패")
            return None
        logger.info("⏳ 페이지 안정화 대기...")
        
        self.page.evaluate("window.scrollBy(0, 400);")
        
        time.sleep(random.uniform(3, 7))
        
        try:
            self.page.wait_for_load_state("networkidle", timeout=15000)
            logger.info("✅ 네트워크 안정화 완료")
        
        except:
            logger.info("⚠️ 네트워크 대기 타임아웃, 계속 진행")
        
        target_frame = wait_for_comments_frame(self.page, logger=logger)
        
        if not target_frame:
            logger.info(f"❌ {stock_symbol} 댓글 프레임을 찾을 수 없습니다")
            return None
        sort_success = self.sort_comments_by_newest(target_frame, logger)
        
        if sort_success:
            logger.info("🎉 최신순 정렬 성공!")
        
        else:
            logger.info("⚠️ 정렬 실패, 모든 댓글을 확인합니다...")
        
        
        start_time = time.time()
        comments = self.collect_comments_optimized(target_frame, stock_symbol, sort_success, logger)
        end_time = time.time()
        logger.info(f"\n🎊 {stock_symbol} 총 {len(comments)}개 댓글 수집 완료!")
        logger.info(f"⏱️ 수집 시간: {end_time - start_time:.2f}초")
        
        if comments:
            self.save_comments_to_db(comments, stock_symbol, logger)
            logger.info(f"\n📊 {stock_symbol} 수집 통계:")
            logger.info(f"   - 총 댓글 수: {len(comments)}")
            logger.info(f"   - 분당 수집률: {len(comments) / ((end_time - start_time) / 60):.1f}개/분")
            logger.info(f"   - 최신 댓글: {comments[0]['time'] if comments else 'N/A'}")
            logger.info(f"   - 가장 오래된 댓글: {comments[-1]['time'] if comments else 'N/A'}")
            return comments
        
        else:
            logger.info(f"⚠️ {stock_symbol} 수집된 댓글이 없습니다.")
            return None

    def crawl_all_stocks(self):
        logger = setup_logger('crawl_all', self.logs_base_dir)
        logger.info(f"\n🚀 다중 주식 댓글 크롤링 시작!")
        logger.info(f"📋 대상 종목: {', '.join(self.stocks)}")
        total_start_time = time.time()
        all_results = {}
        
        for i, stock in enumerate(self.stocks, 1):
            logger.info(f"\n🔄 진행률: {i}/{len(self.stocks)} ({i/len(self.stocks)*100:.1f}%)")
            try:
                comments = self.crawl_stock_comments(stock)
                all_results[stock] = len(comments) if comments else 0
                if i < len(self.stocks):
                    logger.info(f"⏳ 다음 종목까지 대기 중... (5초)")
                    time.sleep(random.uniform(5, 10))
            except KeyboardInterrupt:
                logger.info(f"\n🛑 사용자가 중단했습니다 ({stock}에서)")
                break
            except Exception as e:
                logger.info(f"❌ {stock} 처리 중 오류: {e}")
                all_results[stock] = 0
                continue
        
        total_end_time = time.time()
        logger.info(f"\n{'='*80}")
        logger.info(f"🎉 모든 주식 댓글 수집 완료!")
        logger.info(f"{'='*80}")
        logger.info(f"⏱️ 총 소요 시간: {total_end_time - total_start_time:.2f}초")
        logger.info(f"📊 수집 결과:")
        total_comments = 0
        
        for stock, count in all_results.items():
            status = "✅" if count > 0 else "❌"
            logger.info(f"   {status} {stock}: {count}개")
            total_comments += count
        logger.info(f"\n📈 총 수집 댓글 수: {total_comments}개")
        
        return all_results

    def close(self):
        self.context.close()
        self.browser.close()
        self.playwright.stop()