from playwright.sync_api import sync_playwright
from datetime import datetime
import pandas as pd
import time
import os

""" 다중 주식 종목 최적화 댓글 수집 코드"""

class MultiStockYahooFinanceCrawler:
    def __init__(self, headless=True):
        self.headless = headless
        self.cutoff_date = datetime(2025, 5, 1)
        self.playwright = sync_playwright().start()
        self.stocks = ['AAPL', 'GOOG', 'META', 'TSLA', 'MSFT', 'AMZN', 'NVDA', 'NFLX']
        #self.stocks = [ 'GOOG', 'META', 'TSLA', 'MSFT', 'AMZN', 'NVDA', 'NFLX']
        self.output_dir = "/Users/sunminkim/Desktop/StockPricingProjcet/StockMind/community/data"
        
        # 출력 디렉토리 생성
        os.makedirs(self.output_dir, exist_ok=True)
        
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
        
        self.context = self.browser.new_context(
            user_agent="Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
            #user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1920, "height": 1080},
            ignore_https_errors=True,
            java_script_enabled=True
        )
        
        self.page = self.context.new_page()
        self.page.set_default_timeout(30000)

    def wait_for_element(self, selector, timeout=10000, frame=None):
        target = frame if frame else self.page
        try:
            target.wait_for_selector(selector, timeout=timeout)
            return True
        except:
            return False

    def wait_for_comments_frame(self, max_wait=15):
        start_time = time.time()
        
        while time.time() - start_time < max_wait:
            for frame in self.page.frames:
                if "yahoosandbox.com" in frame.url or "safeframe.html" in frame.url:
                    print(f"✅ 댓글 iframe 발견: {frame.url}")
                    return frame
            time.sleep(1)
        
        print("❌ 댓글 프레임을 찾을 수 없습니다")
        return None

    def sort_comments_by_newest(self, target_frame):
        try:
            if not self.wait_for_element('#spotim-sort-by', frame=target_frame):
                print("❌ Sort by 버튼을 찾을 수 없습니다")
                return False

            button = target_frame.query_selector('#spotim-sort-by')
            button.scroll_into_view_if_needed()
            button.click()
            print("🔄 정렬 드롭다운 열기...")
            
            time.sleep(3)
            
            try:
                newest_button = target_frame.locator('text="Newest"').first
                if newest_button.is_visible():
                    newest_button.click()
                    print("✅ Newest 버튼 클릭 성공!")
                    time.sleep(3)
                    return True
            except:
                pass
            
            newest_clicked = target_frame.evaluate("""
                () => {
                    const elements = document.querySelectorAll('*');
                    for (let el of elements) {
                        if (el.textContent && el.textContent.trim() === 'Newest' && 
                            (el.tagName === 'BUTTON' || el.tagName === 'LI' || el.role === 'menuitem')) {
                            el.click();
                            return true;
                        }
                    }
                    
                    for (let el of elements) {
                        if (el.shadowRoot) {
                            const shadowElements = el.shadowRoot.querySelectorAll('*');
                            for (let shadowEl of shadowElements) {
                                if (shadowEl.textContent && shadowEl.textContent.trim() === 'Newest') {
                                    shadowEl.click();
                                    return true;
                                }
                            }
                        }
                    }
                    
                    const newestByData = document.querySelector('[data-value="newest"]');
                    if (newestByData) {
                        newestByData.click();
                        return true;
                    }
                    
                    return false;
                }
            """)
            
            if newest_clicked:
                print("✅ JavaScript로 Newest 클릭 성공!")
                time.sleep(3)
                return True
            
            print("❌ 모든 방법으로 Newest 버튼을 찾을 수 없습니다")
            return False
            
        except Exception as e:
            print(f"❌ 정렬 오류: {e}")
            return False

    def is_after_cutoff(self, time_str):
        try:
            comment_time = datetime.strptime(time_str, "%d %b, %Y %I:%M %p")
            return comment_time >= self.cutoff_date
        except ValueError:
            print(f"⚠️ 날짜 파싱 오류: {time_str}")
            return False

    def collect_comments_optimized(self, target_frame, sort_success=True):
        """최적화된 댓글 수집 - 증분 처리 방식"""
        collected = []
        seen_ids = set()
        consecutive_old_comments = 0
        max_consecutive_old = 10 if not sort_success else 5
        
        last_processed_index = 0  # 마지막으로 처리한 인덱스
        batch_size = 50  # 한 번에 처리할 댓글 수
        
        print("🚀 최적화된 댓글 수집 시작...")
        print(f"📊 배치 크기: {batch_size}, 최대 연속 오래된 댓글: {max_consecutive_old}")
        
        rounds = 0
        while rounds < 100:  # 최대 100라운드
            rounds += 1
            print(f"\n🔄 라운드 {rounds}")
            
            # 현재 모든 댓글 가져오기 (한 번만)
            all_comments = target_frame.query_selector_all('li[aria-label="Comment"]')
            total_comments = len(all_comments)
            print(f"📋 총 댓글 수: {total_comments}")
            
            # 새로운 댓글만 처리 (증분 처리)
            new_comments = all_comments[last_processed_index:]
            if not new_comments:
                print("⏳ 새 댓글이 없습니다. Show More 시도...")
            else:
                print(f"🆕 새 댓글 {len(new_comments)}개 처리 중...")
                
                # 배치 단위로 처리
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
                        
                        if self.is_after_cutoff(time_str):
                            collected.append({
                                "time": time_str,
                                "text": text_str
                            })
                            print(f"✅ 수집 ({len(collected)}): {time_str}")
                            consecutive_old_comments = 0
                        else:
                            consecutive_old_comments += 1
                            print(f"⏰ 오래된 댓글: {time_str} ({consecutive_old_comments})")
                            
                            if consecutive_old_comments >= max_consecutive_old:
                                print("💀 연속으로 오래된 댓글 발견, 수집 중단")
                                return collected
                    
                    except Exception as e:
                        print(f"⚠️ 댓글 처리 오류: {e}")
                        continue
                
                # 처리된 인덱스 업데이트
                last_processed_index = min(last_processed_index + batch_size, total_comments)
                print(f"📈 처리 진행률: {last_processed_index}/{total_comments}")
            
            # Show More 버튼 클릭
            more_loaded = self.load_more_comments(target_frame)
            if not more_loaded:
                print("📄 더 이상 댓글이 없습니다")
                break
                
            # 메모리 정리 (선택적)
            if rounds % 10 == 0:
                print("🧹 메모리 정리 중...")
                target_frame.evaluate("if (window.gc) window.gc();")
                time.sleep(1)
        
        return collected

    def load_more_comments(self, target_frame):
        """Show More 버튼 클릭 최적화"""
        try:
            # Playwright 방법 먼저 시도
            more_button = target_frame.locator('text="Show More Comments"').first
            if more_button.is_visible(timeout=2000):
                more_button.scroll_into_view_if_needed()
                more_button.click()
                print("📄 더 많은 댓글 로딩... (Playwright)")
                time.sleep(2)  # 로딩 시간 단축
                return True
        except:
            pass
        
        # JavaScript 방법으로 시도
        try:
            clicked = target_frame.evaluate("""
                () => {
                    const buttons = document.querySelectorAll('button');
                    for (let btn of buttons) {
                        const text = btn.textContent?.toLowerCase() || '';
                        if (text.includes('show more') || text.includes('load more') || text.includes('more comments')) {
                            btn.scrollIntoView();
                            btn.click();
                            return true;
                        }
                    }
                    return false;
                }
            """)
            
            if clicked:
                print("📄 더 많은 댓글 로딩... (JavaScript)")
                time.sleep(2)
                return True
        except:
            pass
        
        return False

    def navigate_to_stock_page(self, stock_symbol):
        """특정 주식 페이지로 이동"""
        urls_to_try = [
            f"https://finance.yahoo.com/quote/{stock_symbol}/community/",
            f"https://finance.yahoo.com/quote/{stock_symbol}/",
        ]
        
        for i, url in enumerate(urls_to_try):
            try:
                print(f"🔄 시도 {i+1}: {url}")
                self.page.goto(url, timeout=90000, wait_until="domcontentloaded")
                self.page.wait_for_selector('body', timeout=10000)
                print(f"✅ 페이지 로딩 성공: {url}")
                
                if "community" not in url:
                    print("🔄 커뮤니티 페이지로 이동...")
                    try:
                        community_link = self.page.locator('text="Community"').first
                        if community_link.is_visible(timeout=5000):
                            community_link.click()
                            self.page.wait_for_url("**/community/**", timeout=30000)
                        else:
                            self.page.goto(f"https://finance.yahoo.com/quote/{stock_symbol}/community/", 
                                         timeout=60000, wait_until="domcontentloaded")
                    except:
                        print("⚠️ 커뮤니티 페이지 이동 실패, 현재 페이지에서 진행")
                
                return True
                
            except Exception as e:
                print(f"❌ {url} 로딩 실패: {e}")
                if i < len(urls_to_try) - 1:
                    print("🔄 다른 URL로 재시도...")
                    time.sleep(3)
                continue
        
        return False

    def crawl_stock_comments(self, stock_symbol):
        """특정 주식의 댓글 수집"""
        print(f"\n{'='*60}")
        print(f"🎯 {stock_symbol} 댓글 크롤링 시작...")
        print(f"{'='*60}")
        
        # 페이지 로딩
        if not self.navigate_to_stock_page(stock_symbol):
            print(f"❌ {stock_symbol} 페이지 로딩 실패")
            return None
        
        print("⏳ 페이지 안정화 대기...")
        self.page.evaluate("window.scrollBy(0, 400);")
        time.sleep(5)
        
        try:
            self.page.wait_for_load_state("networkidle", timeout=15000)
            print("✅ 네트워크 안정화 완료")
        except:
            print("⚠️ 네트워크 대기 타임아웃, 계속 진행")
        
        # 댓글 프레임 찾기
        target_frame = self.wait_for_comments_frame()
        if not target_frame:
            print(f"❌ {stock_symbol} 댓글 프레임을 찾을 수 없습니다")
            return None
        
        # 최신순 정렬
        sort_success = self.sort_comments_by_newest(target_frame)
        if sort_success:
            print("🎉 최신순 정렬 성공!")
        else:
            print("⚠️ 정렬 실패, 모든 댓글을 확인합니다...")
        
        # 댓글 수집
        start_time = time.time()
        comments = self.collect_comments_optimized(target_frame, sort_success)
        end_time = time.time()
        
        print(f"\n🎊 {stock_symbol} 총 {len(comments)}개 댓글 수집 완료!")
        print(f"⏱️ 수집 시간: {end_time - start_time:.2f}초")
        
        if comments:
            # 주식 심볼과 날짜를 포함한 파일명 생성
            current_month = datetime.now().strftime("%Y%m")
            filename = f"{stock_symbol}_comments_{current_month}.csv"
            filepath = os.path.join(self.output_dir, filename)
            
            # 댓글에 주식 심볼 정보 추가
            for comment in comments:
                comment['stock_symbol'] = stock_symbol
            
            df = pd.DataFrame(comments)
            df.to_csv(filepath, index=False, encoding='utf-8')
            print(f"📁 댓글이 '{filepath}'에 저장되었습니다.")
            
            print(f"\n📊 {stock_symbol} 수집 통계:")
            print(f"   - 총 댓글 수: {len(comments)}")
            print(f"   - 분당 수집률: {len(comments) / ((end_time - start_time) / 60):.1f}개/분")
            print(f"   - 최신 댓글: {comments[0]['time'] if comments else 'N/A'}")
            print(f"   - 가장 오래된 댓글: {comments[-1]['time'] if comments else 'N/A'}")
            
            return comments
        else:
            print(f"⚠️ {stock_symbol} 수집된 댓글이 없습니다.")
            return None

    def crawl_all_stocks(self):
        """모든 주식의 댓글 수집"""
        print(f"\n🚀 다중 주식 댓글 크롤링 시작!")
        print(f"📋 대상 종목: {', '.join(self.stocks)}")
        print(f"📁 저장 경로: {self.output_dir}")
        
        total_start_time = time.time()
        all_results = {}
        
        for i, stock in enumerate(self.stocks, 1):
            print(f"\n🔄 진행률: {i}/{len(self.stocks)} ({i/len(self.stocks)*100:.1f}%)")
            
            try:
                comments = self.crawl_stock_comments(stock)
                all_results[stock] = len(comments) if comments else 0
                
                # 종목 간 간격 (브라우저 안정화)
                if i < len(self.stocks):
                    print(f"⏳ 다음 종목까지 대기 중... (5초)")
                    time.sleep(5)
                    
            except KeyboardInterrupt:
                print(f"\n🛑 사용자가 중단했습니다 ({stock}에서)")
                break
            except Exception as e:
                print(f"❌ {stock} 처리 중 오류: {e}")
                all_results[stock] = 0
                continue
        
        total_end_time = time.time()
        
        # 최종 결과 출력
        print(f"\n{'='*80}")
        print(f"🎉 모든 주식 댓글 수집 완료!")
        print(f"{'='*80}")
        print(f"⏱️ 총 소요 시간: {total_end_time - total_start_time:.2f}초")
        print(f"📊 수집 결과:")
        
        total_comments = 0
        for stock, count in all_results.items():
            status = "✅" if count > 0 else "❌"
            print(f"   {status} {stock}: {count}개")
            total_comments += count
        
        print(f"\n📈 총 수집 댓글 수: {total_comments}개")
        print(f"📁 저장 위치: {self.output_dir}")
        
        return all_results

    def close(self):
        self.browser.close()
        self.playwright.stop()

# 실행 코드
if __name__ == "__main__":
    crawler = MultiStockYahooFinanceCrawler(headless=False)
    
    try:
        results = crawler.crawl_all_stocks()
        
    except KeyboardInterrupt:
        print("\n🛑 사용자가 전체 프로세스를 중단했습니다")
    except Exception as e:
        print(f"❌ 예상치 못한 오류: {e}")
    finally:
        print("🔚 크롤링 완료")
        crawler.close()