from playwright.sync_api import sync_playwright
from datetime import datetime
import pandas as pd
import time
import os

""" 다중 주식 종목 최적화 댓글 수집 코드 - 개선 버전"""

class MultiStockYahooFinanceCrawler:
    def __init__(self, headless=True):
        self.headless = headless
        self.cutoff_date = datetime(2025, 5, 1)
        self.playwright = sync_playwright().start()
        self.stocks = ['AAPL', 'GOOG', 'META', 'TSLA', 'MSFT', 'AMZN', 'NVDA', 'NFLX']
        self.output_dir = "/Users/sunminkim/Desktop/AIStages/StockMind/community/data"
        
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
                print("❌ Newest 버튼을 찾을 수 없습니다")
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

    def scroll_and_wait(self, target_frame, scroll_distance=500):
        """프레임 내에서 스크롤하고 로딩 대기 - 개선된 버전"""
        try:
            print(f"🔄 스크롤 시도: {scroll_distance}px")
            
            # 1. 프레임이 활성화되어 있는지 확인
            is_frame_active = target_frame.evaluate("() => document.hasFocus()")
            print(f"📍 프레임 활성 상태: {is_frame_active}")
            
            # 2. 프레임에 포커스 설정
            target_frame.evaluate("() => window.focus()")
            
            # 3. 다중 스크롤 전략 시도
            scroll_success = target_frame.evaluate(f"""
                () => {{
                    console.log('스크롤 시작...');
                    let scrolled = false;
                    
                    // 방법 1: window 스크롤
                    try {{
                        const initialY = window.pageYOffset;
                        window.scrollBy(0, {scroll_distance});
                        const newY = window.pageYOffset;
                        console.log(`Window 스크롤: ${{initialY}} -> ${{newY}}`);
                        if (newY > initialY) scrolled = true;
                    }} catch (e) {{
                        console.log('Window 스크롤 실패:', e);
                    }}
                    
                    // 방법 2: document.documentElement 스크롤
                    try {{
                        const initialTop = document.documentElement.scrollTop;
                        document.documentElement.scrollTop += {scroll_distance};
                        const newTop = document.documentElement.scrollTop;
                        console.log(`DocumentElement 스크롤: ${{initialTop}} -> ${{newTop}}`);
                        if (newTop > initialTop) scrolled = true;
                    }} catch (e) {{
                        console.log('DocumentElement 스크롤 실패:', e);
                    }}
                    
                    // 방법 3: document.body 스크롤
                    try {{
                        const initialTop = document.body.scrollTop;
                        document.body.scrollTop += {scroll_distance};
                        const newTop = document.body.scrollTop;
                        console.log(`Body 스크롤: ${{initialTop}} -> ${{newTop}}`);
                        if (newTop > initialTop) scrolled = true;
                    }} catch (e) {{
                        console.log('Body 스크롤 실패:', e);
                    }}
                    
                    // 방법 4: 스크롤 가능한 컨테이너 찾기
                    const scrollableElements = Array.from(document.querySelectorAll('*')).filter(el => {{
                        const style = window.getComputedStyle(el);
                        return (style.overflowY === 'scroll' || style.overflowY === 'auto') && el.scrollHeight > el.clientHeight;
                    }});
                    
                    console.log(`스크롤 가능한 요소 발견: ${{scrollableElements.length}}개`);
                    
                    for (let el of scrollableElements) {{
                        try {{
                            const initialTop = el.scrollTop;
                            el.scrollTop += {scroll_distance};
                            const newTop = el.scrollTop;
                            console.log(`컨테이너 스크롤 (${{el.tagName}}.${{el.className}}): ${{initialTop}} -> ${{newTop}}`);
                            if (newTop > initialTop) scrolled = true;
                        }} catch (e) {{
                            console.log('컨테이너 스크롤 실패:', e);
                        }}
                    }}
                    
                    // 방법 5: 댓글 관련 컨테이너 찾기
                    const commentSelectors = [
                        '[class*="comment"]', '[class*="conversation"]', '[class*="thread"]',
                        '[class*="message"]', '[class*="discussion"]', '[class*="chat"]',
                        '[id*="comment"]', '[id*="conversation"]', '[id*="thread"]'
                    ];
                    
                    for (let selector of commentSelectors) {{
                        try {{
                            const elements = document.querySelectorAll(selector);
                            for (let el of elements) {{
                                const initialTop = el.scrollTop;
                                el.scrollTop += {scroll_distance};
                                const newTop = el.scrollTop;
                                if (newTop > initialTop) {{
                                    console.log(`댓글 컨테이너 스크롤 성공: ${{selector}}`);
                                    scrolled = true;
                                }}
                            }}
                        }} catch (e) {{
                            // 무시
                        }}
                    }}
                    
                    // 방법 6: 키보드 이벤트로 스크롤 시뮬레이션
                    try {{
                        const event = new KeyboardEvent('keydown', {{
                            key: 'PageDown',
                            keyCode: 34,
                            which: 34
                        }});
                        document.dispatchEvent(event);
                        console.log('PageDown 키 이벤트 발송');
                    }} catch (e) {{
                        console.log('키보드 이벤트 실패:', e);
                    }}
                    
                    // 방법 7: 마우스 휠 이벤트
                    try {{
                        const wheelEvent = new WheelEvent('wheel', {{
                            deltaY: {scroll_distance},
                            bubbles: true
                        }});
                        document.dispatchEvent(wheelEvent);
                        console.log('마우스 휠 이벤트 발송');
                    }} catch (e) {{
                        console.log('휠 이벤트 실패:', e);
                    }}
                    
                    return scrolled;
                }}
            """)
            
            print(f"📊 스크롤 결과: {scroll_success}")
            time.sleep(3)  # 스크롤 후 충분한 대기 시간
            
            return scroll_success
            
        except Exception as e:
            print(f"⚠️ 스크롤 오류: {e}")
            return False

    def load_more_comments(self, target_frame):
        """개선된 Show More 버튼 찾기 및 클릭"""
        try:
            print("🔍 Show More 버튼 탐색 시작...")
            
            # 0. 현재 댓글 수 체크
            initial_comment_count = len(target_frame.query_selector_all('li[aria-label="Comment"]'))
            print(f"📊 현재 댓글 수: {initial_comment_count}")
            
            # 1. 먼저 스크롤 시도 (더 많은 콘텐츠 로드를 위해)
            print("🔄 페이지 하단으로 스크롤...")
            scroll_success = self.scroll_and_wait(target_frame, 1000)
            
            # 2. Playwright 셀렉터로 Show More 버튼 찾기
            button_patterns = [
                'text="Show More Comments"',
                'text="Show more comments"', 
                'text="Show More"',
                'text="Load More"',
                'text="Load more"',
                'text="More Comments"',
                'text="See more comments"',
                '[aria-label*="more"]',
                '[aria-label*="More"]',
                '[title*="more"]',
                '[title*="More"]',
            ]
            
            for pattern in button_patterns:
                try:
                    print(f"🔍 패턴 시도: {pattern}")
                    more_button = target_frame.locator(pattern).first
                    if more_button.is_visible():
                        print(f"✅ '{pattern}' 버튼 발견!")
                        
                        # 버튼을 화면 중앙으로 스크롤
                        more_button.scroll_into_view_if_needed()
                        time.sleep(1)
                        
                        # 클릭 전 댓글 수 확인
                        before_click_count = len(target_frame.query_selector_all('li[aria-label="Comment"]'))
                        
                        # 버튼 클릭
                        more_button.click()
                        print("🖱️ 버튼 클릭 완료")
                        
                        # 클릭 후 대기 및 확인
                        time.sleep(4)
                        after_click_count = len(target_frame.query_selector_all('li[aria-label="Comment"]'))
                        
                        if after_click_count > before_click_count:
                            print(f"🎉 버튼 클릭으로 새 댓글 로딩: {before_click_count} -> {after_click_count}")
                            return True
                        else:
                            print(f"⚠️ 버튼 클릭했지만 댓글 수 변화 없음: {before_click_count}")
                            
                except Exception as e:
                    print(f"⚠️ {pattern} 패턴 실패: {e}")
                    continue
            
        except Exception as e:
            print(f"⚠️ Show More 버튼 찾기 전체 오류: {e}")
            return False

    def collect_comments_optimized(self, target_frame, sort_success=True):
        """최적화된 댓글 수집 - 증분 처리 방식"""
        collected = []
        seen_ids = set()
        consecutive_old_comments = 0
        max_consecutive_old = 10 if not sort_success else 5
        
        last_processed_index = 0
        batch_size = 50
        no_new_comments_count = 0  # 새 댓글이 없는 연속 횟수
        max_no_new_comments = 10    # 최대 10번까지 새 댓글이 없어도 시도
        
        print("🚀 최적화된 댓글 수집 시작...")
        print(f"📊 배치 크기: {batch_size}, 최대 연속 오래된 댓글: {max_consecutive_old}")
        
        rounds = 0
        while rounds < 500:  # 최대 500라운드로 증가
            rounds += 1
            print(f"\n🔄 라운드 {rounds}")
            
            # 현재 모든 댓글 가져오기
            all_comments = target_frame.query_selector_all('li[aria-label="Comment"]')
            total_comments = len(all_comments)
            print(f"📋 총 댓글 수: {total_comments}")
            
            # 새로운 댓글만 처리
            new_comments = all_comments[last_processed_index:]
            if not new_comments:
                print("⏳ 새 댓글이 없습니다. Show More 시도...")
                no_new_comments_count += 1
                
                if no_new_comments_count >= max_no_new_comments:
                    print(f"💀 연속 {no_new_comments_count}번 새 댓글 없음, 수집 종료")
                    break
            else:
                print(f"🆕 새 댓글 {len(new_comments)}개 처리 중...")
                no_new_comments_count = 0  # 새 댓글이 있으면 카운터 리셋
                
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
            
            # Show More 버튼 클릭 (개선된 버전)
            more_loaded = self.load_more_comments(target_frame)
            if not more_loaded:
                print("📄 더 이상 댓글 로딩 불가")
                if no_new_comments_count == 0:  # 새 댓글이 있었다면 한 번 더 시도
                    continue
                else:
                    break
                
            # 메모리 정리
            if rounds % 10 == 0:
                print("🧹 메모리 정리 중...")
                target_frame.evaluate("if (window.gc) window.gc();")
                time.sleep(1)
        
        return collected

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