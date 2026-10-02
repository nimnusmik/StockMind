import time
import random
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

def wait_for_element(page_or_frame, selector, timeout=10000):
    try:
        page_or_frame.wait_for_selector(selector, timeout=timeout)
        return True
    except Exception:
        return False

def wait_for_comments_frame(page, max_wait=15, logger=None):
    start_time = time.time()
    while time.time() - start_time < max_wait:
        for frame in page.frames:
            if "yahoosandbox.com" in frame.url or "safeframe.html" in frame.url:
                logger.info(f"✅ 댓글 iframe 발견: {frame.url}")
                return frame
        time.sleep(1)
    logger.info("❌ 댓글 프레임을 찾을 수 없습니다")
    return None

def scroll_and_wait(target_frame, scroll_distance=500, logger=None):
    try:
        logger.info(f"🔄 스크롤 시도: {scroll_distance}px")
        is_frame_active = target_frame.evaluate("() => document.hasFocus()")
        logger.info(f"📍 프레임 활성 상태: {is_frame_active}")
        
        target_frame.evaluate("() => window.focus()")
        
        scroll_success = target_frame.evaluate(f"""
            () => {{
                let scrolled = false;
                try {{
                    const initialY = window.pageYOffset;
                    window.scrollBy(0, {scroll_distance});
                    if (window.pageYOffset > initialY) scrolled = true;
                }} catch (e) {{}}
                try {{
                    const initialTop = document.documentElement.scrollTop;
                    document.documentElement.scrollTop += {scroll_distance};
                    if (document.documentElement.scrollTop > initialTop) scrolled = true;
                }} catch (e) {{}}
                try {{
                    const initialTop = document.body.scrollTop;
                    document.body.scrollTop += {scroll_distance};
                    if (document.body.scrollTop > initialTop) scrolled = true;
                }} catch (e) {{}}
                return scrolled;
            }}
        """)
        
        logger.info(f"📊 스크롤 결과: {scroll_success}")
        time.sleep(random.uniform(2, 5))  # Random delay
        return scroll_success
        
    except Exception as e:
        logger.info(f"⚠️ 스크롤 오류: {e}")
        return False

def is_after_cutoff(time_str, cutoff_date):
    try:
        comment_time = datetime.strptime(time_str, "%d %b, %Y %I:%M %p")
        return comment_time >= cutoff_date
    except ValueError:
        logger.warning(f"⚠️ 날짜 파싱 오류: {time_str}")
        return False

# Optional: CAPTCHA handling (uncomment and configure if needed)
# def solve_captcha(page, logger):
#     from twocaptcha import TwoCaptcha
#     solver = TwoCaptcha('YOUR_2CAPTCHA_API_KEY')
#     try:
#         if page.locator('text="Please verify you are not a robot"').is_visible(timeout=5000):
#             logger.info("⚠️ CAPTCHA 감지됨, 해결 시도...")
#             result = solver.recaptcha(sitekey='SITE_KEY', url=page.url)
#             page.evaluate(f"() => grecaptcha.execute('{result['code']}')")
#             logger.info("✅ CAPTCHA 해결 완료")
#             return True
#         return False
#     except Exception as e:
#         logger.info(f"❌ CAPTCHA 해결 실패: {e}")
#         return False