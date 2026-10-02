# community/src/main_dblinked.py
# DB 직접 연결 버전 크롤러 실행 스크립트
from crawler_dblinked import MultiStockYahooFinanceCrawler

if __name__ == "__main__":
    print("🚀 Yahoo Finance Community Crawler (DB Direct) 시작")
    print("=" * 60)

    # headless=True로 백그라운드 실행 (빠른 수집)
    crawler = MultiStockYahooFinanceCrawler(headless=True)
    try:
        results = crawler.crawl_all_stocks()
    except KeyboardInterrupt:
        print("\n⚠️ 사용자가 전체 프로세스를 중단했습니다")
    except Exception as e:
        print(f"❌ 예상치 못한 오류: {e}")
        import traceback
        traceback.print_exc()
    finally:
        print("✅ 크롤링 완료")
        crawler.close()
