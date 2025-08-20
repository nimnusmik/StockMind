from crawler import MultiStockYahooFinanceCrawler

if __name__ == "__main__":
    crawler = MultiStockYahooFinanceCrawler(headless=False)
    try:
        results = crawler.crawl_all_stocks()
    except KeyboardInterrupt:
        print("\n 사용자가 전체 프로세스를 중단했습니다")
    except Exception as e:
        print(f" 예상치 못한 오류: {e}")
    finally:
        print(" 크롤링 완료")
        crawler.close()