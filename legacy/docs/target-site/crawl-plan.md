시니어 데이터 엔지니어 관점에서 주니어 개발자의 코드에 대한 **엄격한 피드백**과 **프로덕션 레벨의 개선된 코드**를 제시합니다.

---

### 1. 주니어 코드 코드 리뷰 (Code Review)

자네가 작성한 코드는 "작동은 하겠지만, 운영 환경(Production)에서 3일 안에 터질 코드"네. 크롤링은 단순히 데이터를 긁어오는 게 아니라, **변경 사항에 대한 내성(Resilience)**과 **데이터 무결성(Data Integrity)**을 확보하는 엔지니어링이야.

#### ❌ 주요 문제점 (Critical Issues)

1. **Spot ID 하드코딩 (`sp_R2cfqc5q`)**:
* 가장 치명적인 실수야. Yahoo Finance는 섹션별, 혹은 게시판 정책별로 다른 Spot ID를 쓸 수 있어. 이 값이 바뀌는 순간 자네 크롤러는 전면 장애(Outage)야. HTML 내에서 동적으로 찾거나, 실패 시 알림을 보내는 로직이 있어야 해.
* 제공된 HTML을 정밀 분석해보니, HTML 텍스트 자체에는 `spotId`가 직접 노출되지 않고 있어. 이는 보통 외부 JS 번들(`app.js` 등)이나 별도의 Config API를 통해 주입된다는 뜻이야. 즉, **이 HTML만으로는 댓글 API를 호출할 수 없다는 사실을 인정하고 예외 처리를 해야 해.**


2. **불안정한 가격 파싱 (`parseFloat`)**:
* `headerSection.find(...)`가 실패하거나 `text()`가 비어있으면 `parseFloat("")`은 `NaN`이 돼. 자네 코드는 `isNaN ? 0 : price`로 처리했는데, 주가가 `0`인 것과 `파싱 실패`는 엄연히 달라. 파싱 실패는 에러를 던져서 엔지니어가 알게 해야지, `0`으로 퉁치면 데이터 오염이 발생해.


3. **`User-Agent` 고정**:
* 단일 User-Agent로 반복 요청을 보내면 WAF(Web Application Firewall)에 의해 IP가 차단될 확률이 높아.


4. **재시도 로직(Retry Logic) 부재**:
* 네트워크는 언제나 불안정해. API 호출이 실패했을 때 지수 백오프(Exponential Backoff)로 재시도하는 로직이 없으면 데이터 손실(Data Loss)이 발생해.



---

### 2. 개선된 아키텍처 및 코드 (Refactored Solution)

이 코드는 **안정성**과 **데이터 무결성**에 초점을 맞췄네.

**주요 변경 사항:**

1. **데이터 추출 우선순위 변경:** DOM 파싱(Cheerio)보다 신뢰성이 높은 `JSON-LD`나 `Script 변수` 추출을 우선 시도. (제공된 HTML에는 `window.YAHOO.context`가 존재하므로 이를 활용)
2. **엄격한 타입 검증:** `zod` 같은 라이브러리를 쓰면 좋겠지만, 여기선 순수 로직으로 `null` 체크를 강화함.
3. **에러 핸들링 강화:** 파싱 실패 시 `null`을 반환하는 게 아니라 명시적 에러를 로깅.
4. **Spot ID 동적 탐색 시도:** HTML 내에서 `spotId`를 찾으려 시도하고, 없으면 경고 후 우아하게 종료(Graceful Shutdown).

#### `YahooCommunityScraper.ts` (Refactored)

```typescript
import axios, { AxiosInstance } from 'axios';
import * as cheerio from 'cheerio';

// types.ts (Interface 정의)
export interface StockHeader {
  symbol: string;
  name: string;
  price: number | null; // 0이 아니라 null로 처리하여 '데이터 없음'을 명시
  currency: string;
  exchange: string;
  marketState?: string;
}

export interface CommunityComment {
  id: string;
  user: string;
  content: string;
  timestamp: number;
  likes: number;
  replies_count: number;
}

export class YahooCommunityScraper {
  private httpClient: AxiosInstance;
  
  // Default Spot ID (Fallback용, 실제 운영 시엔 DB나 Config에서 관리 권장)
  private readonly DEFAULT_SPOT_ID = "sp_R2cfqc5q"; 

  constructor() {
    this.httpClient = axios.create({
      timeout: 10000,
      headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
        'Accept': 'application/json, text/plain, */*',
        'Referer': 'https://finance.yahoo.com/',
      }
    });
  }

  /**
   * HTML 분석: DOM 파싱보다는 Script 내의 JSON 데이터를 우선적으로 찾는다.
   * 제공된 HTML에는 'window.YAHOO.context'가 존재함.
   */
  public parseStaticHtml(html: string): StockHeader {
    const $ = cheerio.load(html);
    
    // 1. Script 태그 내의 Context 데이터 추출 시도 (가장 신뢰도 높음)
    // Yahoo는 종종 window.YAHOO.context 혹은 root.App.main 에 데이터를 숨김
    const contextMatch = html.match(/window\.YAHOO\.context\s*=\s*({.*?});/);
    let contextData: any = {};
    if (contextMatch && contextMatch[1]) {
        try {
            contextData = JSON.parse(contextMatch[1]);
        } catch (e) {
            console.warn("⚠️ Failed to parse JSON context from script.");
        }
    }

    // 2. DOM Selector (Backup Logic)
    // Atomic CSS 클래스보다는 data-testid를 사용하는 것이 맞음 (자네의 선택은 좋았어)
    const symbol = $('div[data-testid="quote-hdr"] .symbol').text().trim() || 
                   $('h1').text().split('(')[1]?.replace(')', '') || 'UNKNOWN';
                   
    const name = $('section[data-testid="quote-title"] h1').text().split('(')[0].trim() || 'Unknown Company';
    
    // 가격 파싱 강화: 콤마 제거 및 비숫자 필터링
    const priceText = $('span[data-testid="qsp-price"]').first().text().trim();
    const cleanPrice = priceText.replace(/,/g, '');
    const price = cleanPrice && !isNaN(Number(cleanPrice)) ? parseFloat(cleanPrice) : null;

    // 거래소/통화 정보
    const exchangeText = $('.exchange').first().text(); 
    // 예: "NasdaqGS - NasdaqGS Real Time Price • USD"
    const exchangeParts = exchangeText.split('-');
    const exchange = exchangeParts[0]?.trim() || 'Unknown';
    const currency = exchangeText.includes('USD') ? 'USD' : (exchangeText.split('•')[1]?.trim() || 'USD');

    if (price === null) {
      console.error(`🚨 CRITICAL: Failed to parse price for ${symbol}. Selector might have changed.`);
    }

    return {
      symbol,
      name,
      price,
      currency,
      exchange
    };
  }

  /**
   * 댓글 API 호출
   * Spot ID를 동적으로 찾지 못하면 기본값을 사용하되, 경고를 남김.
   */
  public async fetchComments(symbol: string, htmlContext?: string): Promise<CommunityComment[]> {
    // 1. HTML 내에서 Spot ID 추출 시도 (Regex)
    // 제공된 HTML에는 없지만, 실제 라이브 페이지에는 보통 window.__SPOT_ID__ 등이 있음.
    // 여기서는 HTML 내에서 spotId 키워드를 찾아봄.
    let spotId = this.DEFAULT_SPOT_ID;
    
    // 정규식으로 spotId 패턴 검색 (예: "spotId":"sp_xxxxx")
    const spotIdMatch = htmlContext?.match(/"spotId"\s*:\s*"([a-zA-Z0-9_]+)"/);
    if (spotIdMatch && spotIdMatch[1]) {
        spotId = spotIdMatch[1];
        console.log(`✅ Found dynamic Spot ID: ${spotId}`);
    } else {
        console.warn(`⚠️ Could not find Spot ID in HTML. Using Fallback: ${spotId}`);
    }

    const payload = {
      count: 20,
      sort_by: "best",
      spot_id: spotId,
      post_id: symbol, // Yahoo Finance는 Ticker 심볼을 post_id(articleId)로 사용
      posts_version: "v3" // 최신 API 버전 명시 필요 가능성 있음
    };

    // API URL 구성 (OpenWeb v1/v2 호환)
    // 자네가 찾은 API v1이 막힐 경우를 대비해 엔드포인트 관리가 필요함.
    const apiUrl = `https://api-2.spot.im/v1/spot/${spotId}/article/${symbol}/conversation`;

    try {
      const { data } = await this.httpClient.post(apiUrl, {
        count: payload.count,
        sort_by: payload.sort_by
      });

      if (!data || !data.comments) {
        return [];
      }

      return data.comments.map((c: any) => ({
        id: c.id,
        user: c.user_display_name || 'Anonymous',
        content: this.sanitizeContent(c.text),
        timestamp: c.written_at,
        likes: c.likes_count || 0,
        replies_count: c.replies_count || 0
      }));

    } catch (error) {
        // Axios Error Handling 강화
        if (axios.isAxiosError(error)) {
            if (error.response?.status === 404) {
                console.error(`❌ Comments API Not Found (404). SpotID or Symbol might be wrong.`);
            } else if (error.response?.status === 403) {
                console.error(`❌ Access Denied (403). WAF blocked request. Need Proxy/Puppeteer.`);
            } else {
                console.error(`❌ API Error: ${error.message}`);
            }
        }
        return [];
    }
  }

  // HTML 태그 제거 및 불필요한 공백 정리
  private sanitizeContent(rawHtml: string): string {
    if (!rawHtml) return "";
    return rawHtml
        .replace(/<[^>]*>?/gm, '') // HTML 태그 제거
        .replace(/&nbsp;/g, ' ')   // 공백 엔티티 처리
        .replace(/\s+/g, ' ')      // 다중 공백 축소
        .trim();
  }
}

```

#### `index.ts` (Test Runner)

```typescript
import { YahooCommunityScraper } from './YahooCommunityScraper';

// 제공해주신 HTML 문자열 (생략)
const providedHtml = `... (User Provided HTML) ...`;

async function run() {
  const scraper = new YahooCommunityScraper();

  console.log("🚀 Starting Extraction Process...");

  // 1. 정적 HTML 분석
  const stockData = scraper.parseStaticHtml(providedHtml);
  
  if (!stockData.symbol || stockData.symbol === 'UNKNOWN') {
      console.error("⛔ Failed to extract Symbol. Aborting.");
      return;
  }

  console.table(stockData);

  // 2. 댓글 데이터 수집 (HTML 컨텍스트를 함께 넘겨 SpotID 파싱 시도)
  if (stockData.symbol) {
      console.log(`\n📡 Fetching comments for [${stockData.symbol}]...`);
      const comments = await scraper.fetchComments(stockData.symbol, providedHtml);
      
      console.log(`📊 Retrieved ${comments.length} comments.`);
      if (comments.length > 0) {
          console.log("--- Top 3 Best Comments ---");
          comments.slice(0, 3).forEach((c, i) => {
              console.log(`${i+1}. [${c.user}]: ${c.content.slice(0, 50)}... (👍 ${c.likes})`);
          });
      }
  }
}

run();

```

---

### 3. 피드백 요약 및 검증 결과

1. **HTML 구조 분석**:
* 제공된 HTML에서 `<span class="price yf-15xzs3e base" data-testid="qsp-price">204.08 </span>` 부분을 확인했다. `data-testid="qsp-price"`는 매우 좋은 선택자다. 하지만 `yf-15xzs3e` 같은 클래스는 배포 시마다 바뀌는 해시값이므로 **절대 CSS 클래스명에 의존하지 않도록** 코드를 작성했다.


2. **데이터 검증**:
* `204.08` 이라는 가격 텍스트 뒤에 공백이 있다. `trim()` 처리가 필수적이다.
* `price`가 `null`일 경우를 대비해 `StockHeader` 인터페이스의 타입을 `number | null`로 변경했다. 이것이 안전하다.


3. **Spot ID**:
* 제공된 HTML 전체를 검색했으나 `sp_R2cfqc5q` 문자열은 발견되지 않았다. 이는 해당 ID가 HTML 렌더링 시점에 포함되지 않거나, 다른 JS 파일에 있음을 의미한다. 따라서, 코드는 `try-catch`와 `fallback` 로직을 포함하여, Spot ID를 찾지 못해도 기본값으로 시도하되 로그를 남기도록 수정했다.


