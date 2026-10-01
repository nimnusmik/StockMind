# StockMind Frontend

AI 기반 주식 매매 신호 및 감성 분석 플랫폼의 웹 대시보드

## 기술 스택

- **Framework**: Next.js 14 (App Router)
- **Language**: TypeScript
- **Styling**: Tailwind CSS + shadcn/ui
- **State Management**: React Query + Zustand
- **Charts**: Recharts
- **Storage**: IndexedDB (idb)

## 주요 기능

### 1. 대시보드 (/)
- 8개 종목 실시간 매매 신호
- 30초마다 자동 업데이트
- BUY/SELL/HOLD 신호 표시
- 신뢰도 및 감성 점수

### 2. 종목 상세 (/stock/[symbol])
- 가격 예측 차트 (Recharts LineChart)
- 통합 감성 분석 (Recharts RadialBar)
- 커뮤니티 버즈 지표
- 매매 신호 상세 정보

### 3. 포트폴리오 (/portfolio)
- 보유 종목 관리 (IndexedDB)
- 거래 내역 추적
- 수익률 계산
- 실시간 평가액

### 4. 백테스팅 (/backtesting)
- 신호 정확도 분석
- 종목별 성과 비교 (Bar Chart)
- 신호 분포 (Pie Chart)
- 신호 이력 테이블

### 5. 알림 (/alerts)
- 브라우저 알림 (Notification API)
- 조건 기반 알림 (신호, 가격, 감성)
- 쿨다운 설정
- 알림 활성화/비활성화

## 설치 및 실행

```bash
# 의존성 설치
npm install

# 개발 서버 실행
npm run dev

# 프로덕션 빌드
npm run build
npm run start
```

## 환경 변수

`.env.local` 파일을 생성하세요:

```env
NEXT_PUBLIC_API_URL=http://localhost:8001
NEXT_PUBLIC_SITE_URL=http://localhost:3000
```

## 프로젝트 구조

```
frontend/
├── app/                      # Next.js App Router
│   ├── (pages)
│   │   ├── page.tsx         # 대시보드
│   │   ├── portfolio/
│   │   ├── backtesting/
│   │   └── alerts/
│   ├── stock/[symbol]/      # 동적 라우트
│   ├── layout.tsx           # Root 레이아웃
│   └── providers.tsx        # React Query Provider
│
├── components/
│   ├── ui/                  # shadcn/ui
│   ├── dashboard/
│   ├── stock/
│   ├── portfolio/
│   ├── backtesting/
│   ├── alerts/
│   └── shared/
│
├── lib/
│   ├── api/                 # API 클라이언트
│   ├── hooks/               # React Query 훅
│   ├── stores/              # Zustand 스토어
│   ├── types/               # TypeScript 타입
│   └── utils/               # 유틸리티 함수
│
└── public/                  # 정적 파일
```

## API 연동

백엔드 API와 연동되며, 다음 엔드포인트를 사용합니다:

- `GET /api/v1/signals/{symbol}` - 매매 신호
- `GET /api/v1/predictions/{symbol}/price` - 가격 예측
- `GET /api/v1/sentiment/{symbol}/combined` - 통합 감성
- `GET /api/v1/buzz/{symbol}` - 커뮤니티 버즈
- `GET /api/v1/signals/{symbol}/accuracy` - 신호 정확도
- `GET /api/v1/signals/{symbol}/history` - 신호 이력

## React Query 설정

### 캐싱 전략
- **신호**: 5분 staleTime, 30초 refetchInterval
- **예측**: 5분 staleTime, 30초 refetchInterval
- **감성**: 10분 staleTime
- **버즈**: 15분 staleTime
- **정확도**: 10분 staleTime

## 브라우저 지원

- Chrome 90+
- Firefox 88+
- Safari 14+
- Edge 90+

## 라이선스

MIT
