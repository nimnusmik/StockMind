'use client'

import { useTradingSignals } from '@/lib/hooks/use-trading-signal'
import { STOCK_SYMBOLS } from '@/lib/constants'
import { StockSignalCard } from './stock-signal-card'
import { SignalCardSkeleton } from '@/components/shared/loading-skeleton'
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert'
import { AlertCircle, RefreshCw } from 'lucide-react'

export function SignalOverviewGrid() {
  const queries = useTradingSignals(STOCK_SYMBOLS as unknown as string[])

  // 에러 체크
  const hasError = queries.some((query) => query.isError)
  const allLoading = queries.every((query) => query.isLoading)

  if (hasError) {
    return (
      <Alert variant="destructive">
        <AlertCircle className="h-4 w-4" />
        <AlertTitle>API 연결 오류</AlertTitle>
        <AlertDescription>
          백엔드 API에 연결할 수 없습니다. 서버가 실행 중인지 확인해주세요.
          <br />
          <code className="text-xs">http://localhost:8001</code>
        </AlertDescription>
      </Alert>
    )
  }

  return (
    <div className="space-y-6">
      {/* 헤더 */}
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-3xl font-bold tracking-tight">매매 신호 대시보드</h2>
          <p className="text-muted-foreground">AI 기반 실시간 주식 매매 신호</p>
        </div>
        <div className="flex items-center gap-2 text-sm text-muted-foreground">
          <RefreshCw className="h-4 w-4 animate-spin" />
          30초마다 자동 업데이트
        </div>
      </div>

      {/* 신호 카드 그리드 */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
        {queries.map((query, index) => {
          const symbol = STOCK_SYMBOLS[index]

          if (query.isLoading || allLoading) {
            return <SignalCardSkeleton key={symbol} />
          }

          if (query.isError || !query.data) {
            return (
              <Alert key={symbol} variant="destructive" className="p-4">
                <AlertCircle className="h-4 w-4" />
                <AlertTitle className="text-sm font-semibold">{symbol}</AlertTitle>
                <AlertDescription className="text-xs">데이터 로드 실패</AlertDescription>
              </Alert>
            )
          }

          return <StockSignalCard key={symbol} data={query.data} />
        })}
      </div>
    </div>
  )
}
