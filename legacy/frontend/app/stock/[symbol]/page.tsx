'use client'

import { use } from 'react'
import Link from 'next/link'
import { useTradingSignal } from '@/lib/hooks/use-trading-signal'
import { usePricePrediction } from '@/lib/hooks/use-predictions'
import { useCombinedSentiment, useBuzzMetrics } from '@/lib/hooks/use-sentiment'
import { PriceChart } from '@/components/stock/price-chart'
import { SentimentGauge } from '@/components/stock/sentiment-gauge'
import { BuzzIndicators } from '@/components/stock/buzz-indicators'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { Alert, AlertDescription } from '@/components/ui/alert'
import { Button } from '@/components/ui/button'
import { ArrowLeft, AlertCircle } from 'lucide-react'
import { STOCK_INFO, SIGNAL_COLORS, SIGNAL_ICONS, SIGNAL_LABELS } from '@/lib/constants'
import { LoadingSkeleton } from '@/components/shared/loading-skeleton'
import { cn } from '@/lib/utils'

interface StockDetailPageProps {
  params: Promise<{ symbol: string }>
}

export default function StockDetailPage({ params }: StockDetailPageProps) {
  const { symbol } = use(params)
  const symbolUpper = symbol.toUpperCase()

  // 4개 API 병렬 호출
  const signalQuery = useTradingSignal(symbolUpper)
  const predictionQuery = usePricePrediction(symbolUpper)
  const sentimentQuery = useCombinedSentiment(symbolUpper)
  const buzzQuery = useBuzzMetrics(symbolUpper)

  const stockInfo = STOCK_INFO[symbolUpper as keyof typeof STOCK_INFO]

  // 로딩 상태
  const isLoading =
    signalQuery.isLoading || predictionQuery.isLoading || sentimentQuery.isLoading || buzzQuery.isLoading

  // 에러 상태
  const hasError = signalQuery.isError || predictionQuery.isError || sentimentQuery.isError || buzzQuery.isError

  if (isLoading) {
    return (
      <main className="min-h-screen bg-background">
        <div className="container mx-auto p-6 space-y-8">
          <LoadingSkeleton />
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            <LoadingSkeleton />
            <LoadingSkeleton />
            <LoadingSkeleton />
            <LoadingSkeleton />
          </div>
        </div>
      </main>
    )
  }

  if (hasError) {
    return (
      <main className="min-h-screen bg-background">
        <div className="container mx-auto p-6">
          <Alert variant="destructive">
            <AlertCircle className="h-4 w-4" />
            <AlertDescription>
              데이터를 불러오는 중 오류가 발생했습니다. 백엔드 API를 확인해주세요.
            </AlertDescription>
          </Alert>
        </div>
      </main>
    )
  }

  const signalData = signalQuery.data!
  const predictionData = predictionQuery.data!
  const sentimentData = sentimentQuery.data!
  const buzzData = buzzQuery.data!

  return (
    <main className="min-h-screen bg-background">
      <div className="container mx-auto p-6 space-y-8">
        {/* 헤더 */}
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-4">
            <Link href="/">
              <Button variant="outline" size="icon">
                <ArrowLeft className="h-4 w-4" />
              </Button>
            </Link>
            <div>
              <h1 className="text-3xl font-bold">
                {symbolUpper} <span className="text-muted-foreground">({stockInfo?.name})</span>
              </h1>
              <p className="text-sm text-muted-foreground mt-1">상세 분석 및 예측</p>
            </div>
          </div>
        </div>

        {/* 매매 신호 요약 */}
        <Card className="border-2">
          <CardHeader>
            <CardTitle>매매 신호</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="grid grid-cols-1 md:grid-cols-4 gap-6">
              <div>
                <p className="text-sm text-muted-foreground mb-2">신호</p>
                <Badge className={cn('text-lg py-2 px-4', SIGNAL_COLORS[signalData.signal])}>
                  {SIGNAL_ICONS[signalData.signal]} {SIGNAL_LABELS[signalData.signal]}
                </Badge>
              </div>
              <div>
                <p className="text-sm text-muted-foreground mb-2">신뢰도</p>
                <p className="text-2xl font-bold">{signalData.confidence.toFixed(0)}%</p>
              </div>
              <div>
                <p className="text-sm text-muted-foreground mb-2">예측 변동</p>
                <p
                  className={cn(
                    'text-2xl font-bold',
                    signalData.predicted_change_pct > 0 ? 'text-green-600' : 'text-red-600'
                  )}
                >
                  {signalData.predicted_change_pct > 0 ? '+' : ''}
                  {signalData.predicted_change_pct.toFixed(2)}%
                </p>
              </div>
              <div>
                <p className="text-sm text-muted-foreground mb-2">감성 점수</p>
                <p
                  className={cn(
                    'text-2xl font-bold',
                    signalData.sentiment_score > 0.3
                      ? 'text-green-600'
                      : signalData.sentiment_score < -0.3
                      ? 'text-red-600'
                      : 'text-gray-600'
                  )}
                >
                  {signalData.sentiment_score > 0 ? '+' : ''}
                  {signalData.sentiment_score.toFixed(2)}
                </p>
              </div>
            </div>

            {/* 근거 */}
            {signalData.supporting_factors && signalData.supporting_factors.length > 0 && (
              <div className="mt-6 pt-6 border-t">
                <p className="text-sm font-medium mb-3">지원 요인</p>
                <ul className="space-y-2">
                  {signalData.supporting_factors.map((factor, index) => (
                    <li key={index} className="text-sm text-muted-foreground flex items-start gap-2">
                      <span className="text-primary">•</span>
                      <span>{factor}</span>
                    </li>
                  ))}
                </ul>
              </div>
            )}
          </CardContent>
        </Card>

        {/* 차트 그리드 */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          <PriceChart data={predictionData} />
          <SentimentGauge data={sentimentData} />
          <BuzzIndicators data={buzzData} />

          {/* 업데이트 시간 */}
          <Card>
            <CardHeader>
              <CardTitle>업데이트 정보</CardTitle>
            </CardHeader>
            <CardContent className="space-y-3">
              <div className="flex items-center justify-between text-sm">
                <span className="text-muted-foreground">마지막 업데이트</span>
                <span className="font-medium">
                  {new Date(signalData.timestamp).toLocaleString('ko-KR')}
                </span>
              </div>
              <div className="flex items-center justify-between text-sm">
                <span className="text-muted-foreground">자동 갱신</span>
                <span className="font-medium text-green-600">30초마다</span>
              </div>
            </CardContent>
          </Card>
        </div>
      </div>
    </main>
  )
}
