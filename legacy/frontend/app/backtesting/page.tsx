'use client'

import { useState } from 'react'
import Link from 'next/link'
import { useQueries } from '@tanstack/react-query'
import { getSignalAccuracy, getSignalHistory } from '@/lib/api/signals'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { ArrowLeft, TrendingUp, Target, CheckCircle } from 'lucide-react'
import { STOCK_SYMBOLS } from '@/lib/constants'
import { AccuracyChart } from '@/components/backtesting/accuracy-chart'
import { SignalDistributionChart } from '@/components/backtesting/signal-distribution-chart'
import { SignalHistoryTable } from '@/components/backtesting/signal-history-table'
import { LoadingSkeleton } from '@/components/shared/loading-skeleton'

export default function BacktestingPage() {
  const [days, setDays] = useState(30)
  const [selectedSymbol, setSelectedSymbol] = useState<string>('ALL')

  // 모든 종목의 정확도 조회
  const accuracyQueries = useQueries({
    queries: STOCK_SYMBOLS.map((symbol) => ({
      queryKey: ['signal-accuracy', symbol, days],
      queryFn: () => getSignalAccuracy(symbol, days),
      staleTime: 10 * 60 * 1000,
    })),
  })

  // 선택된 종목의 이력 조회
  const historyQuery = useQueries({
    queries:
      selectedSymbol === 'ALL'
        ? STOCK_SYMBOLS.map((symbol) => ({
            queryKey: ['signal-history', symbol, days],
            queryFn: () => getSignalHistory(symbol, days),
            staleTime: 10 * 60 * 1000,
          }))
        : [
            {
              queryKey: ['signal-history', selectedSymbol, days],
              queryFn: () => getSignalHistory(selectedSymbol, days),
              staleTime: 10 * 60 * 1000,
            },
          ],
  })

  const isLoading = accuracyQueries.some((q) => q.isLoading) || historyQuery.some((q) => q.isLoading)
  const accuracyData = accuracyQueries.map((q) => q.data).filter(Boolean)
  const historyData = historyQuery
    .map((q) => q.data)
    .filter(Boolean)
    .flatMap((h) => h!.signals)
    .sort((a, b) => new Date(b.timestamp).getTime() - new Date(a.timestamp).getTime())

  // 전체 통계
  const totalSignals = accuracyData.reduce((sum, item) => sum + item!.total_signals, 0)
  const totalCorrect = accuracyData.reduce((sum, item) => sum + item!.correct_signals, 0)
  const avgAccuracy = totalSignals > 0 ? (totalCorrect / totalSignals) * 100 : 0

  const allSignalTypes = accuracyData.reduce(
    (acc, item) => {
      acc.BUY += item!.by_signal_type.BUY.count
      acc.SELL += item!.by_signal_type.SELL.count
      acc.HOLD += item!.by_signal_type.HOLD.count
      return acc
    },
    { BUY: 0, SELL: 0, HOLD: 0 }
  )

  if (isLoading) {
    return (
      <main className="min-h-screen bg-background">
        <div className="container mx-auto p-6 space-y-8">
          <LoadingSkeleton />
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            <LoadingSkeleton />
            <LoadingSkeleton />
          </div>
        </div>
      </main>
    )
  }

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
              <h1 className="text-3xl font-bold">백테스팅</h1>
              <p className="text-sm text-muted-foreground mt-1">신호 정확도 및 이력 분석</p>
            </div>
          </div>

          {/* 기간 선택 */}
          <Select value={days.toString()} onValueChange={(value) => setDays(parseInt(value))}>
            <SelectTrigger className="w-[180px]">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="7">최근 7일</SelectItem>
              <SelectItem value="30">최근 30일</SelectItem>
              <SelectItem value="60">최근 60일</SelectItem>
              <SelectItem value="90">최근 90일</SelectItem>
            </SelectContent>
          </Select>
        </div>

        {/* 전체 통계 */}
        <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
          <Card>
            <CardHeader className="flex flex-row items-center justify-between pb-2">
              <CardTitle className="text-sm font-medium">평균 정확도</CardTitle>
              <Target className="h-4 w-4 text-muted-foreground" />
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">{avgAccuracy.toFixed(1)}%</div>
              <p className="text-xs text-muted-foreground mt-2">
                {totalCorrect}/{totalSignals} 신호 정확
              </p>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="flex flex-row items-center justify-between pb-2">
              <CardTitle className="text-sm font-medium">총 신호 수</CardTitle>
              <TrendingUp className="h-4 w-4 text-muted-foreground" />
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">{totalSignals}</div>
              <p className="text-xs text-muted-foreground mt-2">
                BUY: {allSignalTypes.BUY}, SELL: {allSignalTypes.SELL}, HOLD: {allSignalTypes.HOLD}
              </p>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="flex flex-row items-center justify-between pb-2">
              <CardTitle className="text-sm font-medium">분석 기간</CardTitle>
              <CheckCircle className="h-4 w-4 text-muted-foreground" />
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">{days}일</div>
              <p className="text-xs text-muted-foreground mt-2">8개 종목 분석</p>
            </CardContent>
          </Card>
        </div>

        {/* 차트 */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          <AccuracyChart data={accuracyData as any} />
          <SignalDistributionChart data={accuracyData as any} />
        </div>

        {/* 신호 이력 */}
        <Card>
          <CardHeader>
            <div className="flex items-center justify-between">
              <CardTitle>신호 이력</CardTitle>
              <Select value={selectedSymbol} onValueChange={setSelectedSymbol}>
                <SelectTrigger className="w-[180px]">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="ALL">전체 종목</SelectItem>
                  {STOCK_SYMBOLS.map((symbol) => (
                    <SelectItem key={symbol} value={symbol}>
                      {symbol}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          </CardHeader>
          <CardContent>
            <SignalHistoryTable data={historyData as any} limit={50} />
          </CardContent>
        </Card>
      </div>
    </main>
  )
}
