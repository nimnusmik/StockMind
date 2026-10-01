'use client'

import { useEffect, useMemo } from 'react'
import Link from 'next/link'
import { usePortfolioStore } from '@/lib/stores/portfolio-store'
import { useTradingSignals } from '@/lib/hooks/use-trading-signal'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { HoldingsTable } from '@/components/portfolio/holdings-table'
import { AddTransactionDialog } from '@/components/portfolio/add-transaction-dialog'
import { TransactionsTable } from '@/components/portfolio/transactions-table'
import { ArrowLeft, TrendingUp, TrendingDown, DollarSign, Briefcase } from 'lucide-react'

export default function PortfolioPage() {
  const { holdings, transactions, isLoaded, loadData, addTransaction, removeHolding, removeTransaction } =
    usePortfolioStore()

  // 초기 로드
  useEffect(() => {
    if (!isLoaded) {
      loadData()
    }
  }, [isLoaded, loadData])

  // 보유 종목 현재가 조회
  const symbols = holdings.map((h) => h.symbol)
  const priceQueries = useTradingSignals(symbols)

  // 현재가 맵 생성
  const currentPrices = useMemo(() => {
    const prices: Record<string, number> = {}
    priceQueries.forEach((query, index) => {
      if (query.data) {
        prices[symbols[index]] = query.data.predicted_price
      }
    })
    return prices
  }, [priceQueries, symbols])

  // 포트폴리오 통계
  const totalValue = holdings.reduce((total, holding) => {
    const currentPrice = currentPrices[holding.symbol] || holding.avgPrice
    return total + holding.quantity * currentPrice
  }, 0)

  const totalCost = holdings.reduce((total, holding) => {
    return total + holding.quantity * holding.avgPrice
  }, 0)

  const totalProfit = totalValue - totalCost
  const totalProfitPct = totalCost > 0 ? (totalProfit / totalCost) * 100 : 0

  return (
    <main className="min-h-screen bg-background">
      <div className="container mx-auto p-6 space-y-8">
        {/* 헤더 */}
        <div className="flex items-center gap-4">
          <Link href="/">
            <Button variant="outline" size="icon">
              <ArrowLeft className="h-4 w-4" />
            </Button>
          </Link>
          <div>
            <h1 className="text-3xl font-bold">포트폴리오</h1>
            <p className="text-sm text-muted-foreground mt-1">나의 보유 종목 및 거래 내역</p>
          </div>
        </div>

        {/* 포트폴리오 요약 */}
        <div className="grid grid-cols-1 md:grid-cols-4 gap-6">
          <Card>
            <CardHeader className="flex flex-row items-center justify-between pb-2">
              <CardTitle className="text-sm font-medium">총 평가액</CardTitle>
              <DollarSign className="h-4 w-4 text-muted-foreground" />
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">${totalValue.toFixed(2)}</div>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="flex flex-row items-center justify-between pb-2">
              <CardTitle className="text-sm font-medium">총 투자액</CardTitle>
              <Briefcase className="h-4 w-4 text-muted-foreground" />
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">${totalCost.toFixed(2)}</div>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="flex flex-row items-center justify-between pb-2">
              <CardTitle className="text-sm font-medium">총 수익</CardTitle>
              {totalProfit >= 0 ? (
                <TrendingUp className="h-4 w-4 text-green-600" />
              ) : (
                <TrendingDown className="h-4 w-4 text-red-600" />
              )}
            </CardHeader>
            <CardContent>
              <div className={`text-2xl font-bold ${totalProfit >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                {totalProfit >= 0 ? '+' : ''}${totalProfit.toFixed(2)}
              </div>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="flex flex-row items-center justify-between pb-2">
              <CardTitle className="text-sm font-medium">수익률</CardTitle>
              {totalProfitPct >= 0 ? (
                <TrendingUp className="h-4 w-4 text-green-600" />
              ) : (
                <TrendingDown className="h-4 w-4 text-red-600" />
              )}
            </CardHeader>
            <CardContent>
              <div className={`text-2xl font-bold ${totalProfitPct >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                {totalProfitPct >= 0 ? '+' : ''}
                {totalProfitPct.toFixed(2)}%
              </div>
            </CardContent>
          </Card>
        </div>

        {/* 보유 종목 및 거래 내역 */}
        <Tabs defaultValue="holdings" className="space-y-4">
          <div className="flex items-center justify-between">
            <TabsList>
              <TabsTrigger value="holdings">보유 종목</TabsTrigger>
              <TabsTrigger value="transactions">거래 내역</TabsTrigger>
            </TabsList>
            <AddTransactionDialog onAdd={addTransaction} />
          </div>

          <TabsContent value="holdings">
            <Card>
              <CardHeader>
                <CardTitle>보유 종목 ({holdings.length})</CardTitle>
              </CardHeader>
              <CardContent>
                <HoldingsTable holdings={holdings} currentPrices={currentPrices} onRemove={removeHolding} />
              </CardContent>
            </Card>
          </TabsContent>

          <TabsContent value="transactions">
            <Card>
              <CardHeader>
                <CardTitle>거래 내역 ({transactions.length})</CardTitle>
              </CardHeader>
              <CardContent>
                <TransactionsTable transactions={transactions} onRemove={removeTransaction} />
              </CardContent>
            </Card>
          </TabsContent>
        </Tabs>
      </div>
    </main>
  )
}
