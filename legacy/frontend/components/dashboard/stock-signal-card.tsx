'use client'

import Link from 'next/link'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { ArrowUp, ArrowDown, TrendingUp } from 'lucide-react'
import type { TradingSignalResponse } from '@/lib/types/api'
import { STOCK_INFO, SIGNAL_COLORS, SIGNAL_ICONS, SIGNAL_LABELS } from '@/lib/constants'
import { cn } from '@/lib/utils'

interface StockSignalCardProps {
  data: TradingSignalResponse
}

export function StockSignalCard({ data }: StockSignalCardProps) {
  const { symbol, signal, confidence, predicted_price, predicted_change_pct, sentiment_score } = data
  const stockInfo = STOCK_INFO[symbol as keyof typeof STOCK_INFO]
  const isPositive = predicted_change_pct > 0

  return (
    <Link href={`/stock/${symbol}`}>
      <Card className="hover:shadow-lg transition-shadow cursor-pointer">
        <CardHeader className="pb-3">
          <div className="flex items-center justify-between">
            <CardTitle className="text-lg font-bold">{symbol}</CardTitle>
            <Badge className={cn('font-semibold', SIGNAL_COLORS[signal])}>
              {SIGNAL_ICONS[signal]} {SIGNAL_LABELS[signal]}
            </Badge>
          </div>
          <p className="text-sm text-muted-foreground">{stockInfo.name}</p>
        </CardHeader>
        <CardContent className="space-y-3">
          {/* 가격 정보 */}
          <div>
            <div className="flex items-baseline gap-2">
              <span className="text-2xl font-bold">${predicted_price.toFixed(2)}</span>
              <span
                className={cn(
                  'text-sm font-semibold flex items-center gap-1',
                  isPositive ? 'text-green-600' : 'text-red-600'
                )}
              >
                {isPositive ? <ArrowUp className="h-4 w-4" /> : <ArrowDown className="h-4 w-4" />}
                {predicted_change_pct > 0 ? '+' : ''}
                {predicted_change_pct.toFixed(2)}%
              </span>
            </div>
          </div>

          {/* 신뢰도 */}
          <div className="space-y-1">
            <div className="flex items-center justify-between text-sm">
              <span className="text-muted-foreground">신뢰도</span>
              <span className="font-semibold">{confidence.toFixed(0)}%</span>
            </div>
            <div className="w-full bg-gray-200 rounded-full h-2">
              <div
                className={cn(
                  'h-2 rounded-full transition-all',
                  confidence > 70 ? 'bg-green-500' : confidence > 50 ? 'bg-yellow-500' : 'bg-red-500'
                )}
                style={{ width: `${Math.min(confidence, 100)}%` }}
              />
            </div>
          </div>

          {/* 감성 점수 */}
          <div className="flex items-center justify-between text-sm pt-2 border-t">
            <span className="text-muted-foreground flex items-center gap-1">
              <TrendingUp className="h-4 w-4" />
              감성 점수
            </span>
            <span
              className={cn(
                'font-semibold',
                sentiment_score > 0.3 ? 'text-green-600' : sentiment_score < -0.3 ? 'text-red-600' : 'text-gray-600'
              )}
            >
              {sentiment_score > 0 ? '+' : ''}
              {sentiment_score.toFixed(2)}
            </span>
          </div>
        </CardContent>
      </Card>
    </Link>
  )
}
