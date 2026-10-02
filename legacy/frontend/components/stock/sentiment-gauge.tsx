'use client'

import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { RadialBarChart, RadialBar, Legend, ResponsiveContainer, PolarAngleAxis } from 'recharts'
import type { CombinedSentimentResponse } from '@/lib/types/api'

interface SentimentGaugeProps {
  data: CombinedSentimentResponse
}

export function SentimentGauge({ data }: SentimentGaugeProps) {
  const { composite_score, confidence_level, news_sentiment, community_sentiment, news_weight, community_weight } = data

  // 감성 점수를 0-100 스케일로 변환 (-1~1 → 0~100)
  const normalizedScore = ((composite_score + 1) / 2) * 100

  const combined_label = composite_score > 0.3 ? '긍정적' : composite_score < -0.3 ? '부정적' : '중립'

  const chartData = [
    {
      name: '통합 감성',
      value: normalizedScore,
      fill: composite_score > 0.3 ? '#10b981' : composite_score < -0.3 ? '#ef4444' : '#f59e0b',
    },
  ]

  const getSentimentColor = (score: number) => {
    if (score > 0.3) return 'text-green-600'
    if (score < -0.3) return 'text-red-600'
    return 'text-yellow-600'
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle>통합 감성 분석</CardTitle>
      </CardHeader>
      <CardContent>
        <div className="space-y-6">
          <div className="h-[200px] flex items-center justify-center">
            <ResponsiveContainer width="100%" height="100%">
              <RadialBarChart
                cx="50%"
                cy="50%"
                innerRadius="60%"
                outerRadius="80%"
                data={chartData}
                startAngle={180}
                endAngle={0}
              >
                <PolarAngleAxis type="number" domain={[0, 100]} angleAxisId={0} tick={false} />
                <RadialBar background dataKey="value" cornerRadius={10} />
              </RadialBarChart>
            </ResponsiveContainer>
            <div className="absolute text-center">
              <p className={`text-3xl font-bold ${getSentimentColor(composite_score)}`}>
                {composite_score > 0 ? '+' : ''}
                {composite_score.toFixed(2)}
              </p>
              <p className="text-sm text-muted-foreground">{combined_label}</p>
            </div>
          </div>

          <div className="grid grid-cols-2 gap-4">
            <div className="space-y-1">
              <p className="text-sm font-medium">뉴스 감성 ({(news_weight * 100).toFixed(0)}%)</p>
              <div className="flex items-center gap-2">
                <span className={`text-lg font-semibold ${getSentimentColor(news_sentiment)}`}>
                  {news_sentiment > 0 ? '+' : ''}
                  {news_sentiment.toFixed(2)}
                </span>
                <span className="text-xs text-muted-foreground">
                  신뢰 {confidence_level.toFixed(0)}%
                </span>
              </div>
            </div>

            <div className="space-y-1">
              <p className="text-sm font-medium">커뮤니티 감성 ({(community_weight * 100).toFixed(0)}%)</p>
              <div className="flex items-center gap-2">
                <span className={`text-lg font-semibold ${getSentimentColor(community_sentiment)}`}>
                  {community_sentiment > 0 ? '+' : ''}
                  {community_sentiment.toFixed(2)}
                </span>
              </div>
            </div>
          </div>
        </div>
      </CardContent>
    </Card>
  )
}
