'use client'

import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { TrendingUp, TrendingDown, Minus, MessageSquare, Users, Activity } from 'lucide-react'
import type { BuzzMetrics } from '@/lib/types/api'

interface BuzzIndicatorsProps {
  data: BuzzMetrics
}

export function BuzzIndicators({ data }: BuzzIndicatorsProps) {
  const { comment_volume, unique_users, avg_sentiment, buzz_score, trend } = data

  const getTrendIcon = () => {
    switch (trend) {
      case 'rising':
        return <TrendingUp className="h-5 w-5 text-green-600" />
      case 'falling':
        return <TrendingDown className="h-5 w-5 text-red-600" />
      default:
        return <Minus className="h-5 w-5 text-gray-600" />
    }
  }

  const getTrendColor = () => {
    switch (trend) {
      case 'rising':
        return 'bg-green-50 text-green-700 border-green-200'
      case 'falling':
        return 'bg-red-50 text-red-700 border-red-200'
      default:
        return 'bg-gray-50 text-gray-700 border-gray-200'
    }
  }

  return (
    <Card>
      <CardHeader>
        <div className="flex items-center justify-between">
          <CardTitle>커뮤니티 버즈</CardTitle>
          <Badge className={getTrendColor()}>
            {getTrendIcon()}
            <span className="ml-1 capitalize">{trend}</span>
          </Badge>
        </div>
      </CardHeader>
      <CardContent>
        <div className="grid grid-cols-2 gap-6">
          {/* 댓글 볼륨 */}
          <div className="space-y-2">
            <div className="flex items-center gap-2 text-muted-foreground">
              <MessageSquare className="h-4 w-4" />
              <span className="text-sm">댓글 수</span>
            </div>
            <p className="text-3xl font-bold">{comment_volume.toLocaleString()}</p>
          </div>

          {/* 활성 사용자 */}
          <div className="space-y-2">
            <div className="flex items-center gap-2 text-muted-foreground">
              <Users className="h-4 w-4" />
              <span className="text-sm">활성 사용자</span>
            </div>
            <p className="text-3xl font-bold">{unique_users.toLocaleString()}</p>
          </div>

          {/* 평균 감성 */}
          <div className="space-y-2">
            <div className="flex items-center gap-2 text-muted-foreground">
              <Activity className="h-4 w-4" />
              <span className="text-sm">평균 감성</span>
            </div>
            <p
              className={`text-3xl font-bold ${
                avg_sentiment > 0.3 ? 'text-green-600' : avg_sentiment < -0.3 ? 'text-red-600' : 'text-gray-600'
              }`}
            >
              {avg_sentiment > 0 ? '+' : ''}
              {avg_sentiment.toFixed(2)}
            </p>
          </div>

          {/* 버즈 점수 */}
          <div className="space-y-2">
            <div className="flex items-center gap-2 text-muted-foreground">
              <TrendingUp className="h-4 w-4" />
              <span className="text-sm">버즈 점수</span>
            </div>
            <p className="text-3xl font-bold text-primary">{buzz_score.toFixed(1)}</p>
          </div>
        </div>

        <div className="mt-6 pt-4 border-t">
          <p className="text-xs text-muted-foreground">버즈 점수는 댓글 볼륨과 감성을 종합한 지표입니다</p>
        </div>
      </CardContent>
    </Card>
  )
}
