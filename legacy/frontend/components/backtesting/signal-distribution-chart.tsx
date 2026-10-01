'use client'

import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { PieChart, Pie, Cell, ResponsiveContainer, Legend, Tooltip } from 'recharts'
import type { SignalAccuracyResponse } from '@/lib/types/api'

interface SignalDistributionChartProps {
  data: SignalAccuracyResponse[]
}

export function SignalDistributionChart({ data }: SignalDistributionChartProps) {
  // 전체 신호 타입별 집계
  const totals = data.reduce(
    (acc, item) => {
      acc.BUY += item.by_signal_type.BUY.count
      acc.SELL += item.by_signal_type.SELL.count
      acc.HOLD += item.by_signal_type.HOLD.count
      return acc
    },
    { BUY: 0, SELL: 0, HOLD: 0 }
  )

  const chartData = [
    { name: '매수 (BUY)', value: totals.BUY, fill: '#10b981' },
    { name: '매도 (SELL)', value: totals.SELL, fill: '#ef4444' },
    { name: '보유 (HOLD)', value: totals.HOLD, fill: '#f59e0b' },
  ]

  return (
    <Card>
      <CardHeader>
        <CardTitle>신호 분포</CardTitle>
      </CardHeader>
      <CardContent>
        <div className="h-[300px]">
          <ResponsiveContainer width="100%" height="100%">
            <PieChart>
              <Pie
                data={chartData}
                cx="50%"
                cy="50%"
                labelLine={false}
                label={({ name, percent }) => `${name} ${(percent * 100).toFixed(0)}%`}
                outerRadius={100}
                fill="#8884d8"
                dataKey="value"
              >
                {chartData.map((entry, index) => (
                  <Cell key={`cell-${index}`} fill={entry.fill} />
                ))}
              </Pie>
              <Tooltip />
              <Legend />
            </PieChart>
          </ResponsiveContainer>
        </div>
      </CardContent>
    </Card>
  )
}
