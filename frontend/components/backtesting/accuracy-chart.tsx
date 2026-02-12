'use client'

import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, Cell } from 'recharts'
import type { SignalAccuracyResponse } from '@/lib/types/api'

interface AccuracyChartProps {
  data: SignalAccuracyResponse[]
}

export function AccuracyChart({ data }: AccuracyChartProps) {
  const chartData = data.map((item) => ({
    symbol: item.symbol,
    accuracy: (item.accuracy * 100).toFixed(1),
    totalSignals: item.total_signals,
  }))

  const getColor = (accuracy: number) => {
    if (accuracy >= 70) return '#10b981' // green
    if (accuracy >= 50) return '#f59e0b' // yellow
    return '#ef4444' // red
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle>종목별 신호 정확도</CardTitle>
      </CardHeader>
      <CardContent>
        <div className="h-[300px]">
          <ResponsiveContainer width="100%" height="100%">
            <BarChart data={chartData}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="symbol" />
              <YAxis domain={[0, 100]} label={{ value: '정확도 (%)', angle: -90, position: 'insideLeft' }} />
              <Tooltip
                content={({ active, payload }) => {
                  if (active && payload && payload.length) {
                    return (
                      <div className="bg-white p-3 border rounded shadow-lg">
                        <p className="font-bold">{payload[0].payload.symbol}</p>
                        <p className="text-sm">정확도: {payload[0].value}%</p>
                        <p className="text-sm text-muted-foreground">총 신호: {payload[0].payload.totalSignals}개</p>
                      </div>
                    )
                  }
                  return null
                }}
              />
              <Legend />
              <Bar dataKey="accuracy" name="정확도 (%)">
                {chartData.map((entry, index) => (
                  <Cell key={`cell-${index}`} fill={getColor(parseFloat(entry.accuracy))} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </div>
      </CardContent>
    </Card>
  )
}
