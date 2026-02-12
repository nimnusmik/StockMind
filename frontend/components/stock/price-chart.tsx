'use client'

import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, Area, AreaChart } from 'recharts'
import type { PricePredictionResponse } from '@/lib/types/api'

interface PriceChartProps {
  data: PricePredictionResponse
}

export function PriceChart({ data }: PriceChartProps) {
  const { current_price, predicted_price, confidence_interval } = data

  const chartData = [
    {
      name: '현재가',
      price: current_price,
      predicted: null,
      lower: null,
      upper: null,
    },
    {
      name: '예측가',
      price: null,
      predicted: predicted_price,
      lower: confidence_interval.lower,
      upper: confidence_interval.upper,
    },
  ]

  return (
    <Card>
      <CardHeader>
        <CardTitle>가격 예측</CardTitle>
      </CardHeader>
      <CardContent>
        <div className="space-y-4">
          <div className="grid grid-cols-2 gap-4">
            <div>
              <p className="text-sm text-muted-foreground">현재가</p>
              <p className="text-2xl font-bold">${current_price.toFixed(2)}</p>
            </div>
            <div>
              <p className="text-sm text-muted-foreground">예측가</p>
              <p className="text-2xl font-bold text-primary">${predicted_price.toFixed(2)}</p>
            </div>
          </div>

          <div className="h-[200px]">
            <ResponsiveContainer width="100%" height="100%">
              <AreaChart data={chartData}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="name" />
                <YAxis domain={['dataMin - 1', 'dataMax + 1']} />
                <Tooltip />
                <Area
                  type="monotone"
                  dataKey="upper"
                  stroke="#8884d8"
                  fill="#8884d8"
                  fillOpacity={0.2}
                  strokeDasharray="3 3"
                />
                <Area
                  type="monotone"
                  dataKey="lower"
                  stroke="#8884d8"
                  fill="#8884d8"
                  fillOpacity={0.2}
                  strokeDasharray="3 3"
                />
                <Line type="monotone" dataKey="price" stroke="#10b981" strokeWidth={2} dot={{ r: 6 }} />
                <Line type="monotone" dataKey="predicted" stroke="#3b82f6" strokeWidth={2} dot={{ r: 6 }} />
                <Legend />
              </AreaChart>
            </ResponsiveContainer>
          </div>

          <div className="text-sm text-muted-foreground">
            <p>신뢰 구간: ${confidence_interval.lower.toFixed(2)} - ${confidence_interval.upper.toFixed(2)}</p>
          </div>
        </div>
      </CardContent>
    </Card>
  )
}
