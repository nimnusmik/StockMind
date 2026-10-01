'use client'

import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'
import { Badge } from '@/components/ui/badge'
import { CheckCircle, XCircle, Clock } from 'lucide-react'
import type { SignalHistoryItem } from '@/lib/types/api'
import { format } from 'date-fns'
import { SIGNAL_COLORS, SIGNAL_ICONS, SIGNAL_LABELS } from '@/lib/constants'
import { cn } from '@/lib/utils'

interface SignalHistoryTableProps {
  data: SignalHistoryItem[]
  limit?: number
}

export function SignalHistoryTable({ data, limit = 20 }: SignalHistoryTableProps) {
  const displayData = limit ? data.slice(0, limit) : data

  if (displayData.length === 0) {
    return (
      <div className="text-center py-12 text-muted-foreground">
        <p>신호 이력이 없습니다</p>
      </div>
    )
  }

  return (
    <div className="border rounded-lg overflow-hidden">
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead>날짜</TableHead>
            <TableHead>종목</TableHead>
            <TableHead>신호</TableHead>
            <TableHead className="text-right">예측가</TableHead>
            <TableHead className="text-right">실제가</TableHead>
            <TableHead className="text-right">예측 변동</TableHead>
            <TableHead className="text-right">실제 변동</TableHead>
            <TableHead className="text-center">정확도</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {displayData.map((item) => (
            <TableRow key={item.id}>
              <TableCell>{format(new Date(item.timestamp), 'yyyy-MM-dd HH:mm')}</TableCell>
              <TableCell className="font-bold">{item.symbol}</TableCell>
              <TableCell>
                <Badge className={cn('text-sm', SIGNAL_COLORS[item.signal])}>
                  {SIGNAL_ICONS[item.signal]} {SIGNAL_LABELS[item.signal]}
                </Badge>
              </TableCell>
              <TableCell className="text-right">${item.predicted_price.toFixed(2)}</TableCell>
              <TableCell className="text-right">
                {item.actual_price ? `$${item.actual_price.toFixed(2)}` : '-'}
              </TableCell>
              <TableCell className="text-right">
                <span
                  className={cn(
                    'font-semibold',
                    item.predicted_change_pct > 0 ? 'text-green-600' : 'text-red-600'
                  )}
                >
                  {item.predicted_change_pct > 0 ? '+' : ''}
                  {item.predicted_change_pct.toFixed(2)}%
                </span>
              </TableCell>
              <TableCell className="text-right">
                {item.actual_change_pct !== null ? (
                  <span
                    className={cn(
                      'font-semibold',
                      item.actual_change_pct > 0 ? 'text-green-600' : 'text-red-600'
                    )}
                  >
                    {item.actual_change_pct > 0 ? '+' : ''}
                    {item.actual_change_pct.toFixed(2)}%
                  </span>
                ) : (
                  '-'
                )}
              </TableCell>
              <TableCell className="text-center">
                {item.is_correct === null ? (
                  <Clock className="h-5 w-5 text-gray-400 inline" />
                ) : item.is_correct ? (
                  <CheckCircle className="h-5 w-5 text-green-600 inline" />
                ) : (
                  <XCircle className="h-5 w-5 text-red-600 inline" />
                )}
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </div>
  )
}
