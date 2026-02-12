'use client'

import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { Trash2 } from 'lucide-react'
import type { Holding } from '@/lib/types/api'
import { STOCK_INFO } from '@/lib/constants'

interface HoldingsTableProps {
  holdings: Holding[]
  currentPrices: Record<string, number>
  onRemove: (symbol: string) => void
}

export function HoldingsTable({ holdings, currentPrices, onRemove }: HoldingsTableProps) {
  if (holdings.length === 0) {
    return (
      <div className="text-center py-12 text-muted-foreground">
        <p>보유 종목이 없습니다</p>
        <p className="text-sm mt-2">거래를 추가하여 포트폴리오를 시작하세요</p>
      </div>
    )
  }

  return (
    <div className="border rounded-lg overflow-hidden">
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead>종목</TableHead>
            <TableHead className="text-right">수량</TableHead>
            <TableHead className="text-right">평균단가</TableHead>
            <TableHead className="text-right">현재가</TableHead>
            <TableHead className="text-right">평가액</TableHead>
            <TableHead className="text-right">수익률</TableHead>
            <TableHead className="w-[50px]"></TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {holdings.map((holding) => {
            const currentPrice = currentPrices[holding.symbol] || holding.avgPrice
            const totalValue = holding.quantity * currentPrice
            const totalCost = holding.quantity * holding.avgPrice
            const profit = totalValue - totalCost
            const profitPct = (profit / totalCost) * 100
            const stockInfo = STOCK_INFO[holding.symbol as keyof typeof STOCK_INFO]

            return (
              <TableRow key={holding.symbol}>
                <TableCell className="font-medium">
                  <div>
                    <p className="font-bold">{holding.symbol}</p>
                    <p className="text-sm text-muted-foreground">{stockInfo?.name}</p>
                  </div>
                </TableCell>
                <TableCell className="text-right">{holding.quantity}</TableCell>
                <TableCell className="text-right">${holding.avgPrice.toFixed(2)}</TableCell>
                <TableCell className="text-right">${currentPrice.toFixed(2)}</TableCell>
                <TableCell className="text-right font-semibold">${totalValue.toFixed(2)}</TableCell>
                <TableCell className="text-right">
                  <div>
                    <Badge
                      variant={profit >= 0 ? 'default' : 'destructive'}
                      className={profit >= 0 ? 'bg-green-600' : ''}
                    >
                      {profit >= 0 ? '+' : ''}${profit.toFixed(2)}
                    </Badge>
                    <p
                      className={`text-sm mt-1 ${
                        profitPct >= 0 ? 'text-green-600' : 'text-red-600'
                      }`}
                    >
                      {profitPct >= 0 ? '+' : ''}
                      {profitPct.toFixed(2)}%
                    </p>
                  </div>
                </TableCell>
                <TableCell>
                  <Button variant="ghost" size="icon" onClick={() => onRemove(holding.symbol)}>
                    <Trash2 className="h-4 w-4 text-destructive" />
                  </Button>
                </TableCell>
              </TableRow>
            )
          })}
        </TableBody>
      </Table>
    </div>
  )
}
