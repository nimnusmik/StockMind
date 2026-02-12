'use client'

import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { Trash2 } from 'lucide-react'
import type { Transaction } from '@/lib/types/api'
import { format } from 'date-fns'

interface TransactionsTableProps {
  transactions: Transaction[]
  onRemove: (id: string) => void
}

export function TransactionsTable({ transactions, onRemove }: TransactionsTableProps) {
  if (transactions.length === 0) {
    return (
      <div className="text-center py-12 text-muted-foreground">
        <p>거래 내역이 없습니다</p>
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
            <TableHead>거래 유형</TableHead>
            <TableHead className="text-right">수량</TableHead>
            <TableHead className="text-right">가격</TableHead>
            <TableHead className="text-right">총액</TableHead>
            <TableHead className="w-[50px]"></TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {transactions.map((transaction) => {
            const totalAmount = transaction.quantity * transaction.price

            return (
              <TableRow key={transaction.id}>
                <TableCell>{format(new Date(transaction.date), 'yyyy-MM-dd')}</TableCell>
                <TableCell className="font-bold">{transaction.symbol}</TableCell>
                <TableCell>
                  <Badge variant={transaction.type === 'BUY' ? 'default' : 'destructive'}>
                    {transaction.type === 'BUY' ? '매수' : '매도'}
                  </Badge>
                </TableCell>
                <TableCell className="text-right">{transaction.quantity}</TableCell>
                <TableCell className="text-right">${transaction.price.toFixed(2)}</TableCell>
                <TableCell className="text-right font-semibold">${totalAmount.toFixed(2)}</TableCell>
                <TableCell>
                  <Button variant="ghost" size="icon" onClick={() => onRemove(transaction.id)}>
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
