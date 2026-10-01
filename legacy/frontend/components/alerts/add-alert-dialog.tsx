'use client'

import { useState } from 'react'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from '@/components/ui/dialog'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { Plus } from 'lucide-react'
import { STOCK_SYMBOLS } from '@/lib/constants'
import type { AlertCondition, SignalType } from '@/lib/types/api'

interface AddAlertDialogProps {
  onAdd: (alert: Omit<AlertCondition, 'id'>) => void
}

export function AddAlertDialog({ onAdd }: AddAlertDialogProps) {
  const [open, setOpen] = useState(false)
  const [formData, setFormData] = useState({
    symbol: '',
    signal: '' as SignalType | '',
    confidenceMin: '',
    priceAbove: '',
    priceBelow: '',
    sentimentAbove: '',
    sentimentBelow: '',
    cooldownMinutes: '60',
  })

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault()

    if (!formData.symbol) {
      alert('종목을 선택해주세요')
      return
    }

    const conditions: AlertCondition['conditions'] = {}
    if (formData.signal) conditions.signal = formData.signal as SignalType
    if (formData.confidenceMin) conditions.confidenceMin = parseFloat(formData.confidenceMin) / 100
    if (formData.priceAbove) conditions.priceAbove = parseFloat(formData.priceAbove)
    if (formData.priceBelow) conditions.priceBelow = parseFloat(formData.priceBelow)
    if (formData.sentimentAbove) conditions.sentimentAbove = parseFloat(formData.sentimentAbove)
    if (formData.sentimentBelow) conditions.sentimentBelow = parseFloat(formData.sentimentBelow)

    const alert: Omit<AlertCondition, 'id'> = {
      symbol: formData.symbol,
      enabled: true,
      conditions,
      cooldownMinutes: parseInt(formData.cooldownMinutes),
    }

    onAdd(alert)
    setOpen(false)
    setFormData({
      symbol: '',
      signal: '',
      confidenceMin: '',
      priceAbove: '',
      priceBelow: '',
      sentimentAbove: '',
      sentimentBelow: '',
      cooldownMinutes: '60',
    })
  }

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger asChild>
        <Button>
          <Plus className="h-4 w-4 mr-2" />
          알림 추가
        </Button>
      </DialogTrigger>
      <DialogContent className="sm:max-w-[500px]">
        <form onSubmit={handleSubmit}>
          <DialogHeader>
            <DialogTitle>새 알림 추가</DialogTitle>
            <DialogDescription>조건이 충족되면 브라우저 알림을 받습니다</DialogDescription>
          </DialogHeader>
          <div className="grid gap-4 py-4 max-h-[60vh] overflow-y-auto">
            <div className="grid grid-cols-4 items-center gap-4">
              <Label htmlFor="symbol" className="text-right">
                종목 *
              </Label>
              <Select value={formData.symbol} onValueChange={(value) => setFormData({ ...formData, symbol: value })}>
                <SelectTrigger className="col-span-3">
                  <SelectValue placeholder="종목 선택" />
                </SelectTrigger>
                <SelectContent>
                  {STOCK_SYMBOLS.map((symbol) => (
                    <SelectItem key={symbol} value={symbol}>
                      {symbol}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>

            <div className="grid grid-cols-4 items-center gap-4">
              <Label htmlFor="signal" className="text-right">
                신호
              </Label>
              <Select value={formData.signal} onValueChange={(value: SignalType | '') => setFormData({ ...formData, signal: value })}>
                <SelectTrigger className="col-span-3">
                  <SelectValue placeholder="선택 안 함" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="">선택 안 함</SelectItem>
                  <SelectItem value="BUY">매수 (BUY)</SelectItem>
                  <SelectItem value="SELL">매도 (SELL)</SelectItem>
                  <SelectItem value="HOLD">보유 (HOLD)</SelectItem>
                </SelectContent>
              </Select>
            </div>

            <div className="grid grid-cols-4 items-center gap-4">
              <Label htmlFor="confidenceMin" className="text-right">
                최소 신뢰도 (%)
              </Label>
              <Input
                id="confidenceMin"
                type="number"
                step="1"
                min="0"
                max="100"
                className="col-span-3"
                placeholder="예: 70"
                value={formData.confidenceMin}
                onChange={(e) => setFormData({ ...formData, confidenceMin: e.target.value })}
              />
            </div>

            <div className="grid grid-cols-4 items-center gap-4">
              <Label htmlFor="priceAbove" className="text-right">
                가격 이상 ($)
              </Label>
              <Input
                id="priceAbove"
                type="number"
                step="0.01"
                className="col-span-3"
                placeholder="예: 200"
                value={formData.priceAbove}
                onChange={(e) => setFormData({ ...formData, priceAbove: e.target.value })}
              />
            </div>

            <div className="grid grid-cols-4 items-center gap-4">
              <Label htmlFor="priceBelow" className="text-right">
                가격 이하 ($)
              </Label>
              <Input
                id="priceBelow"
                type="number"
                step="0.01"
                className="col-span-3"
                placeholder="예: 150"
                value={formData.priceBelow}
                onChange={(e) => setFormData({ ...formData, priceBelow: e.target.value })}
              />
            </div>

            <div className="grid grid-cols-4 items-center gap-4">
              <Label htmlFor="sentimentAbove" className="text-right">
                감성 이상
              </Label>
              <Input
                id="sentimentAbove"
                type="number"
                step="0.1"
                min="-1"
                max="1"
                className="col-span-3"
                placeholder="예: 0.5"
                value={formData.sentimentAbove}
                onChange={(e) => setFormData({ ...formData, sentimentAbove: e.target.value })}
              />
            </div>

            <div className="grid grid-cols-4 items-center gap-4">
              <Label htmlFor="sentimentBelow" className="text-right">
                감성 이하
              </Label>
              <Input
                id="sentimentBelow"
                type="number"
                step="0.1"
                min="-1"
                max="1"
                className="col-span-3"
                placeholder="예: -0.5"
                value={formData.sentimentBelow}
                onChange={(e) => setFormData({ ...formData, sentimentBelow: e.target.value })}
              />
            </div>

            <div className="grid grid-cols-4 items-center gap-4">
              <Label htmlFor="cooldownMinutes" className="text-right">
                쿨다운 (분)
              </Label>
              <Input
                id="cooldownMinutes"
                type="number"
                step="1"
                min="1"
                className="col-span-3"
                value={formData.cooldownMinutes}
                onChange={(e) => setFormData({ ...formData, cooldownMinutes: e.target.value })}
                required
              />
            </div>
          </div>
          <DialogFooter>
            <Button type="submit">저장</Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  )
}
