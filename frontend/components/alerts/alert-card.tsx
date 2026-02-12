'use client'

import { Card, CardContent, CardHeader } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { Switch } from '@/components/ui/switch'
import { Trash2, Edit } from 'lucide-react'
import type { AlertCondition } from '@/lib/types/api'
import { STOCK_INFO, SIGNAL_LABELS } from '@/lib/constants'
import { formatDistanceToNow } from 'date-fns'
import { ko } from 'date-fns/locale'

interface AlertCardProps {
  alert: AlertCondition
  onToggle: () => void
  onEdit: () => void
  onDelete: () => void
}

export function AlertCard({ alert, onToggle, onEdit, onDelete }: AlertCardProps) {
  const stockInfo = STOCK_INFO[alert.symbol as keyof typeof STOCK_INFO]
  const { conditions } = alert

  const conditionText = []
  if (conditions.signal) conditionText.push(`신호: ${SIGNAL_LABELS[conditions.signal]}`)
  if (conditions.confidenceMin) conditionText.push(`신뢰도 ≥ ${(conditions.confidenceMin * 100).toFixed(0)}%`)
  if (conditions.priceAbove) conditionText.push(`가격 > $${conditions.priceAbove}`)
  if (conditions.priceBelow) conditionText.push(`가격 < $${conditions.priceBelow}`)
  if (conditions.sentimentAbove) conditionText.push(`감성 > ${conditions.sentimentAbove}`)
  if (conditions.sentimentBelow) conditionText.push(`감성 < ${conditions.sentimentBelow}`)

  return (
    <Card className={alert.enabled ? 'border-primary' : 'opacity-60'}>
      <CardHeader className="pb-3">
        <div className="flex items-start justify-between">
          <div className="flex-1">
            <div className="flex items-center gap-2 mb-1">
              <Badge variant="outline">{alert.symbol}</Badge>
              {alert.enabled && <Badge variant="default">활성</Badge>}
            </div>
            <p className="text-sm text-muted-foreground">{stockInfo?.name}</p>
          </div>
          <Switch checked={alert.enabled} onCheckedChange={onToggle} />
        </div>
      </CardHeader>
      <CardContent className="space-y-3">
        <div>
          <p className="text-sm font-medium mb-2">조건</p>
          <ul className="space-y-1">
            {conditionText.map((text, index) => (
              <li key={index} className="text-sm text-muted-foreground flex items-start gap-2">
                <span className="text-primary">•</span>
                <span>{text}</span>
              </li>
            ))}
          </ul>
        </div>

        <div className="flex items-center justify-between pt-3 border-t">
          <div className="text-xs text-muted-foreground">
            {alert.lastTriggered ? (
              <span>
                마지막 트리거:{' '}
                {formatDistanceToNow(new Date(alert.lastTriggered), { addSuffix: true, locale: ko })}
              </span>
            ) : (
              <span>트리거된 적 없음</span>
            )}
          </div>
          <div className="flex gap-2">
            <Button variant="ghost" size="icon" onClick={onEdit}>
              <Edit className="h-4 w-4" />
            </Button>
            <Button variant="ghost" size="icon" onClick={onDelete}>
              <Trash2 className="h-4 w-4 text-destructive" />
            </Button>
          </div>
        </div>
      </CardContent>
    </Card>
  )
}
