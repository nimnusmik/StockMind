import { Card } from '@/components/ui/card'

export function LoadingSkeleton() {
  return (
    <Card className="p-6 animate-pulse">
      <div className="space-y-3">
        <div className="h-4 bg-gray-200 rounded w-1/4"></div>
        <div className="h-8 bg-gray-200 rounded w-1/2"></div>
        <div className="h-4 bg-gray-200 rounded w-3/4"></div>
        <div className="h-4 bg-gray-200 rounded w-2/3"></div>
      </div>
    </Card>
  )
}

export function SignalCardSkeleton() {
  return (
    <Card className="p-6 animate-pulse">
      <div className="space-y-4">
        <div className="flex items-center justify-between">
          <div className="h-6 bg-gray-200 rounded w-20"></div>
          <div className="h-8 bg-gray-200 rounded w-24"></div>
        </div>
        <div className="h-10 bg-gray-200 rounded w-32"></div>
        <div className="space-y-2">
          <div className="h-3 bg-gray-200 rounded w-full"></div>
          <div className="h-3 bg-gray-200 rounded w-5/6"></div>
        </div>
      </div>
    </Card>
  )
}
