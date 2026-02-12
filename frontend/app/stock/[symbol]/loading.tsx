import { LoadingSkeleton } from '@/components/shared/loading-skeleton'

export default function Loading() {
  return (
    <main className="min-h-screen bg-background">
      <div className="container mx-auto p-6 space-y-8">
        <LoadingSkeleton />
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          <LoadingSkeleton />
          <LoadingSkeleton />
          <LoadingSkeleton />
          <LoadingSkeleton />
        </div>
      </div>
    </main>
  )
}
