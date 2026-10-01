import { SignalOverviewGrid } from '@/components/dashboard/signal-overview-grid'

export default function DashboardPage() {
  return (
    <main className="min-h-screen bg-background">
      <div className="container mx-auto p-6 space-y-8">
        {/* 신호 대시보드 */}
        <SignalOverviewGrid />
      </div>
    </main>
  )
}
