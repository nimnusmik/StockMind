'use client'

import { useEffect } from 'react'
import Link from 'next/link'
import { useAlertsStore } from '@/lib/stores/alerts-store'
import { useNotifications } from '@/lib/hooks/use-notifications'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Alert, AlertDescription } from '@/components/ui/alert'
import { AlertCard } from '@/components/alerts/alert-card'
import { AddAlertDialog } from '@/components/alerts/add-alert-dialog'
import { ArrowLeft, Bell, BellOff, AlertCircle } from 'lucide-react'

export default function AlertsPage() {
  const { alerts, permissionGranted, requestPermission, addAlert, toggleAlert, removeAlert } = useAlertsStore()

  // 알림 조건 체크 (백그라운드)
  useNotifications()

  useEffect(() => {
    // 페이지 로드 시 알림 권한 확인
    if (!permissionGranted && 'Notification' in window) {
      if (Notification.permission === 'granted') {
        useAlertsStore.setState({ permissionGranted: true })
      }
    }
  }, [permissionGranted])

  const handleRequestPermission = async () => {
    const granted = await requestPermission()
    if (!granted) {
      alert('알림 권한이 거부되었습니다. 브라우저 설정에서 알림을 허용해주세요.')
    }
  }

  return (
    <main className="min-h-screen bg-background">
      <div className="container mx-auto p-6 space-y-8">
        {/* 헤더 */}
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-4">
            <Link href="/">
              <Button variant="outline" size="icon">
                <ArrowLeft className="h-4 w-4" />
              </Button>
            </Link>
            <div>
              <h1 className="text-3xl font-bold">알림 설정</h1>
              <p className="text-sm text-muted-foreground mt-1">매매 신호 및 조건 알림</p>
            </div>
          </div>
          <AddAlertDialog onAdd={addAlert} />
        </div>

        {/* 알림 권한 상태 */}
        {!permissionGranted && (
          <Alert>
            <AlertCircle className="h-4 w-4" />
            <AlertDescription className="flex items-center justify-between">
              <span>브라우저 알림 권한이 필요합니다</span>
              <Button onClick={handleRequestPermission} size="sm">
                <Bell className="h-4 w-4 mr-2" />
                권한 요청
              </Button>
            </AlertDescription>
          </Alert>
        )}

        {permissionGranted && (
          <Alert className="bg-green-50 border-green-200">
            <Bell className="h-4 w-4 text-green-600" />
            <AlertDescription className="text-green-700">알림이 활성화되었습니다</AlertDescription>
          </Alert>
        )}

        {/* 알림 통계 */}
        <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-sm font-medium">전체 알림</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">{alerts.length}</div>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-sm font-medium">활성 알림</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold text-green-600">
                {alerts.filter((a) => a.enabled).length}
              </div>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-sm font-medium">비활성 알림</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold text-gray-600">
                {alerts.filter((a) => !a.enabled).length}
              </div>
            </CardContent>
          </Card>
        </div>

        {/* 알림 목록 */}
        <div>
          <h2 className="text-xl font-bold mb-4">활성 알림 ({alerts.filter((a) => a.enabled).length})</h2>
          {alerts.filter((a) => a.enabled).length === 0 ? (
            <Card>
              <CardContent className="flex flex-col items-center justify-center py-12 text-center">
                <BellOff className="h-12 w-12 text-muted-foreground mb-4" />
                <p className="text-muted-foreground">활성 알림이 없습니다</p>
                <p className="text-sm text-muted-foreground mt-2">알림을 추가하여 중요한 신호를 놓치지 마세요</p>
              </CardContent>
            </Card>
          ) : (
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
              {alerts
                .filter((a) => a.enabled)
                .map((alert) => (
                  <AlertCard
                    key={alert.id}
                    alert={alert}
                    onToggle={() => toggleAlert(alert.id)}
                    onEdit={() => {
                      /* TODO: 편집 기능 */
                    }}
                    onDelete={() => removeAlert(alert.id)}
                  />
                ))}
            </div>
          )}
        </div>

        {alerts.filter((a) => !a.enabled).length > 0 && (
          <div>
            <h2 className="text-xl font-bold mb-4">비활성 알림 ({alerts.filter((a) => !a.enabled).length})</h2>
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
              {alerts
                .filter((a) => !a.enabled)
                .map((alert) => (
                  <AlertCard
                    key={alert.id}
                    alert={alert}
                    onToggle={() => toggleAlert(alert.id)}
                    onEdit={() => {
                      /* TODO: 편집 기능 */
                    }}
                    onDelete={() => removeAlert(alert.id)}
                  />
                ))}
            </div>
          </div>
        )}
      </div>
    </main>
  )
}
