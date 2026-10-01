'use client'

import { useEffect } from 'react'
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert'
import { Button } from '@/components/ui/button'
import { AlertCircle } from 'lucide-react'

export default function Error({
  error,
  reset,
}: {
  error: Error & { digest?: string }
  reset: () => void
}) {
  useEffect(() => {
    console.error(error)
  }, [error])

  return (
    <main className="min-h-screen bg-background flex items-center justify-center p-6">
      <Alert variant="destructive" className="max-w-lg">
        <AlertCircle className="h-4 w-4" />
        <AlertTitle>오류가 발생했습니다</AlertTitle>
        <AlertDescription className="space-y-4">
          <p>{error.message}</p>
          <Button onClick={reset} variant="outline">
            다시 시도
          </Button>
        </AlertDescription>
      </Alert>
    </main>
  )
}
