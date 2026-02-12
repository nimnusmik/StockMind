'use client'

import Link from 'next/link'
import { usePathname } from 'next/navigation'
import { Button } from '@/components/ui/button'
import { cn } from '@/lib/utils'
import { Home, TrendingUp, Briefcase, BarChart3, Bell } from 'lucide-react'

const navItems = [
  { href: '/', label: '대시보드', icon: Home },
  { href: '/portfolio', label: '포트폴리오', icon: Briefcase },
  { href: '/backtesting', label: '백테스팅', icon: BarChart3 },
  { href: '/alerts', label: '알림', icon: Bell },
]

export function Navigation() {
  const pathname = usePathname()

  // 종목 상세 페이지에서는 네비게이션 숨김
  if (pathname.startsWith('/stock/')) {
    return null
  }

  return (
    <nav className="border-b bg-background sticky top-0 z-50">
      <div className="container mx-auto px-6 py-3">
        <div className="flex items-center justify-between">
          <Link href="/" className="flex items-center gap-2">
            <TrendingUp className="h-6 w-6 text-primary" />
            <span className="text-xl font-bold">StockMind</span>
          </Link>

          <div className="flex items-center gap-2">
            {navItems.map((item) => {
              const Icon = item.icon
              const isActive = pathname === item.href
              return (
                <Link key={item.href} href={item.href}>
                  <Button
                    variant={isActive ? 'default' : 'ghost'}
                    className={cn('gap-2', isActive && 'bg-primary text-primary-foreground')}
                  >
                    <Icon className="h-4 w-4" />
                    {item.label}
                  </Button>
                </Link>
              )
            })}
          </div>
        </div>
      </div>
    </nav>
  )
}
