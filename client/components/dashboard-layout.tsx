"use client"

import type React from "react"

import { useState } from "react"
import { Button } from "@/components/ui/button"
import {
  LayoutDashboard,
  Search,
  Package,
  ShieldCheck,
  Truck,
  BarChart3,
  Settings,
  Bell,
  User,
  Menu,
  X,
  LogOut,
} from "lucide-react"
import Link from "next/link"
import Image from "next/image"
import { cn } from "@/lib/utils"
import { usePathname } from "next/navigation"
import { useAuth } from "@/lib/auth-context"
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu"

const navigation = [
  { name: "ダッシュボード", href: "/", icon: LayoutDashboard },
  { name: "商品リサーチ", href: "/research", icon: Search },
  { name: "商品管理", href: "/products", icon: Package },
  // 一時的に非表示: { name: "コンプライアンス", href: "/compliance", icon: ShieldCheck },
  { name: "物流管理", href: "/logistics", icon: Truck },
  // 一時的に非表示: { name: "パフォーマンス", href: "/analytics", icon: BarChart3 },
  { name: "設定", href: "/settings", icon: Settings },
]

export function DashboardLayout({ children }: { children: React.ReactNode }) {
  const [sidebarOpen, setSidebarOpen] = useState(false)
  const currentPath = usePathname()
  const { user, logout } = useAuth()

  return (
    <div className="min-h-screen bg-white">
      <header className="fixed top-0 left-0 right-0 z-header border-b border-border bg-white shadow-sm safe-top">
        <div className="flex h-14 sm:h-16 items-center justify-between px-3 sm:px-4 lg:px-6">
          <div className="flex items-center gap-3">
            <Button
              variant="ghost"
              size="icon"
              onClick={() => setSidebarOpen(!sidebarOpen)}
              className="lg:hidden hover:bg-primary/10 transition-colors"
              aria-label="メニューを切り替え"
            >
              {sidebarOpen ? <X className="h-5 w-5 text-foreground" /> : <Menu className="h-5 w-5 text-foreground" />}
            </Button>
            <div className="flex items-center gap-3">
              <div className="h-9 w-9 rounded-lg flex items-center justify-center shadow-md overflow-hidden bg-white">
                <Image 
                  src="/logo.webp" 
                  alt="Licel Store Logo" 
                  width={36} 
                  height={36} 
                  className="object-contain"
                  priority
                />
              </div>
              <div>
                <h1 className="text-sm sm:text-base md:text-lg font-bold text-foreground leading-tight">licel ストア自動化</h1>
                <p className="text-xs text-muted-foreground hidden sm:block">licel ストア自動化システム</p>
              </div>
            </div>
          </div>

          <div className="flex items-center gap-1.5 sm:gap-2">
            <Button variant="ghost" size="icon" className="relative hover:bg-muted transition-colors h-9 w-9 sm:h-10 sm:w-10">
              <Bell className="h-4 w-4 sm:h-5 sm:w-5 text-foreground" />
              <span className="absolute top-1 right-1 sm:top-1.5 sm:right-1.5 h-2 w-2 rounded-full bg-destructive" />
            </Button>
            <DropdownMenu>
              <DropdownMenuTrigger asChild>
                <Button variant="ghost" size="icon" className="hover:bg-muted transition-colors h-9 w-9 sm:h-10 sm:w-10">
                  <User className="h-4 w-4 sm:h-5 sm:w-5 text-foreground" />
                </Button>
              </DropdownMenuTrigger>
              <DropdownMenuContent align="end" className="w-56 bg-white">
                <DropdownMenuLabel>
                  <div className="flex flex-col space-y-1">
                    <p className="text-sm font-medium leading-none">{user?.name || "ユーザー"}</p>
                    <p className="text-xs leading-none text-muted-foreground">{user?.email || ""}</p>
                  </div>
                </DropdownMenuLabel>
                <DropdownMenuSeparator />
                <DropdownMenuItem onClick={logout} className="cursor-pointer text-destructive focus:text-destructive">
                  <LogOut className="mr-2 h-4 w-4" />
                  <span>ログアウト</span>
                </DropdownMenuItem>
              </DropdownMenuContent>
            </DropdownMenu>
          </div>
        </div>
      </header>

      {/* Mobile overlay */}
      {sidebarOpen && (
        <div
          className="fixed inset-0 bg-black/50 z-40 lg:hidden"
          onClick={() => setSidebarOpen(false)}
          aria-hidden="true"
        />
      )}

      <aside
        className={cn(
          "fixed left-0 top-14 sm:top-16 z-50 h-[calc(100vh-3.5rem)] sm:h-[calc(100vh-4rem)] w-64 sm:w-72 border-r border-border bg-white shadow-xl lg:shadow-none transition-transform duration-300 ease-in-out lg:translate-x-0",
          sidebarOpen ? "translate-x-0" : "-translate-x-full",
        )}
      >
        <nav className="flex flex-col gap-1 p-3 sm:p-4 h-full overflow-y-auto custom-scrollbar">
          {navigation.map((item) => {
            const Icon = item.icon
            const isActive = currentPath === item.href
            return (
              <Link
                key={item.name}
                href={item.href}
                onClick={() => {
                  setSidebarOpen(false)
                }}
                className={cn(
                  "flex items-center gap-2 sm:gap-3 rounded-lg px-3 py-2.5 sm:py-3 text-sm font-medium transition-all duration-200 min-h-[44px] sm:min-h-0",
                  isActive
                    ? "bg-primary text-primary-foreground shadow-md"
                    : "text-muted-foreground hover:bg-muted hover:text-foreground active:bg-muted/80",
                )}
              >
                <Icon className="h-4 w-4 sm:h-5 sm:w-5 flex-shrink-0" />
                <span className="truncate">{item.name}</span>
              </Link>
            )
          })}
        </nav>
      </aside>

      <main className="pt-14 sm:pt-16 lg:pl-64 xl:pl-72 transition-all duration-300 min-h-screen bg-white safe-bottom">
        <div className="p-3 sm:p-4 md:p-6 lg:p-8 max-w-[1920px] mx-auto">{children}</div>
      </main>
    </div>
  )
}
