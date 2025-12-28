import type React from "react"
import type { Metadata } from "next"
import { Geist, Geist_Mono } from "next/font/google"
import { Analytics } from "@vercel/analytics/next"
import "./globals.css"
import { AuthProvider } from "@/lib/auth-context"
import PatchRemoveChild from "@/components/patch-remove-child"
import { Toaster } from "@/components/ui/toaster"

const _geist = Geist({ subsets: ["latin"] })
const _geistMono = Geist_Mono({ subsets: ["latin"] })

export const metadata: Metadata = {
  title: "Licelストア自動化システム",
  description: "楽天市場向けの自動化・商品管理・分析プラットフォーム",
  icons: {
    icon: "/logo.webp",
    shortcut: "/logo.webp",
    apple: "/logo.webp",
  },
}

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode
}>) {
  // Googleフォントのクラス名を<html>に適用し、サーバー/クライアントの属性差異を回避。
  // 非重要な動的属性に対しては`suppressHydrationWarning`でハイドレーション警告を抑制。
  const htmlClass = `${_geist.className ?? ""} ${_geistMono.className ?? ""}`.trim()

  return (
    // Reactのハイドレーション前にブラウザ拡張や環境で<html>属性が変化しても
    // 警告が氾濫しないようにするための設定。
    <html lang="ja" className={htmlClass} suppressHydrationWarning>
      <body className={`font-sans antialiased`}>
        {/* 既に切り離されたノードに対しても寛容なremoveChildパッチ */}
        <PatchRemoveChild />
        <AuthProvider>{children}</AuthProvider>
        <Toaster />
        <Analytics />
      </body>
    </html>
  )
}
