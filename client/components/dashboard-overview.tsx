"use client"

import { useEffect, useState } from "react"
import { Card } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import {
  TrendingUp,
  Package,
  ShoppingCart,
  AlertTriangle,
  CheckCircle2,
  Clock,
  DollarSign,
  ArrowUpRight,
  Play,
  Search,
  ShieldCheck,
  Truck,
  BarChart3,
  Database,
  RefreshCcw,
  Key,
  FileText,
} from "lucide-react"
import { ServerConnectionStatus } from "@/components/server-connection-status"
import { ApiTestComponent } from "@/components/api-test"
import { apiService } from "@/lib/api-service"
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  PieChart,
  Pie,
  Cell,
} from "recharts"

const salesData = [
  { date: "1/1", sales: 45000, orders: 120 },
  { date: "1/2", sales: 52000, orders: 145 },
  { date: "1/3", sales: 48000, orders: 132 },
  { date: "1/4", sales: 61000, orders: 168 },
  { date: "1/5", sales: 55000, orders: 151 },
  { date: "1/6", sales: 67000, orders: 189 },
  { date: "1/7", sales: 72000, orders: 203 },
]


const recentProducts = [
  { id: 1, name: "ワイヤレスイヤホン Bluetooth5.3", status: "active", sales: 234, stock: "rakumart" },
  { id: 2, name: "スマートウォッチ 健康管理", status: "active", sales: 189, stock: "rakumart" },
  { id: 3, name: "USB充電ケーブル 3本セット", status: "pending", sales: 156, stock: "stockcrew" },
  { id: 4, name: "LEDデスクライト 調光機能付き", status: "active", sales: 142, stock: "rakumart" },
  { id: 5, name: "モバイルバッテリー 大容量", status: "warning", sales: 98, stock: "rakumart" },
]

export function DashboardOverview() {
  const [serverConnected, setServerConnected] = useState<boolean | null>(null)
  const [serverInfo, setServerInfo] = useState<any>(null)
  const [registeredCount, setRegisteredCount] = useState<number>(0)
  const [productOriginCount, setProductOriginCount] = useState<number>(0)
  const [failedCount, setFailedCount] = useState<number>(0)
  const [recentProducts, setRecentProducts] = useState<Array<{item_number: string, title: string, rakuten_registered_at: string}>>([])
  const [categoryData, setCategoryData] = useState<Array<{name: string, value: number, color: string}>>([])

  useEffect(() => {
    (async () => {
      try {
        const stats = await apiService.getStats()
        if ((stats as any)?.success && (stats as any)?.data) {
          setRegisteredCount((stats as any).data.product_management ?? 0)
          setProductOriginCount((stats as any).data.products_origin ?? 0)
          setFailedCount((stats as any).data.failed_registrations ?? 0)
          setRecentProducts((stats as any).data.recent_products ?? [])
          
          // Process category registration counts
          const categoryCounts = (stats as any).data.category_registration_counts ?? []
          const colors = [
            "rgb(139, 92, 246)", "rgb(59, 130, 246)", "rgb(34, 197, 94)", "rgb(251, 146, 60)",
            "rgb(236, 72, 153)", "rgb(14, 165, 233)", "rgb(168, 85, 247)", "rgb(234, 179, 8)",
            "rgb(239, 68, 68)", "rgb(16, 185, 129)", "rgb(245, 158, 11)", "rgb(99, 102, 241)"
          ]
          const processedCategories = categoryCounts.map((cat: any, index: number) => ({
            name: cat.category_name || `カテゴリ${cat.id}`,
            value: cat.count || 0,
            color: colors[index % colors.length]
          }))
          setCategoryData(processedCategories)
        }
      } catch {
        // ignore network errors for dashboard soft metric
      }
    })()
  }, [])

  return (
    <div className="space-y-6">
      {/* Server Connection Status */}
      <ServerConnectionStatus 
        onStatusChange={setServerConnected}
        onServerInfoChange={setServerInfo}
      />

      {/* Header */}
      <div className="flex flex-col md:flex-row md:items-center md:justify-between gap-4">
        <div>
          <h1 className="text-2xl sm:text-3xl font-bold text-foreground">ダッシュボード</h1>
          <p className="text-muted-foreground mt-1.5 text-sm sm:text-base">楽天市場自動化システムの概要</p>
        </div>
        <div className="flex items-center gap-3 flex-wrap">
          <Badge 
            variant="outline" 
            className={`gap-2 transition-all ${
              serverConnected === false ? "border-destructive bg-destructive/10 text-destructive" : "bg-success/10 border-success/30"
            }`}
          >
            <div className={`h-2 w-2 rounded-full ${
              serverConnected === false ? "bg-destructive" : "bg-success animate-pulse"
            }`} />
            {serverConnected === false ? "サーバー接続エラー" : "システム稼働中"}
          </Badge>
          <Button className="gap-2 bg-primary hover:bg-primary/90 shadow-md shadow-primary/20 transition-all">
            <Play className="h-4 w-4" />
            <span className="hidden sm:inline">自動化開始</span>
            <span className="sm:hidden">開始</span>
          </Button>
        </div>
      </div>

      {/* Key Metrics */}
      <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
        <Card className="p-4 md:p-6 border-border bg-card shadow-md">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm font-medium text-muted-foreground">登録商品数</p>
              <p className="text-2xl md:text-3xl font-bold text-foreground mt-2">{registeredCount.toLocaleString()}</p>
              <div className="flex items-center gap-1 mt-2">
                <TrendingUp className="h-4 w-4 text-success" />
                <span className="text-sm text-muted-foreground">データベース</span>
              </div>
            </div>
            <div className="h-12 w-12 rounded-lg bg-primary/10 flex items-center justify-center">
              <Package className="h-6 w-6 text-primary" />
            </div>
          </div>
        </Card>

        <Card className="p-4 md:p-6 border-border bg-card shadow-md">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm font-medium text-muted-foreground">rakumartから読み込んだ商品数</p>
              <p className="text-2xl md:text-3xl font-bold text-foreground mt-2">{productOriginCount.toLocaleString()}</p>
              <div className="flex items-center gap-1 mt-2">
               
              </div>
            </div>
            <div className="h-12 w-12 rounded-lg bg-info/10 flex items-center justify-center">
              <Package className="h-6 w-6 text-info" />
            </div>
          </div>
        </Card>

        <Card className="p-4 md:p-6 border-border bg-card shadow-md">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm font-medium text-muted-foreground">受注件数</p>
              <p className="text-2xl md:text-3xl font-bold text-foreground mt-2">0</p>
              <div className="flex items-center gap-1 mt-2">
                <ShoppingCart className="h-4 w-4 text-info" />
                <span className="text-sm text-muted-foreground">現在</span>
              </div>
            </div>
            <div className="h-12 w-12 rounded-lg bg-info/10 flex items-center justify-center">
              <ShoppingCart className="h-6 w-6 text-info" />
            </div>
          </div>
        </Card>

        <Card className="p-4 md:p-6 border-border bg-card shadow-md">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm font-medium text-muted-foreground">要確認商品</p>
              <p className="text-2xl md:text-3xl font-bold text-foreground mt-2">{failedCount.toLocaleString()}</p>
              <div className="flex items-center gap-1 mt-2">
                <AlertTriangle className="h-4 w-4 text-warning" />
                <span className="text-sm text-warning">対応必要</span>
              </div>
            </div>
            <div className="h-12 w-12 rounded-lg bg-warning/10 flex items-center justify-center">
              <AlertTriangle className="h-6 w-6 text-warning" />
            </div>
          </div>
        </Card>
      </div>

      {/* API Connection Test moved to Settings page */}

      {/* Charts Row */}
      <div className="grid gap-4 lg:grid-cols-7">
        <Card className="lg:col-span-4 p-6 border-border bg-card">
          <div className="flex items-center justify-between mb-6">
            <div>
              <h3 className="text-lg font-semibold text-foreground">売上推移</h3>
              <p className="text-sm text-muted-foreground">過去7日間の売上データ</p>
            </div>
            <Button variant="outline" size="sm">
              詳細
            </Button>
          </div>
          <ResponsiveContainer width="100%" height={300}>
            <LineChart data={salesData}>
              <CartesianGrid strokeDasharray="3 3" stroke="rgb(39, 39, 42)" />
              <XAxis dataKey="date" stroke="rgb(161, 161, 170)" />
              <YAxis stroke="rgb(161, 161, 170)" />
              <Tooltip
                contentStyle={{
                  backgroundColor: "rgb(20, 20, 20)",
                  border: "1px solid rgb(39, 39, 42)",
                  borderRadius: "8px",
                }}
              />
              <Line
                type="monotone"
                dataKey="sales"
                stroke="rgb(139, 92, 246)"
                strokeWidth={2}
                dot={{ fill: "rgb(139, 92, 246)" }}
              />
            </LineChart>
          </ResponsiveContainer>
        </Card>

        <Card className="lg:col-span-3 p-6 border-border bg-card">
          <div className="flex items-center justify-between mb-6">
            <div>
              <h3 className="text-lg font-semibold text-foreground">カテゴリー別登録状態</h3>
              <p className="text-sm text-muted-foreground">メインカテゴリー別の登録数</p>
            </div>
          </div>
          {categoryData.length === 0 ? (
            <div className="flex items-center justify-center h-[300px] text-muted-foreground">
              登録された商品がありません
            </div>
          ) : (
            <>
              <ResponsiveContainer width="100%" height={300}>
                <PieChart>
                  <Pie
                    data={categoryData}
                    cx="50%"
                    cy="50%"
                    innerRadius={60}
                    outerRadius={100}
                    paddingAngle={5}
                    dataKey="value"
                  >
                    {categoryData.map((entry, index) => (
                      <Cell key={`cell-${index}`} fill={entry.color} />
                    ))}
                  </Pie>
                  <Tooltip
                    contentStyle={{
                      backgroundColor: "rgb(20, 20, 20)",
                      border: "1px solid rgb(39, 39, 42)",
                      borderRadius: "8px",
                    }}
                  />
                </PieChart>
              </ResponsiveContainer>
              <div className="grid grid-cols-2 gap-3 mt-4">
                {categoryData.map((cat) => (
                  <div key={cat.name} className="flex items-center gap-2">
                    <div className="h-3 w-3 rounded-full" style={{ backgroundColor: cat.color }} />
                    <span className="text-sm text-muted-foreground">{cat.name}</span>
                    <span className="text-sm font-medium text-foreground ml-auto">{cat.value}件</span>
                  </div>
                ))}
              </div>
            </>
          )}
        </Card>
      </div>

      {/* Recent Products & System Status */}
      <div className="grid gap-4 lg:grid-cols-2">
        <Card className="p-6 border-border bg-card">
          <div className="flex items-center justify-between mb-6">
            <div>
              <h3 className="text-lg font-semibold text-foreground">最近登録された商品</h3>
              <p className="text-sm text-muted-foreground">自動登録された商品一覧</p>
            </div>
            <Button variant="outline" size="sm" className="gap-2 bg-transparent">
              すべて表示
              <ArrowUpRight className="h-4 w-4" />
            </Button>
          </div>
          <div className="space-y-3">
            {recentProducts.length === 0 ? (
              <div className="text-center text-muted-foreground py-8">
                最近登録された商品がありません
              </div>
            ) : (
              recentProducts.map((product) => {
                const registeredDate = product.rakuten_registered_at 
                  ? (() => {
                      const date = new Date(product.rakuten_registered_at)
                      const year = date.getFullYear()
                      const month = String(date.getMonth() + 1).padStart(2, '0')
                      const day = String(date.getDate()).padStart(2, '0')
                      const hours = String(date.getHours()).padStart(2, '0')
                      const minutes = String(date.getMinutes()).padStart(2, '0')
                      return `${year}-${month}-${day} ${hours}:${minutes}`
                    })()
                  : ''
                return (
                  <div
                    key={product.item_number}
                    className="flex items-center justify-between p-3 rounded-lg bg-muted/50 hover:bg-muted transition-colors"
                  >
                    <div className="flex-1">
                      <p className="text-sm font-medium text-foreground">{product.title || product.item_number}</p>
                      <div className="flex items-center gap-2 mt-1">
                        <Badge variant="outline" className="text-xs">
                          {product.item_number}
                        </Badge>
                        {registeredDate && (
                          <span className="text-xs text-muted-foreground">{registeredDate}</span>
                        )}
                      </div>
                    </div>
                    <div>
                      <Badge className="bg-success/10 text-success border-success/20">
                        <CheckCircle2 className="h-3 w-3 mr-1" />
                        登録済み
                      </Badge>
                    </div>
                  </div>
                )
              })
            )}
          </div>
        </Card>

        <Card className="p-6 border-border bg-card">
          <div className="flex items-center justify-between mb-6">
            <div>
              <h3 className="text-lg font-semibold text-foreground">システムステータス</h3>
              <p className="text-sm text-muted-foreground">各機能の稼働状況</p>
            </div>
          </div>
          <div className="space-y-4">
            {/* Database Status */}
            <div className={`flex items-center justify-between p-4 rounded-lg border ${
              serverInfo?.subsystems?.database?.ok
                ? "bg-green-500/10 border-green-500/20"
                : "bg-red-500/10 border-red-500/20"
            }`}>
              <div className="flex items-center gap-3">
                <div className={`h-10 w-10 rounded-lg flex items-center justify-center ${
                  serverInfo?.subsystems?.database?.ok
                    ? "bg-green-500/20"
                    : "bg-red-500/20"
                }`}>
                  <Database className={`h-5 w-5 ${
                    serverInfo?.subsystems?.database?.ok ? "text-green-500" : "text-red-500"
                  }`} />
                </div>
                <div>
                  <p className="text-sm font-medium text-foreground">データベース</p>
                  <p className="text-xs text-muted-foreground">
                    {serverInfo?.subsystems?.database?.ok
                      ? "接続正常"
                      : serverInfo?.subsystems?.database?.details || "接続エラー"}
                  </p>
                </div>
              </div>
              <Badge className={
                serverInfo?.subsystems?.database?.ok
                  ? "bg-green-500/10 text-green-600 border-green-500/20"
                  : "bg-red-500/10 text-red-600 border-red-500/20"
              }>
                {serverInfo?.subsystems?.database?.ok ? "正常" : "異常"}
              </Badge>
            </div>

            {/* Auto Refresh Status */}
            <div className={`flex items-center justify-between p-4 rounded-lg border ${
              serverInfo?.subsystems?.auto_refresh?.ok
                ? "bg-green-500/10 border-green-500/20"
                : "bg-red-500/10 border-red-500/20"
            }`}>
              <div className="flex items-center gap-3">
                <div className={`h-10 w-10 rounded-lg flex items-center justify-center ${
                  serverInfo?.subsystems?.auto_refresh?.ok
                    ? "bg-green-500/20"
                    : "bg-red-500/20"
                }`}>
                  <RefreshCcw className={`h-5 w-5 ${
                    serverInfo?.subsystems?.auto_refresh?.ok ? "text-green-500" : "text-red-500"
                  }`} />
                </div>
                <div>
                  <p className="text-sm font-medium text-foreground">自動更新</p>
                  <p className="text-xs text-muted-foreground">
                    {serverInfo?.subsystems?.auto_refresh?.running
                      ? `稼働中${serverInfo?.subsystems?.auto_refresh?.keywords_count ? ` (${serverInfo.subsystems.auto_refresh.keywords_count}キーワード)` : ""}`
                      : "停止中"}
                  </p>
                </div>
              </div>
              <Badge className={
                serverInfo?.subsystems?.auto_refresh?.ok && serverInfo?.subsystems?.auto_refresh?.running
                  ? "bg-green-500/10 text-green-600 border-green-500/20"
                  : serverInfo?.subsystems?.auto_refresh?.ok
                    ? "bg-yellow-500/10 text-yellow-600 border-yellow-500/20"
                    : "bg-red-500/10 text-red-600 border-red-500/20"
              }>
                {serverInfo?.subsystems?.auto_refresh?.running ? "稼働中" : "停止中"}
              </Badge>
            </div>

            {/* Rakumart Config Status */}
            <div className={`flex items-center justify-between p-4 rounded-lg border ${
              serverInfo?.subsystems?.rakumart_config?.ok
                ? "bg-green-500/10 border-green-500/20"
                : "bg-red-500/10 border-red-500/20"
            }`}>
              <div className="flex items-center gap-3">
                <div className={`h-10 w-10 rounded-lg flex items-center justify-center ${
                  serverInfo?.subsystems?.rakumart_config?.ok
                    ? "bg-green-500/20"
                    : "bg-red-500/20"
                }`}>
                  <Key className={`h-5 w-5 ${
                    serverInfo?.subsystems?.rakumart_config?.ok ? "text-green-500" : "text-red-500"
                  }`} />
                </div>
                <div>
                  <p className="text-sm font-medium text-foreground">Rakumart設定</p>
                  <p className="text-xs text-muted-foreground">
                    {serverInfo?.subsystems?.rakumart_config?.has_api_key
                      ? "APIキー設定済み"
                      : "APIキー未設定"}
                  </p>
                </div>
              </div>
              <Badge className={
                serverInfo?.subsystems?.rakumart_config?.ok
                  ? "bg-green-500/10 text-green-600 border-green-500/20"
                  : "bg-red-500/10 text-red-600 border-red-500/20"
              }>
                {serverInfo?.subsystems?.rakumart_config?.ok ? "正常" : "異常"}
              </Badge>
            </div>

            {/* Logs Store Status */}
            <div className={`flex items-center justify-between p-4 rounded-lg border ${
              serverInfo?.subsystems?.logs_store?.ok
                ? "bg-green-500/10 border-green-500/20"
                : "bg-red-500/10 border-red-500/20"
            }`}>
              <div className="flex items-center gap-3">
                <div className={`h-10 w-10 rounded-lg flex items-center justify-center ${
                  serverInfo?.subsystems?.logs_store?.ok
                    ? "bg-green-500/20"
                    : "bg-red-500/20"
                }`}>
                  <FileText className={`h-5 w-5 ${
                    serverInfo?.subsystems?.logs_store?.ok ? "text-green-500" : "text-red-500"
                  }`} />
                </div>
                <div>
                  <p className="text-sm font-medium text-foreground">ログストア</p>
                  <p className="text-xs text-muted-foreground">
                    {serverInfo?.subsystems?.logs_store?.ok
                      ? "書き込み可能"
                      : "書き込みエラー"}
                  </p>
                </div>
              </div>
              <Badge className={
                serverInfo?.subsystems?.logs_store?.ok
                  ? "bg-green-500/10 text-green-600 border-green-500/20"
                  : "bg-red-500/10 text-red-600 border-red-500/20"
              }>
                {serverInfo?.subsystems?.logs_store?.ok ? "正常" : "異常"}
              </Badge>
            </div>

            {/* Product Research - Always available */}
            <div className="flex items-center justify-between p-4 rounded-lg bg-muted/50">
              <div className="flex items-center gap-3">
                <div className="h-10 w-10 rounded-lg bg-success/10 flex items-center justify-center">
                  <Search className="h-5 w-5 text-success" />
                </div>
                <div>
                  <p className="text-sm font-medium text-foreground">商品リサーチ</p>
                  <p className="text-xs text-muted-foreground">ラクマート連携</p>
                </div>
              </div>
              <Badge className="bg-success/10 text-success border-success/20">稼働中</Badge>
            </div>
          </div>
        </Card>
      </div>

      {/* Quick Actions */}
      <Card className="p-6 border-border bg-card">
        <h3 className="text-lg font-semibold text-foreground mb-4">クイックアクション</h3>
        <div className="grid gap-3 md:grid-cols-2 lg:grid-cols-4">
          <Button variant="outline" className="h-auto flex-col gap-2 py-4 bg-transparent">
            <Search className="h-5 w-5" />
            <span className="text-sm">新規商品リサーチ</span>
          </Button>
          <Button variant="outline" className="h-auto flex-col gap-2 py-4 bg-transparent">
            <Package className="h-5 w-5" />
            <span className="text-sm">商品一括登録</span>
          </Button>
          <Button variant="outline" className="h-auto flex-col gap-2 py-4 bg-transparent">
            <ShieldCheck className="h-5 w-5" />
            <span className="text-sm">コンプライアンス確認</span>
          </Button>
          <Button variant="outline" className="h-auto flex-col gap-2 py-4 bg-transparent">
            <BarChart3 className="h-5 w-5" />
            <span className="text-sm">レポート生成</span>
          </Button>
        </div>
      </Card>
    </div>
  )
}

