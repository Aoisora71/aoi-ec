"use client"

import { Card } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { Truck, Package, Clock, CheckCircle2, AlertCircle, MapPin } from "lucide-react"

const orders = [
  {
    id: "ORD-2024-001234",
    product: "ワイヤレスイヤホン Bluetooth5.3",
    customer: "田中 太郎",
    status: "processing",
    source: "rakumart",
    method: "自社出荷",
    date: "2024-01-15",
    shipping: 800,
  },
  {
    id: "ORD-2024-001235",
    product: "スマートウォッチ 健康管理",
    customer: "佐藤 花子",
    status: "shipped",
    source: "rakumart",
    method: "StockCrew",
    date: "2024-01-15",
    shipping: 600,
  },
  {
    id: "ORD-2024-001236",
    product: "USB充電ケーブル 3本セット",
    customer: "鈴木 一郎",
    status: "delivered",
    source: "rakumart",
    method: "直送",
    date: "2024-01-14",
    shipping: 0,
  },
  {
    id: "ORD-2024-001237",
    product: "LEDデスクライト 調光機能付き",
    customer: "高橋 美咲",
    status: "processing",
    source: "rakumart",
    method: "自社出荷",
    date: "2024-01-15",
    shipping: 900,
  },
]

export function LogisticsManagement() {
  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold text-foreground">物流管理</h1>
          <p className="text-muted-foreground mt-1">受注・発送・配送の自動化管理</p>
        </div>
        <Button className="gap-2">
          <Truck className="h-4 w-4" />
          一括発送処理
        </Button>
      </div>

      {/* Stats */}
      <div className="grid gap-4 md:grid-cols-4">
        <Card className="p-6 border-border bg-card">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm font-medium text-muted-foreground">処理待ち</p>
              <p className="text-3xl font-bold text-foreground mt-2">127</p>
            </div>
            <div className="h-12 w-12 rounded-lg bg-warning/10 flex items-center justify-center">
              <Clock className="h-6 w-6 text-warning" />
            </div>
          </div>
        </Card>

        <Card className="p-6 border-border bg-card">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm font-medium text-muted-foreground">発送済み</p>
              <p className="text-3xl font-bold text-foreground mt-2">342</p>
            </div>
            <div className="h-12 w-12 rounded-lg bg-info/10 flex items-center justify-center">
              <Truck className="h-6 w-6 text-info" />
            </div>
          </div>
        </Card>

        <Card className="p-6 border-border bg-card">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm font-medium text-muted-foreground">配達完了</p>
              <p className="text-3xl font-bold text-foreground mt-2">1,089</p>
            </div>
            <div className="h-12 w-12 rounded-lg bg-success/10 flex items-center justify-center">
              <CheckCircle2 className="h-6 w-6 text-success" />
            </div>
          </div>
        </Card>

        <Card className="p-6 border-border bg-card">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm font-medium text-muted-foreground">問題あり</p>
              <p className="text-3xl font-bold text-foreground mt-2">8</p>
            </div>
            <div className="h-12 w-12 rounded-lg bg-destructive/10 flex items-center justify-center">
              <AlertCircle className="h-6 w-6 text-destructive" />
            </div>
          </div>
        </Card>
      </div>

      {/* Shipping Methods */}
      <div className="grid gap-4 md:grid-cols-3">
        <Card className="p-6 border-border bg-card">
          <div className="flex items-center gap-3 mb-4">
            <div className="h-10 w-10 rounded-lg bg-primary/10 flex items-center justify-center">
              <Package className="h-5 w-5 text-primary" />
            </div>
            <div>
              <h3 className="text-sm font-semibold text-foreground">自社出荷</h3>
              <p className="text-xs text-muted-foreground">無在庫販売</p>
            </div>
          </div>
          <div className="space-y-2">
            <div className="flex items-center justify-between text-sm">
              <span className="text-muted-foreground">今月の件数</span>
              <span className="font-bold text-foreground">234件</span>
            </div>
            <div className="flex items-center justify-between text-sm">
              <span className="text-muted-foreground">平均配送日数</span>
              <span className="font-bold text-foreground">3.2日</span>
            </div>
          </div>
        </Card>

        <Card className="p-6 border-border bg-card">
          <div className="flex items-center gap-3 mb-4">
            <div className="h-10 w-10 rounded-lg bg-success/10 flex items-center justify-center">
              <Truck className="h-5 w-5 text-success" />
            </div>
            <div>
              <h3 className="text-sm font-semibold text-foreground">StockCrew</h3>
              <p className="text-xs text-muted-foreground">発送代行</p>
            </div>
          </div>
          <div className="space-y-2">
            <div className="flex items-center justify-between text-sm">
              <span className="text-muted-foreground">今月の件数</span>
              <span className="font-bold text-foreground">567件</span>
            </div>
            <div className="flex items-center justify-between text-sm">
              <span className="text-muted-foreground">平均配送日数</span>
              <span className="font-bold text-foreground">2.1日</span>
            </div>
          </div>
        </Card>

        <Card className="p-6 border-border bg-card">
          <div className="flex items-center gap-3 mb-4">
            <div className="h-10 w-10 rounded-lg bg-info/10 flex items-center justify-center">
              <MapPin className="h-5 w-5 text-info" />
            </div>
            <div>
              <h3 className="text-sm font-semibold text-foreground">直送</h3>
              <p className="text-xs text-muted-foreground">ラクマートから</p>
            </div>
          </div>
          <div className="space-y-2">
            <div className="flex items-center justify-between text-sm">
              <span className="text-muted-foreground">今月の件数</span>
              <span className="font-bold text-foreground">89件</span>
            </div>
            <div className="flex items-center justify-between text-sm">
              <span className="text-muted-foreground">平均配送日数</span>
              <span className="font-bold text-foreground">5.8日</span>
            </div>
          </div>
        </Card>
      </div>

      {/* Orders Table */}
      <Card className="border-border bg-card overflow-hidden">
        <div className="p-6 border-b border-border">
          <h3 className="text-lg font-semibold text-foreground">最近の注文</h3>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full">
            <thead className="border-b border-border bg-muted/50">
              <tr>
                <th className="text-left p-4 text-sm font-medium text-muted-foreground">注文番号</th>
                <th className="text-left p-4 text-sm font-medium text-muted-foreground">商品</th>
                <th className="text-left p-4 text-sm font-medium text-muted-foreground">顧客</th>
                <th className="text-left p-4 text-sm font-medium text-muted-foreground">仕入元</th>
                <th className="text-left p-4 text-sm font-medium text-muted-foreground">配送方法</th>
                <th className="text-left p-4 text-sm font-medium text-muted-foreground">送料</th>
                <th className="text-left p-4 text-sm font-medium text-muted-foreground">日付</th>
                <th className="text-left p-4 text-sm font-medium text-muted-foreground">ステータス</th>
                <th className="text-left p-4 text-sm font-medium text-muted-foreground"></th>
              </tr>
            </thead>
            <tbody>
              {orders.map((order) => (
                <tr key={order.id} className="border-b border-border hover:bg-muted/50 transition-colors">
                  <td className="p-4">
                    <p className="text-sm font-mono text-foreground">{order.id}</p>
                  </td>
                  <td className="p-4">
                    <p className="text-sm text-foreground">{order.product}</p>
                  </td>
                  <td className="p-4">
                    <p className="text-sm text-foreground">{order.customer}</p>
                  </td>
                  <td className="p-4">
                    <Badge variant="outline" className="text-xs">
                      {order.source}
                    </Badge>
                  </td>
                  <td className="p-4">
                    <p className="text-sm text-muted-foreground">{order.method}</p>
                  </td>
                  <td className="p-4">
                    <p className="text-sm font-medium text-foreground">
                      {order.shipping === 0 ? "無料" : `¥${order.shipping}`}
                    </p>
                  </td>
                  <td className="p-4">
                    <p className="text-sm text-muted-foreground">{order.date}</p>
                  </td>
                  <td className="p-4">
                    {order.status === "processing" && (
                      <Badge className="bg-warning/10 text-warning border-warning/20">
                        <Clock className="h-3 w-3 mr-1" />
                        処理中
                      </Badge>
                    )}
                    {order.status === "shipped" && (
                      <Badge className="bg-info/10 text-info border-info/20">
                        <Truck className="h-3 w-3 mr-1" />
                        発送済み
                      </Badge>
                    )}
                    {order.status === "delivered" && (
                      <Badge className="bg-success/10 text-success border-success/20">
                        <CheckCircle2 className="h-3 w-3 mr-1" />
                        配達完了
                      </Badge>
                    )}
                  </td>
                  <td className="p-4">
                    <Button size="sm" variant="outline">
                      詳細
                    </Button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </Card>
    </div>
  )
}
