"use client"

import { Card } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { TrendingUp, TrendingDown, Download, Calendar, DollarSign, ShoppingCart, Eye, Target } from "lucide-react"
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, AreaChart, Area } from "recharts"

const salesData = [
  { month: "7月", sales: 1200000, orders: 320, conversion: 1.8 },
  { month: "8月", sales: 1450000, orders: 385, conversion: 1.9 },
  { month: "9月", sales: 1680000, orders: 445, conversion: 2.0 },
  { month: "10月", sales: 1890000, orders: 502, conversion: 2.1 },
  { month: "11月", sales: 2100000, orders: 558, conversion: 2.2 },
  { month: "12月", sales: 2450000, orders: 651, conversion: 2.3 },
  { month: "1月", sales: 1847000, orders: 489, conversion: 2.0 },
]

const topProducts = [
  { name: "ワイヤレスイヤホン", sales: 234, revenue: 1165320, trend: "up" },
  { name: "スマートウォッチ", sales: 189, revenue: 1319220, trend: "up" },
  { name: "USB充電ケーブル", sales: 156, revenue: 308880, trend: "down" },
  { name: "LEDデスクライト", sales: 142, revenue: 778160, trend: "up" },
  { name: "モバイルバッテリー", sales: 98, revenue: 586040, trend: "down" },
]

export function PerformanceAnalytics() {
  return (
    <div className="space-y-4 md:space-y-6">
      {/* Header */}
      <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
        <div className="min-w-0">
          <h1 className="text-2xl sm:text-3xl font-bold text-foreground break-words">パフォーマンス分析</h1>
          <p className="text-sm text-muted-foreground mt-1 break-words">売上・商品・SEOの分析</p>
        </div>
        <div className="flex items-center gap-2 flex-shrink-0">
          <Button variant="outline" className="gap-2 bg-transparent text-xs sm:text-sm">
            <Calendar className="h-4 w-4" />
            <span className="hidden sm:inline">期間選択</span>
          </Button>
          <Button className="gap-2 text-xs sm:text-sm">
            <Download className="h-4 w-4" />
            <span className="hidden sm:inline">レポート出力</span>
          </Button>
        </div>
      </div>

      {/* Key Metrics */}
      <div className="grid grid-cols-2 gap-3 md:grid-cols-4 md:gap-4">
        <Card className="p-4 md:p-6 border-border bg-card">
          <div className="flex items-center justify-between mb-2">
            <p className="text-xs md:text-sm font-medium text-muted-foreground">今月の売上</p>
            <DollarSign className="h-4 w-4 md:h-5 md:w-5 text-muted-foreground" />
          </div>
          <p className="text-xl md:text-3xl font-bold text-foreground break-words">¥1,847,000</p>
          <div className="flex items-center gap-1 mt-2">
            <TrendingUp className="h-3 w-3 md:h-4 md:w-4 text-success" />
            <span className="text-xs md:text-sm text-success">+18.2%</span>
            <span className="text-xs md:text-sm text-muted-foreground">前月比</span>
          </div>
        </Card>

        <Card className="p-4 md:p-6 border-border bg-card">
          <div className="flex items-center justify-between mb-2">
            <p className="text-xs md:text-sm font-medium text-muted-foreground">受注件数</p>
            <ShoppingCart className="h-4 w-4 md:h-5 md:w-5 text-muted-foreground" />
          </div>
          <p className="text-xl md:text-3xl font-bold text-foreground">489</p>
          <div className="flex items-center gap-1 mt-2">
            <TrendingDown className="h-3 w-3 md:h-4 md:w-4 text-destructive" />
            <span className="text-xs md:text-sm text-destructive">-12.4%</span>
            <span className="text-xs md:text-sm text-muted-foreground">前月比</span>
          </div>
        </Card>

        <Card className="p-4 md:p-6 border-border bg-card">
          <div className="flex items-center justify-between mb-2">
            <p className="text-xs md:text-sm font-medium text-muted-foreground">平均客単価</p>
            <Target className="h-4 w-4 md:h-5 md:w-5 text-muted-foreground" />
          </div>
          <p className="text-xl md:text-3xl font-bold text-foreground">¥3,777</p>
          <div className="flex items-center gap-1 mt-2">
            <TrendingUp className="h-3 w-3 md:h-4 md:w-4 text-success" />
            <span className="text-xs md:text-sm text-success">+35.1%</span>
            <span className="text-xs md:text-sm text-muted-foreground">前月比</span>
          </div>
        </Card>

        <Card className="p-4 md:p-6 border-border bg-card">
          <div className="flex items-center justify-between mb-2">
            <p className="text-xs md:text-sm font-medium text-muted-foreground">転換率</p>
            <Eye className="h-4 w-4 md:h-5 md:w-5 text-muted-foreground" />
          </div>
          <p className="text-xl md:text-3xl font-bold text-foreground">2.0%</p>
          <div className="flex items-center gap-1 mt-2">
            <TrendingDown className="h-3 w-3 md:h-4 md:w-4 text-destructive" />
            <span className="text-xs md:text-sm text-destructive">-0.3pt</span>
            <span className="text-xs md:text-sm text-muted-foreground">前月比</span>
          </div>
        </Card>
      </div>

      {/* Sales Trend */}
      <Card className="p-4 md:p-6 border-border bg-card">
        <div className="flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between mb-4 md:mb-6">
          <div className="min-w-0">
            <h3 className="text-base md:text-lg font-semibold text-foreground break-words">売上推移</h3>
            <p className="text-xs md:text-sm text-muted-foreground break-words">過去7ヶ月の売上・受注データ</p>
          </div>
        </div>
        <ResponsiveContainer width="100%" height={300} className="md:h-[350px]">
          <AreaChart data={salesData}>
            <defs>
              <linearGradient id="salesGradient" x1="0" y1="0" x2="0" y2="1">
                <stop offset="5%" stopColor="rgb(139, 92, 246)" stopOpacity={0.3} />
                <stop offset="95%" stopColor="rgb(139, 92, 246)" stopOpacity={0} />
              </linearGradient>
            </defs>
            <CartesianGrid strokeDasharray="3 3" stroke="rgb(39, 39, 42)" />
            <XAxis dataKey="month" stroke="rgb(161, 161, 170)" style={{ fontSize: "12px" }} />
            <YAxis stroke="rgb(161, 161, 170)" style={{ fontSize: "12px" }} />
            <Tooltip
              contentStyle={{
                backgroundColor: "rgb(20, 20, 20)",
                border: "1px solid rgb(39, 39, 42)",
                borderRadius: "8px",
                fontSize: "12px",
              }}
            />
            <Area
              type="monotone"
              dataKey="sales"
              stroke="rgb(139, 92, 246)"
              strokeWidth={2}
              fill="url(#salesGradient)"
            />
          </AreaChart>
        </ResponsiveContainer>
      </Card>

      {/* Top Products & Conversion */}
      <div className="grid gap-4 lg:grid-cols-2">
        <Card className="p-4 md:p-6 border-border bg-card">
          <div className="flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between mb-4 md:mb-6">
            <div className="min-w-0">
              <h3 className="text-base md:text-lg font-semibold text-foreground break-words">売れ筋商品TOP5</h3>
              <p className="text-xs md:text-sm text-muted-foreground break-words">今月の販売実績</p>
            </div>
          </div>
          <div className="space-y-3 md:space-y-4">
            {topProducts.map((product, index) => (
              <div key={product.name} className="flex items-center gap-3">
                <div className="flex items-center justify-center h-7 w-7 md:h-8 md:w-8 rounded-lg bg-primary/10 text-primary font-bold text-xs md:text-sm flex-shrink-0">
                  {index + 1}
                </div>
                <div className="flex-1 min-w-0">
                  <div className="flex items-center justify-between mb-1 gap-2">
                    <p className="text-xs md:text-sm font-medium text-foreground truncate">{product.name}</p>
                    {product.trend === "up" ? (
                      <TrendingUp className="h-3 w-3 md:h-4 md:w-4 text-success flex-shrink-0" />
                    ) : (
                      <TrendingDown className="h-3 w-3 md:h-4 md:w-4 text-destructive flex-shrink-0" />
                    )}
                  </div>
                  <div className="flex items-center justify-between gap-2">
                    <p className="text-xs text-muted-foreground">{product.sales}件販売</p>
                    <p className="text-xs md:text-sm font-bold text-foreground whitespace-nowrap">
                      ¥{product.revenue.toLocaleString()}
                    </p>
                  </div>
                </div>
              </div>
            ))}
          </div>
        </Card>

        <Card className="p-4 md:p-6 border-border bg-card">
          <div className="flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between mb-4 md:mb-6">
            <div className="min-w-0">
              <h3 className="text-base md:text-lg font-semibold text-foreground break-words">転換率推移</h3>
              <p className="text-xs md:text-sm text-muted-foreground break-words">過去7ヶ月のデータ</p>
            </div>
          </div>
          <ResponsiveContainer width="100%" height={240} className="md:h-[280px]">
            <LineChart data={salesData}>
              <CartesianGrid strokeDasharray="3 3" stroke="rgb(39, 39, 42)" />
              <XAxis dataKey="month" stroke="rgb(161, 161, 170)" style={{ fontSize: "12px" }} />
              <YAxis stroke="rgb(161, 161, 170)" style={{ fontSize: "12px" }} />
              <Tooltip
                contentStyle={{
                  backgroundColor: "rgb(20, 20, 20)",
                  border: "1px solid rgb(39, 39, 42)",
                  borderRadius: "8px",
                  fontSize: "12px",
                }}
              />
              <Line
                type="monotone"
                dataKey="conversion"
                stroke="rgb(34, 197, 94)"
                strokeWidth={2}
                dot={{ fill: "rgb(34, 197, 94)" }}
              />
            </LineChart>
          </ResponsiveContainer>
        </Card>
      </div>

      {/* PDCA Recommendations */}
      <Card className="p-4 md:p-6 border-border bg-card">
        <h3 className="text-base md:text-lg font-semibold text-foreground mb-3 md:mb-4 break-words">
          改善提案（PDCA）
        </h3>
        <div className="grid gap-3 md:gap-4 md:grid-cols-2">
          <div className="p-3 md:p-4 rounded-lg bg-info/10 border border-info/20">
            <div className="flex items-start gap-2 md:gap-3">
              <div className="h-7 w-7 md:h-8 md:w-8 rounded-lg bg-info/20 flex items-center justify-center flex-shrink-0">
                <TrendingUp className="h-3 w-3 md:h-4 md:w-4 text-info" />
              </div>
              <div className="min-w-0">
                <h4 className="text-xs md:text-sm font-semibold text-foreground mb-1 break-words">
                  売れ筋商品の在庫化
                </h4>
                <p className="text-xs text-muted-foreground break-words">
                  ワイヤレスイヤホンとスマートウォッチは安定した売上があります。StockCrewへの在庫移管を検討してください。
                </p>
              </div>
            </div>
          </div>

          <div className="p-3 md:p-4 rounded-lg bg-warning/10 border border-warning/20">
            <div className="flex items-start gap-2 md:gap-3">
              <div className="h-7 w-7 md:h-8 md:w-8 rounded-lg bg-warning/20 flex items-center justify-center flex-shrink-0">
                <Target className="h-3 w-3 md:h-4 md:w-4 text-warning" />
              </div>
              <div className="min-w-0">
                <h4 className="text-xs md:text-sm font-semibold text-foreground mb-1 break-words">
                  売れない商品の入れ替え
                </h4>
                <p className="text-xs text-muted-foreground break-words">
                  過去30日間で販売実績のない商品が1,234件あります。自動入れ替えを実行してください。
                </p>
              </div>
            </div>
          </div>

          <div className="p-3 md:p-4 rounded-lg bg-success/10 border border-success/20">
            <div className="flex items-start gap-2 md:gap-3">
              <div className="h-7 w-7 md:h-8 md:w-8 rounded-lg bg-success/20 flex items-center justify-center flex-shrink-0">
                <Eye className="h-3 w-3 md:h-4 md:w-4 text-success" />
              </div>
              <div className="min-w-0">
                <h4 className="text-xs md:text-sm font-semibold text-foreground mb-1 break-words">SEO最適化の効果</h4>
                <p className="text-xs text-muted-foreground break-words">
                  AI生成タイトルを使用した商品の閲覧数が平均32%向上しています。全商品への適用を推奨します。
                </p>
              </div>
            </div>
          </div>

          <div className="p-3 md:p-4 rounded-lg bg-primary/10 border border-primary/20">
            <div className="flex items-start gap-2 md:gap-3">
              <div className="h-7 w-7 md:h-8 md:w-8 rounded-lg bg-primary/20 flex items-center justify-center flex-shrink-0">
                <ShoppingCart className="h-3 w-3 md:h-4 md:w-4 text-primary" />
              </div>
              <div className="min-w-0">
                <h4 className="text-xs md:text-sm font-semibold text-foreground mb-1 break-words">
                  クーポン施策の実施
                </h4>
                <p className="text-xs text-muted-foreground break-words">
                  楽天スーパーSALE期間中のクーポン発行で転換率が2.8倍に向上しました。次回イベントでも実施を推奨します。
                </p>
              </div>
            </div>
          </div>
        </div>
      </Card>
    </div>
  )
}
