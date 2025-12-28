"use client"

import { Card } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { ShieldCheck, AlertTriangle, CheckCircle2, XCircle, FileText, Zap, Battery, Copyright } from "lucide-react"

const complianceCategories = [
  {
    name: "PSEマーク",
    icon: Zap,
    checked: 45234,
    passed: 44891,
    failed: 343,
    status: "good",
  },
  {
    name: "薬機法",
    icon: FileText,
    checked: 12453,
    passed: 12234,
    failed: 219,
    status: "good",
  },
  {
    name: "食品衛生法",
    icon: FileText,
    checked: 8765,
    passed: 8621,
    failed: 144,
    status: "good",
  },
  {
    name: "危険物",
    icon: AlertTriangle,
    checked: 23456,
    passed: 22987,
    failed: 469,
    status: "warning",
  },
  {
    name: "液体・電池",
    icon: Battery,
    checked: 15678,
    passed: 15234,
    failed: 444,
    status: "warning",
  },
  {
    name: "著作権",
    icon: Copyright,
    checked: 48234,
    passed: 47891,
    failed: 343,
    status: "good",
  },
]

const recentIssues = [
  {
    id: 1,
    product: "ワイヤレス充電器 急速充電対応",
    issue: "PSEマーク未確認",
    severity: "high",
    date: "2時間前",
  },
  {
    id: 2,
    product: "美容液 ヒアルロン酸配合",
    issue: "薬機法表現要確認",
    severity: "medium",
    date: "4時間前",
  },
  {
    id: 3,
    product: "リチウムイオンバッテリー",
    issue: "危険物配送制限",
    severity: "high",
    date: "6時間前",
  },
  {
    id: 4,
    product: "ブランドロゴ入りケース",
    issue: "著作権要確認",
    severity: "high",
    date: "8時間前",
  },
]

export function ComplianceCheck() {
  return (
    <div className="space-y-4 sm:space-y-6 p-4 sm:p-6">
      {/* Header */}
      <div className="flex flex-col gap-4 sm:flex-row sm:items-center sm:justify-between">
        <div className="min-w-0">
          <h1 className="text-2xl sm:text-3xl font-bold text-foreground break-words">コンプライアンスチェック</h1>
          <p className="text-sm sm:text-base text-muted-foreground mt-1 break-words">
            法規制・規約違反の自動検出システム
          </p>
        </div>
        <div className="flex flex-col sm:flex-row items-stretch sm:items-center gap-2 sm:gap-3">
          <Badge variant="outline" className="gap-2 justify-center py-2">
            <div className="h-2 w-2 rounded-full bg-success animate-pulse" />
            自動監視中
          </Badge>
          <Button className="gap-2 w-full sm:w-auto">
            <ShieldCheck className="h-4 w-4" />
            <span className="sm:inline">全商品再チェック</span>
          </Button>
        </div>
      </div>

      {/* Overview Stats */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-3 sm:gap-4">
        <Card className="p-4 sm:p-6 border-border bg-card">
          <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-3">
            <div className="min-w-0">
              <p className="text-xs sm:text-sm font-medium text-muted-foreground">チェック済み</p>
              <p className="text-2xl sm:text-3xl font-bold text-foreground mt-1 sm:mt-2">48,234</p>
            </div>
            <div className="h-10 w-10 sm:h-12 sm:w-12 rounded-lg bg-primary/10 flex items-center justify-center shrink-0">
              <ShieldCheck className="h-5 w-5 sm:h-6 sm:w-6 text-primary" />
            </div>
          </div>
        </Card>

        <Card className="p-4 sm:p-6 border-border bg-card">
          <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-3">
            <div className="min-w-0">
              <p className="text-xs sm:text-sm font-medium text-muted-foreground">適合</p>
              <p className="text-2xl sm:text-3xl font-bold text-success mt-1 sm:mt-2">47,011</p>
              <p className="text-xs sm:text-sm text-muted-foreground mt-1">97.5%</p>
            </div>
            <div className="h-10 w-10 sm:h-12 sm:w-12 rounded-lg bg-success/10 flex items-center justify-center shrink-0">
              <CheckCircle2 className="h-5 w-5 sm:h-6 sm:w-6 text-success" />
            </div>
          </div>
        </Card>

        <Card className="p-4 sm:p-6 border-border bg-card">
          <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-3">
            <div className="min-w-0">
              <p className="text-xs sm:text-sm font-medium text-muted-foreground">要確認</p>
              <p className="text-2xl sm:text-3xl font-bold text-warning mt-1 sm:mt-2">980</p>
              <p className="text-xs sm:text-sm text-muted-foreground mt-1">2.0%</p>
            </div>
            <div className="h-10 w-10 sm:h-12 sm:w-12 rounded-lg bg-warning/10 flex items-center justify-center shrink-0">
              <AlertTriangle className="h-5 w-5 sm:h-6 sm:w-6 text-warning" />
            </div>
          </div>
        </Card>

        <Card className="p-4 sm:p-6 border-border bg-card">
          <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-3">
            <div className="min-w-0">
              <p className="text-xs sm:text-sm font-medium text-muted-foreground">不適合</p>
              <p className="text-2xl sm:text-3xl font-bold text-destructive mt-1 sm:mt-2">243</p>
              <p className="text-xs sm:text-sm text-muted-foreground mt-1">0.5%</p>
            </div>
            <div className="h-10 w-10 sm:h-12 sm:w-12 rounded-lg bg-destructive/10 flex items-center justify-center shrink-0">
              <XCircle className="h-5 w-5 sm:h-6 sm:w-6 text-destructive" />
            </div>
          </div>
        </Card>
      </div>

      {/* Compliance Categories */}
      <Card className="p-4 sm:p-6 border-border bg-card">
        <h3 className="text-base sm:text-lg font-semibold text-foreground mb-3 sm:mb-4">カテゴリー別チェック状況</h3>
        <div className="grid gap-3 sm:gap-4 sm:grid-cols-2 lg:grid-cols-3">
          {complianceCategories.map((category) => {
            const Icon = category.icon
            const passRate = ((category.passed / category.checked) * 100).toFixed(1)
            return (
              <Card key={category.name} className="p-3 sm:p-4 border-border bg-muted/50">
                <div className="flex items-start justify-between mb-3">
                  <div className="flex items-center gap-2 sm:gap-3 min-w-0">
                    <div
                      className={`h-8 w-8 sm:h-10 sm:w-10 rounded-lg flex items-center justify-center shrink-0 ${
                        category.status === "good" ? "bg-success/10" : "bg-warning/10"
                      }`}
                    >
                      <Icon
                        className={`h-4 w-4 sm:h-5 sm:w-5 ${category.status === "good" ? "text-success" : "text-warning"}`}
                      />
                    </div>
                    <div className="min-w-0">
                      <p className="text-xs sm:text-sm font-semibold text-foreground break-words">{category.name}</p>
                      <p className="text-xs text-muted-foreground">{category.checked.toLocaleString()}件</p>
                    </div>
                  </div>
                </div>
                <div className="space-y-2">
                  <div className="flex items-center justify-between text-xs sm:text-sm">
                    <span className="text-muted-foreground">適合率</span>
                    <span className={`font-bold ${category.status === "good" ? "text-success" : "text-warning"}`}>
                      {passRate}%
                    </span>
                  </div>
                  <div className="h-2 bg-muted rounded-full overflow-hidden">
                    <div
                      className={`h-full ${category.status === "good" ? "bg-success" : "bg-warning"}`}
                      style={{ width: `${passRate}%` }}
                    />
                  </div>
                  <div className="flex items-center justify-between text-xs text-muted-foreground">
                    <span>適合: {category.passed.toLocaleString()}</span>
                    <span>不適合: {category.failed.toLocaleString()}</span>
                  </div>
                </div>
              </Card>
            )
          })}
        </div>
      </Card>

      {/* Recent Issues */}
      <Card className="p-4 sm:p-6 border-border bg-card">
        <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-3 sm:gap-0 mb-3 sm:mb-4">
          <h3 className="text-base sm:text-lg font-semibold text-foreground">最近検出された問題</h3>
          <Button variant="outline" size="sm" className="w-full sm:w-auto bg-transparent">
            すべて表示
          </Button>
        </div>
        <div className="space-y-2 sm:space-y-3">
          {recentIssues.map((issue) => (
            <div
              key={issue.id}
              className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-3 p-3 sm:p-4 rounded-lg bg-muted/50 hover:bg-muted transition-colors"
            >
              <div className="flex items-center gap-3 min-w-0">
                <div
                  className={`h-8 w-8 sm:h-10 sm:w-10 rounded-lg flex items-center justify-center shrink-0 ${
                    issue.severity === "high" ? "bg-destructive/10" : "bg-warning/10"
                  }`}
                >
                  {issue.severity === "high" ? (
                    <XCircle className="h-4 w-4 sm:h-5 sm:w-5 text-destructive" />
                  ) : (
                    <AlertTriangle className="h-4 w-4 sm:h-5 sm:w-5 text-warning" />
                  )}
                </div>
                <div className="min-w-0">
                  <p className="text-xs sm:text-sm font-medium text-foreground break-words">{issue.product}</p>
                  <p className="text-xs text-muted-foreground mt-1">{issue.issue}</p>
                </div>
              </div>
              <div className="flex items-center justify-between sm:justify-end gap-2 sm:gap-3">
                <span className="text-xs text-muted-foreground">{issue.date}</span>
                <Badge variant={issue.severity === "high" ? "destructive" : "default"} className="shrink-0">
                  {issue.severity === "high" ? "高" : "中"}
                </Badge>
                <Button size="sm" variant="outline" className="shrink-0 bg-transparent">
                  対応
                </Button>
              </div>
            </div>
          ))}
        </div>
      </Card>
    </div>
  )
}
