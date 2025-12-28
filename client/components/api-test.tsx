"use client"

import { useState } from "react"
import { Button } from "@/components/ui/button"
import { Card } from "@/components/ui/card"
import { apiService } from "@/lib/api-service"

export function ApiTestComponent() {
  const [status, setStatus] = useState<string>("")
  const [loading, setLoading] = useState(false)

  const testConnection = async () => {
    setLoading(true)
    setStatus("接続テスト中...")
    
    try {
      const response = await apiService.healthCheck()
      setStatus(`✅ 接続に成功しました。サーバー: ${response.service}`)
    } catch (error) {
      setStatus(`❌ 接続に失敗しました: ${error instanceof Error ? error.message : '不明なエラー'}`)
    } finally {
      setLoading(false)
    }
  }

  const testGetProducts = async () => {
    setLoading(true)
    setStatus("商品取得テスト中...")
    
    try {
      const response = await apiService.getProductsFromDatabase(5)
      if (response.success) {
        setStatus(`✅ 取得に成功しました。件数: ${response.data?.length || 0}`)
      } else {
        setStatus(`❌ 取得に失敗しました: ${response.error}`)
      }
    } catch (error) {
      setStatus(`❌ 取得に失敗しました: ${error instanceof Error ? error.message : '不明なエラー'}`)
    } finally {
      setLoading(false)
    }
  }

  return (
    <Card className="p-6">
      <h3 className="text-lg font-semibold mb-4">API接続テスト</h3>
      <div className="space-y-4">
        <div className="flex gap-2">
          <Button onClick={testConnection} disabled={loading}>
            ヘルスチェック実行
          </Button>
          <Button onClick={testGetProducts} disabled={loading}>
            商品取得テスト
          </Button>
        </div>
        {status && (
          <div className="p-3 bg-muted rounded-md">
            <pre className="text-sm">{status}</pre>
          </div>
        )}
      </div>
    </Card>
  )
}

