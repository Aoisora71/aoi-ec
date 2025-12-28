"use client"

import type React from "react"

import { useState, useEffect } from "react"
import { useRouter } from "next/navigation"
import { useAuth } from "@/lib/auth-context"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Card } from "@/components/ui/card"
import { Label } from "@/components/ui/label"
import { Loader2 } from "lucide-react"
import Image from "next/image"
import Link from "next/link"

export default function LoginPage() {
  const [email, setEmail] = useState("")
  const [password, setPassword] = useState("")
  const [error, setError] = useState("")
  const [isLoading, setIsLoading] = useState(false)
  const [connectionStatus, setConnectionStatus] = useState<"checking" | "connected" | "disconnected">("checking")
  const { login } = useAuth()
  const router = useRouter()
  
  // Test server connection on mount
  useEffect(() => {
    const testConnection = async () => {
      try {
        const { apiService } = await import("@/lib/api-service")
        await apiService.testConnection()
        setConnectionStatus("connected")
      } catch (error) {
        console.error("Server connection test failed:", error)
        setConnectionStatus("disconnected")
      }
    }
    testConnection()
  }, [])

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault()
    setError("")
    setIsLoading(true)

    if (!email || !password) {
      setError("メールアドレスとパスワードを入力してください")
      setIsLoading(false)
      return
    }

    try {
      await login(email, password)
      router.push("/")
    } catch (error: any) {
      setError(error?.message || "ログインに失敗しました。メールアドレスとパスワードを確認してください。")
    } finally {
      setIsLoading(false)
    }
  }

  return (
    <div className="min-h-screen flex items-center justify-center bg-gradient-to-br from-primary/5 via-background to-primary/10 p-4">
      <Card className="w-full max-w-md p-8 space-y-6 shadow-xl">
        <div className="text-center space-y-2">
          <div className="flex justify-center mb-4">
            <div className="h-16 w-16 rounded-lg flex items-center justify-center overflow-hidden bg-white shadow-md">
              <Image 
                src="/logo.webp" 
                alt="Licel Store Logo" 
                width={64} 
                height={64} 
                className="object-contain"
                priority
              />
            </div>
          </div>
          <h1 className="text-2xl font-bold text-foreground">ログイン</h1>
        </div>

        <form onSubmit={handleSubmit} className="space-y-4">
          <div className="space-y-2">
            <Label htmlFor="email">メールアドレス</Label>
            <Input
              id="email"
              type="email"
              value={email}
              onChange={(e) => setEmail(e.target.value)}
              className="bg-background"
              disabled={isLoading}
            />
          </div>

          <div className="space-y-2">
            <Label htmlFor="password">パスワード</Label>
            <Input
              id="password"
              type="password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              className="bg-background"
              disabled={isLoading}
            />
          </div>

          {connectionStatus === "disconnected" && (
            <div className="p-3 rounded-lg bg-yellow-500/10 border border-yellow-500/20">
              <p className="text-sm text-yellow-600 dark:text-yellow-400">
                ⚠️ サーバーに接続できません。サーバーが起動していることを確認してください。
                <br />
                <span className="text-xs mt-1 block">
                  確認方法: ブラウザで <code className="bg-gray-100 dark:bg-gray-800 px-1 rounded">http://162.43.44.223:8000/api/health</code> にアクセスしてください。
                </span>
              </p>
            </div>
          )}
          
          {connectionStatus === "checking" && (
            <div className="p-3 rounded-lg bg-blue-500/10 border border-blue-500/20">
              <p className="text-sm text-blue-600 dark:text-blue-400">
                🔄 サーバー接続を確認中...
              </p>
            </div>
          )}
          
          {error && (
            <div className="p-3 rounded-lg bg-destructive/10 border border-destructive/20">
              <p className="text-sm text-destructive">{error}</p>
            </div>
          )}

          <Button type="submit" className="w-full" disabled={isLoading}>
            {isLoading ? (
              <>
                <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                ログイン中...
              </>
            ) : (
              "ログイン"
            )}
          </Button>
        </form>

        <div className="text-center text-sm">
          <span className="text-muted-foreground">アカウントをお持ちでないですか？ </span>
          <Link href="/signup" className="text-primary hover:underline font-medium">
            新規登録
          </Link>
        </div>
      </Card>
    </div>
  )
}
