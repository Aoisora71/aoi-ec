"use client"

import { createContext, useContext, useState, useEffect, type ReactNode } from "react"
import { useRouter } from "next/navigation"
import { apiService } from "@/lib/api-service"

interface User {
  id: string
  email: string
  name: string
  is_active?: boolean
}

interface AuthContextType {
  user: User | null
  login: (email: string, password: string) => Promise<boolean>
  signup: (email: string, password: string, name: string) => Promise<boolean>
  logout: () => void
  isLoading: boolean
}

const AuthContext = createContext<AuthContextType | undefined>(undefined)

export function AuthProvider({ children }: { children: ReactNode }) {
  const [user, setUser] = useState<User | null>(null)
  const [isLoading, setIsLoading] = useState(true)
  const router = useRouter()

  useEffect(() => {
    // Check if user is logged in on mount
    const checkAuth = async () => {
      try {
        // First, test server connection
        try {
          await apiService.testConnection()
          console.log("✅ Server connection test successful")
        } catch (connectionError) {
          console.error("❌ Server connection test failed:", connectionError)
          // Continue anyway - might be network issue
        }
        
        const storedToken = localStorage.getItem("access_token")
        const storedUser = localStorage.getItem("user")
        
        if (storedToken && storedUser) {
          // Verify token is still valid
          try {
            const result = await apiService.verifyToken(storedToken)
            if (result.valid && result.user) {
              setUser(result.user)
            } else {
              // Token invalid, try to refresh
              const refreshToken = localStorage.getItem("refresh_token")
              if (refreshToken) {
                try {
                  const refreshResult = await apiService.refreshToken(refreshToken)
                  localStorage.setItem("access_token", refreshResult.access_token)
                  const userResult = await apiService.getCurrentUser(refreshResult.access_token)
                  setUser(userResult.user)
                  localStorage.setItem("user", JSON.stringify(userResult.user))
                } catch {
                  // Refresh failed, clear everything
                  localStorage.removeItem("access_token")
                  localStorage.removeItem("refresh_token")
                  localStorage.removeItem("user")
                }
              } else {
                localStorage.removeItem("access_token")
                localStorage.removeItem("user")
              }
            }
          } catch {
            // Verification failed, clear tokens
            localStorage.removeItem("access_token")
            localStorage.removeItem("refresh_token")
            localStorage.removeItem("user")
          }
        }
      } catch (error) {
        console.error("Auth check error:", error)
      } finally {
        setIsLoading(false)
      }
    }
    
    checkAuth()
  }, [])

  const login = async (email: string, password: string): Promise<boolean> => {
    try {
      console.log("Login attempt for:", email)
      const result = await apiService.login(email, password)
      console.log("Login successful:", result)
      
      // Store tokens and user data
      localStorage.setItem("access_token", result.access_token)
      localStorage.setItem("refresh_token", result.refresh_token)
      localStorage.setItem("user", JSON.stringify(result.user))
      
      setUser(result.user)
      return true
    } catch (error: any) {
      console.error("Login error details:", error)
      // Extract error message from various possible locations
      let errorMessage = "ログインに失敗しました。メールアドレスとパスワードを確認してください。"
      
      if (error?.detail) {
        errorMessage = error.detail
      } else if (error?.message) {
        errorMessage = error.message
      } else if (error?.error) {
        errorMessage = error.error
      } else if (typeof error === 'string') {
        errorMessage = error
      }
      
      console.error("Login error message:", errorMessage)
      throw new Error(errorMessage)
    }
  }

  const signup = async (email: string, password: string, name: string): Promise<boolean> => {
    try {
      console.log("Attempting signup for:", email)
      const result = await apiService.signup(email, password, name)
      console.log("Signup successful:", result)
      
      // Store tokens and user data
      localStorage.setItem("access_token", result.access_token)
      localStorage.setItem("refresh_token", result.refresh_token)
      localStorage.setItem("user", JSON.stringify(result.user))
      
      setUser(result.user)
      return true
    } catch (error: any) {
      console.error("Signup error details:", error)
      // Extract error message from various possible locations
      let errorMessage = "アカウント作成に失敗しました。もう一度お試しください。"
      
      if (error?.detail) {
        errorMessage = error.detail
      } else if (error?.message) {
        errorMessage = error.message
      } else if (error?.error) {
        errorMessage = error.error
      } else if (typeof error === 'string') {
        errorMessage = error
      }
      
      console.error("Signup error message:", errorMessage)
      throw new Error(errorMessage)
    }
  }

  const logout = () => {
    setUser(null)
    localStorage.removeItem("access_token")
    localStorage.removeItem("refresh_token")
    localStorage.removeItem("user")
    router.push("/login")
  }

  return <AuthContext.Provider value={{ user, login, signup, logout, isLoading }}>{children}</AuthContext.Provider>
}

export function useAuth() {
  const context = useContext(AuthContext)
  if (context === undefined) {
    throw new Error("useAuth must be used within an AuthProvider")
  }
  return context
}
