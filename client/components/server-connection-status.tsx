"use client"

import { useState, useEffect, useCallback } from "react"
import { Card } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Loader2, RefreshCw } from "lucide-react"
import { apiService } from "@/lib/api-service"

interface ServerStatusProps {
  onStatusChange?: (isConnected: boolean) => void
  onServerInfoChange?: (info: any) => void
}

export function ServerConnectionStatus({ onStatusChange, onServerInfoChange }: ServerStatusProps) {
  const [isConnected, setIsConnected] = useState<boolean | null>(null)
  const [isChecking, setIsChecking] = useState(false)
  const [serverInfo, setServerInfo] = useState<{ 
    service?: string
    status?: string
    subsystems?: any
    settings?: any
    uptime_ok?: boolean
  } | null>(null)

  const checkConnection = useCallback(async () => {
    setIsChecking(true)
    try {
      const response = await apiService.fullHealth()
      const ok = response && (response.status === 'healthy' || response.status === 'degraded')
      setIsConnected(!!ok)
      setServerInfo(response)
      if (onStatusChange) {
        onStatusChange(true)
      }
      if (onServerInfoChange) {
        onServerInfoChange(response)
      }
    } catch (error) {
      setIsConnected(false)
      setServerInfo(null)
      if (onStatusChange) {
        onStatusChange(false)
      }
    } finally {
      setIsChecking(false)
    }
  }, [onStatusChange, onServerInfoChange])

  // Check connection on mount and then periodically every 30 seconds
  useEffect(() => {
    checkConnection()
    const interval = setInterval(checkConnection, 30000) // Check every 30 seconds
    return () => clearInterval(interval)
  }, [checkConnection])

  // Calculate signal strength (bars) based on status
  const getSignalStrength = () => {
    if (isChecking) return 2 // 2 bars while checking (amber)
    if (isConnected === null) return 1 // 1 bar for unconfirmed (gray)
    if (isConnected === false) return 1 // 1 bar for error (red)
    if (serverInfo?.status === "degraded") return 3 // 3 bars for degraded (yellow)
    return 4 // 4 bars for healthy (green)
  }

  const getSignalColor = () => {
    if (isChecking) return "bg-amber-500"
    if (isConnected === null) return "bg-zinc-400"
    if (isConnected === false) return "bg-red-500"
    if (serverInfo?.status === "degraded") return "bg-yellow-500"
    return "bg-green-500"
  }

  const signalStrength = getSignalStrength()
  const signalColor = getSignalColor()

  return (
    <Card className="p-4 border-border bg-card shadow-md">
      <div className="flex items-center justify-between">
        {/* Signal Strength Bars */}
        <div className="flex items-end gap-1">
          {/* Bar 1 - Always visible */}
          <div 
            className={`w-1.5 rounded-t transition-all ${
              signalStrength >= 1 ? signalColor : "bg-gray-200"
            }`}
            style={{ height: signalStrength >= 1 ? "8px" : "4px" }}
          />
          {/* Bar 2 */}
          <div 
            className={`w-1.5 rounded-t transition-all ${
              signalStrength >= 2 ? signalColor : "bg-gray-200"
            }`}
            style={{ height: signalStrength >= 2 ? "12px" : "4px" }}
          />
          {/* Bar 3 */}
          <div 
            className={`w-1.5 rounded-t transition-all ${
              signalStrength >= 3 ? signalColor : "bg-gray-200"
            }`}
            style={{ height: signalStrength >= 3 ? "16px" : "4px" }}
          />
          {/* Bar 4 */}
          <div 
            className={`w-1.5 rounded-t transition-all ${
              signalStrength >= 4 ? signalColor : "bg-gray-200"
            }`}
            style={{ height: signalStrength >= 4 ? "20px" : "4px" }}
          />
        </div>

        {/* Refresh Button */}
        <Button
          variant="outline"
          size="sm"
          onClick={checkConnection}
          disabled={isChecking}
          className="h-8 gap-2"
        >
          {isChecking ? (
            <Loader2 className="h-3 w-3 animate-spin" />
          ) : (
            <RefreshCw className="h-3 w-3" />
          )}
          <span className="text-xs">更新</span>
        </Button>
      </div>
    </Card>
  )
}

