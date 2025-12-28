import { ProtectedRoute } from "@/components/protected-route"
import { DashboardLayout } from "@/components/dashboard-layout"
import { PerformanceAnalytics } from "@/components/performance-analytics"

export default function AnalyticsPage() {
  return (
    <ProtectedRoute>
      <DashboardLayout>
        <PerformanceAnalytics />
      </DashboardLayout>
    </ProtectedRoute>
  )
}
