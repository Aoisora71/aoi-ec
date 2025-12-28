import { ProtectedRoute } from "@/components/protected-route"
import { DashboardLayout } from "@/components/dashboard-layout"
import { LogisticsManagement } from "@/components/logistics-management"

export default function LogisticsPage() {
  return (
    <ProtectedRoute>
      <DashboardLayout>
        <LogisticsManagement />
      </DashboardLayout>
    </ProtectedRoute>
  )
}
