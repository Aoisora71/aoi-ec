import { ProtectedRoute } from "@/components/protected-route"
import { DashboardLayout } from "@/components/dashboard-layout"
import { ComplianceCheck } from "@/components/compliance-check"

export default function CompliancePage() {
  return (
    <ProtectedRoute>
      <DashboardLayout>
        <ComplianceCheck />
      </DashboardLayout>
    </ProtectedRoute>
  )
}
