import { ProtectedRoute } from "@/components/protected-route"
import { DashboardLayout } from "@/components/dashboard-layout"
import { ProductResearch } from "@/components/product-research"

export default function ResearchPage() {
  return (
    <ProtectedRoute>
      <DashboardLayout>
        <ProductResearch />
      </DashboardLayout>
    </ProtectedRoute>
  )
}
