import { ProtectedRoute } from "@/components/protected-route"
import { DashboardLayout } from "@/components/dashboard-layout"
import { ProductManagement } from "@/components/product-management"

export default function ProductsPage() {
  return (
    <ProtectedRoute>
      <DashboardLayout>
        <ProductManagement />
      </DashboardLayout>
    </ProtectedRoute>
  )
}
