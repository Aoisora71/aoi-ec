/**
 * @deprecated This file is deprecated. Use the category management table from the database instead.
 * Import CategoryRecord from @/lib/api-service and use apiService.getCategories() to fetch categories.
 */

// Legacy type for backwards compatibility (if needed)
export type ProductCategory = {
  id: number
  name: string
  nameEn?: string
}
