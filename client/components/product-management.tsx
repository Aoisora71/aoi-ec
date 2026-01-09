"use client"

import { useEffect, useMemo, useRef, useState, useCallback, memo } from "react"
import { useMediaQuery } from "@/hooks/use-media-query"
import { useDebounce } from "@/hooks/use-debounce"
import { ErrorBoundary } from "@/components/error-boundary"
import { ProductTableSkeleton, ProductCardSkeleton } from "@/components/skeleton-loader"
import { parseError, getUserErrorMessage, ErrorCode, createStandardError } from "@/lib/error-utils"
import { ValidationRules, validateOnChange, getValidationClassName, type FieldValidation } from "@/lib/form-validation"
import { SkipLink } from "@/components/skip-link"
import { useKeyboardNavigation } from "@/hooks/use-keyboard-navigation"
import { Card } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Badge } from "@/components/ui/badge"
import { Toggle } from "@/components/ui/toggle"
import {
  Search,
  MoreVertical,
  Edit,
  Trash2,
  RefreshCw,
  Upload,
  Download,
  CheckCircle2,
  XCircle,
  Clock,
  TrendingUp,
  TrendingDown,
  CheckSquare,
  Square,
  Settings,
  Image as ImageIcon,
  Hash,
  Power,
  Loader2,
  Package,
  Eye,
  EyeOff,
  ExternalLink,
  Save,
  Lock,
  Pencil,
} from "lucide-react"
import { DropdownMenu, DropdownMenuContent, DropdownMenuItem, DropdownMenuTrigger } from "@/components/ui/dropdown-menu"
import { formatNumberJa } from "@/lib/locale"
import { apiService, CategoryRecord } from "@/lib/api-service"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { cn } from "@/lib/utils"
import { useToast } from "@/hooks/use-toast"
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from "@/components/ui/alert-dialog"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import { Label } from "@/components/ui/label"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"

// Type definitions
interface ProductVariantSelector {
  key: string
  displayName: string
  values: Array<{ displayValue: string }>
}

interface ProductVariant {
  selectorValues?: Record<string, string>
  standardPrice?: string
  articleNumber?: string | { exemptionReason?: number }
  images?: Array<{ type: string; location: string }>
  attributes?: Array<{ name: string; values: string[] }>
  shipping?: {
    postageIncluded?: boolean
    postageSegment?: number
  }
  features?: {
    restockNotification?: boolean
    displayNormalCartButton?: boolean
    noshi?: boolean
  }
  normalDeliveryDateId?: number
}

interface ProductFeatures {
  restockNotification?: boolean
  displayNormalCartButton?: boolean
  noshi?: boolean
  [key: string]: unknown
}

interface ProductPayment {
  cashOnDelivery?: boolean
  creditCard?: boolean
  convenienceStore?: boolean
  [key: string]: unknown
}

interface Product {
  id?: string
  item_number: string
  name?: string
  title?: string
  tagline?: string
  product_description?: string | Record<string, unknown>
  sales_description?: string
  genre_id?: string
  tags?: string[]
  images?: Array<{ type: string; location: string }>
  hide_item?: boolean
  block?: boolean
  variant_selectors?: ProductVariantSelector[]
  variants?: Record<string, ProductVariant>
  created_at?: string
  rakuten_registration_status?: string | number | boolean
  image_registration_status?: boolean | string | number
  inventory_registration_status?: boolean | string | number
  src_url?: string
  main_category?: string
  middle_category?: string
  registration_status?: number
  features?: ProductFeatures
  payment?: ProductPayment
  item_type?: string
  unlimited_inventory_flag?: boolean
  sku?: string
}

const products: Product[] = []
const PAGE_SIZE = 100
const ROW_HEIGHT = 55
const VIRTUAL_WINDOW = 30
const OVERSCAN_ROWS = 10

// Function to translate error messages from English to Japanese
const translateErrorMessage = (error: string | undefined | null): string => {
  if (!error) return "エラーが発生しました"
  
  const errorStr = error.toString().trim()
  
  // Common error message translations
  const translations: Record<string, string> = {
    // Network and connection errors
    "Request timeout": "リクエストタイムアウト",
    "Request timeout:": "リクエストタイムアウト:",
    "Cannot connect to backend": "バックエンドサーバーに接続できません",
    "Network error": "ネットワークエラー",
    "Failed to fetch": "フェッチに失敗しました",
    "Empty response from server": "サーバーからの応答が空です",
    "Invalid JSON response": "無効なJSON応答",
    
    // API errors
    "Product with item_number": "商品番号",
    "not found": "が見つかりません",
    "HTTP 404": "HTTP 404 - 見つかりません",
    "HTTP 500": "HTTP 500 - サーバーエラー",
    "HTTP 400": "HTTP 400 - 不正なリクエスト",
    "HTTP 401": "HTTP 401 - 認証が必要です",
    "HTTP 403": "HTTP 403 - アクセスが拒否されました",
    
    // Rakuten API specific errors
    "Product not found on Rakuten": "楽天市場で商品が見つかりません",
    "Product is registered on Rakuten": "商品は楽天市場に登録されています",
    "Product not found on Rakuten (likely deleted)": "楽天市場で商品が見つかりません（削除された可能性があります）",
    "Request timeout: Rakuten API": "リクエストタイムアウト: 楽天APIの応答が遅延しています",
    "Connection error": "接続エラー",
    
    // Generic errors
    "Unknown error": "不明なエラー",
    "An error occurred": "エラーが発生しました",
    "Error occurred": "エラーが発生しました",
  }
  
  // Check for exact matches first
  if (translations[errorStr]) {
    return translations[errorStr]
  }
  
  // Check for partial matches (case insensitive)
  const lowerError = errorStr.toLowerCase()
  for (const [key, value] of Object.entries(translations)) {
    if (lowerError.includes(key.toLowerCase())) {
      return errorStr.replace(new RegExp(key, 'gi'), value)
    }
  }
  
  // If the error contains common English phrases, translate them
  let translated = errorStr
  
  // Translate common patterns
  translated = translated.replace(/Request timeout/i, "リクエストタイムアウト")
  translated = translated.replace(/Network error/i, "ネットワークエラー")
  translated = translated.replace(/Failed to fetch/i, "フェッチに失敗しました")
  translated = translated.replace(/Empty response/i, "空の応答")
  translated = translated.replace(/Invalid JSON/i, "無効なJSON")
  translated = translated.replace(/not found/i, "見つかりません")
  translated = translated.replace(/error occurred/i, "エラーが発生しました")
  translated = translated.replace(/unknown error/i, "不明なエラー")
  
  // If no translation was applied, return original with a prefix
  if (translated === errorStr && /[a-zA-Z]/.test(errorStr)) {
    // If it looks like an English error, add context
    return `エラー: ${errorStr}`
  }
  
  return translated
}

export function ProductManagement() {
  const isMobile = useMediaQuery("(max-width: 768px)")
  const isTablet = useMediaQuery("(min-width: 769px) and (max-width: 1024px)")
  const [searchQuery, setSearchQuery] = useState("")
  const debouncedSearchQuery = useDebounce(searchQuery, 300)
  const [selectedCategory, setSelectedCategory] = useState<string>("all")
  const [sortBy, setSortBy] = useState<string>("created_at")
  const [sortOrder, setSortOrder] = useState<"asc" | "desc">("desc")
  const [selectedProducts, setSelectedProducts] = useState<string[]>([])
  const [items, setItems] = useState<Product[]>([])
  const [isLoading, setIsLoading] = useState(false)
  const [categories, setCategories] = useState<CategoryRecord[]>([])
  const [primaryCategories, setPrimaryCategories] = useState<Array<{id: number, category_name: string, default_category_ids: string[]}>>([])
  const { toast } = useToast()
  const [deleteConfirmOpen, setDeleteConfirmOpen] = useState(false)
  const [productToDelete, setProductToDelete] = useState<{ id: string; name: string } | null>(null)
  const [rakutenDeleteConfirmOpen, setRakutenDeleteConfirmOpen] = useState(false)
  const [productToDeleteFromRakuten, setProductToDeleteFromRakuten] = useState<{ id: string; name: string } | null>(null)
  const [isDeletingFromRakuten, setIsDeletingFromRakuten] = useState(false)
  const [bulkRakutenDeleteConfirmOpen, setBulkRakutenDeleteConfirmOpen] = useState(false)
  const [isBulkDeletingFromRakuten, setIsBulkDeletingFromRakuten] = useState(false)
  const [registering, setRegistering] = useState<Record<string, boolean>>({})
  const [registeringInventory, setRegisteringInventory] = useState<Record<string, boolean>>({})
  const [checkingStatus, setCheckingStatus] = useState<Record<string, boolean>>({})
  const [isBulkCheckingStatus, setIsBulkCheckingStatus] = useState(false)
  const [isUpdatingChanges, setIsUpdatingChanges] = useState(false)
  const [toggling, setToggling] = useState<Record<string, boolean>>({})
  const [togglingAll, setTogglingAll] = useState(false)
  const [blocking, setBlocking] = useState<Record<string, boolean>>({})
  const [settingsModalOpen, setSettingsModalOpen] = useState(false)
  const [selectedProductForSettings, setSelectedProductForSettings] = useState<Product | null>(null)
  const [csvUploadModalOpen, setCsvUploadModalOpen] = useState(false)
  const [isExportingCSV, setIsExportingCSV] = useState(false)
  const [isImportingCSV, setIsImportingCSV] = useState(false)
  const fileInputRef = useRef<HTMLInputElement>(null)
  
  // Settings form state
  const [settingsForm, setSettingsForm] = useState({
    title: "",
    itemType: "NORMAL",
    genreId: "",
    tags: "",
    unlimitedInventoryFlag: "false",
    searchVisibility: "ALWAYS_VISIBLE",
    inventoryDisplay: "DISPLAY_ABSOLUTE_STOCK_COUNT",
    review: "SHOP_SETTING",
    taxIncluded: "false",
    taxRate: "",
    cashOnDeliveryFeeIncluded: "false",
    normalDeliveryDateId: "1",
  })
  const [settingsFormErrors, setSettingsFormErrors] = useState<Record<string, string>>({})
  const [touchedFields, setTouchedFields] = useState<Set<string>>(new Set())
  const [isSavingSettings, setIsSavingSettings] = useState(false)
  const [skuModalOpen, setSkuModalOpen] = useState(false)
  const [selectedProductForSku, setSelectedProductForSku] = useState<Product | null>(null)
  const [editingSkuData, setEditingSkuData] = useState<{
    variants: Record<string, ProductVariant>
    selectors: ProductVariantSelector[]
  } | null>(null)
  const [isSavingSku, setIsSavingSku] = useState(false)
  const [editingVariantId, setEditingVariantId] = useState<string | null>(null)
  const [imageModalOpen, setImageModalOpen] = useState(false)
  const [selectedProductForImages, setSelectedProductForImages] = useState<Product | null>(null)
  const [deletingImage, setDeletingImage] = useState<Record<string, boolean>>({})
  const [uploadingToRakuten, setUploadingToRakuten] = useState(false)
  const [bulkDeleteConfirmOpen, setBulkDeleteConfirmOpen] = useState(false)
  const [isDeletingProduct, setIsDeletingProduct] = useState(false)
  const [isBulkDeleting, setIsBulkDeleting] = useState(false)
  const [isBulkRegistering, setIsBulkRegistering] = useState(false)
  const [bulkRegistrationProgress, setBulkRegistrationProgress] = useState<{ current: number; total: number; currentItem: string | null }>({
    current: 0,
    total: 0,
    currentItem: null
  })
  const [isBulkRegisteringInventory, setIsBulkRegisteringInventory] = useState(false)
  const [bulkInventoryRegistrationProgress, setBulkInventoryRegistrationProgress] = useState<{ current: number; total: number; currentItem: string | null }>({
    current: 0,
    total: 0,
    currentItem: null
  })
  const [isBulkUploadingImages, setIsBulkUploadingImages] = useState(false)
  const [bulkImageUploadProgress, setBulkImageUploadProgress] = useState<{ 
    current: number; 
    total: number; 
    uploadedImages: number;
    totalImages: number;
    completed: Array<{ item_number: string; status: 'success' | 'error'; message: string }> 
  }>({
    current: 0,
    total: 0,
    uploadedImages: 0,
    totalImages: 0,
    completed: []
  })
  const [currentOffset, setCurrentOffset] = useState(0)
  const [hasMoreProducts, setHasMoreProducts] = useState(true)
  const [isLoadingMore, setIsLoadingMore] = useState(false)
  const [virtualStartIndex, setVirtualStartIndex] = useState(0)
  const tableScrollRef = useRef<HTMLDivElement | null>(null)
  const [productStats, setProductStats] = useState<{
    total: number
    registered: number
    unregistered: number
    failed: number
    deleted: number
    stop: number
    onsale: number
  }>({
    total: 0,
    registered: 0,
    unregistered: 0,
    failed: 0,
    deleted: 0,
    stop: 0,
    onsale: 0,
  })

  // Calculate processing count from all bulk operations
  const processingCount = useMemo(() => {
    let count = 0
    if (bulkRegistrationProgress.total > 0) {
      count += bulkRegistrationProgress.total - bulkRegistrationProgress.current
    }
    if (bulkInventoryRegistrationProgress.total > 0) {
      count += bulkInventoryRegistrationProgress.total - bulkInventoryRegistrationProgress.current
    }
    if (bulkImageUploadProgress.total > 0) {
      count += bulkImageUploadProgress.total - bulkImageUploadProgress.current
    }
    return count
  }, [bulkRegistrationProgress, bulkInventoryRegistrationProgress, bulkImageUploadProgress])

  // Load product statistics
  const loadProductStats = useCallback(async () => {
    try {
      const response = await apiService.getProductManagementStats()
      if (response.success && response.data) {
        setProductStats(response.data)
      }
    } catch (error) {
      console.error("Failed to load product stats:", error)
    }
  }, [])

  useEffect(() => {
    loadProductStats()
    const interval = setInterval(loadProductStats, 30000) // Refresh every 30 seconds
    return () => clearInterval(interval)
  }, [loadProductStats])

  // Reset offset when sort or search changes
  useEffect(() => {
    setCurrentOffset(0)
    setHasMoreProducts(true)
  }, [sortBy, sortOrder, debouncedSearchQuery])

  // Load products from backend (with search if query exists)
  useEffect(() => {
    const load = async () => {
      setIsLoading(true)
      try {
        // If search query exists, search all products in database
        // Otherwise, load normally with pagination
        const resp = await apiService.getProductManagement(
          debouncedSearchQuery ? 10000 : PAGE_SIZE, // Large limit for search, normal pagination otherwise
          0, 
          sortBy, 
          sortOrder,
          debouncedSearchQuery || undefined
        )
        if (resp.success && Array.isArray(resp.data)) {
          setItems(resp.data)
          setCurrentOffset(resp.data.length)
          // For search, disable "load more" since we get all results
          // For normal load, enable if we got a full page
          setHasMoreProducts(debouncedSearchQuery ? false : resp.data.length === PAGE_SIZE)
        } else {
          setItems([])
          setCurrentOffset(0)
          setHasMoreProducts(false)
        }
      } finally {
        setIsLoading(false)
      }
    }
    load()
  }, [sortBy, sortOrder, debouncedSearchQuery])

  useEffect(() => {
    const loadPrimaryCategories = async () => {
      try {
        const response = await apiService.getPrimaryCategories()
        if (response.success && response.categories) {
          setPrimaryCategories(response.categories.map((cat: { id: number; category_name: string; default_category_ids?: string[] | number[] | unknown }) => {
            // Ensure default_category_ids is always an array of strings
            let defaultIds: string[] = []
            if (Array.isArray(cat.default_category_ids)) {
              defaultIds = cat.default_category_ids.map((id: string | number) => String(id).trim()).filter((id: string) => id !== '')
            }
            
            return {
              id: cat.id,
              category_name: cat.category_name,
              default_category_ids: defaultIds
            }
          }))
        }
      } catch (error) {
        console.error("Failed to load primary categories:", error)
      }
    }
    loadPrimaryCategories()
  }, [])

  const toggleProduct = useCallback((id: string) => {
    setSelectedProducts((prev) => (prev.includes(id) ? prev.filter((p) => p !== id) : [...prev, id]))
  }, [])
  
  const refreshProducts = async () => {
    setIsLoading(true)
    try {
      const resp = await apiService.getProductManagement(PAGE_SIZE, 0, sortBy, sortOrder)
      if (resp.success && Array.isArray(resp.data)) {
        setItems(resp.data)
        setCurrentOffset(resp.data.length)
        setHasMoreProducts(resp.data.length === PAGE_SIZE)
        // Refresh statistics after loading products
        await loadProductStats()
        toast({
          title: "成功",
          description: "商品リストを更新しました",
        })
      }
    } catch (error) {
      toast({
        title: "エラー",
        description: "商品リストの更新に失敗しました",
        variant: "destructive",
      })
    } finally {
      setIsLoading(false)
    }
  }

  const handleLoadMoreProducts = async () => {
    if (isLoadingMore || isLoading || !hasMoreProducts) return
    setIsLoadingMore(true)
    try {
      const resp = await apiService.getProductManagement(PAGE_SIZE, currentOffset, sortBy, sortOrder)
      const newItems = Array.isArray(resp.data) ? resp.data : []
      if (resp.success && newItems.length > 0) {
        setItems(prev => [...prev, ...newItems])
        const newOffset = currentOffset + newItems.length
        setCurrentOffset(newOffset)
        if (newItems.length < PAGE_SIZE) {
          setHasMoreProducts(false)
        }
      } else {
        setHasMoreProducts(false)
      }
    } finally {
      setIsLoadingMore(false)
    }
  }
  
  const handleToggleHideItem = async (product: Product) => {
    // Use item_number as the identifier (not id)
    const productId = product.item_number || product.id
    if (!productId) return
    
    setToggling(prev => ({ ...prev, [productId]: true }))
    try {
      // Toggle: if currently hidden (true), make visible (false), and vice versa
      const newHideItem = product.hide_item === true ? false : true
      
      // Call API to update database
      const response = await apiService.updateProductHideItem(productId, newHideItem)
      
      if (response.success) {
        // Update local state - match by item_number first, then id
        setItems(prev => prev.map(item => {
          const itemId = item.item_number || item.id
          return itemId === productId 
            ? { ...item, hide_item: newHideItem }
            : item
        }))
        toast({
          title: "成功",
          description: `商品を${newHideItem ? '非表示' : '表示'}に設定しました`,
        })
      } else {
        throw new Error(response.error || '更新に失敗しました')
      }
    } catch (error) {
      toast({
        title: "エラー",
        description: "状態の更新に失敗しました",
        variant: "destructive",
      })
    } finally {
      setToggling(prev => ({ ...prev, [productId]: false }))
    }
  }

  const handleToggleBlock = async (product: Product) => {
    // Use item_number as the identifier (not id)
    const productId = product.item_number || product.id
    if (!productId) return
    
    setBlocking(prev => ({ ...prev, [productId]: true }))
    try {
      // Toggle: if currently blocked (true), unblock (false), and vice versa
      const newBlock = product.block === true ? false : true
      
      // Call API to update database
      const response = await apiService.updateProductBlock(productId, newBlock)
      
      if (response.success) {
        // Update local state - match by item_number first, then id
        setItems(prev => prev.map(item => {
          const itemId = item.item_number || item.id
          return itemId === productId 
            ? { ...item, block: newBlock }
            : item
        }))
        toast({
          title: "成功",
          description: `商品を${newBlock ? 'ブロック' : 'アンブロック'}しました`,
        })
      } else {
        throw new Error(response.error || '更新に失敗しました')
      }
    } catch (error) {
      toast({
        title: "エラー",
        description: "ブロック状態の更新に失敗しました",
        variant: "destructive",
      })
    } finally {
      setBlocking(prev => ({ ...prev, [productId]: false }))
    }
  }
  
  const handleRegisterToRakuten = async (product: Product) => {
    // Use item_number as the identifier (not id)
    const itemNumber = product.item_number
    if (!itemNumber) {
      toast({
        title: "エラー",
        description: "商品番号が見つかりません",
        variant: "destructive",
      })
      return
    }
    
    setRegistering(prev => ({ ...prev, [itemNumber]: true }))
    try {
      toast({
        title: "登録中",
        description: "楽天市場に商品を登録しています...",
      })
      
      // Call API to register product to Rakuten
      const result = await apiService.registerProductToRakuten(itemNumber)
      
      if (result.success) {
        toast({
          title: "成功",
          description: result.message || "商品を楽天市場に登録しました",
        })
      } else {
        const errorMsg = translateErrorMessage(result.error_details || result.error) || "登録に失敗しました"
        toast({
          title: "エラー",
          description: errorMsg,
          variant: "destructive",
        })
      }
      // Always refresh the product list to update status badge (both success and failure)
      await refreshProducts()
    } catch (error: any) {
      toast({
        title: "エラー",
        description: translateErrorMessage(error?.message) || "登録に失敗しました",
        variant: "destructive",
      })
    } finally {
      setRegistering(prev => ({ ...prev, [itemNumber]: false }))
    }
  }

  const handleCheckRegistrationStatus = async (product: Product) => {
    // Use item_number as the identifier (not id)
    const itemNumber = product.item_number
    if (!itemNumber) {
      toast({
        title: "エラー",
        description: "商品番号が見つかりません",
        variant: "destructive",
      })
      return
    }
    
    setCheckingStatus(prev => ({ ...prev, [itemNumber]: true }))
    try {
      toast({
        title: "確認中",
        description: "楽天市場の商品登録状態を確認しています...",
      })
      
      // Call API to check product registration status
      const result = await apiService.checkProductRegistrationStatus(itemNumber)
      
      if (result.success) {
        const status = result.new_status || result.status
        let message = result.message || "状態を確認しました"
        if (status === "registered") {
          message = "商品は楽天市場に登録されています"
        } else if (status === "deleted") {
          message = "商品は楽天市場から削除されています"
        }
        
        toast({
          title: "確認完了",
          description: message,
        })
      } else {
        const errorMsg = translateErrorMessage(result.error) || "状態確認に失敗しました"
        toast({
          title: "エラー",
          description: errorMsg,
          variant: "destructive",
        })
      }
      // Always refresh the product list to update status badge
      await refreshProducts()
    } catch (error: any) {
      toast({
        title: "エラー",
        description: translateErrorMessage(error?.message) || "状態確認に失敗しました",
        variant: "destructive",
      })
    } finally {
      setCheckingStatus(prev => ({ ...prev, [itemNumber]: false }))
    }
  }

  const handleBulkCheckRegistrationStatus = async () => {
    if (selectedProducts.length === 0) {
      toast({
        title: "エラー",
        description: "商品を選択してください",
        variant: "destructive",
      })
      return
    }
    
    setIsBulkCheckingStatus(true)
    try {
      toast({
        title: "一括確認中",
        description: `${selectedProducts.length}件の商品の登録状態を確認しています...`,
      })
      
      // Call API to check multiple products registration status
      const result = await apiService.checkMultipleProductsRegistrationStatus(selectedProducts)
      
      if (result.success) {
        const successCount = result.success_count || 0
        const errorCount = result.error_count || 0
        const total = result.total || selectedProducts.length
        
        toast({
          title: "一括確認完了",
          description: `${total}件中 ${successCount}件成功、${errorCount}件エラー`,
        })
      } else {
        const errorMsg = translateErrorMessage(result.error) || "一括確認に失敗しました"
        toast({
          title: "エラー",
          description: errorMsg,
          variant: "destructive",
        })
      }
      // Always refresh the product list to update status badges
      await refreshProducts()
      // Clear selection after bulk operation
      setSelectedProducts([])
    } catch (error: any) {
      toast({
        title: "エラー",
        description: translateErrorMessage(error?.message) || "一括確認に失敗しました",
        variant: "destructive",
      })
    } finally {
      setIsBulkCheckingStatus(false)
    }
  }

  const handleRegisterInventoryToRakuten = async (product: Product) => {
    // Use item_number as the identifier (not id)
    const itemNumber = product.item_number
    if (!itemNumber) {
      toast({
        title: "エラー",
        description: "商品番号が見つかりません",
        variant: "destructive",
      })
      return
    }
    
    setRegisteringInventory(prev => ({ ...prev, [itemNumber]: true }))
    try {
      toast({
        title: "登録中",
        description: "楽天市場に在庫を登録しています...",
      })
      
      // Call API to register inventory to Rakuten
      const result = await apiService.registerInventoryToRakuten(itemNumber)
      
      if (result.success) {
        const registeredCount = result.registered_count || 0
        const totalCount = result.total_count || 0
        const failedCount = result.failed_count || 0
        
        if (failedCount && failedCount > 0) {
          toast({
            title: "部分成功",
            description: result.message || `${registeredCount}/${totalCount}件の在庫を登録しました（${failedCount}件失敗）`,
          })
        } else {
          toast({
            title: "成功",
            description: result.message || `${registeredCount}件の在庫を楽天市場に登録しました`,
          })
        }
      } else {
        const errorMsg = translateErrorMessage(result.error_details || result.error) || "在庫登録に失敗しました"
        toast({
          title: "エラー",
          description: errorMsg,
          variant: "destructive",
        })
      }
    } catch (error: any) {
      toast({
        title: "エラー",
        description: translateErrorMessage(error?.message) || "在庫登録に失敗しました",
        variant: "destructive",
      })
    } finally {
      setRegisteringInventory(prev => ({ ...prev, [itemNumber]: false }))
    }
  }
  
  const handleDeleteClick = (product: Product) => {
    // Use item_number as the identifier (not id)
    const productId = (product.item_number || product.id)?.toString()
    const productName = product.title || product.name || '商品'
    if (productId) {
      setProductToDelete({ id: productId, name: productName })
      setDeleteConfirmOpen(true)
    } else {
      toast({
        title: "エラー",
        description: "商品番号が見つかりません",
        variant: "destructive",
      })
    }
  }
  
  const handleConfirmDelete = async () => {
    if (!productToDelete) return
    setIsDeletingProduct(true)
    try {
      const response = await apiService.deleteProductManagement(productToDelete.id)
      if (response.success) {
        setItems(prev => prev.filter(item => {
          const itemId = (item.item_number || item.id)?.toString()
          return itemId !== productToDelete.id
        }))
        setSelectedProducts(prev => prev.filter(id => id !== productToDelete.id))
        toast({
          title: "成功",
          description: "商品を削除しました",
        })
      } else {
        toast({
          title: "エラー",
          description: response.error || "商品の削除に失敗しました",
          variant: "destructive",
        })
      }
    } catch (error) {
      toast({
        title: "エラー",
        description: "商品の削除に失敗しました",
        variant: "destructive",
      })
    } finally {
      setIsDeletingProduct(false)
      setDeleteConfirmOpen(false)
      setProductToDelete(null)
    }
  }

  const handleDeleteFromRakutenClick = (product: Product) => {
    // Use item_number as the identifier (not id)
    const productId = (product.item_number || product.id)?.toString()
    const productName = product.title || product.name || '商品'
    if (productId) {
      setProductToDeleteFromRakuten({ id: productId, name: productName })
      setRakutenDeleteConfirmOpen(true)
    } else {
      toast({
        title: "エラー",
        description: "商品番号が見つかりません",
        variant: "destructive",
      })
    }
  }

  const handleConfirmDeleteFromRakuten = async () => {
    if (!productToDeleteFromRakuten) return
    setIsDeletingFromRakuten(true)
    try {
      toast({
        title: "削除中",
        description: "楽天市場から商品を削除しています...",
      })
      
      const result = await apiService.deleteProductFromRakuten(productToDeleteFromRakuten.id)
      
      if (result.success) {
        toast({
          title: "成功",
          description: result.message || "商品を楽天市場から削除しました",
        })
        // Refresh the product list to update status badge
        await refreshProducts()
      } else {
        const errorMsg = translateErrorMessage(result.error_details || result.error) || "削除に失敗しました"
        toast({
          title: "エラー",
          description: errorMsg,
          variant: "destructive",
        })
      }
    } catch (error: any) {
      toast({
        title: "エラー",
        description: translateErrorMessage(error?.message) || "削除に失敗しました",
        variant: "destructive",
      })
    } finally {
      setIsDeletingFromRakuten(false)
      setRakutenDeleteConfirmOpen(false)
      setProductToDeleteFromRakuten(null)
    }
  }

  const handleBulkDeleteFromRakutenClick = () => {
    if (selectedProducts.length === 0) {
      toast({
        title: "エラー",
        description: "削除する商品を選択してください",
        variant: "destructive",
      })
      return
    }
    setBulkRakutenDeleteConfirmOpen(true)
  }

  const handleConfirmBulkDeleteFromRakuten = async () => {
    if (selectedProducts.length === 0) {
      setBulkRakutenDeleteConfirmOpen(false)
      return
    }
    setIsBulkDeletingFromRakuten(true)
    try {
      toast({
        title: "削除中",
        description: `${selectedProducts.length}件の商品を楽天市場から削除しています...`,
      })

      // Call API to delete multiple products from Rakuten
      const result = await apiService.deleteMultipleProductsFromRakuten(selectedProducts)

      if (result.success || (result.success_count && result.success_count > 0)) {
        const successCount = result.success_count || 0
        const failureCount = result.failure_count || 0
        const totalCount = result.total_count || selectedProducts.length

        if (failureCount > 0) {
          toast({
            title: "部分成功",
            description: `${successCount}/${totalCount}件の商品を楽天市場から削除しました（${failureCount}件失敗）`,
          })
        } else {
          toast({
            title: "成功",
            description: `${successCount}件の商品を楽天市場から削除しました`,
          })
        }
      } else {
        const errorMsg = translateErrorMessage(result.error || result.message) || "削除に失敗しました"
        toast({
          title: "エラー",
          description: errorMsg,
          variant: "destructive",
        })
      }

      // Always refresh the product list to update status badges
      await refreshProducts()
    } catch (error: any) {
      toast({
        title: "エラー",
        description: translateErrorMessage(error?.message) || "削除に失敗しました",
        variant: "destructive",
      })
    } finally {
      setIsBulkDeletingFromRakuten(false)
      setBulkRakutenDeleteConfirmOpen(false)
    }
  }
  
  const handleDetailSettings = (product: Product) => {
    // Initialize form with product data
    const features = product.features || {}
    const payment = product.payment || {}
    
    // Extract delivery date IDs from first variant if available
    const variants = (product.variants || {}) as Record<string, ProductVariant>
    const firstVariantKey = Object.keys(variants)[0]
    const firstVariant = firstVariantKey ? variants[firstVariantKey] : null
    const normalDeliveryDateId = firstVariant?.normalDeliveryDateId?.toString() || "1"
    
    setSettingsForm({
      title: product.title || "",
      itemType: product.item_type || "NORMAL",
      genreId: product.genre_id || "",
      tags: Array.isArray(product.tags) ? product.tags.join(", ") : "",
      unlimitedInventoryFlag: product.unlimited_inventory_flag ? "true" : "false",
      searchVisibility: (features && typeof features === 'object' && 'searchVisibility' in features && typeof features.searchVisibility === 'string') ? features.searchVisibility : "ALWAYS_VISIBLE",
      inventoryDisplay: (features && typeof features === 'object' && 'inventoryDisplay' in features && typeof features.inventoryDisplay === 'string') ? features.inventoryDisplay : "DISPLAY_ABSOLUTE_STOCK_COUNT",
      review: (features && typeof features === 'object' && 'review' in features && typeof features.review === 'string') ? features.review : "SHOP_SETTING",
      taxIncluded: (payment && typeof payment === 'object' && 'taxIncluded' in payment) ? (payment.taxIncluded ? "true" : "false") : "false",
      taxRate: payment.taxRate?.toString() || "",
      cashOnDeliveryFeeIncluded: payment.cashOnDeliveryFeeIncluded ? "true" : "false",
      normalDeliveryDateId: normalDeliveryDateId,
    })
    setSettingsFormErrors({})
    setTouchedFields(new Set())
    setSelectedProductForSettings(product)
    setSettingsModalOpen(true)
  }
  
  // Real-time validation for form fields
  const validateFieldOnChange = useCallback((fieldName: string, value: string) => {
    const rules: Array<{ validate: (value: unknown) => boolean; message: string }> = []
    
    if (fieldName === 'genreId' && value) {
      rules.push(ValidationRules.numeric('ジャンルIDは数値である必要があります'))
      rules.push(ValidationRules.custom(
        (val) => {
          const num = parseInt(String(val))
          return isNaN(num) || (num >= 100000 && num <= 999999)
        },
        'ジャンルIDは100,000から999,999の間の数値である必要があります'
      ))
    }
    
    if (fieldName === 'tags' && value) {
      rules.push(ValidationRules.custom(
        (val) => {
          const tagStrings = String(val).split(",").map(t => t.trim()).filter(t => t)
          if (tagStrings.length === 0) return true
          return tagStrings.every(tagStr => {
            const tagNum = parseInt(tagStr)
            return !isNaN(tagNum) && tagNum >= 5000000 && tagNum <= 9999999
          })
        },
        'タグは5,000,000から9,999,999の間の数値をカンマ区切りで入力してください'
      ))
    }
    
    if (fieldName === 'taxRate' && value) {
      rules.push(ValidationRules.positiveNumber('税率は0以上の数値である必要があります'))
      rules.push(ValidationRules.custom(
        (val) => {
          const decimalParts = String(val).split(".")
          return decimalParts.length <= 1 || decimalParts[1].length <= 2
        },
        '税率は小数点以下2桁まで入力できます'
      ))
    }
    
    if (fieldName === 'title' && value) {
      rules.push(ValidationRules.maxLength(200, 'タイトルは200文字まで入力できます'))
    }
    
    const result = validateOnChange(settingsForm, fieldName as keyof typeof settingsForm, rules, touchedFields)
    setSettingsFormErrors(prev => ({
      ...prev,
      [fieldName]: result.error || ''
    }))
    
    return result.isValid
  }, [settingsForm, touchedFields])

  const handleFieldBlur = useCallback((fieldName: string) => {
    setTouchedFields(prev => new Set(prev).add(fieldName))
    validateFieldOnChange(fieldName, settingsForm[fieldName as keyof typeof settingsForm] as string)
  }, [settingsForm, validateFieldOnChange])
  
  const validateSettingsForm = (): boolean => {
    // Mark all fields as touched for validation
    const allFields = ['title', 'genreId', 'tags', 'taxRate']
    allFields.forEach(field => {
      setTouchedFields(prev => new Set(prev).add(field))
    })
    
    const errors: Record<string, string> = {}
    
    // Validate genre ID
    if (settingsForm.genreId) {
      const genreIdNum = parseInt(settingsForm.genreId)
      if (isNaN(genreIdNum) || genreIdNum < 100000 || genreIdNum > 999999) {
        errors.genreId = "ジャンルIDは100,000から999,999の間の数値である必要があります"
      }
    }
    
    // Validate tags
    if (settingsForm.tags) {
      const tagStrings = settingsForm.tags.split(",").map(t => t.trim()).filter(t => t)
      for (const tagStr of tagStrings) {
        const tagNum = parseInt(tagStr)
        if (isNaN(tagNum) || tagNum < 5000000 || tagNum > 9999999) {
          errors.tags = "タグは5,000,000から9,999,999の間の数値である必要があります"
          break
        }
      }
    }
    
    // Validate tax rate
    if (settingsForm.taxRate) {
      const taxRateNum = parseFloat(settingsForm.taxRate)
      if (isNaN(taxRateNum) || taxRateNum < 0) {
        errors.taxRate = "税率は0以上の数値である必要があります"
      }
      // Check decimal places
      const decimalParts = settingsForm.taxRate.split(".")
      if (decimalParts.length > 1 && decimalParts[1].length > 2) {
        errors.taxRate = "税率は小数点以下2桁まで入力できます"
      }
    }
    
    setSettingsFormErrors(errors)
    return Object.keys(errors).length === 0
  }
  
  const handleSaveSettings = async () => {
    if (!validateSettingsForm()) {
      toast({
        title: "エラー",
        description: "入力内容に誤りがあります。確認してください。",
        variant: "destructive",
      })
      return
    }
    
    if (!selectedProductForSettings) return
    
    setIsSavingSettings(true)
    try {
      const productId = selectedProductForSettings.item_number || selectedProductForSettings.id
      
      // Parse tags
      const tagsArray = settingsForm.tags
        ? settingsForm.tags.split(",").map(t => parseInt(t.trim())).filter(t => !isNaN(t))
        : []
      
      // Build features object
      const features = {
        searchVisibility: settingsForm.searchVisibility,
        inventoryDisplay: settingsForm.inventoryDisplay,
        review: settingsForm.review,
      }
      
      // Build payment object
      const payment: any = {
        taxIncluded: settingsForm.taxIncluded === "true",
        cashOnDeliveryFeeIncluded: settingsForm.cashOnDeliveryFeeIncluded === "true",
      }
      if (settingsForm.taxRate) {
        const taxRateStr = settingsForm.taxRate || '0'
        payment.taxRate = parseFloat(taxRateStr)
      }
      
      if (!productId) {
        toast({
          title: "エラー",
          description: "商品IDが見つかりません",
          variant: "destructive",
        })
        return
      }
      
      const response = await apiService.updateProductSettings(productId, {
        title: settingsForm.title || undefined,
        item_type: settingsForm.itemType,
        genre_id: settingsForm.genreId || null,
        tags: tagsArray,
        unlimited_inventory_flag: settingsForm.unlimitedInventoryFlag === "true",
        features: features,
        payment: payment,
        normalDeliveryDateId: parseInt(settingsForm.normalDeliveryDateId) || 1,
      })
      
      if (response.success) {
        toast({
          title: "成功",
          description: "設定を保存しました",
        })
        
        // Reload products to get updated data
        const resp = await apiService.getProductManagement(200, 0)
        if (resp.success && Array.isArray(resp.data)) {
          setItems(resp.data)
        }
        
        // Close modal
        setSettingsModalOpen(false)
        setSelectedProductForSettings(null)
      } else {
        throw new Error(response.error || '設定の保存に失敗しました')
      }
    } catch (error: unknown) {
      const standardError = parseError(error)
      toast({
        title: "エラー",
        description: standardError.userMessage,
        variant: "destructive",
      })
    } finally {
      setIsSavingSettings(false)
    }
  }
  
  const handleImageSettings = (product: any) => {
    setSelectedProductForImages(product)
    setImageModalOpen(true)
  }

  const getImageUrl = (location: string): string => {
    if (!location) return "/placeholder.svg"
    // Construct full URL from base URL + location
    const baseUrl = "https://licel-product-image.s3.ap-southeast-2.amazonaws.com/products"
    // Ensure location starts with /
    const cleanLocation = location.startsWith("/") ? location : `/${location}`
    return `${baseUrl}${cleanLocation}`
  }

  const handleDeleteImage = async (image: { type: string; location: string }) => {
    if (!selectedProductForImages) return
    
    const itemNumber = selectedProductForImages.item_number
    const imageLocation = image.location
    
    if (!itemNumber || !imageLocation) {
      toast({
        title: "エラー",
        description: "商品番号または画像の場所が見つかりません",
        variant: "destructive",
      })
      return
    }
    
    const deleteKey = `${itemNumber}-${imageLocation}`
    setDeletingImage(prev => ({ ...prev, [deleteKey]: true }))
    
    try {
      const result = await apiService.deleteProductImage(itemNumber, imageLocation)
      
      if (result.success) {
        toast({
          title: "成功",
          description: "画像を削除しました",
        })
        
        // Normalize image location for comparison
        const normalizedLocation = imageLocation?.trim() || ""
        
        // Update local state - remove image from product
        setItems(prev => prev.map(item => {
          const itemId = item.item_number || item.id
          if (itemId === itemNumber) {
            const updatedImages = Array.isArray(item.images) 
              ? item.images.filter((img: any) => {
                  const imgLocation = img?.location?.trim() || ""
                  return imgLocation !== normalizedLocation
                })
              : []
            return { ...item, images: updatedImages }
          }
          return item
        }))
        
        // Update selected product for images modal
        setSelectedProductForImages((prev: Product | null) => {
          if (!prev) return prev
          const updatedImages = Array.isArray(prev.images)
            ? prev.images.filter((img: { type: string; location: string }) => {
                const imgLocation = img?.location?.trim() || ""
                return imgLocation !== normalizedLocation
              })
            : []
          return { ...prev, images: updatedImages }
        })
      } else {
        const errorMsg = translateErrorMessage(result.error) || "画像の削除に失敗しました"
        toast({
          title: "エラー",
          description: errorMsg,
          variant: "destructive",
        })
      }
    } catch (error: any) {
      toast({
        title: "エラー",
        description: translateErrorMessage(error?.message) || "画像の削除に失敗しました",
        variant: "destructive",
      })
    } finally {
      setDeletingImage(prev => ({ ...prev, [deleteKey]: false }))
    }
  }

  const handleBulkDeleteClick = () => {
    if (selectedProducts.length === 0) {
      toast({
        title: "エラー",
        description: "削除する商品を選択してください",
        variant: "destructive",
      })
      return
    }
    setBulkDeleteConfirmOpen(true)
  }

  const handleBulkRegisterToRakuten = async () => {
    if (selectedProducts.length === 0) {
      toast({
        title: "エラー",
        description: "登録する商品を選択してください",
        variant: "destructive",
      })
      return
    }

    setIsBulkRegistering(true)
    setBulkRegistrationProgress({
      current: 0,
      total: selectedProducts.length,
      currentItem: null
    })

    try {
      toast({
        title: "登録開始",
        description: `${selectedProducts.length}件の商品を楽天市場に登録します...`,
      })

      let successCount = 0
      let failureCount = 0
      let skippedCount = 0

      // Process each product sequentially to show progress
      for (let i = 0; i < selectedProducts.length; i++) {
        const itemNumber = selectedProducts[i]
        
        // Find product name for display
        const product = items.find(p => p.item_number === itemNumber)
        const productName = product?.title || itemNumber

        // Update progress before processing (showing which item is being processed)
        setBulkRegistrationProgress({
          current: i,
          total: selectedProducts.length,
          currentItem: productName
        })

        try {
          // Register individual product
          const result = await apiService.registerProductToRakuten(itemNumber)
          
          if (result.success) {
            successCount++
          } else if (result.skipped) {
            skippedCount++
            console.warn(`Skipped product ${itemNumber}: ${result.error || result.message}`)
          } else {
            failureCount++
            console.warn(`Failed to register product ${itemNumber}: ${result.error || result.message}`)
          }
        } catch (error: any) {
          failureCount++
          console.error(`Error registering product ${itemNumber}:`, error)
        }

        // Update progress after processing (showing completed count)
        setBulkRegistrationProgress({
          current: i + 1,
          total: selectedProducts.length,
          currentItem: null
        })

        // Small delay to prevent overwhelming the API
        if (i < selectedProducts.length - 1) {
          await new Promise(resolve => setTimeout(resolve, 100))
        }
      }

      // Show result toast
      if (failureCount > 0 || skippedCount > 0) {
        const parts = []
        if (successCount > 0) parts.push(`${successCount}件成功`)
        if (failureCount > 0) parts.push(`${failureCount}件失敗`)
        if (skippedCount > 0) parts.push(`${skippedCount}件スキップ`)
        toast({
          title: "部分成功",
          description: `${parts.join('、')}（合計${selectedProducts.length}件）`,
        })
      } else {
        toast({
          title: "成功",
          description: `${successCount}件の商品を楽天市場に登録しました`,
        })
      }

      // Always refresh the product list to update status badges
      await refreshProducts()
    } catch (error: any) {
      toast({
        title: "エラー",
        description: translateErrorMessage(error?.message) || "登録に失敗しました",
        variant: "destructive",
      })
    } finally {
      setIsBulkRegistering(false)
      setBulkRegistrationProgress({
        current: 0,
        total: 0,
        currentItem: null
      })
    }
  }

  const handleBulkRegisterInventoryToRakuten = async () => {
    if (selectedProducts.length === 0) {
      toast({
        title: "エラー",
        description: "登録する商品を選択してください",
        variant: "destructive",
      })
      return
    }

    setIsBulkRegisteringInventory(true)
    setBulkInventoryRegistrationProgress({
      current: 0,
      total: selectedProducts.length,
      currentItem: null
    })

    try {
      toast({
        title: "登録開始",
        description: `${selectedProducts.length}件の商品の在庫を楽天市場に登録します...`,
      })

      // Call API to register multiple inventory to Rakuten
      const result = await apiService.registerMultipleInventoryToRakuten(selectedProducts)

      if (result.success || (result.success_products && result.success_products > 0)) {
        const successProducts = result.success_products || 0
        const failureProducts = result.failure_products || 0
        const totalProducts = result.total_products || selectedProducts.length
        const registeredVariants = result.total_registered_variants || 0
        const failedVariants = result.total_failed_variants || 0

        if (failureProducts > 0 || failedVariants > 0) {
          toast({
            title: "部分成功",
            description: `${successProducts}/${totalProducts}件の商品の在庫を登録しました（${registeredVariants}バリアント登録、${failedVariants}バリアント失敗）`,
          })
        } else {
          toast({
            title: "成功",
            description: `${successProducts}件の商品の在庫（${registeredVariants}バリアント）を楽天市場に登録しました`,
          })
        }
      } else {
        const errorMsg = translateErrorMessage(result.error || result.message) || "在庫登録に失敗しました"
        toast({
          title: "エラー",
          description: errorMsg,
          variant: "destructive",
        })
      }

      // Always refresh the product list to update status badges
      await refreshProducts()
    } catch (error: any) {
      toast({
        title: "エラー",
        description: translateErrorMessage(error?.message) || "在庫登録に失敗しました",
        variant: "destructive",
      })
    } finally {
      setIsBulkRegisteringInventory(false)
      setBulkInventoryRegistrationProgress({
        current: 0,
        total: 0,
        currentItem: null
      })
    }
  }

  const handleBulkUploadImagesToRakuten = async () => {
    if (selectedProducts.length === 0) {
      toast({
        title: "エラー",
        description: "アップロードする商品を選択してください",
        variant: "destructive",
      })
      return
    }

    // Filter products that should be processed
    // Skip products where:
    // 1. image_registration_status is 't' (true) - already registered
    // 2. rakuten_registration_status is false - not registered to Rakuten
    const productsToProcess = selectedProducts.filter(itemNumber => {
      const product = items.find(p => p.item_number === itemNumber)
      if (!product) return false
      
      // Skip if image_registration_status is 't' (true)
      const imageStatus = product.image_registration_status
      if (imageStatus === true || (typeof imageStatus === 'string' && (imageStatus === 't' || imageStatus === 'true'))) {
        return false
      }
      
      // Skip if rakuten_registration_status is false
      const rakutenStatus = product.rakuten_registration_status
      if (
        (typeof rakutenStatus === 'boolean' && rakutenStatus === false) ||
        (typeof rakutenStatus === 'number' && rakutenStatus === 0) ||
        (typeof rakutenStatus === 'string' && (rakutenStatus === '0' || rakutenStatus.toLowerCase() === 'f' || rakutenStatus.toLowerCase() === 'false'))
      ) {
        return false
      }
      
      return true
    })

    if (productsToProcess.length === 0) {
      toast({
        title: "スキップ",
        description: "アップロード対象の商品がありません（既に登録済み、または楽天未登録の商品はスキップされます）",
        variant: "default",
      })
      return
    }

    // Calculate total images to upload
    let totalImages = 0
    productsToProcess.forEach(itemNumber => {
      const product = items.find(p => p.item_number === itemNumber)
      if (product?.images) {
        try {
          const images = typeof product.images === 'string' ? JSON.parse(product.images) : product.images
          if (Array.isArray(images)) {
            totalImages += images.length
          }
        } catch (e) {
          // Ignore parse errors
        }
      }
    })

    setIsBulkUploadingImages(true)
    setBulkImageUploadProgress({
      current: 0,
      total: productsToProcess.length,
      uploadedImages: 0,
      totalImages: totalImages,
      completed: []
    })

    try {
      toast({
        title: "アップロード開始",
        description: `${productsToProcess.length}件の商品（${totalImages}画像）を楽天市場にアップロードします...`,
      })

      let successCount = 0
      let failureCount = 0
      let uploadedImagesCount = 0
      let failedImagesCount = 0
      const completedItems: Array<{ item_number: string; status: 'success' | 'error'; message: string }> = []

      // Process each product sequentially to show progress
      for (let i = 0; i < productsToProcess.length; i++) {
        const itemNumber = productsToProcess[i]
        
        // Find product name for display
        const product = items.find(p => p.item_number === itemNumber)
        const productName = product?.title || itemNumber

        // Update progress before processing
        setBulkImageUploadProgress(prev => ({
          ...prev,
          current: i,
        }))

        try {
          // Upload images for individual product
          const result = await apiService.uploadProductImagesToRakuten(itemNumber)
          
          if (result.success) {
            successCount++
            const uploaded = result.uploaded_count || 0
            const failed = result.failed_count || 0
            uploadedImagesCount += uploaded
            failedImagesCount += failed
            
            completedItems.push({
              item_number: itemNumber,
              status: 'success',
              message: `${uploaded}画像アップロード成功${failed > 0 ? ` (${failed}画像失敗)` : ''}`
            })
          } else {
            failureCount++
            const total = result.total || 0
            failedImagesCount += total
            
            completedItems.push({
            item_number: itemNumber,
            status: 'error',
            message: translateErrorMessage(result.error || result.message) || 'アップロード失敗'
          })
          }
        } catch (error: any) {
          failureCount++
          console.error(`Error uploading images for product ${itemNumber}:`, error)
          
          completedItems.push({
            item_number: itemNumber,
            status: 'error',
            message: translateErrorMessage(error?.message) || 'アップロードエラー'
          })
        }

        // Update progress after processing
        setBulkImageUploadProgress(prev => ({
          ...prev,
          current: i + 1,
          uploadedImages: uploadedImagesCount,
          completed: [...completedItems]
        }))

        // Small delay to prevent overwhelming the API
        if (i < productsToProcess.length - 1) {
          await new Promise(resolve => setTimeout(resolve, 100))
        }
      }

      // Show result toast
      if (failureCount > 0 || failedImagesCount > 0) {
        toast({
          title: "部分成功",
          description: `${successCount}/${productsToProcess.length}件の商品の画像をアップロードしました（${uploadedImagesCount}画像登録、${failedImagesCount}画像失敗）`,
        })
      } else {
        toast({
          title: "成功",
          description: `${successCount}件の商品の画像（${uploadedImagesCount}画像）を楽天市場にアップロードしました`,
        })
      }

      // Always refresh the product list to update status badges
      await refreshProducts()
    } catch (error: any) {
      toast({
        title: "エラー",
        description: translateErrorMessage(error?.message) || "画像のアップロードに失敗しました",
        variant: "destructive",
      })
    } finally {
      setIsBulkUploadingImages(false)
      // Keep completed items for a moment to show final status
      setTimeout(() => {
        setBulkImageUploadProgress({
          current: 0,
          total: 0,
          uploadedImages: 0,
          totalImages: 0,
          completed: []
        })
      }, 3000)
    }
  }

  const handleConfirmBulkDelete = async () => {
    if (selectedProducts.length === 0) {
      setBulkDeleteConfirmOpen(false)
      return
    }
    setIsBulkDeleting(true)
    try {
      const response = await apiService.deleteProductManagementBatch(selectedProducts)
      if (response.success) {
        const deletedCount = response.deleted_count ?? selectedProducts.length
        setItems(prev =>
          prev.filter(item => {
            const itemId = (item.item_number || item.id)?.toString()
            return itemId ? !selectedProducts.includes(itemId) : true
          }),
        )
        toast({
          title: "成功",
          description: `${deletedCount}件の商品を削除しました`,
        })
        setSelectedProducts([])
      } else {
        toast({
          title: "エラー",
          description: response.error || "商品の削除に失敗しました",
          variant: "destructive",
        })
      }
    } catch (error: any) {
        toast({
          title: "エラー",
          description: translateErrorMessage(error?.message) || "商品の削除に失敗しました",
          variant: "destructive",
        })
    } finally {
      setIsBulkDeleting(false)
      setBulkDeleteConfirmOpen(false)
    }
  }

  const handleViewImage = (image: any) => {
    const imageUrl = getImageUrl(image.location)
    window.open(imageUrl, "_blank")
  }

  const handleUploadImagesToRakuten = async () => {
    if (!selectedProductForImages) return
    
    const itemNumber = selectedProductForImages.item_number
    if (!itemNumber) {
      toast({
        title: "エラー",
        description: "商品番号が見つかりません",
        variant: "destructive",
      })
      return
    }
    
    setUploadingToRakuten(true)
    try {
      toast({
        title: "アップロード中",
        description: "楽天市場に画像をアップロードしています...",
      })
      
      const result = await apiService.uploadProductImagesToRakuten(itemNumber)
      
      if (result.success || (result.uploaded_count && result.uploaded_count > 0)) {
        const uploadedCount = result.uploaded_count || 0
        const totalCount = result.total || 0
        const failedCount = result.failed_count || 0
        
        if (failedCount && failedCount > 0) {
          toast({
            title: "部分成功",
            description: result.message || `${uploadedCount}/${totalCount}件の画像をアップロードしました（${failedCount}件失敗）`,
          })
        } else {
          toast({
            title: "成功",
            description: result.message || `${uploadedCount}件の画像を楽天市場にアップロードしました`,
          })
        }
        
        // Optionally refresh product data to get updated information
        if (result.folder_id || result.folder_name) {
        }
      } else {
        const errorMsg = result.error || "画像のアップロードに失敗しました"
        toast({
          title: "エラー",
          description: errorMsg,
          variant: "destructive",
        })
      }
    } catch (error: any) {
      toast({
        title: "エラー",
        description: translateErrorMessage(error?.message) || "画像のアップロードに失敗しました",
        variant: "destructive",
      })
    } finally {
      setUploadingToRakuten(false)
    }
  }
  
  const handleSkuSettings = (product: any) => {
    setSelectedProductForSku(product)
    // Initialize editing state with current product data
    const { selectors, variants } = parseSkuData(product)
    const variantsObj: Record<string, any> = {}
    variants.forEach((v: any) => {
      variantsObj[v.skuId] = v
    })
    setEditingSkuData({ selectors: JSON.parse(JSON.stringify(selectors)), variants: variantsObj })
    setEditingVariantId(null)
    setSkuModalOpen(true)
  }
  
  // ==================== Variant Selector Handlers ====================
  
  const handleUpdateSelectorKey = (selectorIdx: number, newKey: string) => {
    if (!editingSkuData) return
    setEditingSkuData(prev => {
      if (!prev) return prev
      const newSelectors = [...prev.selectors]
      newSelectors[selectorIdx] = { ...newSelectors[selectorIdx], key: newKey }
      return { ...prev, selectors: newSelectors }
    })
  }
  
  const handleUpdateSelectorDisplayName = (selectorIdx: number, newDisplayName: string) => {
    if (!editingSkuData) return
    setEditingSkuData(prev => {
      if (!prev) return prev
      const newSelectors = [...prev.selectors]
      newSelectors[selectorIdx] = { ...newSelectors[selectorIdx], displayName: newDisplayName }
      return { ...prev, selectors: newSelectors }
    })
  }
  
  const handleUpdateSelectorValue = (selectorIdx: number, valueIdx: number, newDisplayValue: string) => {
    if (!editingSkuData) return
    setEditingSkuData(prev => {
      if (!prev) return prev
      const newSelectors = [...prev.selectors]
      const newValues = [...(newSelectors[selectorIdx].values || [])]
      newValues[valueIdx] = { ...newValues[valueIdx], displayValue: newDisplayValue }
      newSelectors[selectorIdx] = { ...newSelectors[selectorIdx], values: newValues }
      return { ...prev, selectors: newSelectors }
    })
  }
  
  const handleAddSelectorValue = (selectorIdx: number) => {
    if (!editingSkuData) return
    setEditingSkuData(prev => {
      if (!prev) return prev
      const newSelectors = [...prev.selectors]
      const newValues = [...(newSelectors[selectorIdx].values || []), { displayValue: "" }]
      newSelectors[selectorIdx] = { ...newSelectors[selectorIdx], values: newValues }
      return { ...prev, selectors: newSelectors }
    })
  }
  
  const handleRemoveSelectorValue = (selectorIdx: number, valueIdx: number) => {
    if (!editingSkuData) return
    setEditingSkuData(prev => {
      if (!prev) return prev
      const newSelectors = [...prev.selectors]
      const newValues = newSelectors[selectorIdx].values?.filter((_: any, i: number) => i !== valueIdx) || []
      newSelectors[selectorIdx] = { ...newSelectors[selectorIdx], values: newValues }
      return { ...prev, selectors: newSelectors }
    })
  }
  
  const handleAddSelector = () => {
    if (!editingSkuData) return
    setEditingSkuData(prev => {
      if (!prev) return prev
      const newSelector = { key: `selector_${prev.selectors.length + 1}`, displayName: "", values: [] }
      return { ...prev, selectors: [...prev.selectors, newSelector] }
    })
  }
  
  const handleRemoveSelector = (selectorIdx: number) => {
    if (!editingSkuData) return
    setEditingSkuData(prev => {
      if (!prev) return prev
      const newSelectors = prev.selectors.filter((_: any, i: number) => i !== selectorIdx)
      return { ...prev, selectors: newSelectors }
    })
  }
  
  // ==================== Variant Handlers ====================
  
  const handleUpdateVariantField = (skuId: string, field: string, value: any) => {
    if (!editingSkuData) return
    setEditingSkuData(prev => {
      if (!prev) return prev
      return {
        ...prev,
        variants: {
          ...prev.variants,
          [skuId]: {
            ...prev.variants[skuId],
            [field]: value
          }
        }
      }
    })
  }
  
  const handleUpdateVariantSelectorValue = (skuId: string, selectorKey: string, value: string) => {
    if (!editingSkuData) return
    setEditingSkuData(prev => {
      if (!prev) return prev
      return {
        ...prev,
        variants: {
          ...prev.variants,
          [skuId]: {
            ...prev.variants[skuId],
            selectorValues: {
              ...prev.variants[skuId]?.selectorValues,
              [selectorKey]: value
            }
          }
        }
      }
    })
  }
  
  const handleUpdateVariantShipping = (skuId: string, field: string, value: any) => {
    if (!editingSkuData) return
    setEditingSkuData(prev => {
      if (!prev) return prev
      return {
        ...prev,
        variants: {
          ...prev.variants,
          [skuId]: {
            ...prev.variants[skuId],
            shipping: {
              ...prev.variants[skuId]?.shipping,
              [field]: value
            }
          }
        }
      }
    })
  }
  
  const handleUpdateVariantFeatures = (skuId: string, field: string, value: any) => {
    if (!editingSkuData) return
    setEditingSkuData(prev => {
      if (!prev) return prev
      return {
        ...prev,
        variants: {
          ...prev.variants,
          [skuId]: {
            ...prev.variants[skuId],
            features: {
              ...prev.variants[skuId]?.features,
              [field]: value
            }
          }
        }
      }
    })
  }
  
  const handleAddVariant = () => {
    if (!editingSkuData) return
    const newSkuId = `sku_${Date.now()}`
    const selectorValues: Record<string, string> = {}
    editingSkuData.selectors.forEach((selector: any) => {
      selectorValues[selector.key] = ""
    })
    setEditingSkuData(prev => {
      if (!prev) return prev
      return {
        ...prev,
        variants: {
          ...prev.variants,
          [newSkuId]: {
            skuId: newSkuId,
            selectorValues,
            standardPrice: "0",
            articleNumber: "",
            shipping: { postageIncluded: true, postageSegment: 1 },
            features: { restockNotification: false, displayNormalCartButton: true }
          }
        }
      }
    })
  }
  
  const handleRemoveVariant = (skuId: string) => {
    if (!editingSkuData) return
    setEditingSkuData(prev => {
      if (!prev) return prev
      const newVariants = { ...prev.variants }
      delete newVariants[skuId]
      return { ...prev, variants: newVariants }
    })
  }
  
  const handleSaveSkuData = async () => {
    if (!selectedProductForSku || !editingSkuData) {
      console.warn("Cannot save SKU: missing product or data", { 
        hasProduct: !!selectedProductForSku, 
        hasData: !!editingSkuData 
      })
      return
    }
    
    setIsSavingSku(true)
    let saveCompleted = false
    try {
      // Clean and validate variant_selectors
      const selectorsToSave = editingSkuData.selectors
        .filter((selector: any) => selector && selector.key && selector.key.trim() !== "")
        .map((selector: any, idx: number) => {
          const cleaned = {
            key: selector.key.trim(),
            displayName: selector.displayName?.trim() || selector.key.trim(),
            values: (selector.values || [])
              .filter((v: any) => v && v.displayValue && v.displayValue.trim() !== "")
              .map((v: any) => ({
                displayValue: v.displayValue.trim()
              }))
          }
          cleaned.values.forEach((v: any, vIdx: number) => {
            // Value processed
          })
          return cleaned
        })
        .filter((selector: any) => selector.values.length > 0) // Only keep selectors with at least one value
      
      // Convert variants back to the expected format, cleaning up the data
      const variantsToSave: Record<string, any> = {}
      
      Object.entries(editingSkuData.variants).forEach(([skuId, variant]: [string, any], index: number) => {
        if (!skuId || !variant) {
          console.warn(`⚠️ [SKU Save] Skipping invalid variant at index ${index}:`, { skuId, hasVariant: !!variant })
          return
        }
        
        // Clean selectorValues - include all selector keys, even if empty
        // Empty values are allowed and will be saved as empty strings to preserve structure
        const cleanedSelectorValues: Record<string, string> = {}
        selectorsToSave.forEach((selector: any) => {
          const value = variant.selectorValues?.[selector.key]
          // Include all selector values, even if empty (to preserve structure)
          if (value !== undefined && value !== null) {
            cleanedSelectorValues[selector.key] = String(value).trim()
          } else {
            // Set empty string for missing selector values to preserve structure
            cleanedSelectorValues[selector.key] = ""
          }
        })
        
        // Build the variant object, including all fields (even if empty)
        const variantToSave: any = {}
        
        // Always include selectorValues if there are selectors defined
        if (selectorsToSave.length > 0) {
          variantToSave.selectorValues = cleanedSelectorValues
        }
        
        if (variant.standardPrice !== undefined && variant.standardPrice !== null && String(variant.standardPrice).trim() !== "") {
          variantToSave.standardPrice = String(variant.standardPrice).trim()
        }
        
        if (variant.images && Array.isArray(variant.images) && variant.images.length > 0) {
          variantToSave.images = variant.images
        }
        
        // Attributes array - always include if it exists, even if empty
        if (variant.attributes !== undefined) {
          if (Array.isArray(variant.attributes) && variant.attributes.length > 0) {
            // Filter out completely empty attributes (no name), but keep those with names even if values are empty
            const validAttributes = variant.attributes
              .filter((attr: any) => attr && attr.name && String(attr.name).trim() !== "")
              .map((attr: any) => {
                // Ensure values is always an array
                let values = attr.values
                if (!Array.isArray(values)) {
                  values = values !== undefined && values !== null ? [values] : []
                }
                // Filter out empty values but keep the structure
                const validValues = values
                  .filter((v: any) => v !== undefined && v !== null && String(v).trim() !== "")
                  .map((v: any) => String(v).trim())
                
                return {
                  name: String(attr.name).trim(),
                  values: validValues.length > 0 ? validValues : ["-"] // Use "-" as default if no values
                }
              })
            
            if (validAttributes.length > 0) {
              variantToSave.attributes = validAttributes
            }
          } else if (Array.isArray(variant.attributes)) {
            // Empty array - include it to preserve structure
            variantToSave.attributes = []
          }
        }
        
        // Shipping object
        if (variant.shipping && typeof variant.shipping === 'object') {
          variantToSave.shipping = {}
          if (variant.shipping.postageIncluded !== undefined) {
            variantToSave.shipping.postageIncluded = Boolean(variant.shipping.postageIncluded)
          }
          if (variant.shipping.postageSegment !== undefined && variant.shipping.postageSegment !== null) {
            variantToSave.shipping.postageSegment = Number(variant.shipping.postageSegment)
          }
        }
        
        // Features object
        if (variant.features && typeof variant.features === 'object') {
          variantToSave.features = {}
          if (variant.features.restockNotification !== undefined) {
            variantToSave.features.restockNotification = Boolean(variant.features.restockNotification)
          }
          if (variant.features.displayNormalCartButton !== undefined) {
            variantToSave.features.displayNormalCartButton = Boolean(variant.features.displayNormalCartButton)
          }
          if (variant.features.noshi !== undefined) {
            variantToSave.features.noshi = Boolean(variant.features.noshi)
          }
        }
        
        // Article Number object - handle both object and string formats
        if (variant.articleNumber !== undefined && variant.articleNumber !== null) {
          if (typeof variant.articleNumber === 'object' && variant.articleNumber.exemptionReason !== undefined) {
            variantToSave.articleNumber = { exemptionReason: Number(variant.articleNumber.exemptionReason) }
          } else if (typeof variant.articleNumber === 'string' && variant.articleNumber.trim() !== "") {
            variantToSave.articleNumber = String(variant.articleNumber).trim()
          }
        }
        
        // Always add variant if it has any data (selectorValues, standardPrice, or other fields)
        // This ensures that variants with only attributes, shipping, or features are also saved
        const hasData = (
          Object.keys(cleanedSelectorValues).length > 0 ||
          variantToSave.standardPrice ||
          variantToSave.articleNumber ||
          variantToSave.attributes ||
          variantToSave.shipping ||
          variantToSave.features
        )
        
        if (hasData) {
          variantsToSave[skuId] = variantToSave
        } else {
          console.warn(`   ⚠️  Variant ${skuId} skipped (no data to save)`)
        }
      })
      
      
      // Validate that we have data to save
      if (selectorsToSave.length === 0 && Object.keys(variantsToSave).length === 0) {
        toast({
          variant: "destructive",
          title: "エラー",
          description: "保存するSKU情報がありません。選択肢とSKUのいずれかが必要です。",
        })
        setIsSavingSku(false)
        return
      }
      
      // Build request payload, only including fields that have data
      const requestPayload: any = {}
      if (selectorsToSave.length > 0) {
        requestPayload.variant_selectors = selectorsToSave
      }
      if (Object.keys(variantsToSave).length > 0) {
        requestPayload.variants = variantsToSave
      }
      
      
      const result = await apiService.updateProductSku(
        selectedProductForSku.item_number,
        requestPayload
      )
      
      if (result.error) {
        console.error(`   Error: ${result.error}`)
      }
      
      if (result.success) {
        toast({
          title: "SKU情報を更新しました",
          description: result.message,
        })
        // Refresh products list
        setIsLoading(true)
        try {
          const resp = await apiService.getProductManagement(PAGE_SIZE, 0, sortBy, sortOrder)
          if (resp.success && Array.isArray(resp.data)) {
            setItems(resp.data)
            setCurrentOffset(resp.data.length)
            setHasMoreProducts(resp.data.length === PAGE_SIZE)
          }
        } finally {
          setIsLoading(false)
        }
        setSkuModalOpen(false)
        saveCompleted = true
      } else {
        toast({
          variant: "destructive",
          title: "SKU情報の更新に失敗しました",
          description: translateErrorMessage(result.error || result.message) || "不明なエラーが発生しました",
        })
        saveCompleted = true
      }
    } catch (error: any) {
      console.error("Failed to save SKU data:", error)
      const errorMessage = translateErrorMessage(error?.message || error?.toString()) || "SKU情報の保存中にエラーが発生しました"
      toast({
        variant: "destructive",
        title: "エラー",
        description: errorMessage,
      })
      saveCompleted = true
    } finally {
      // Always reset saving state
      setIsSavingSku(false)
      if (!saveCompleted) {
        console.warn("Save operation did not complete properly - state reset")
      }
    }
  }
  
  const handleRefreshSkuFromOrigin = async () => {
    if (!selectedProductForSku) return
    
    setIsSavingSku(true)
    try {
      const result = await apiService.updateVariantsOnly([selectedProductForSku.item_number])
      
      if (result.success) {
        toast({
          title: "SKU情報を元データから更新しました",
          description: result.message,
        })
        // Refresh products list and re-open modal with new data
        setIsLoading(true)
        try {
          const resp = await apiService.getProductManagement(PAGE_SIZE, 0, sortBy, sortOrder)
          if (resp.success && Array.isArray(resp.data)) {
            setItems(resp.data)
            setCurrentOffset(resp.data.length)
            setHasMoreProducts(resp.data.length === PAGE_SIZE)
          }
        } finally {
          setIsLoading(false)
        }
        // Re-fetch the updated product
        const updatedProducts = await apiService.getProductManagement(1000, 0)
        const updatedProduct = updatedProducts.data?.find((p: any) => p.item_number === selectedProductForSku.item_number)
        if (updatedProduct) {
          handleSkuSettings(updatedProduct)
        }
      } else {
        toast({
          variant: "destructive",
          title: "SKU情報の更新に失敗しました",
          description: translateErrorMessage(result.error || result.message),
        })
      }
    } catch (error) {
      console.error("Failed to refresh SKU data:", error)
      toast({
        variant: "destructive",
        title: "エラー",
        description: "SKU情報の更新中にエラーが発生しました",
      })
    } finally {
      setIsSavingSku(false)
    }
  }
  
  // Get registration status badge info based on rakuten_registration_status field
  const getRegistrationStatus = useCallback((product: Product): { label: string; variant: "default" | "destructive" | "secondary"; className: string } => {
    // Get status - handle null, undefined, empty string, or actual value
    const status = product?.rakuten_registration_status
    
    // Status processing (debug logging removed for production)
    
    // Check for "onsale" status (hideItem is false)
    if (status === "onsale") {
      return {
        label: "販売中",
        variant: "default" as const,
        className: "bg-green-500 text-white hover:bg-green-600 border-green-600"
      }
    }
    
    // Check for "stop" status (hideItem is true)
    if (status === "stop") {
      return {
        label: "販売停止",
        variant: "destructive" as const,
        className: "bg-red-500 text-white hover:bg-red-600 border-red-600"
      }
    }
    
    // Check for explicit success status
    if (
      (typeof status === 'boolean' && status === true) ||
      (typeof status === 'number' && status === 1) ||
      (typeof status === 'string' && (status === "true" || status === "1" || status.toLowerCase() === 'true'))
    ) {
      // Successfully registered - green badge
      return {
        label: "登録成功",
        variant: "default" as const,
        className: "bg-green-500 text-white hover:bg-green-600 border-green-600"
      }
    }
    
    // Check for deleted status
    if (status === "deleted") {
      // Deleted from Rakuten - orange badge
      return {
        label: "楽天削除",
        variant: "destructive" as const,
        className: "bg-orange-500 text-white hover:bg-orange-600 border-orange-600"
      }
    }
    
    // Check for explicit failure status
    if (
      (typeof status === 'boolean' && status === false) ||
      (typeof status === 'number' && status === 0) ||
      (typeof status === 'string' && (status === "false" || status === "0" || status.toLowerCase() === 'false'))
    ) {
      // Registration failed - red badge
      return {
        label: "登録失敗",
        variant: "destructive" as const,
        className: "bg-red-500 text-white hover:bg-red-600 border-red-600"
      }
    }
    
    // Default: Not registered yet (null/undefined/empty/"unregistered") - gray badge
    return {
      label: "未登録",
      variant: "secondary" as const,
      className: "bg-gray-500 text-white hover:bg-gray-600 border-gray-600"
    }
  }, [])
  
  // Parse variant_selectors and variants for display
  const parseSkuData = useCallback((product: Product | null) => {
    if (!product) return { selectors: [], variants: [] }
    
    const variantSelectors = product.variant_selectors || []
    const variants = product.variants || {}
    
    // Convert variants object to array for easier display
    const variantsArray = Object.entries(variants).map(([skuId, variantData]: [string, ProductVariant]) => ({
      skuId,
      ...variantData,
    }))
    
    return {
      selectors: variantSelectors,
      variants: variantsArray,
    }
  }, [])
  

  const deselectAllProducts = useCallback(() => {
    setSelectedProducts([])
  }, [])

  const dataSource = items.length ? items : products

  const activeProcessingIds = useMemo(() => {
    return new Set(
      Object.entries(registering)
        .filter(([, value]) => value)
        .map(([key]) => key.toString()),
    )
  }, [registering])

  const filteredProducts = useMemo(() => {
    return dataSource.filter((product: Product) => {
    // Category filter: Check if product's main_category matches selected category's default_category_ids
    if (selectedCategory !== "all") {
      // Find the selected primary category
      const selectedPrimaryCategory = primaryCategories.find(cat => cat.id.toString() === selectedCategory)
      
      if (!selectedPrimaryCategory) {
        // Category not found, don't show any products
        return false
      }
      
      // Ensure default_category_ids exists and is an array
      const defaultCategoryIds = selectedPrimaryCategory.default_category_ids
      if (!defaultCategoryIds || !Array.isArray(defaultCategoryIds) || defaultCategoryIds.length === 0) {
        // If no default_category_ids, don't show any products
        return false
      }
      
      // Get product's main_category and convert to string for comparison
      // Handle both string and number types
      const productMainCategory = product.main_category 
        ? String(product.main_category).trim() 
        : null
      
      if (!productMainCategory || productMainCategory === '') {
        // Product has no main_category, exclude it
        return false
      }
      
      // default_category_ids should already be strings from the mapping above
      // But ensure they are strings and filter out empty values
      const categoryIds = defaultCategoryIds
          .map((id: string | number) => {
          if (id === null || id === undefined) return null
          const strId = String(id).trim()
          return strId !== '' ? strId : null
        })
          .filter((id: string | null): id is string => id !== null)
      
      // Check if product's main_category is in the default_category_ids
      // IMPORTANT: Do NOT filter by rakuten_registration_status - show all products regardless of status
      const isMatch = categoryIds.includes(productMainCategory)
      if (!isMatch) {
        return false
      }
    }
    // IMPORTANT: Do NOT filter by rakuten_registration_status - show all products (unregistered, failed, registered)
    // If search query exists, backend already filtered the results, so skip client-side search filtering
    if (debouncedSearchQuery && debouncedSearchQuery.trim()) {
      // Backend already filtered by search, so just return true for category-filtered products
      return true
    }
    // No search query, return all products (category filter already applied above)
    return true
    })
  }, [dataSource, selectedCategory, primaryCategories, debouncedSearchQuery])

  const selectAllProducts = useCallback(() => {
    const allIds = filteredProducts
      .map((p: Product) => p.item_number || p.id)
      .filter(Boolean)
      .map((id: string | undefined) => id?.toString() ?? '')
    setSelectedProducts(allIds)
  }, [filteredProducts])

  const isAllSelected = filteredProducts.length > 0 && selectedProducts.length === filteredProducts.length
  
  // Calculate if all filtered products are visible (not hidden)
  const allProductsVisible = filteredProducts.length > 0 && filteredProducts.every((p: any) => !p.hide_item)
  const allProductsHidden = filteredProducts.length > 0 && filteredProducts.every((p: any) => p.hide_item === true)
  
  const handleToggleAllHideItem = async (pressed: boolean) => {
    setTogglingAll(true)
    try {
      // Toggle logic:
      // When toggle is pressed (pressed=true), all products should be visible (hide_item=false)
      // When toggle is not pressed (pressed=false), all products should be hidden (hide_item=true)
      const newHideItem = !pressed
      
      // Call API to update ALL products in database
      const response = await apiService.updateAllProductsHideItem(newHideItem)
      
      if (response.success) {
        // Update local state for ALL products
        setItems(prev => prev.map(item => ({
          ...item,
          hide_item: newHideItem
        })))
        
        toast({
          title: "成功",
          description: `すべての商品（${response.updated_count || items.length}件）を${newHideItem ? '非表示' : '表示'}に設定しました`,
        })
        
        // Reload products to ensure we have the latest data
        try {
          const resp = await apiService.getProductManagement(200, 0)
          if (resp.success && Array.isArray(resp.data)) {
            setItems(resp.data)
          }
        } catch (reloadError) {
          console.warn("Failed to reload products after update:", reloadError)
        }
      } else {
        throw new Error(response.error || '一括更新に失敗しました')
      }
    } catch (error) {
      toast({
        title: "エラー",
        description: error instanceof Error ? error.message : "一括更新に失敗しました",
        variant: "destructive",
      })
    } finally {
      setTogglingAll(false)
    }
  }

  const registrationStats = useMemo(() => {
    const stats = {
      total: dataSource.length,
      registered: 0,
      failed: 0,
      processing: 0,
      unregistered: 0,
    }
    const processingKeywords = new Set(["processing", "in_process", "in-progress", "pending", "registering"])

    dataSource.forEach((product: any) => {
      const itemId = (product.item_number || product.id)?.toString()
      const rawStatus = product?.rakuten_registration_status
      const normalized = typeof rawStatus === "string" ? rawStatus.trim().toLowerCase() : rawStatus

      if (normalized === "true" || rawStatus === true) {
        stats.registered++
      } else if (normalized === "false" || rawStatus === false) {
        stats.failed++
      } else if (
        (typeof normalized === "string" && processingKeywords.has(normalized)) ||
        (itemId && activeProcessingIds.has(itemId))
      ) {
        stats.processing++
      } else {
        stats.unregistered++
      }
    })

    return stats
  }, [dataSource, activeProcessingIds])

  // Virtualization: calculate visible window within currently loaded (client-side) products
  const totalRows = filteredProducts.length
  const [virtualEndIndex, visibleStartIndex, visibleEndIndex] = useMemo(() => {
    if (totalRows === 0) return [0, 0, 0] as const
    const start = Math.max(0, Math.min(virtualStartIndex, totalRows - 1))
    const end = Math.min(totalRows, start + VIRTUAL_WINDOW)
    const withOverscanStart = Math.max(0, start - OVERSCAN_ROWS)
    const withOverscanEnd = Math.min(totalRows, end + OVERSCAN_ROWS)
    return [end, withOverscanStart, withOverscanEnd] as const
  }, [virtualStartIndex, totalRows])

  const visibleProducts = useMemo(
    () => filteredProducts.slice(visibleStartIndex, visibleEndIndex),
    [filteredProducts, visibleStartIndex, visibleEndIndex],
  )

  const topSpacerHeight = visibleStartIndex * ROW_HEIGHT
  const bottomSpacerHeight =
    totalRows > 0 ? Math.max(0, (totalRows - visibleEndIndex) * ROW_HEIGHT) : 0

  const handleTableScroll = (e: React.UIEvent<HTMLDivElement>) => {
    const target = e.currentTarget
    const scrollTop = target.scrollTop
    // 余裕を持って1行分ずらすことで、スクロールに応じてスムーズに入れ替え
    const startIndex = Math.floor(scrollTop / ROW_HEIGHT)
    if (startIndex !== virtualStartIndex) {
      setVirtualStartIndex(startIndex)
    }
  }

  // Keyboard navigation for modals
  const settingsModalRef = useKeyboardNavigation({
    onEscape: () => {
      if (!isSavingSettings) {
        setSettingsModalOpen(false)
        setTouchedFields(new Set())
        setSettingsFormErrors({})
      }
    },
    enabled: settingsModalOpen,
    trapFocus: true,
  })

  const skuModalRef = useKeyboardNavigation({
    onEscape: () => {
      if (!isSavingSku) {
        setSkuModalOpen(false)
        setEditingVariantId(null)
      }
    },
    enabled: skuModalOpen,
    trapFocus: true,
  })

  const imageModalRef = useKeyboardNavigation({
    onEscape: () => {
      if (!uploadingToRakuten) {
        setImageModalOpen(false)
      }
    },
    enabled: imageModalOpen,
    trapFocus: true,
  })

  return (
    <ErrorBoundary>
      <SkipLink targetId="main-content" label="メインコンテンツへスキップ" />
      <div id="main-content" className="space-y-3 sm:space-y-4 md:space-y-6" tabIndex={-1}>
      {/* Header */}
      <div className="flex flex-col gap-3 sm:gap-4 md:flex-row md:items-center md:justify-between">
        <div>
          <h1 className="text-xl sm:text-2xl md:text-3xl font-bold text-foreground">商品管理</h1>
          <p className="text-xs sm:text-sm md:text-base text-muted-foreground mt-1">楽天市場に登録された商品の管理</p>
        </div>
        <div className="flex flex-wrap items-center gap-2 sm:gap-3">
          <Button 
            variant="outline" 
            size="sm" 
            className="gap-2 bg-white flex-1 md:flex-none"
            onClick={async () => {
              setIsExportingCSV(true)
              try {
                // Get item_numbers from selected products
                const selectedItemNumbers: string[] = []
                if (selectedProducts.length > 0) {
                  // Get item_number for each selected product
                  selectedProducts.forEach(productId => {
                    const product = items.find(p => (p.item_number || p.id) === productId)
                    if (product && product.item_number) {
                      selectedItemNumbers.push(product.item_number)
                    }
                  })
                }
                
                const blob = await apiService.exportProductManagementCSV(
                  selectedItemNumbers.length > 0 ? selectedItemNumbers : undefined
                )
                const url = window.URL.createObjectURL(blob)
                const a = document.createElement('a')
                a.href = url
                a.download = `product_management_${new Date().toISOString().slice(0, 10)}.csv`
                document.body.appendChild(a)
                a.click()
                window.URL.revokeObjectURL(url)
                document.body.removeChild(a)
                toast({
                  title: "成功",
                  description: selectedItemNumbers.length > 0 
                    ? `${selectedItemNumbers.length}件の商品をCSVファイルでダウンロードしました`
                    : "CSVファイルをダウンロードしました",
                })
              } catch (error: any) {
                toast({
                  title: "エラー",
                  description: error?.message || "CSVのダウンロードに失敗しました",
                  variant: "destructive",
                })
              } finally {
                setIsExportingCSV(false)
              }
            }}
            disabled={isExportingCSV}
          >
            {isExportingCSV ? (
              <Loader2 className="h-4 w-4 animate-spin" />
            ) : (
              <Download className="h-4 w-4" />
            )}
            <span className="hidden sm:inline">CSVでダウンロード</span>
          </Button>
          <Button 
            variant="outline" 
            size="sm" 
            className="gap-2 bg-white flex-1 md:flex-none"
            onClick={() => {
              fileInputRef.current?.click()
            }}
            disabled={isImportingCSV}
          >
            {isImportingCSV ? (
              <Loader2 className="h-4 w-4 animate-spin" />
            ) : (
              <Upload className="h-4 w-4" />
            )}
            <span className="hidden sm:inline">CSVからアップロード</span>
          </Button>
          <input
            ref={fileInputRef}
            type="file"
            accept=".csv"
            style={{ display: 'none' }}
            onChange={async (e) => {
              const file = e.target.files?.[0]
              if (!file) return
              
              setIsImportingCSV(true)
              try {
                const result = await apiService.importProductManagementCSV(file)
                if (result.success) {
                  toast({
                    title: "成功",
                    description: result.message || `${result.updated_count || 0}件の商品を更新しました`,
                  })
                  // Refresh products
                  await refreshProducts()
                } else {
                  toast({
                    title: "エラー",
                    description: translateErrorMessage(result.message) || "CSVのアップロードに失敗しました",
                    variant: "destructive",
                  })
                }
                if (result.errors && result.errors.length > 0) {
                  console.error("CSV import errors:", result.errors)
                }
              } catch (error: any) {
                toast({
                  title: "エラー",
                  description: error?.message || "CSVのアップロードに失敗しました",
                  variant: "destructive",
                })
              } finally {
                setIsImportingCSV(false)
                // Reset file input
                if (fileInputRef.current) {
                  fileInputRef.current.value = ''
                }
              }
            }}
          />
          <Button 
            size="sm" 
            className="gap-2 flex-1 md:flex-none"
            onClick={refreshProducts}
            disabled={isLoading}
          >
            {isLoading ? (
              <Loader2 className="h-4 w-4 animate-spin" />
            ) : (
            <RefreshCw className="h-4 w-4" />
            )}
            <span className="hidden sm:inline">更新</span>
          </Button>
          <Button 
            size="sm" 
            variant="outline"
            className="gap-2 flex-1 md:flex-none"
            onClick={handleBulkCheckRegistrationStatus}
            disabled={isBulkCheckingStatus || selectedProducts.length === 0}
          >
            {isBulkCheckingStatus ? (
              <Loader2 className="h-4 w-4 animate-spin" />
            ) : (
              <RefreshCw className="h-4 w-4" />
            )}
            <span className="hidden sm:inline">登録状態を一括更新</span>
          </Button>
          <Button 
            size="sm" 
            variant="default"
            className="gap-2 flex-1 md:flex-none bg-primary hover:bg-primary/90"
            onClick={async () => {
              setIsUpdatingChanges(true)
              try {
                toast({
                  title: "更新開始",
                  description: "変更内容を楽天市場に更新しています...",
                })
                
                const result = await apiService.updateChangesToRakuten()
                
                if (result.success) {
                  toast({
                    title: "成功",
                    description: result.message || `変更内容更新: ${result.success_count || 0}件の商品を楽天に登録しました`,
                  })
                  // Refresh products to show updated change_status
                  await refreshProducts()
                } else {
                  const errorMsg = translateErrorMessage(result.error) || "変更内容更新に失敗しました"
                  toast({
                    title: "エラー",
                    description: errorMsg,
                    variant: "destructive",
                  })
                }
              } catch (error: any) {
                toast({
                  title: "エラー",
                  description: translateErrorMessage(error?.message) || "変更内容更新に失敗しました",
                  variant: "destructive",
                })
              } finally {
                setIsUpdatingChanges(false)
              }
            }}
            disabled={isUpdatingChanges}
          >
            {isUpdatingChanges ? (
              <Loader2 className="h-4 w-4 animate-spin" />
            ) : (
              <Save className="h-4 w-4" />
            )}
            <span className="hidden sm:inline">変更内容更新</span>
          </Button>
        </div>
      </div>

      {/* Status Cards */}
      <div className="grid gap-3 md:gap-4 grid-cols-2 md:grid-cols-4 xl:grid-cols-8">
        {/* 総商品数 */}
        <Card className="p-3 md:p-4 border-border bg-white">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-xs md:text-sm text-muted-foreground">総商品数</p>
              <p className="text-xl md:text-2xl font-bold text-foreground mt-1">{formatNumberJa(productStats.total)}</p>
            </div>
            <Package className="h-6 w-6 md:h-7 md:w-7 text-primary" />
          </div>
        </Card>

        {/* 登録数 */}
        <Card className="p-3 md:p-4 border-border bg-white">
          <div className="flex items-center justify-between">
            <div className="flex-1">
              <p className="text-xs md:text-sm text-muted-foreground">登録数</p>
              <p className="text-xl md:text-2xl font-bold text-foreground mt-1">{formatNumberJa(productStats.registered + productStats.onsale + productStats.stop)}</p>
            </div>
              <CheckCircle2 className="h-6 w-6 md:h-7 md:w-7 text-green-500" />
          </div>
        </Card>

        {/* 未登録数 */}
        <Card className="p-3 md:p-4 border-border bg-white">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-xs md:text-sm text-muted-foreground">未登録数</p>
              <p className="text-xl md:text-2xl font-bold text-foreground mt-1">{formatNumberJa(productStats.unregistered)}</p>
            </div>
            <Square className="h-6 w-6 md:h-7 md:w-7 text-muted-foreground" />
          </div>
        </Card>

        {/* 登録失敗 */}
        <Card className="p-3 md:p-4 border-border bg-white">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-xs md:text-sm text-muted-foreground">登録失敗</p>
              <p className="text-xl md:text-2xl font-bold text-foreground mt-1">{formatNumberJa(productStats.failed)}</p>
            </div>
            <XCircle className="h-6 w-6 md:h-7 md:w-7 text-red-500" />
          </div>
        </Card>

        {/* 楽天削除 */}
        <Card className="p-3 md:p-4 border-border bg-white">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-xs md:text-sm text-muted-foreground">楽天削除</p>
              <p className="text-xl md:text-2xl font-bold text-foreground mt-1">{formatNumberJa(productStats.deleted)}</p>
            </div>
            <Trash2 className="h-6 w-6 md:h-7 md:w-7 text-orange-500" />
          </div>
        </Card>

        {/* 販売停止 */}
        <Card className="p-3 md:p-4 border-border bg-white">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-xs md:text-sm text-muted-foreground">販売停止</p>
              <p className="text-xl md:text-2xl font-bold text-foreground mt-1">{formatNumberJa(productStats.stop)}</p>
            </div>
            <Power className="h-6 w-6 md:h-7 md:w-7 text-red-600" />
          </div>
        </Card>

        {/* 販売中 */}
        <Card className="p-3 md:p-4 border-border bg-white">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-xs md:text-sm text-muted-foreground">販売中</p>
              <p className="text-xl md:text-2xl font-bold text-foreground mt-1">{formatNumberJa(productStats.onsale)}</p>
            </div>
            <TrendingUp className="h-6 w-6 md:h-7 md:w-7 text-green-600" />
          </div>
        </Card>

        {/* 処理中 */}
        <Card className="p-3 md:p-4 border-border bg-white">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-xs md:text-sm text-muted-foreground">処理中</p>
              <p className="text-xl md:text-2xl font-bold text-foreground mt-1">{formatNumberJa(processingCount)}</p>
            </div>
              <Clock className="h-6 w-6 md:h-7 md:w-7 text-blue-500" />
          </div>
        </Card>
      </div>

      {/* Search & Filters */}
      <Card className="p-3 md:p-4 border-border bg-white">
        <div className="flex flex-col gap-2 md:flex-row md:gap-3">
          <div className="relative flex-1">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
            <Input
              placeholder="商品名、SKUで検索..."
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              className="pl-10 text-sm"
              aria-label="商品検索"
              aria-describedby="search-description"
              autoComplete="off"
              role="searchbox"
            />
            <span id="search-description" className="sr-only">商品名またはSKUで検索できます</span>
          </div>
          <div className="flex gap-2 flex-wrap">
            <Select value={selectedCategory} onValueChange={setSelectedCategory}>
              <SelectTrigger 
                className="flex-1 md:w-[240px] bg-white"
                aria-label="メインカテゴリーを選択"
                aria-describedby="category-description"
              >
                <SelectValue placeholder="メインカテゴリーを選択" />
              </SelectTrigger>
              <SelectContent className="bg-white">
                <SelectItem value="all">すべてのメインカテゴリー</SelectItem>
                {primaryCategories.map((category) => (
                  <SelectItem key={category.id} value={category.id.toString()}>
                    {category.category_name}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
            <span id="category-description" className="sr-only">商品をフィルタリングするメインカテゴリーを選択します</span>
            
            {/* Time-based Sort Filter */}
            <Select 
              value={sortBy} 
              onValueChange={(value) => {
                setSortBy(value)
                setCurrentOffset(0)
              }}
            >
              <SelectTrigger 
                className="flex-1 md:w-[200px] bg-white"
                aria-label="ソート基準を選択"
              >
                <SelectValue placeholder="ソート基準" />
              </SelectTrigger>
              <SelectContent className="bg-white">
                <SelectItem value="created_at">登録時間</SelectItem>
                <SelectItem value="rakuten_registered_at">更新時間</SelectItem>
              </SelectContent>
            </Select>
            
            <Select 
              value={sortOrder} 
              onValueChange={(value: "asc" | "desc") => {
                setSortOrder(value)
                setCurrentOffset(0)
              }}
            >
              <SelectTrigger 
                className="flex-1 md:w-[150px] bg-white"
                aria-label="ソート順を選択"
              >
                <SelectValue placeholder="ソート順" />
              </SelectTrigger>
              <SelectContent className="bg-white">
                <SelectItem value="desc">降順（新しい順）</SelectItem>
                <SelectItem value="asc">昇順（古い順）</SelectItem>
              </SelectContent>
            </Select>
          </div>
        </div>
      </Card>

      {/* Select All / Deselect All controls above the table */}
      {filteredProducts.length > 0 && (
        <div className="flex flex-col gap-2 md:flex-row md:items-center md:justify-between p-3 bg-muted/50 rounded-lg border border-border">
          <div className="flex items-center gap-2 md:gap-3">
            <Button
              variant="outline"
              size="sm"
              onClick={isAllSelected ? deselectAllProducts : selectAllProducts}
              className="gap-2 bg-white hover:bg-muted text-xs md:text-sm"
            >
              {isAllSelected ? (
                <>
                  <CheckSquare className="h-4 w-4" />
                  <span className="hidden sm:inline">すべて選択解除</span>
                  <span className="sm:hidden">解除</span>
                </>
              ) : (
                <>
                  <Square className="h-4 w-4" />
                  <span className="hidden sm:inline">すべて選択</span>
                  <span className="sm:hidden">選択</span>
                </>
              )}
            </Button>
            <span className="text-xs md:text-sm text-muted-foreground">
              {selectedProducts.length} / {filteredProducts.length} 件
            </span>
          </div>
          {selectedProducts.length > 0 && (
            <div className="flex items-center gap-2">

              <Button
                variant="outline"
                size="sm"
                className="gap-2 bg-white text-xs md:text-sm flex-1 md:flex-none"
                onClick={handleBulkRegisterToRakuten}
                disabled={isBulkRegistering}
              >
                {isBulkRegistering ? (
                  <>
                    <Loader2 className="h-4 w-4 animate-spin" />
                    <span className="hidden sm:inline">
                      {bulkRegistrationProgress.total > 0 
                        ? `登録中: ${bulkRegistrationProgress.current}/${bulkRegistrationProgress.total}`
                        : "登録中..."}
                    </span>
                    <span className="sm:hidden">
                      {bulkRegistrationProgress.total > 0 
                        ? `${bulkRegistrationProgress.current}/${bulkRegistrationProgress.total}`
                        : "中..."}
                    </span>
                  </>
                ) : (
                  <>
                    <Upload className="h-4 w-4" />
                    <span className="hidden sm:inline">一括登録</span>
                    <span className="sm:hidden">登録</span>
                  </>
                )}
              </Button>
              <Button
                variant="outline"
                size="sm"
                className="gap-2 bg-white text-xs md:text-sm flex-1 md:flex-none"
                onClick={handleBulkRegisterInventoryToRakuten}
                disabled={isBulkRegisteringInventory}
              >
                {isBulkRegisteringInventory ? (
                  <>
                    <Loader2 className="h-4 w-4 animate-spin" />
                    <span className="hidden sm:inline">
                      {bulkInventoryRegistrationProgress.total > 0 
                        ? `在庫登録中: ${bulkInventoryRegistrationProgress.current}/${bulkInventoryRegistrationProgress.total}`
                        : "在庫登録中..."}
                    </span>
                    <span className="sm:hidden">
                      {bulkInventoryRegistrationProgress.total > 0 
                        ? `${bulkInventoryRegistrationProgress.current}/${bulkInventoryRegistrationProgress.total}`
                        : "中..."}
                    </span>
                  </>
                ) : (
                  <>
                    <Package className="h-4 w-4" />
                    <span className="hidden sm:inline">一括在庫登録</span>
                    <span className="sm:hidden">在庫</span>
                  </>
                )}
              </Button>
              <Button
                variant="outline"
                size="sm"
                className="gap-2 bg-white text-xs md:text-sm flex-1 md:flex-none"
                onClick={handleBulkUploadImagesToRakuten}
                disabled={isBulkUploadingImages}
              >
                {isBulkUploadingImages ? (
                  <>
                    <Loader2 className="h-4 w-4 animate-spin" />
                    <span className="hidden sm:inline">
                      {bulkImageUploadProgress.total > 0 
                        ? `画像アップロード中: ${bulkImageUploadProgress.current}/${bulkImageUploadProgress.total} (${bulkImageUploadProgress.uploadedImages}/${bulkImageUploadProgress.totalImages}画像)`
                        : "画像アップロード中..."}
                    </span>
                    <span className="sm:hidden">
                      {bulkImageUploadProgress.total > 0 
                        ? `${bulkImageUploadProgress.current}/${bulkImageUploadProgress.total} (${bulkImageUploadProgress.uploadedImages}/${bulkImageUploadProgress.totalImages})`
                        : "中..."}
                    </span>
                  </>
                ) : (
                  <>
                    <ImageIcon className="h-4 w-4" />
                    <span className="hidden sm:inline">一括画像登録</span>
                    <span className="sm:hidden">画像</span>
                  </>
                )}
              </Button>
              <Button
                variant="outline"
                size="sm"
                className="gap-2 text-destructive hover:text-destructive bg-white text-xs md:text-sm flex-1 md:flex-none"
                onClick={handleBulkDeleteClick}
                disabled={isBulkDeleting}
              >
                {isBulkDeleting ? (
                  <Loader2 className="h-4 w-4 animate-spin" />
                ) : (
                  <Trash2 className="h-4 w-4" />
                )}
                <span className="hidden sm:inline">{isBulkDeleting ? "削除中..." : "一括削除"}</span>
                <span className="sm:hidden">{isBulkDeleting ? "中..." : "削除"}</span>
              </Button>
              <Button
                variant="outline"
                size="sm"
                className="gap-2 text-destructive hover:text-destructive bg-white text-xs md:text-sm flex-1 md:flex-none border-red-600"
                onClick={handleBulkDeleteFromRakutenClick}
                disabled={isBulkDeletingFromRakuten}
              >
                {isBulkDeletingFromRakuten ? (
                  <Loader2 className="h-4 w-4 animate-spin" />
                ) : (
                  <Trash2 className="h-4 w-4" />
                )}
                <span className="hidden sm:inline">{isBulkDeletingFromRakuten ? "楽天削除中..." : "楽天一括削除"}</span>
                <span className="sm:hidden">{isBulkDeletingFromRakuten ? "中..." : "楽天削除"}</span>
              </Button>
            </div>
          )}
        </div>
      )}

      {/* Bulk Image Upload Progress Display */}
      {isBulkUploadingImages && bulkImageUploadProgress.completed.length > 0 && (
        <Card className="p-3 md:p-4 border-border bg-white">
          <div className="space-y-2">
            <div className="flex items-center justify-between">
              <h3 className="text-sm font-semibold">画像アップロード進捗</h3>
              <span className="text-xs text-muted-foreground">
                {bulkImageUploadProgress.current} / {bulkImageUploadProgress.total} 商品 ({bulkImageUploadProgress.uploadedImages} / {bulkImageUploadProgress.totalImages} 画像)
              </span>
            </div>
            <div className="space-y-1 max-h-40 overflow-y-auto">
              {bulkImageUploadProgress.completed.map((item, index) => (
                <div
                  key={`${item.item_number}-${index}`}
                  className={`flex items-center gap-2 p-2 rounded text-xs ${
                    item.status === 'success' 
                      ? 'bg-green-50 border border-green-200' 
                      : 'bg-red-50 border border-red-200'
                  }`}
                >
                  {item.status === 'success' ? (
                    <CheckCircle2 className="h-4 w-4 text-green-600 flex-shrink-0" />
                  ) : (
                    <XCircle className="h-4 w-4 text-red-600 flex-shrink-0" />
                  )}
                  <span className="font-medium">{item.item_number}</span>
                  <span className="text-muted-foreground flex-1 truncate">: {item.message}</span>
                </div>
              ))}
            </div>
          </div>
        </Card>
      )}

      {/* Products List - Compact Layout */}
      {isLoading ? (
        isMobile ? <ProductCardSkeleton /> : <ProductTableSkeleton />
      ) : filteredProducts.length === 0 ? (
        <Card className="p-12 border-border bg-white">
          <div className="flex flex-col items-center gap-4 text-center">
            <Package className="h-12 w-12 text-muted-foreground" />
            <div>
              <h3 className="text-lg font-semibold text-foreground">商品が見つかりません</h3>
              <p className="text-muted-foreground mt-2">
                {items.length === 0 
                  ? "商品管理テーブルに商品が登録されていません"
                  : "検索条件に一致する商品がありません"
                }
              </p>
            </div>
          </div>
        </Card>
      ) : isMobile ? (
        /* Mobile Card View - Optimized for touch devices */
        <div className="space-y-3">
          {filteredProducts.map((product: Product) => {
            const productId = (product.item_number || product.id)?.toString()
            if (!productId) return null
            
            const productName = product.title || product.name || '商品名なし'
            const firstImageLocation = Array.isArray(product.images) && product.images[0]?.location 
              ? product.images[0].location 
              : null
            const productImage = firstImageLocation 
              ? getImageUrl(firstImageLocation)
              : "/placeholder.svg"
            const isHidden = product.hide_item === true
            const registrationStatus = getRegistrationStatus(product)
            const isRegistering = registering[productId] || false
            const isRegisteringInventory = registeringInventory[productId] || false
            const isToggling = toggling[productId] || false
            const isSelected = selectedProducts.includes(productId)
            
            return (
              <Card key={productId} className="p-4 border-border bg-white shadow-sm">
                <div className="flex gap-3">
                  {/* Checkbox and Image */}
                  <div className="flex flex-col items-center gap-2 shrink-0">
                    <input
                      type="checkbox"
                      checked={isSelected}
                      onChange={() => toggleProduct(productId)}
                      className="rounded border-2 border-primary accent-primary cursor-pointer h-5 w-5"
                      aria-label={`Select ${productName}`}
                    />
                    <div className="h-16 w-16 rounded bg-muted overflow-hidden flex-shrink-0 border border-border">
                      <img 
                        src={productImage} 
                        alt={productName}
                        className="h-full w-full object-cover"
                        loading="lazy"
                        onError={(e) => {
                          const target = e.currentTarget as HTMLImageElement
                          if (target.src !== "/placeholder.svg") {
                            target.src = "/placeholder.svg"
                          }
                        }}
                      />
                    </div>
                  </div>
                  
                  {/* Product Info */}
                  <div className="flex-1 min-w-0">
                    <div className="flex items-start justify-between gap-2 mb-2">
                      <div className="flex-1 min-w-0">
                        <p className="text-xs font-mono text-muted-foreground mb-1 truncate">
                          {product.item_number || 'N/A'}
                        </p>
                        <h3 
                          className="text-sm font-semibold text-foreground line-clamp-2 mb-2 cursor-help" 
                          title={productName}
                        >
                          {productName}
                        </h3>
                      </div>
                      <Badge className={cn("text-xs shrink-0", registrationStatus.className)}>
                        {registrationStatus.label}
                      </Badge>
                    </div>
                    
                    {/* Action Buttons - Mobile Optimized Grid */}
                    <div className="grid grid-cols-2 gap-2 mt-3">
                      <Button
                        size="sm"
                        variant="outline"
                        className="h-10 text-xs"
                        onClick={() => handleRegisterToRakuten(product)}
                        disabled={isRegistering}
                        aria-label={`${productName}を楽天市場に登録`}
                        aria-busy={isRegistering}
                      >
                        {isRegistering ? (
                          <Loader2 className="h-3 w-3 animate-spin mr-1" aria-hidden="true" />
                        ) : (
                          <Upload className="h-3 w-3 mr-1" aria-hidden="true" />
                        )}
                        登録
                      </Button>
                      <Button
                        size="sm"
                        variant="outline"
                        className="h-10 text-xs"
                        onClick={() => handleDetailSettings(product)}
                        aria-label={`${productName}の詳細設定を開く`}
                      >
                        <Settings className="h-3 w-3 mr-1" aria-hidden="true" />
                        設定
                      </Button>
                      <Button
                        size="sm"
                        variant="outline"
                        className="h-10 text-xs"
                        onClick={() => handleRegisterInventoryToRakuten(product)}
                        disabled={isRegisteringInventory}
                        aria-label={`${productName}の在庫を楽天市場に登録`}
                        aria-busy={isRegisteringInventory}
                      >
                        {isRegisteringInventory ? (
                          <Loader2 className="h-3 w-3 animate-spin mr-1" aria-hidden="true" />
                        ) : (
                          <Package className="h-3 w-3 mr-1" aria-hidden="true" />
                        )}
                        在庫
                      </Button>
                      <DropdownMenu>
                        <DropdownMenuTrigger asChild>
                          <Button size="sm" variant="outline" className="h-10 text-xs">
                            <MoreVertical className="h-3 w-3 mr-1" />
                            その他
                          </Button>
                        </DropdownMenuTrigger>
                        <DropdownMenuContent align="end" className="w-48">
                          <DropdownMenuItem onClick={() => handleToggleHideItem(product)} disabled={isToggling}>
                            {isToggling ? (
                              <Loader2 className="h-4 w-4 animate-spin mr-2" />
                            ) : isHidden ? (
                              <Eye className="h-4 w-4 mr-2" />
                            ) : (
                              <EyeOff className="h-4 w-4 mr-2" />
                            )}
                            {isHidden ? "表示" : "非表示"}
                          </DropdownMenuItem>
                          <DropdownMenuItem onClick={() => handleCheckRegistrationStatus(product)}>
                            <RefreshCw className="h-4 w-4 mr-2" />
                            状態確認
                          </DropdownMenuItem>
                          <DropdownMenuItem onClick={() => window.open(`https://item.rakuten.co.jp/rakumart/${product.item_number}`, '_blank')}>
                            <ExternalLink className="h-4 w-4 mr-2" />
                            楽天で表示
                          </DropdownMenuItem>
                          <DropdownMenuItem onClick={() => handleDeleteClick(product)} className="text-destructive">
                            <Trash2 className="h-4 w-4 mr-2" />
                            削除
                          </DropdownMenuItem>
                        </DropdownMenuContent>
                      </DropdownMenu>
                    </div>
                  </div>
                </div>
              </Card>
            )
          })}
        </div>
      ) : (
      <Card className="border-border bg-white overflow-hidden">
      <div
        className="overflow-x-auto max-h-[640px] overflow-y-auto"
        ref={tableScrollRef}
        onScroll={handleTableScroll}
      >
            <table className="w-full">
            <thead className="border-b border-border bg-muted/50">
                <tr style={{ height: '40px' }}>
                  <th className="text-center px-2 text-xs font-medium text-muted-foreground w-10">
                  <input
                    type="checkbox"
                    checked={isAllSelected}
                    onChange={isAllSelected ? deselectAllProducts : selectAllProducts}
                      className="rounded border-2 border-primary accent-primary cursor-pointer h-4 w-4"
                  />
                </th>
                  <th className="text-center px-2 text-xs font-medium text-muted-foreground w-20">画像</th>
                  <th className="text-left px-3 text-xs font-medium text-muted-foreground min-w-[120px]">管理番号</th>
                  <th className="text-left px-3 text-xs font-medium text-muted-foreground w-[400px] max-w-[400px]">タイトル</th>
                  <th className="text-center px-2 text-xs font-medium text-muted-foreground w-28">
                    <div className="flex items-center justify-center gap-2">
                  
                      {togglingAll ? (
                        <Loader2 className="h-3.5 w-3.5 animate-spin text-muted-foreground" />
                      ) : (
                        <Toggle
                          pressed={allProductsVisible}
                          onPressedChange={(pressed) => handleToggleAllHideItem(pressed)}
                          disabled={togglingAll || items.length === 0}
                          size="sm"
                          variant="outline"
                          className={cn(
                            "h-6 w-6 p-0 min-w-6 rounded-md transition-all",
                            allProductsVisible 
                              ? "bg-green-500 text-white hover:bg-green-600 data-[state=on]:bg-green-500 data-[state=on]:text-white border-green-600" 
                              : "bg-gray-300 text-gray-700 hover:bg-gray-400 border-gray-400"
                          )}
                          aria-label={allProductsVisible ? "すべて表示" : "すべて非表示"}
                        >
                          {allProductsVisible ? (
                            <Eye className="h-4 w-4" />
                          ) : (
                            <EyeOff className="h-4 w-4" />
                          )}
                        </Toggle>
                      )}
                    </div>
                </th>
                  <th className="text-center px-2 text-xs font-medium text-muted-foreground w-20">ブロック</th>
                  <th className="text-center px-2 text-xs font-medium text-muted-foreground w-24">登録状態</th>
                  <th className="text-center px-2 text-xs font-medium text-muted-foreground w-20">登録</th>
                  <th className="text-center px-2 text-xs font-medium text-muted-foreground w-24">在庫登録</th>
                  <th className="text-center px-2 text-xs font-medium text-muted-foreground w-32">削除</th>
                  <th className="text-center px-2 text-xs font-medium text-muted-foreground w-28">楽天で表示</th>
                  <th className="text-center px-2 text-xs font-medium text-muted-foreground w-20">編集</th>
                  <th className="text-center px-2 text-xs font-medium text-muted-foreground w-24">詳細設定</th>
                  <th className="text-center px-2 text-xs font-medium text-muted-foreground w-24">イメージ設定</th>
                  <th className="text-center px-2 text-xs font-medium text-muted-foreground w-20">SKU設定</th>
              </tr>
            </thead>
            <tbody>
                {filteredProducts.map((product: any) => {
                  // Use item_number as the identifier (not id)
                  const productId = (product.item_number || product.id)?.toString()
                  if (!productId) {
                    return null
                  }
                  const productName = product.title || product.name || '商品名なし'
                  // Get first image location and construct full URL
                  const firstImageLocation = Array.isArray(product.images) && product.images[0]?.location 
                    ? product.images[0].location 
                    : null
                  const productImage = firstImageLocation 
                    ? getImageUrl(firstImageLocation)
                    : "/placeholder.svg"
                  const isHidden = product.hide_item === true
                  const isBlocked = product.block === true
                  const registrationStatus = getRegistrationStatus(product)
                  const isRegistering = registering[productId] || false
                  const isRegisteringInventory = registeringInventory[productId] || false
                  const isToggling = toggling[productId] || false
                  const isBlocking = blocking[productId] || false
                  const imageRegistrationStatus = product.image_registration_status === true
                  const inventoryRegistrationStatus = product.inventory_registration_status === true
                  
                  return (
                    <tr 
                      key={productId} 
                      className="border-b border-border hover:bg-muted/30 transition-colors"
                      style={{ height: '55px' }}
                    >
                      {/* Checkbox */}
                      <td className="px-2 align-middle text-center">
                        <div className="flex items-center justify-center">
                          <input
                            type="checkbox"
                            checked={selectedProducts.includes(productId)}
                            onChange={() => toggleProduct(productId)}
                            className="rounded border-2 border-primary accent-primary cursor-pointer h-4 w-4"
                          />
                        </div>
                      </td>
                      
                      {/* Product Image */}
                      <td className="px-2 align-middle text-center">
                        <div className="flex items-center justify-center">
                          <div className="h-8 w-8 rounded bg-muted overflow-hidden flex-shrink-0 border border-border">
                            <img 
                              src={productImage} 
                              alt={productName}
                              className="h-full w-full object-cover"
                              loading="lazy"
                              onError={(e) => {
                                const target = e.currentTarget as HTMLImageElement
                                if (target.src !== "/placeholder.svg") {
                                  target.src = "/placeholder.svg"
                                }
                              }}
                            />
                          </div>
                        </div>
                      </td>
                      
                      {/* Product Management Number */}
                      <td className="px-3 align-middle">
                        <p className="text-xs font-mono text-muted-foreground truncate leading-normal" title={product.item_number || 'N/A'}>
                          {product.item_number || 'N/A'}
                        </p>
                      </td>
                      
                      {/* Product Title */}
                      <td className="px-3 align-middle w-[400px] max-w-[400px]">
                        <p 
                          className="text-xs font-medium text-foreground truncate leading-normal cursor-help" 
                          title={productName}
                        >
                          {productName}
                        </p>
                      </td>
                      
                      {/* Hide Item Toggle */}
                      <td className="px-2 align-middle text-center">
                        <div className="flex items-center justify-center">
                          {isToggling ? (
                            <Loader2 className="h-4 w-4 animate-spin text-muted-foreground" />
                          ) : (
                            <Toggle
                              pressed={!isHidden}
                              onPressedChange={() => handleToggleHideItem(product)}
                              disabled={isToggling}
                              size="sm"
                              variant="outline"
                              className={cn(
                                "h-6 w-6 p-0 min-w-6 rounded-md transition-all",
                                !isHidden 
                                  ? "bg-green-500 text-white hover:bg-green-600 data-[state=on]:bg-green-500 data-[state=on]:text-white border-green-600" 
                                  : "bg-gray-300 text-gray-700 hover:bg-gray-400 border-gray-400"
                              )}
                              aria-label={!isHidden ? "表示中" : "非表示"}
                            >
                              {!isHidden ? (
                                <Eye className="h-4 w-4" />
                              ) : (
                                <EyeOff className="h-4 w-4" />
                              )}
                            </Toggle>
                          )}
                        </div>
                      </td>
                      
                      {/* Block Button */}
                      <td className="px-2 align-middle text-center">
                        <div className="flex items-center justify-center">
                          {isBlocking ? (
                            <Loader2 className="h-4 w-4 animate-spin text-muted-foreground" />
                          ) : (
                            <Toggle
                              pressed={isBlocked}
                              onPressedChange={() => handleToggleBlock(product)}
                              disabled={isBlocking}
                              size="sm"
                              variant="outline"
                              className={cn(
                                "h-6 w-6 p-0 min-w-6 rounded-md transition-all",
                                isBlocked
                                  ? "bg-red-500 text-white hover:bg-red-600 data-[state=on]:bg-red-500 data-[state=on]:text-white border-red-600" 
                                  : "bg-gray-200 text-gray-500 hover:bg-gray-300 border-gray-300"
                              )}
                              aria-label={isBlocked ? "ブロック中" : "ブロック解除"}
                            >
                              <Lock className="h-4 w-4" />
                            </Toggle>
                          )}
                        </div>
                      </td>
                      
                      {/* Registration Status */}
                      <td className="px-2 align-middle text-center">
                        <div className="flex items-center justify-center">
                          <Button
                            size="sm"
                            variant={product?.rakuten_registration_status === "deleted" ? "outline" : registrationStatus.variant}
                            className={`text-[11px] px-2 py-0.5 h-5 leading-tight font-medium ${registrationStatus.className} ${checkingStatus[productId] ? "opacity-50 cursor-not-allowed" : "cursor-pointer"}`}
                            onClick={() => handleCheckRegistrationStatus(product)}
                            disabled={checkingStatus[productId] || false}
                          >
                            {checkingStatus[productId] ? (
                              <>
                                <Loader2 className="h-3 w-3 mr-1 animate-spin" />
                                <span>確認中</span>
                              </>
                            ) : (
                              registrationStatus.label
                            )}
                          </Button>
                        </div>
                      </td>
                      
                      {/* Register Button */}
                      <td className="px-2 align-middle text-center">
                        <div className="flex items-center justify-center">
                          <Button
                            size="sm"
                            variant="default"
                            className="h-6 px-2.5 text-[11px] gap-1.5 leading-tight font-medium"
                            onClick={() => handleRegisterToRakuten(product)}
                            disabled={isRegistering}
                          >
                            {isRegistering ? (
                              <Loader2 className="h-3 w-3 animate-spin" />
                            ) : (
                              <Upload className="h-3 w-3" />
                            )}
                            <span>登録</span>
                          </Button>
                        </div>
                      </td>
                      
                      {/* Inventory Registration Button */}
                      <td className="px-2 align-middle text-center">
                        <div className="flex items-center justify-center">
                          <Button
                            size="sm"
                            variant="outline"
                            className={`h-6 px-2.5 text-[11px] gap-1.5 leading-tight font-medium ${
                              inventoryRegistrationStatus
                                ? "border border-green-600 text-green-600 bg-green-50 hover:bg-green-100 hover:text-green-700 hover:border-green-700"
                                : "border border-blue-600 text-blue-600 hover:bg-blue-50 hover:text-blue-700 hover:border-blue-700"
                            }`}
                            onClick={() => handleRegisterInventoryToRakuten(product)}
                            disabled={isRegisteringInventory}
                            aria-label={`${productName}の在庫を楽天市場に登録`}
                            aria-busy={isRegisteringInventory}
                            aria-pressed={inventoryRegistrationStatus}
                          >
                            {isRegisteringInventory ? (
                              <Loader2 className="h-3 w-3 animate-spin" aria-hidden="true" />
                            ) : (
                              <Package className="h-3 w-3" aria-hidden="true" />
                            )}
                            <span>在庫登録</span>
                          </Button>
                        </div>
                      </td>
                      
                      {/* Delete Button */}
                      <td className="px-2 align-middle text-center">
                        <div className="flex items-center justify-center gap-1">
                          <Button
                            size="sm"
                            variant="outline"
                            className="h-6 px-3 text-[11px] leading-tight font-medium border border-gray-800 text-blue-600 hover:bg-blue-50 hover:text-blue-700 hover:border-blue-700"
                            onClick={() => handleDeleteClick(product)}
                          >
                            <span>削除</span>
                          </Button>
                          <Button
                            size="sm"
                            variant="outline"
                            className="h-6 px-2 text-[11px] leading-tight font-medium border border-red-600 text-red-600 hover:bg-red-50 hover:text-red-700 hover:border-red-700"
                            onClick={() => handleDeleteFromRakutenClick(product)}
                            disabled={isDeletingFromRakuten}
                          >
                            {isDeletingFromRakuten && productToDeleteFromRakuten?.id === productId ? (
                              <Loader2 className="h-3 w-3 animate-spin" />
                            ) : (
                              <span>楽天削除</span>
                            )}
                          </Button>
                        </div>
                      </td>
                      
                      {/* View on Rakuten Button */}
                      <td className="px-2 align-middle text-center">
                        <div className="flex items-center justify-center">
                          <Button
                            size="sm"
                            variant="outline"
                            className="h-6 w-6 p-0 min-w-6 rounded-md transition-all border border-green-600 text-green-600 hover:bg-green-50 hover:text-green-700 hover:border-green-700"
                            onClick={() => {
                              const itemNumber = product.item_number
                              if (itemNumber) {
                                window.open(`https://item.rakuten.co.jp/licel-store/${itemNumber}`, '_blank', 'noopener,noreferrer')
                              }
                            }}
                            disabled={!product.item_number}
                            aria-label={`${productName}を楽天で表示`}
                          >
                            <ExternalLink className="h-4 w-4" />
                          </Button>
                        </div>
                      </td>
                      
                      {/* Edit on Rakuten Button */}
                      <td className="px-2 align-middle text-center">
                        <div className="flex items-center justify-center">
                          <Button
                            size="sm"
                            variant="outline"
                            className="h-6 w-6 p-0 min-w-6 rounded-md transition-all border border-blue-600 text-blue-600 hover:bg-blue-50 hover:text-blue-700 hover:border-blue-700"
                            onClick={() => {
                              const itemNumber = product.item_number
                              if (itemNumber) {
                                window.open(`https://item.rms.rakuten.co.jp/rms-sku/shops/437067/item/edit/${itemNumber}`, '_blank', 'noopener,noreferrer')
                              }
                            }}
                            disabled={!product.item_number}
                            aria-label={`${productName}を楽天で編集`}
                          >
                            <Pencil className="h-4 w-4" />
                          </Button>
                        </div>
                      </td>
                      
                      {/* Detail Settings Button */}
                      <td className="px-2 align-middle text-center">
                        <div className="flex items-center justify-center">
                          <Button
                            size="sm"
                            variant="outline"
                            className="h-6 px-2.5 text-[11px] gap-1.5 leading-tight font-medium"
                            onClick={() => handleDetailSettings(product)}
                            aria-label={`${productName}の詳細設定を開く`}
                          >
                            <Settings className="h-3 w-3" aria-hidden="true" />
                            <span>詳細</span>
                          </Button>
                        </div>
                      </td>
                      
                      {/* Image Settings Button */}
                      <td className="px-2 align-middle text-center">
                        <div className="flex items-center justify-center">
                          <Button
                            size="sm"
                            variant="outline"
                            className={`h-6 px-2.5 text-[11px] gap-1.5 leading-tight font-medium whitespace-nowrap ${
                              imageRegistrationStatus
                                ? "border border-green-600 text-green-600 bg-green-50 hover:bg-green-100 hover:text-green-700 hover:border-green-700"
                                : ""
                            }`}
                            onClick={() => handleImageSettings(product)}
                          >
                            <ImageIcon className="h-3 w-3" />
                            <span>画像</span>
                          </Button>
                        </div>
                      </td>
                      
                      {/* SKU Settings Button */}
                      <td className="px-2 align-middle text-center">
                        <div className="flex items-center justify-center">
                          <Button
                            size="sm"
                            variant="outline"
                            className="h-6 px-2.5 text-[11px] gap-1.5 leading-tight font-medium"
                            onClick={() => handleSkuSettings(product)}
                          >
                            <Hash className="h-3 w-3" />
                            <span>SKU</span>
                          </Button>
                        </div>
                      </td>
                    </tr>
                  )
                })}
            </tbody>
          </table>
        </div>
      </Card>
      )}

      {filteredProducts.length > 0 && hasMoreProducts && (
        <div className="flex justify-center py-4">
          <Button
            variant="outline"
            size="sm"
            onClick={handleLoadMoreProducts}
            disabled={isLoadingMore}
            className="gap-2 bg-white"
          >
            {isLoadingMore ? (
              <>
                <Loader2 className="h-4 w-4 animate-spin" />
                <span>さらに読み込み中...</span>
              </>
            ) : (
              <>
                <RefreshCw className="h-4 w-4" />
                <span>さらに読み込む</span>
              </>
            )}
          </Button>
        </div>
      )}
      
      {/* Delete Confirmation Dialog */}
      <AlertDialog open={deleteConfirmOpen} onOpenChange={setDeleteConfirmOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>商品を削除しますか？</AlertDialogTitle>
            <AlertDialogDescription>
              {productToDelete?.name} を削除してもよろしいですか？この操作は元に戻せません。
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel disabled={isDeletingProduct}>キャンセル</AlertDialogCancel>
            <AlertDialogAction
              onClick={handleConfirmDelete}
              className="bg-destructive hover:bg-destructive/90"
              disabled={isDeletingProduct}
            >
              {isDeletingProduct ? (
                <>
                  <Loader2 className="h-4 w-4 animate-spin mr-2" />
                  削除中...
                </>
              ) : (
                "削除"
              )}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      {/* Bulk Delete Confirmation Dialog */}
      <AlertDialog open={bulkDeleteConfirmOpen} onOpenChange={setBulkDeleteConfirmOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>選択した商品を削除しますか？</AlertDialogTitle>
            <AlertDialogDescription>
              選択中の商品（{selectedProducts.length}件）を削除します。この操作は元に戻せません。
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel disabled={isBulkDeleting}>キャンセル</AlertDialogCancel>
            <AlertDialogAction
              onClick={handleConfirmBulkDelete}
              className="bg-destructive hover:bg-destructive/90"
              disabled={isBulkDeleting}
            >
              {isBulkDeleting ? (
                <>
                  <Loader2 className="h-4 w-4 animate-spin mr-2" />
                  削除中...
                </>
              ) : (
                "削除"
              )}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      {/* Rakuten Delete Confirmation Dialog */}
      <AlertDialog open={rakutenDeleteConfirmOpen} onOpenChange={setRakutenDeleteConfirmOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>楽天市場から商品を削除しますか？</AlertDialogTitle>
            <AlertDialogDescription>
              {productToDeleteFromRakuten?.name} を楽天市場から削除してもよろしいですか？この操作は元に戻せません。
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel disabled={isDeletingFromRakuten}>キャンセル</AlertDialogCancel>
            <AlertDialogAction
              onClick={handleConfirmDeleteFromRakuten}
              className="bg-destructive hover:bg-destructive/90"
              disabled={isDeletingFromRakuten}
            >
              {isDeletingFromRakuten ? (
                <>
                  <Loader2 className="h-4 w-4 animate-spin mr-2" />
                  削除中...
                </>
              ) : (
                "削除"
              )}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      {/* Bulk Rakuten Delete Confirmation Dialog */}
      <AlertDialog open={bulkRakutenDeleteConfirmOpen} onOpenChange={setBulkRakutenDeleteConfirmOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>選択した商品を楽天市場から削除しますか？</AlertDialogTitle>
            <AlertDialogDescription>
              選択中の商品（{selectedProducts.length}件）を楽天市場から削除します。この操作は元に戻せません。
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel disabled={isBulkDeletingFromRakuten}>キャンセル</AlertDialogCancel>
            <AlertDialogAction
              onClick={handleConfirmBulkDeleteFromRakuten}
              className="bg-destructive hover:bg-destructive/90"
              disabled={isBulkDeletingFromRakuten}
            >
              {isBulkDeletingFromRakuten ? (
                <>
                  <Loader2 className="h-4 w-4 animate-spin mr-2" />
                  削除中...
                </>
              ) : (
                "削除"
              )}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
      
      {/* Settings Modal */}
      <Dialog open={settingsModalOpen} onOpenChange={(open) => {
        if (!open && !isSavingSettings) {
          setTouchedFields(new Set())
          setSettingsFormErrors({})
        }
        setSettingsModalOpen(open)
      }}>
        <DialogContent 
          ref={settingsModalRef as React.RefObject<HTMLDivElement>}
          className="max-w-2xl max-h-[90vh] overflow-y-auto"
          aria-labelledby="settings-dialog-title"
          aria-describedby="settings-dialog-description"
          role="dialog"
          aria-modal="true"
        >
          <DialogHeader>
            <DialogTitle id="settings-dialog-title">詳細設定</DialogTitle>
            <DialogDescription id="settings-dialog-description">
              {selectedProductForSettings?.title || "商品の詳細設定を変更します"}
            </DialogDescription>
          </DialogHeader>
          
          <div className="space-y-6 py-4">
            {/* Product Title */}
            <div className="space-y-2">
              <Label htmlFor="title">商品タイトル</Label>
              <Input
                id="title"
                value={settingsForm.title}
                onChange={(e) => {
                  setSettingsForm(prev => ({ ...prev, title: e.target.value }))
                  if (touchedFields.has('title')) {
                    validateFieldOnChange('title', e.target.value)
                  }
                }}
                onBlur={() => handleFieldBlur('title')}
                placeholder="商品タイトルを入力してください"
                aria-invalid={touchedFields.has('title') && !!settingsFormErrors.title}
                aria-describedby={touchedFields.has('title') && settingsFormErrors.title ? "title-error" : "title-help"}
                className={getValidationClassName(
                  !settingsFormErrors.title,
                  touchedFields.has('title'),
                  "w-full"
                )}
              />
              {touchedFields.has('title') && settingsFormErrors.title && (
                <p id="title-error" className="text-sm text-red-600" role="alert">
                  {settingsFormErrors.title}
                </p>
              )}
              <p id="title-help" className="text-xs text-muted-foreground sr-only">
                商品のタイトルを変更できます（最大200文字）
              </p>
              <p className="text-xs text-muted-foreground">
                商品のタイトルを変更できます
              </p>
            </div>
            
            {/* Item Type */}
            <div className="space-y-2">
              <Label htmlFor="itemType">商品タイプ</Label>
              <Select
                value={settingsForm.itemType}
                onValueChange={(value) => setSettingsForm(prev => ({ ...prev, itemType: value }))}
              >
                <SelectTrigger id="itemType">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="NORMAL">NORMAL</SelectItem>
                  <SelectItem value="PRE_ORDER">PRE_ORDER</SelectItem>
                  <SelectItem value="BUYING_CLUB">BUYING_CLUB</SelectItem>
                </SelectContent>
              </Select>
            </div>
            
            {/* Genre ID */}
            <div className="space-y-2">
              <Label htmlFor="genreId">ジャンルID</Label>
              <Input
                id="genreId"
                type="number"
                value={settingsForm.genreId}
                onChange={(e) => {
                  setSettingsForm(prev => ({ ...prev, genreId: e.target.value }))
                  if (touchedFields.has('genreId')) {
                    validateFieldOnChange('genreId', e.target.value)
                  }
                }}
                onBlur={() => handleFieldBlur('genreId')}
                placeholder="100000-999999"
                aria-invalid={touchedFields.has('genreId') && !!settingsFormErrors.genreId}
                aria-describedby={touchedFields.has('genreId') && settingsFormErrors.genreId ? "genreId-error" : "genreId-help"}
                className={getValidationClassName(
                  !settingsFormErrors.genreId,
                  touchedFields.has('genreId'),
                  "w-full"
                )}
              />
              {touchedFields.has('genreId') && settingsFormErrors.genreId && (
                <p id="genreId-error" className="text-sm text-red-600" role="alert">
                  {settingsFormErrors.genreId}
                </p>
              )}
              <p id="genreId-help" className="text-xs text-muted-foreground">
                100,000から999,999の間の数値を入力してください
              </p>
            </div>
            
            {/* Tags */}
            <div className="space-y-2">
              <Label htmlFor="tags">タグ（カンマ区切り）</Label>
              <Input
                id="tags"
                value={settingsForm.tags}
                onChange={(e) => {
                  setSettingsForm(prev => ({ ...prev, tags: e.target.value }))
                  if (touchedFields.has('tags')) {
                    validateFieldOnChange('tags', e.target.value)
                  }
                }}
                onBlur={() => handleFieldBlur('tags')}
                placeholder="5000000, 6000000, 7000000"
                aria-invalid={touchedFields.has('tags') && !!settingsFormErrors.tags}
                aria-describedby={touchedFields.has('tags') && settingsFormErrors.tags ? "tags-error" : "tags-help"}
                className={getValidationClassName(
                  !settingsFormErrors.tags,
                  touchedFields.has('tags'),
                  "w-full"
                )}
              />
              {touchedFields.has('tags') && settingsFormErrors.tags && (
                <p id="tags-error" className="text-sm text-red-600" role="alert">
                  {settingsFormErrors.tags}
                </p>
              )}
              <p id="tags-help" className="text-xs text-muted-foreground">
                5,000,000から9,999,999の間の数値をカンマで区切って入力
              </p>
            </div>
            
            {/* Unlimited Inventory */}
            <div className="space-y-2">
              <Label htmlFor="unlimitedInventory">無制限在庫</Label>
              <Select
                value={settingsForm.unlimitedInventoryFlag}
                onValueChange={(value) => setSettingsForm(prev => ({ ...prev, unlimitedInventoryFlag: value }))}
              >
                <SelectTrigger id="unlimitedInventory">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="true">true</SelectItem>
                  <SelectItem value="false">false</SelectItem>
                </SelectContent>
              </Select>
            </div>
            
            {/* Features Section */}
            <div className="space-y-4 border-t pt-4">
              <h3 className="font-semibold">機能設定</h3>
              
              {/* Search Visibility */}
              <div className="space-y-2">
                <Label htmlFor="searchVisibility">検索表示</Label>
                <Select
                  value={settingsForm.searchVisibility}
                  onValueChange={(value) => setSettingsForm(prev => ({ ...prev, searchVisibility: value }))}
                >
                  <SelectTrigger id="searchVisibility">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="ALWAYS_VISIBLE">ALWAYS_VISIBLE</SelectItem>
                    <SelectItem value="ALWAYS_HIDDEN">ALWAYS_HIDDEN</SelectItem>
                  </SelectContent>
                </Select>
              </div>
              
              {/* Inventory Display */}
              <div className="space-y-2">
                <Label htmlFor="inventoryDisplay">在庫表示</Label>
                <Select
                  value={settingsForm.inventoryDisplay}
                  onValueChange={(value) => setSettingsForm(prev => ({ ...prev, inventoryDisplay: value }))}
                >
                  <SelectTrigger id="inventoryDisplay">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="DISPLAY_ABSOLUTE_STOCK_COUNT">DISPLAY_ABSOLUTE_STOCK_COUNT</SelectItem>
                    <SelectItem value="HIDDEN_STOCK">HIDDEN_STOCK</SelectItem>
                    <SelectItem value="DISPLAY_LOW_STOCK">DISPLAY_LOW_STOCK</SelectItem>
                  </SelectContent>
                </Select>
              </div>
              
              {/* Review (Evaluation) */}
              <div className="space-y-2">
                <Label htmlFor="review">評価</Label>
                <Select
                  value={settingsForm.review}
                  onValueChange={(value) => setSettingsForm(prev => ({ ...prev, review: value }))}
                >
                  <SelectTrigger id="review">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="SHOP_SETTING">SHOP_SETTING</SelectItem>
                    <SelectItem value="VISIBLE">VISIBLE</SelectItem>
                    <SelectItem value="HIDDEN">HIDDEN</SelectItem>
                  </SelectContent>
                </Select>
              </div>
            </div>
            
            {/* Payment Section */}
            <div className="space-y-4 border-t pt-4">
              <h3 className="font-semibold">支払い設定</h3>
              
              {/* Tax Included */}
              <div className="space-y-2">
                <Label htmlFor="taxIncluded">税込</Label>
                <Select
                  value={settingsForm.taxIncluded}
                  onValueChange={(value) => setSettingsForm(prev => ({ ...prev, taxIncluded: value }))}
                >
                  <SelectTrigger id="taxIncluded">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="true">true</SelectItem>
                    <SelectItem value="false">false</SelectItem>
                  </SelectContent>
                </Select>
              </div>
              
              {/* Tax Rate */}
              <div className="space-y-2">
                <Label htmlFor="taxRate">税率</Label>
                <Input
                  id="taxRate"
                  type="number"
                  step="0.01"
                  value={settingsForm.taxRate}
                  onChange={(e) => {
                    setSettingsForm(prev => ({ ...prev, taxRate: e.target.value }))
                    if (touchedFields.has('taxRate')) {
                      validateFieldOnChange('taxRate', e.target.value)
                    }
                  }}
                  onBlur={() => handleFieldBlur('taxRate')}
                  placeholder="10.0"
                  aria-invalid={touchedFields.has('taxRate') && !!settingsFormErrors.taxRate}
                  aria-describedby={touchedFields.has('taxRate') && settingsFormErrors.taxRate ? "taxRate-error" : "taxRate-help"}
                  className={getValidationClassName(
                    !settingsFormErrors.taxRate,
                    touchedFields.has('taxRate'),
                    "w-full"
                  )}
                />
                {touchedFields.has('taxRate') && settingsFormErrors.taxRate && (
                  <p id="taxRate-error" className="text-sm text-red-600" role="alert">
                    {settingsFormErrors.taxRate}
                  </p>
                )}
                <p id="taxRate-help" className="text-xs text-muted-foreground">
                  小数点以下2桁まで入力可能
                </p>
              </div>
              
              {/* Delivery Date IDs Section */}
              <div className="space-y-4 border-t pt-4">
                <h3 className="font-semibold">配送日設定</h3>
                
                {/* Normal Delivery Date ID */}
                <div className="space-y-2">
                  <Label htmlFor="normalDeliveryDateId">通常配送日ID</Label>
                  <Input
                    id="normalDeliveryDateId"
                    type="number"
                    value={settingsForm.normalDeliveryDateId}
                    onChange={(e) => {
                      setSettingsForm(prev => ({ ...prev, normalDeliveryDateId: e.target.value }))
                    }}
                    placeholder="1000"
                    className="w-full"
                  />
                  <p className="text-sm text-muted-foreground">
                    通常配送日のIDを入力してください（デフォルト: 1000）
                  </p>
                </div>
              </div>
              
              {/* Cash on Delivery Fee Included */}
              <div className="space-y-2">
                <Label htmlFor="cashOnDeliveryFeeIncluded">代金引換手数料込み</Label>
                <Select
                  value={settingsForm.cashOnDeliveryFeeIncluded}
                  onValueChange={(value) => setSettingsForm(prev => ({ ...prev, cashOnDeliveryFeeIncluded: value }))}
                >
                  <SelectTrigger id="cashOnDeliveryFeeIncluded">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="true">true</SelectItem>
                    <SelectItem value="false">false</SelectItem>
                  </SelectContent>
                </Select>
              </div>
            </div>
          </div>
          
          <DialogFooter>
            <Button 
              variant="outline" 
              onClick={() => {
                setSettingsModalOpen(false)
                setTouchedFields(new Set())
                setSettingsFormErrors({})
              }} 
              disabled={isSavingSettings}
              aria-label="設定をキャンセル"
            >
              キャンセル
            </Button>
            <Button 
              onClick={handleSaveSettings} 
              disabled={isSavingSettings || Object.keys(settingsFormErrors).length > 0}
              aria-label="設定を保存"
            >
              {isSavingSettings ? (
                <>
                  <Loader2 className="h-4 w-4 animate-spin mr-2" aria-hidden="true" />
                  <span>保存中...</span>
                </>
              ) : (
                "保存"
              )}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
      
      {/* Image Settings Modal */}
      <Dialog open={imageModalOpen} onOpenChange={setImageModalOpen}>
        <DialogContent 
          ref={imageModalRef as React.RefObject<HTMLDivElement>}
          className="max-w-6xl max-h-[90vh] overflow-y-auto"
          role="dialog"
          aria-modal="true"
          aria-labelledby="image-dialog-title"
        >
          <DialogHeader>
            <DialogTitle id="image-dialog-title">画像設定</DialogTitle>
            <DialogDescription>
              {selectedProductForImages?.title || "商品の画像を管理します"}
            </DialogDescription>
          </DialogHeader>
          
          <div className="space-y-4 py-4">
            {selectedProductForImages && (() => {
              const images = selectedProductForImages.images || []
              
              if (images.length === 0) {
                return (
                  <div className="text-center py-12 text-muted-foreground">
                    <ImageIcon className="h-16 w-16 mx-auto mb-4 opacity-50" />
                    <p>画像が登録されていません</p>
                  </div>
                )
              }
              
              return (
                <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                  {images.map((image: any, index: number) => {
                    const imageUrl = getImageUrl(image.location)
                    const deleteKey = `${selectedProductForImages.item_number}-${image.location}`
                    const isDeleting = deletingImage[deleteKey] || false
                    
                    return (
                      <Card key={index} className="relative overflow-hidden group">
                        {/* Image Preview */}
                        <div className="relative aspect-square bg-muted overflow-hidden cursor-pointer" onClick={() => handleViewImage(image)}>
                          <img
                            src={imageUrl}
                            alt={image.alt || `Image ${index + 1}`}
                            className="w-full h-full object-cover"
                            onError={(e) => {
                              const target = e.currentTarget as HTMLImageElement
                              if (target.src !== "/placeholder.svg") {
                                target.src = "/placeholder.svg"
                              }
                            }}
                          />
                          
                          {/* Watermark-style overlay with actions - always visible but subtle */}
                          <div className="absolute inset-0 bg-black/40 hover:bg-black/60 transition-opacity flex items-center justify-center gap-2">
                            {/* View Button - Preview icon */}
                            <Button
                              size="sm"
                              variant="secondary"
                              className="gap-1.5 bg-white/90 hover:bg-white text-gray-900 border border-gray-300 shadow-md"
                              onClick={(e) => {
                                e.stopPropagation()
                                handleViewImage(image)
                              }}
                              disabled={isDeleting}
                            >
                              <Eye className="h-4 w-4" />
                              <span className="text-xs">表示</span>
                            </Button>
                            
                            {/* Delete Button */}
                            <Button
                              size="sm"
                              variant="destructive"
                              className="gap-1.5 bg-red-600/90 hover:bg-red-700 text-white shadow-md"
                              onClick={(e) => {
                                e.stopPropagation()
                                handleDeleteImage(image)
                              }}
                              disabled={isDeleting}
                            >
                              {isDeleting ? (
                                <Loader2 className="h-4 w-4 animate-spin" />
                              ) : (
                                <Trash2 className="h-4 w-4" />
                              )}
                              <span className="text-xs">削除</span>
                            </Button>
                          </div>
                        </div>
                        
                        {/* Image Info */}
                        <div className="p-3 space-y-1">
                          <p className="text-xs font-medium text-foreground truncate" title={image.alt || image.location}>
                            {image.alt || image.location}
                          </p>
                          <p className="text-xs text-muted-foreground truncate" title={image.location}>
                            {image.location}
                          </p>
                          {image.type && (
                            <Badge variant="outline" className="text-xs mt-1">
                              {image.type}
                            </Badge>
                          )}
                        </div>
                      </Card>
                    )
                  })}
                </div>
              )
            })()}
          </div>
          
          <DialogFooter>
            <Button
              variant="default"
              onClick={handleUploadImagesToRakuten}
              disabled={uploadingToRakuten || !selectedProductForImages || (selectedProductForImages?.images || []).length === 0}
              className="gap-2"
            >
              {uploadingToRakuten ? (
                <>
                  <Loader2 className="h-4 w-4 animate-spin" />
                  アップロード中...
                </>
              ) : (
                <>
                  <Upload className="h-4 w-4" />
                  楽天市場にアップロード
                </>
              )}
            </Button>
            <Button variant="outline" onClick={() => setImageModalOpen(false)}>
              閉じる
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
      
      {/* SKU Settings Modal */}
      <Dialog open={skuModalOpen} onOpenChange={(open) => {
        if (!open) {
          setEditingVariantId(null)
        }
        setSkuModalOpen(open)
      }}>
        <DialogContent 
          ref={skuModalRef as React.RefObject<HTMLDivElement>}
          className="max-w-6xl max-h-[90vh] overflow-y-auto"
          role="dialog"
          aria-modal="true"
          aria-labelledby="sku-dialog-title"
        >
          <DialogHeader>
            <DialogTitle id="sku-dialog-title" className="flex items-center gap-2">
              <Hash className="h-5 w-5" aria-hidden="true" />
              SKU設定
            </DialogTitle>
            <DialogDescription>
              {selectedProductForSku?.title || "商品のSKU情報を表示・管理します"}
              {selectedProductForSku?.item_number && (
                <span className="ml-2 text-xs text-muted-foreground">
                  (管理番号: {selectedProductForSku.item_number})
                </span>
              )}
            </DialogDescription>
          </DialogHeader>
          
          <div className="space-y-6 py-4">
            {selectedProductForSku && editingSkuData && (() => {
              const { selectors, variants } = editingSkuData
              const variantsArray = Object.entries(variants).map(([skuId, data]) => ({
                skuId,
                ...data
              }))
              
              if (selectors.length === 0 && variantsArray.length === 0) {
                return (
                  <div className="text-center py-8 text-muted-foreground">
                    <Package className="h-12 w-12 mx-auto mb-4 opacity-50" />
                    <p>SKU情報が登録されていません</p>
                    <div className="flex gap-2 justify-center mt-4">
                      <Button
                        variant="outline"
                        size="sm"
                        onClick={handleRefreshSkuFromOrigin}
                        disabled={isSavingSku}
                      >
                        {isSavingSku ? (
                          <Loader2 className="h-4 w-4 animate-spin mr-2" />
                        ) : (
                          <RefreshCw className="h-4 w-4 mr-2" />
                        )}
                        元データからSKU情報を生成
                      </Button>
                      <Button
                        variant="outline"
                        size="sm"
                        onClick={handleAddSelector}
                      >
                        <Settings className="h-4 w-4 mr-2" />
                        選択肢を追加
                      </Button>
                    </div>
                  </div>
                )
              }
              
              return (
                <>
                  {/* Variant Selectors Editor */}
                  <div className="space-y-3">
                    <div className="flex items-center justify-between">
                      <h3 className="font-semibold text-sm">バリアント選択肢</h3>
                      <Button
                        variant="outline"
                        size="sm"
                        onClick={handleAddSelector}
                        className="gap-1.5"
                      >
                        <Settings className="h-3.5 w-3.5" />
                        選択肢を追加
                      </Button>
                    </div>
                    
                    {selectors.length > 0 ? (
                      <div className="grid gap-4 md:grid-cols-2">
                        {selectors.map((selector: any, selectorIdx: number) => (
                          <Card key={selectorIdx} className="p-4">
                            <div className="space-y-3">
                              {/* Selector Key & Display Name */}
                              <div className="flex items-center gap-2">
                                <div className="flex-1 space-y-1">
                                  <Label className="text-xs text-muted-foreground">キー</Label>
                                  <Input
                                    value={selector.key || ""}
                                    onChange={(e) => handleUpdateSelectorKey(selectorIdx, e.target.value)}
                                    className="h-8 text-xs"
                                    placeholder="color, size..."
                                  />
                                </div>
                                <div className="flex-1 space-y-1">
                                  <Label className="text-xs text-muted-foreground">表示名</Label>
                                  <Input
                                    value={selector.displayName || ""}
                                    onChange={(e) => handleUpdateSelectorDisplayName(selectorIdx, e.target.value)}
                                    className="h-8 text-xs"
                                    placeholder="カラー, サイズ..."
                                  />
                                </div>
                                <Button
                                  variant="ghost"
                                  size="sm"
                                  className="h-8 w-8 p-0 text-destructive hover:text-destructive mt-5"
                                  onClick={() => handleRemoveSelector(selectorIdx)}
                                >
                                  <Trash2 className="h-4 w-4" />
                                </Button>
                              </div>
                              
                              {/* Selector Values */}
                              <div className="space-y-2">
                                <Label className="text-xs text-muted-foreground">値一覧</Label>
                                <div className="flex flex-wrap gap-2">
                                  {selector.values?.map((value: any, valueIdx: number) => (
                                    <div key={valueIdx} className="flex items-center gap-1">
                                      <Input
                                        value={value.displayValue || ""}
                                        onChange={(e) => handleUpdateSelectorValue(selectorIdx, valueIdx, e.target.value)}
                                        className="h-7 w-24 text-xs"
                                        placeholder="値..."
                                      />
                                      <Button
                                        variant="ghost"
                                        size="sm"
                                        className="h-7 w-7 p-0 text-muted-foreground hover:text-destructive"
                                        onClick={() => handleRemoveSelectorValue(selectorIdx, valueIdx)}
                                      >
                                        <XCircle className="h-3.5 w-3.5" />
                                      </Button>
                                    </div>
                                  ))}
                                  <Button
                                    variant="outline"
                                    size="sm"
                                    className="h-7 px-2 text-xs"
                                    onClick={() => handleAddSelectorValue(selectorIdx)}
                                  >
                                    + 値を追加
                                  </Button>
                                </div>
                              </div>
                            </div>
                          </Card>
                        ))}
                      </div>
                    ) : (
                      <div className="text-center py-4 text-muted-foreground text-sm">
                        選択肢がありません
                      </div>
                    )}
                  </div>
                  
                  {/* Variants Editor */}
                  <div className="space-y-3">
                    <div className="flex items-center justify-between">
                      <h3 className="font-semibold text-sm">SKU一覧 ({variantsArray.length}件)</h3>
                      <div className="flex items-center gap-2">
                        <Button
                          variant="outline"
                          size="sm"
                          onClick={handleRefreshSkuFromOrigin}
                          disabled={isSavingSku}
                          className="gap-1.5"
                        >
                          {isSavingSku ? (
                            <Loader2 className="h-3.5 w-3.5 animate-spin" />
                          ) : (
                            <RefreshCw className="h-3.5 w-3.5" />
                          )}
                          元データから再生成
                        </Button>
                        <Button
                          variant="outline"
                          size="sm"
                          onClick={handleAddVariant}
                          className="gap-1.5"
                        >
                          <Hash className="h-3.5 w-3.5" />
                          SKUを追加
                        </Button>
                      </div>
                    </div>
                    
                    {variantsArray.length > 0 ? (
                      <div className="border rounded-lg overflow-hidden">
                        <div className="overflow-x-auto">
                          <Table>
                            <TableHeader>
                              <TableRow className="bg-muted/50">
                                <TableHead className="w-40 font-medium text-xs">SKU ID</TableHead>
                                {selectors.map((selector: any) => (
                                  <TableHead key={selector.key} className="font-medium text-xs min-w-28">
                                    {selector.displayName || selector.key}
                                  </TableHead>
                                ))}
                                <TableHead className="font-medium text-xs w-28 text-right">価格 (円)</TableHead>
                                <TableHead className="font-medium text-xs w-32">商品番号</TableHead>
                                <TableHead className="font-medium text-xs w-20 text-center">配送料</TableHead>
                                <TableHead className="font-medium text-xs w-20 text-center">通知</TableHead>
                                <TableHead className="font-medium text-xs w-20 text-center">カート</TableHead>
                                <TableHead className="font-medium text-xs w-16 text-center">操作</TableHead>
                              </TableRow>
                            </TableHeader>
                            <TableBody>
                              {variantsArray.map((variant: any, idx: number) => (
                                <TableRow key={variant.skuId || idx} className="hover:bg-muted/30">
                                  {/* SKU ID (editable) */}
                                  <TableCell className="text-xs">
                                    <Input
                                      value={variant.skuId || ""}
                                      className="h-7 text-xs font-mono w-36"
                                      disabled
                                      title="SKU IDは変更できません"
                                    />
                                  </TableCell>
                                  
                                  {/* Selector Values (editable) */}
                                  {selectors.map((selector: any) => {
                                    const selectorValue = variant.selectorValues?.[selector.key] || ""
                                    return (
                                      <TableCell key={selector.key} className="text-xs">
                                        <Input
                                          value={selectorValue}
                                          onChange={(e) => handleUpdateVariantSelectorValue(variant.skuId, selector.key, e.target.value)}
                                          className="h-7 text-xs w-24"
                                          placeholder="-"
                                        />
                                      </TableCell>
                                    )
                                  })}
                                  
                                  {/* Price (editable) */}
                                  <TableCell className="text-xs text-right">
                                    <Input
                                      type="number"
                                      value={variant.standardPrice || ""}
                                      onChange={(e) => handleUpdateVariantField(variant.skuId, "standardPrice", e.target.value)}
                                      className="w-24 h-7 text-xs text-right"
                                      min="0"
                                    />
                                  </TableCell>
                                  
                                  {/* Article Number (editable) */}
                                  <TableCell className="text-xs">
                                    <div className="space-y-1">
                                      <Input
                                        value={typeof variant.articleNumber === 'object' ? variant.articleNumber?.exemptionReason || "" : variant.articleNumber || ""}
                                        onChange={(e) => {
                                          const value = e.target.value
                                          if (value) {
                                            // If articleNumber is an object, update exemptionReason
                                            if (typeof variant.articleNumber === 'object') {
                                              handleUpdateVariantField(variant.skuId, "articleNumber", { exemptionReason: parseInt(value) || 0 })
                                            } else {
                                              // If it's a string, convert to object
                                              handleUpdateVariantField(variant.skuId, "articleNumber", { exemptionReason: parseInt(value) || 0 })
                                            }
                                          } else {
                                            handleUpdateVariantField(variant.skuId, "articleNumber", "")
                                          }
                                        }}
                                        className="h-7 text-xs w-28"
                                        placeholder="商品番号"
                                        type="number"
                                      />
                                    </div>
                                  </TableCell>
                                  
                                  {/* Shipping - Postage Included (toggle) */}
                                  <TableCell className="text-xs text-center">
                                    <Button
                                      variant={variant.shipping?.postageIncluded ? "default" : "outline"}
                                      size="sm"
                                      className="h-6 px-2 text-xs"
                                      onClick={() => handleUpdateVariantShipping(variant.skuId, "postageIncluded", !variant.shipping?.postageIncluded)}
                                    >
                                      {variant.shipping?.postageIncluded ? "込み" : "別途"}
                                    </Button>
                                  </TableCell>
                                  
                                  {/* Features - Restock Notification (toggle) */}
                                  <TableCell className="text-xs text-center">
                                    <Button
                                      variant={variant.features?.restockNotification ? "default" : "outline"}
                                      size="sm"
                                      className="h-6 px-2 text-xs"
                                      onClick={() => handleUpdateVariantFeatures(variant.skuId, "restockNotification", !variant.features?.restockNotification)}
                                    >
                                      {variant.features?.restockNotification ? "ON" : "OFF"}
                                    </Button>
                                  </TableCell>
                                  
                                  {/* Features - Display Normal Cart Button (toggle) */}
                                  <TableCell className="text-xs text-center">
                                    <Button
                                      variant={variant.features?.displayNormalCartButton !== false ? "default" : "outline"}
                                      size="sm"
                                      className="h-6 px-2 text-xs"
                                      onClick={() => handleUpdateVariantFeatures(variant.skuId, "displayNormalCartButton", variant.features?.displayNormalCartButton === false)}
                                    >
                                      {variant.features?.displayNormalCartButton !== false ? "表示" : "非表示"}
                                    </Button>
                                  </TableCell>
                                  
                                  {/* Features - Noshi (toggle) */}
                                  <TableCell className="text-xs text-center">
                                    <Button
                                      variant={variant.features?.noshi ? "default" : "outline"}
                                      size="sm"
                                      className="h-6 px-2 text-xs"
                                      onClick={() => handleUpdateVariantFeatures(variant.skuId, "noshi", !variant.features?.noshi)}
                                    >
                                      {variant.features?.noshi ? "ON" : "OFF"}
                                    </Button>
                                  </TableCell>
                                  
                                  {/* Delete Button */}
                                  <TableCell className="text-xs text-center">
                                    <Button
                                      variant="ghost"
                                      size="sm"
                                      className="h-6 w-6 p-0 text-destructive hover:text-destructive"
                                      onClick={() => handleRemoveVariant(variant.skuId)}
                                    >
                                      <Trash2 className="h-3.5 w-3.5" />
                                    </Button>
                                  </TableCell>
                                </TableRow>
                              ))}
                            </TableBody>
                          </Table>
                        </div>
                      </div>
                    ) : (
                      <div className="text-center py-4 text-muted-foreground text-sm">
                        SKUがありません
                      </div>
                    )}
                  </div>
                  
                  {/* Variant Details Editor - Expandable section for attributes, shipping.postageSegment, etc. */}
                  {variantsArray.length > 0 && (
                    <div className="space-y-3 border-t pt-4">
                      <h3 className="font-semibold text-sm">詳細設定</h3>
                      <div className="space-y-4">
                        {variantsArray.map((variant: any, idx: number) => (
                          <Card key={variant.skuId || idx} className="p-4">
                            <div className="space-y-4">
                              <div className="flex items-center justify-between">
                                <h4 className="font-medium text-sm">SKU: {variant.skuId}</h4>
                              </div>
                              
                              {/* Shipping Postage Segment */}
                              <div className="grid grid-cols-2 gap-4">
                                <div className="space-y-1">
                                  <Label className="text-xs text-muted-foreground">配送料区分</Label>
                                  <Input
                                    type="number"
                                    value={variant.shipping?.postageSegment || ""}
                                    onChange={(e) => handleUpdateVariantShipping(variant.skuId, "postageSegment", e.target.value ? parseInt(e.target.value) : undefined)}
                                    className="h-8 text-xs"
                                    placeholder="1"
                                    min="1"
                                  />
                                </div>
                              </div>
                              
                              {/* Attributes Editor */}
                              <div className="space-y-2">
                                <Label className="text-xs text-muted-foreground">属性</Label>
                                {variant.attributes && Array.isArray(variant.attributes) && variant.attributes.length > 0 ? (
                                  <div className="space-y-2">
                                    {variant.attributes.map((attr: any, attrIdx: number) => (
                                      <div key={attrIdx} className="flex items-center gap-2 p-2 border rounded">
                                        <Input
                                          value={attr.name || ""}
                                          onChange={(e) => {
                                            const newAttributes = [...(variant.attributes || [])]
                                            newAttributes[attrIdx] = { ...attr, name: e.target.value }
                                            handleUpdateVariantField(variant.skuId, "attributes", newAttributes)
                                          }}
                                          className="h-7 text-xs flex-1"
                                          placeholder="属性名"
                                        />
                                        <Input
                                          value={Array.isArray(attr.values) ? attr.values.join(", ") : attr.values || ""}
                                          onChange={(e) => {
                                            const newAttributes = [...(variant.attributes || [])]
                                            const values = e.target.value.split(",").map(v => v.trim()).filter(v => v)
                                            newAttributes[attrIdx] = { ...attr, values }
                                            handleUpdateVariantField(variant.skuId, "attributes", newAttributes)
                                          }}
                                          className="h-7 text-xs flex-1"
                                          placeholder="値 (カンマ区切り)"
                                        />
                                        <Button
                                          variant="ghost"
                                          size="sm"
                                          className="h-7 w-7 p-0 text-destructive hover:text-destructive"
                                          onClick={() => {
                                            const newAttributes = variant.attributes.filter((_: any, i: number) => i !== attrIdx)
                                            handleUpdateVariantField(variant.skuId, "attributes", newAttributes)
                                          }}
                                        >
                                          <XCircle className="h-3.5 w-3.5" />
                                        </Button>
                                      </div>
                                    ))}
                                    <Button
                                      variant="outline"
                                      size="sm"
                                      className="h-7 px-2 text-xs"
                                      onClick={() => {
                                        const newAttributes = [...(variant.attributes || []), { name: "", values: [] }]
                                        handleUpdateVariantField(variant.skuId, "attributes", newAttributes)
                                      }}
                                    >
                                      + 属性を追加
                                    </Button>
                                  </div>
                                ) : (
                                  <div className="flex items-center gap-2">
                                    <p className="text-xs text-muted-foreground">属性がありません</p>
                                    <Button
                                      variant="outline"
                                      size="sm"
                                      className="h-7 px-2 text-xs"
                                      onClick={() => {
                                        handleUpdateVariantField(variant.skuId, "attributes", [{ name: "", values: [] }])
                                      }}
                                    >
                                      + 属性を追加
                                    </Button>
                                  </div>
                                )}
                              </div>
                            </div>
                          </Card>
                        ))}
                      </div>
                    </div>
                  )}
                </>
              )
            })()}
          </div>
          
          <DialogFooter className="gap-2 sm:gap-0">
            <Button
              variant="outline"
              onClick={() => setSkuModalOpen(false)}
              disabled={isSavingSku}
            >
              キャンセル
            </Button>
            <Button
              variant="default"
              onClick={handleSaveSkuData}
              disabled={isSavingSku || !editingSkuData}
              className="gap-2"
            >
              {isSavingSku ? (
                <>
                  <Loader2 className="h-4 w-4 animate-spin" />
                  保存中...
                </>
              ) : (
                <>
                  <Save className="h-4 w-4" />
                  変更を保存
                </>
              )}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
    </ErrorBoundary>
  )
}
