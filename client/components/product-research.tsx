"use client"

import { useState, useEffect, useRef } from "react"
import { Card } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Badge } from "@/components/ui/badge"
import {
  Search,
  TrendingUp,
  Star,
  AlertCircle,
  CheckCircle2,
  ExternalLink,
  Plus,
  Download,
  Package,
  CheckSquare,
  Square,
  Loader2,
  RefreshCw,
  Trash2,
} from "lucide-react"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { RakumartImportModal } from "@/components/rakumart-import-modal"
import { apiService, CategoryRecord } from "@/lib/api-service"
import { useToast } from "@/hooks/use-toast"
import { calculateActualPurchasePrice, formatPurchasePrice, PurchasePriceSettings, getWeightKgFromProduct, WeightRequiredError } from "@/lib/purchase-price-utils"
import { formatNumberJa } from "@/lib/locale"
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
// use native img for external Rakumart/1688 domains
// removed ApiTestComponent from research page

const researchResults = [
  {
    id: 1,
    name: "ワイヤレスイヤホン Bluetooth5.3 ノイズキャンセリング",
    image: "/wireless-earbuds.png",
    price: 1580,
    sales: 125000,
    ratingScore: 4.8,
    repurchaseRate: 85,
    descriptionHtml: "<p>高品質なBluetoothイヤホン</p>",
    actualPurchasePrice: 727,
    compliance: "pass",
    source: "rakumart",
    category: "electronics",
  },
  {
    id: 2,
    name: "スマートウォッチ 健康管理 心拍数測定",
    image: "/modern-smartwatch.png",
    price: 2340,
    sales: 98000,
    ratingScore: 4.6,
    repurchaseRate: 92,
    descriptionHtml: "<p>多機能スマートウォッチ</p>",
    actualPurchasePrice: 850,
    compliance: "pass",
    source: "rakumart",
    category: "electronics",
  },
  {
    id: 3,
    name: "USB充電ケーブル 3本セット 急速充電",
    image: "/usb-cable.png",
    price: 680,
    sales: 156000,
    ratingScore: 4.9,
    repurchaseRate: 78,
    descriptionHtml: "<p>高速充電対応ケーブル</p>",
    actualPurchasePrice: 450,
    compliance: "pass",
    source: "rakumart",
    category: "accessories",
  },
  {
    id: 4,
    name: "LEDデスクライト 調光機能付き USB給電",
    image: "/modern-desk-lamp.png",
    price: 1890,
    sales: 67000,
    ratingScore: 4.7,
    repurchaseRate: 88,
    descriptionHtml: "<p>Qi対応ワイヤレス充電器</p>",
    actualPurchasePrice: 680,
    compliance: "warning",
    source: "rakumart",
    category: "home",
  },
  {
    id: 5,
    name: "モバイルバッテリー 大容量 20000mAh",
    image: "/portable-power-bank.png",
    price: 2100,
    sales: 89000,
    ratingScore: 4.5,
    repurchaseRate: 82,
    descriptionHtml: "<p>高音質Bluetoothスピーカー</p>",
    actualPurchasePrice: 750,
    compliance: "warning",
    source: "rakumart",
    category: "electronics",
  },
]

export function ProductResearch() {
  const [searchQuery, setSearchQuery] = useState("")
  const [selectedProducts, setSelectedProducts] = useState<any[]>([])
  const [selectedCategory, setSelectedCategory] = useState<string>("all")
  const [isImportModalOpen, setIsImportModalOpen] = useState(false)
  const [products, setProducts] = useState<any[]>([])
  const [isLoading, setIsLoading] = useState(false)
  const [isRefreshing, setIsRefreshing] = useState(false)
  const [sortKey, setSortKey] = useState<'none' | 'price' | 'repurchaseRate' | 'sales'>("none")
  const [sortOrder, setSortOrder] = useState<'asc' | 'desc'>("desc")
  const [deleteConfirmOpen, setDeleteConfirmOpen] = useState(false)
  const [batchDeleteConfirmOpen, setBatchDeleteConfirmOpen] = useState(false)
  const [productToDelete, setProductToDelete] = useState<{ id: string; name: string } | null>(null)
  const [detailOpen, setDetailOpen] = useState(false)
  const [detailProduct, setDetailProduct] = useState<any | null>(null)
  const [purchasePriceSettings, setPurchasePriceSettings] = useState<PurchasePriceSettings>({
    exchangeRate: 20.0,
    domesticShippingCost: 300,
    domesticShippingCosts: {
      regular: 300,
      size60: 500,
      size80: 700,
      size100: 1000,
    },
    internationalShippingRate: 17,
    customsDutyRate: 100,
    profitMarginPercent: 5,
    salesCommissionPercent: 10,
  })
  const { toast } = useToast()
  const [registering, setRegistering] = useState(false)
  const [registeringCount, setRegisteringCount] = useState(0) // 残り登録商品数
  const [registeringProducts, setRegisteringProducts] = useState<Set<string>>(new Set())
  const [updating, setUpdating] = useState(false)
  const [updatingCount, setUpdatingCount] = useState(0) // 残り更新商品数
  const [categories, setCategories] = useState<CategoryRecord[]>([])
  const [primaryCategories, setPrimaryCategories] = useState<Array<{id: number, category_name: string, default_category_ids: string[]}>>([])
  const weightErrorToastShownRef = useRef(false)

  // Load products and settings from database on component mount
  useEffect(() => {
    loadProductsFromDatabase()
    loadPurchasePriceSettings()
    loadCategories()
  }, [])

  const loadProductsFromDatabase = async () => {
    setIsLoading(true)
    try {
      const pageSize = 500
      let offset = 0
      const all: any[] = []
      while (true) {
        const resp = await apiService.getProductsFromDatabase(pageSize, offset)
        if (resp.success && Array.isArray(resp.data)) {
          all.push(...resp.data)
          if (resp.data.length < pageSize) break
          offset += pageSize
        } else {
          break
        }
      }
      // Always update products list, even if empty, to reflect current database state
        setProducts(all)
    } catch (error) {
      console.error("Error loading products:", error)
      toast({
        title: "エラー",
        description: "商品データの取得に失敗しました",
        variant: "destructive",
      })
    } finally {
      setIsLoading(false)
    }
  }

  const loadPurchasePriceSettings = async () => {
    try {
      const response = await apiService.getSettings()
      if (response.success && response.settings) {
        const pricingSettings = response.settings as any
        const shippingCosts = pricingSettings.domestic_shipping_costs ?? {}
        setPurchasePriceSettings(prev => {
          const parseNumber = (value: any, fallback: number): number => {
            const parsed = Number(value)
            return Number.isFinite(parsed) ? parsed : fallback
          }

          const fallbackDomestic = parseNumber(
            pricingSettings.domestic_shipping_cost,
            prev.domesticShippingCosts?.regular ?? prev.domesticShippingCost ?? 300,
          )
          const regularCost = parseNumber(shippingCosts.regular, fallbackDomestic)
          const size60Cost = parseNumber(shippingCosts.size60, regularCost)
          const size80Cost = parseNumber(shippingCosts.size80, regularCost)
          const size100Cost = parseNumber(shippingCosts.size100, regularCost)

          return {
            exchangeRate: parseNumber(pricingSettings.exchange_rate, prev.exchangeRate),
            domesticShippingCost: regularCost,
            domesticShippingCosts: {
              regular: regularCost,
              size60: size60Cost,
              size80: size80Cost,
              size100: size100Cost,
            },
            internationalShippingRate: parseNumber(pricingSettings.international_shipping_rate, prev.internationalShippingRate),
            customsDutyRate: parseNumber(pricingSettings.customs_duty_rate, prev.customsDutyRate),
            profitMarginPercent: parseNumber(pricingSettings.profit_margin_percent, prev.profitMarginPercent),
            salesCommissionPercent: parseNumber(pricingSettings.sales_commission_percent, prev.salesCommissionPercent),
          }
        })
      }
    } catch (error) {
      console.error('Failed to load purchase price settings:', error)
    }
  }

  const loadCategories = async () => {
    try {
      const response = await apiService.getPrimaryCategories()
      if (response.success && response.categories) {
        setPrimaryCategories(response.categories.map((cat: any) => ({
          id: cat.id,
          category_name: cat.category_name,
          default_category_ids: cat.default_category_ids || []
        })))
      }
    } catch (error) {
      console.error("Failed to load primary categories:", error)
    }
  }

  const refreshProducts = async () => {
    setIsRefreshing(true)
    await loadProductsFromDatabase()
    setIsRefreshing(false)
  }

  const handleProductsLoaded = (newProducts: any[]) => {
    // Add new products to existing list
    setProducts(prev => [...newProducts, ...prev])
    toast({
      title: "成功",
      description: `${newProducts.length}件の新しい商品が追加されました`,
    })
  }

  const openDetail = (p: any) => {
    try {
      const raw = p?.originalData?.detail_json ?? p?.originalData?.detailNormalized
      const parsed = typeof raw === 'string' ? JSON.parse(raw) : raw
      const extractKV = (items: any[]): { key: string; value: string }[] => {
        if (!Array.isArray(items)) return []
        return items
          .map((it) => ({ key: it?.keyT ?? it?.keyt ?? '', value: it?.valueT ?? it?.valuet ?? '' }))
          .filter((kv) => typeof kv.key === 'string' && kv.key.trim() !== '' && typeof kv.value === 'string' && kv.value.trim() !== '')
      }

      const extractInventory = (items: any[]): { key: string; rows: { price?: string | number; amountOnSale?: number; startQuantity?: number; skuId?: any }[] }[] => {
        if (!Array.isArray(items)) return []
        return items
          .filter((it) => typeof (it?.keyT ?? '') === 'string' && (Array.isArray(it?.valueT) || typeof it?.valueT === 'object'))
          .map((it) => {
            const v = Array.isArray(it.valueT) ? it.valueT : (it.valueT ? [it.valueT] : [])
            const rows = v.map((x: any) => ({
              price: x?.price ?? x?.priceT ?? x?.priceValue ?? x?.priceRmb ?? x?.priceJpy,
              amountOnSale: x?.amountOnSale,
              startQuantity: x?.startQuantity,
              skuId: x?.skuId,
            }))
            return { key: it.keyT, rows }
          })
      }

      const goodsInfo = parsed?.goodsInfo ?? parsed ?? {}
      const fromUrl = goodsInfo?.fromUrl || goodsInfo?.sourceUrl || parsed?.fromUrl || parsed?.sourceUrl || ''
      const extracted = {
        titleT: goodsInfo?.titleT ?? parsed?.titleT ?? '',
        shopname: goodsInfo?.shopName ?? goodsInfo?.shopname ?? '',
        address: goodsInfo?.address ?? goodsInfo?.shopAddress ?? '',
        unit: goodsInfo?.unit ?? goodsInfo?.goodsUnit ?? '',
        pricerangetype: goodsInfo?.priceRangeType ?? goodsInfo?.pricerangeType ?? goodsInfo?.pricerangetype ?? goodsInfo?.priceRangesType ?? '',
        minorquantity: goodsInfo?.minorQuantity ?? goodsInfo?.minOrderQuantity ?? goodsInfo?.minorquantity ?? '',
        priceRange: goodsInfo?.priceRange ?? goodsInfo?.priceRangeList ?? goodsInfo?.priceRanges ?? [],
        detail: extractKV(parsed?.detail ?? goodsInfo?.detail ?? []),
        specification: extractKV(parsed?.specification ?? parsed?.specifications ?? goodsInfo?.specification ?? []),
        goodsInventory: extractInventory(goodsInfo?.goodsInventory ?? []),
        fromUrl,
      }
      setDetailProduct({
        id: p.id,
        name: p.name,
        detail: parsed,
        extracted,
        fallback: p.originalData,
      })
      setDetailOpen(true)
    } catch (e) {
      setDetailProduct({ id: p.id, name: p.name, detail: null, extracted: null, fallback: p.originalData })
      setDetailOpen(true)
    }
  }

  const handleDeleteClick = (productId: string, productName: string) => {
    setProductToDelete({ id: productId, name: productName })
    setDeleteConfirmOpen(true)
  }

  const handleConfirmDelete = async () => {
    if (!productToDelete) return
    
    try {
      const response = await apiService.deleteProduct(productToDelete.id)
      
      if (response.success) {
        // Remove product from local state
        setProducts(prev => prev.filter(p => p.product_id !== productToDelete.id && p.goods_id !== productToDelete.id))
        
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
      console.error("Error deleting product:", error)
      toast({
        title: "エラー",
        description: "商品の削除に失敗しました",
        variant: "destructive",
      })
    } finally {
      setDeleteConfirmOpen(false)
      setProductToDelete(null)
    }
  }

  const handleDeleteSelectedProducts = () => {
    
    if (selectedProducts.length === 0) {
      toast({
        title: "エラー",
        description: "削除する商品を選択してください",
        variant: "destructive",
      })
      return
    }
    
    // Open confirmation dialog
    setBatchDeleteConfirmOpen(true)
  }

  const handleConfirmBatchDelete = async () => {
    if (selectedProducts.length === 0) {
      setBatchDeleteConfirmOpen(false)
      return
    }

    try {
      // Get product IDs from the transformed products
      const productIds = filteredResults
        .filter(p => selectedProducts.includes(p.id))
        .map(p => p.originalData?.product_id || p.originalData?.goods_id)
        .filter(id => id !== undefined) as string[]
      
      if (productIds.length === 0) {
        toast({
          title: "エラー",
          description: "有効な商品IDが見つかりません",
          variant: "destructive",
        })
        return
      }

      const response = await apiService.deleteProductsBatch(productIds)
      
      if (response.success) {
        // Remove products from local state
        const deletedIds = productIds
        setProducts(prev => prev.filter(p => !deletedIds.includes(p.product_id) && !deletedIds.includes(p.goods_id)))
        
        // Clear selected products
        setSelectedProducts([])
        
        toast({
          title: "成功",
          description: `${response.deleted_count || productIds.length}件の商品を削除しました`,
        })
      } else {
        toast({
          title: "エラー",
          description: response.error || "商品の削除に失敗しました",
          variant: "destructive",
        })
      }
    } catch (error) {
      console.error("Error deleting products:", error)
      toast({
        title: "エラー",
        description: "商品の削除に失敗しました",
        variant: "destructive",
      })
    } finally {
      setBatchDeleteConfirmOpen(false)
    }
  }

  // Transform database products to match the expected format
  const transformedProducts = products.map((product, index) => {
    // Create unique key using multiple fields to ensure uniqueness
    const uniqueKey = product.product_id || product.goods_id || 
      `${product.image_url || 'img'}-${index}` ||
      `product-${Date.now()}-${Math.random()}-${index}`
    
    let detailJson: any = null
    if (product.detail_json) {
      if (typeof product.detail_json === 'string') {
        try {
          detailJson = JSON.parse(product.detail_json)
        } catch (err) {
          console.warn('Failed to parse detail_json', err)
        }
      } else if (typeof product.detail_json === 'object') {
        detailJson = product.detail_json
      }
    }
    
    const jpyPrice = typeof product.jpy_price === 'number' ? product.jpy_price
      : (product.jpy_price ? parseFloat(product.jpy_price) : undefined)
    const rmbPrice = typeof product.wholesale_price === 'number' ? product.wholesale_price
      : (product.wholesale_price ? parseFloat(product.wholesale_price) : undefined)
    // Get weightKg from product_origin table's weight field
    const productWeightKg = getWeightKgFromProduct(product)
    const productWeight = productWeightKg ?? 0
    const productSizeValue = (() => {
      if (product?.size === null || product?.size === undefined) {
        return null
      }
      if (typeof product.size === "number") {
        return Number.isFinite(product.size) ? product.size : null
      }
      const parsed = parseFloat(String(product.size).trim())
      return Number.isFinite(parsed) ? parsed : null
    })()

    // Calculate purchase price - will throw error if weight is missing
    let actualPurchasePrice: number | null = null
    let purchasePriceError = false
    
    try {
      actualPurchasePrice = calculateActualPurchasePrice(
        rmbPrice || (jpyPrice ? jpyPrice / purchasePriceSettings.exchangeRate : 0),
        productWeightKg,
        purchasePriceSettings,
        productSizeValue,
      )
    } catch (error) {
      if (error instanceof WeightRequiredError) {
        purchasePriceError = true
        // Don't show toast here - we'll handle it in useEffect after render
      } else {
        // Re-throw other errors
        throw error
      }
    }

    const basePrice = jpyPrice ?? (rmbPrice ? rmbPrice * purchasePriceSettings.exchangeRate : 0)

    // Extract image URL - check multiple possible field names and sources
    // Priority: enriched detailImages > imgUrl > image_2 > image_url > detailImages array
    let productImage = "/placeholder.svg"
    // Prefer first image from detail_json.images (DB source of truth)
    if (detailJson?.images && Array.isArray(detailJson.images) && detailJson.images.length > 0) {
      productImage = detailJson.images[0] || "/placeholder.svg"
    } else if (product.detailImages && Array.isArray(product.detailImages) && product.detailImages.length > 0) {
      // Use first image from enriched detail images
      productImage = product.detailImages[0] || "/placeholder.svg"
    } else if (product.imgUrl) {
      // Use imgUrl from API response
      productImage = product.imgUrl
    } else if (product.image_url) {
      // Use image_url (alternative field name)
      productImage = product.image_url
    } else if (product.detailNormalized?.images && Array.isArray(product.detailNormalized.images) && product.detailNormalized.images.length > 0) {
      // Use first image from normalized detail data
      productImage = product.detailNormalized.images[0] || "/placeholder.svg"
    } else if (product.images && Array.isArray(product.images) && product.images.length > 0) {
      // Use first image from images array
      productImage = typeof product.images[0] === 'string' ? product.images[0] : (product.images[0]?.url || product.images[0]?.location || "/placeholder.svg")
    }
    
    // Extract description - check multiple possible field names and sources
    let productDescription = ""
    if (product.detailDescription) {
      productDescription = product.detailDescription
    } else if (product.product_description) {
      productDescription = product.product_description
    } else if (product.detailNormalized?.description) {
      productDescription = product.detailNormalized.description
    } else if (product.description) {
      productDescription = product.description
    }
    
    return {
      id: uniqueKey,
      name: product.product_name || product.title_c || product.titleT || product.titleC || "商品名なし",
      image: productImage,
      price: basePrice,
      rmbPrice: rmbPrice ?? (jpyPrice && purchasePriceSettings.exchangeRate ? jpyPrice / purchasePriceSettings.exchangeRate : undefined),
      sales: parseInt(product.monthly_sales || product.month_sold || product.monthSold || "0"),
      ratingScore: typeof product.rating_score === 'number' ? product.rating_score : parseFloat(product.rating_score || product.rating || product.tradeScore || "0"),
      repurchaseRate: typeof product.repurchase_rate === 'number' ? product.repurchase_rate : parseFloat(product.repurchase_rate || product.repurchaseRate || product.rePurchaseRate || "0"),
      descriptionHtml: productDescription,
      actualPurchasePrice: actualPurchasePrice ?? 0,
      purchasePriceError: purchasePriceError,
      compliance: product.compliance_status === "pass" ? "pass" : "warning",
      source: "rakumart",
      category: product.category || "electronics",
      main_category: product.main_category || null,
      registrationStatus: (() => {
        const status = product.registration_status;
        // Handle numeric values: 1=unregistered, 2=registered, 3=previously_registered
        if (typeof status === 'number') {
          return status === 2 ? 'registered' : status === 3 ? 'previously_registered' : 'unregistered';
        }
        // Handle string values (backward compatibility)
        if (typeof status === 'string') {
          return status === 'registered' ? 'registered' : status === 'previously_registered' ? 'previously_registered' : 'unregistered';
        }
        return 'unregistered';
      })(),
      originalData: product, // Keep original data for reference
    }
  })

  // Show toast for products with missing weight after products change
  useEffect(() => {
    // Reset ref when products change
    weightErrorToastShownRef.current = false
    
    // Check if any products have missing weight
    const hasMissingWeight = products.some((product) => {
      const productWeightKg = getWeightKgFromProduct(product)
      return productWeightKg === null || productWeightKg === undefined
    })
    
    if (hasMissingWeight && !weightErrorToastShownRef.current) {
      weightErrorToastShownRef.current = true
      toast({
        title: "エラー",
        description: "重量が指定されていない商品があります",
        variant: "destructive",
      })
    }
  }, [products, toast])

  const filteredResults = transformedProducts.filter((product) => {
    if (selectedCategory !== "all") {
      // Find the selected primary category
      const selectedPrimaryCategory = primaryCategories.find(cat => cat.id.toString() === selectedCategory)
      if (selectedPrimaryCategory && selectedPrimaryCategory.default_category_ids.length > 0) {
        // Check if product's main_category is in the default_category_ids
        const productMainCategory = product.main_category
        if (!productMainCategory || !selectedPrimaryCategory.default_category_ids.includes(productMainCategory)) {
          return false
        }
      } else {
        // If no default_category_ids, don't show any products
        return false
      }
    }
    
    // If search query is empty, show all products
    if (!searchQuery.trim()) {
      return true
    }
    
    const queryLower = searchQuery.toLowerCase().trim()
    
    // Search by product name
    const nameMatch = product.name.toLowerCase().includes(queryLower)
    
    // Search by management number (product_id)
    const productId = product.originalData?.product_id
    const managementNumberMatch = productId && productId.toString().toLowerCase().includes(queryLower)
    
    return nameMatch || managementNumberMatch
  })

  // Pagination: 100 items per page
  const PAGE_SIZE = 100
  const [currentPage, setCurrentPage] = useState(1)

  // Reset to first page when filters/search/sort change or results shrink
  useEffect(() => {
    setCurrentPage(1)
  }, [searchQuery, selectedCategory, products, sortKey, sortOrder])

  // Sort across the full result set BEFORE pagination
  const sortedResults = (() => {
    if (sortKey === 'none') return filteredResults
    const dir = sortOrder === 'asc' ? 1 : -1
    return [...filteredResults].sort((a: any, b: any) => {
      const av = (sortKey === 'price') ? (a.price ?? 0)
        : (sortKey === 'repurchaseRate') ? (a.repurchaseRate ?? 0)
        : (a.sales ?? 0)
      const bv = (sortKey === 'price') ? (b.price ?? 0)
        : (sortKey === 'repurchaseRate') ? (b.repurchaseRate ?? 0)
        : (b.sales ?? 0)
      if (av === bv) return 0
      return av > bv ? dir : -dir
    })
  })()

  const totalItems = sortedResults.length
  const totalPages = Math.max(1, Math.ceil(totalItems / PAGE_SIZE))
  const startIndex = (currentPage - 1) * PAGE_SIZE
  const endIndex = startIndex + PAGE_SIZE
  const pagedResults = sortedResults.slice(startIndex, endIndex)

  const toggleProduct = (id: any) => {
    setSelectedProducts((prev) => (prev.includes(id) ? prev.filter((p) => p !== id) : [...prev, id]))
  }

  const selectAllProducts = () => {
    const pageIds = pagedResults.map((p) => p.id)
    setSelectedProducts((prev) => {
      const set = new Set(prev)
      pageIds.forEach((id) => set.add(id))
      return Array.from(set)
    })
  }

  const deselectAllProducts = () => {
    const pageIdSet = new Set(pagedResults.map((p) => p.id))
    setSelectedProducts((prev) => prev.filter((id) => !pageIdSet.has(id)))
  }

  const isAllSelected = pagedResults.length > 0 && pagedResults.every((p) => selectedProducts.includes(p.id))

  return (
    <div className="space-y-5 md:space-y-6 bg-white">
      {/* Header */}
      <div className="flex flex-col gap-3 md:flex-row md:items-center md:justify-between">
        <div>
          <h1 className="text-2xl sm:text-3xl font-bold text-foreground">商品リサーチ</h1>
          <p className="text-muted-foreground mt-1.5 text-sm sm:text-base">
            ラクマートから売れ筋商品を自動検索
            {products.length > 0 && (
              <span className="ml-2 text-primary font-semibold">
                ({products.length}件を取得)
              </span>
            )}
          </p>
        </div>
        <div className="flex flex-col sm:flex-row items-stretch sm:items-center gap-2.5">
          <Button
            variant="outline"
            size="sm"
            onClick={refreshProducts}
            disabled={isRefreshing}
            className="gap-2 bg-white border-border hover:bg-muted transition-colors"
          >
            {isRefreshing ? (
              <Loader2 className="h-4 w-4 animate-spin" />
            ) : (
              <RefreshCw className="h-4 w-4" />
            )}
            <span className="text-sm">更新</span>
          </Button>
          <Button
            variant="outline"
            className="gap-2 flex-1 sm:flex-initial bg-white border-border hover:bg-muted hover:border-primary/50 transition-all shadow-sm"
            onClick={() => setIsImportModalOpen(true)}
          >
            <Package className="h-4 w-4 flex-shrink-0" />
            <span className="text-sm sm:inline">商品を呼び出す</span>
          </Button>
          <Button variant="outline" className="gap-2 bg-white border-border hover:bg-muted transition-all shadow-sm flex-shrink-0">
            <Download className="h-4 w-4" />
            <span className="hidden lg:inline text-sm">エクスポート</span>
          </Button>
          {/* Bulk Update Button - Only show when registered products are selected */}
          {(() => {
            const selectedProductData = selectedProducts
              .map((id) => transformedProducts.find((x) => x.id === id))
              .filter(Boolean) as any[]
            const registeredProducts = selectedProductData.filter(
              (p) => p?.registrationStatus === 'registered'
            )
            const hasRegisteredProducts = registeredProducts.length > 0

            if (!hasRegisteredProducts) {
              return null
            }

            return (
              <Button
                className="gap-2 flex-1 sm:flex-none bg-emerald-600 hover:bg-emerald-700 transition-all shadow-md disabled:opacity-50"
                disabled={registeredProducts.length === 0 || updating || registering}
                onClick={async () => {
                  try {
                    if (registeredProducts.length === 0) {
                      toast({ title: "エラー", description: "更新対象の登録済み商品がありません", variant: "destructive" })
                      return
                    }

                    const ids = registeredProducts
                      .map((p) => p?.originalData?.product_id || p?.originalData?.goods_id)
                      .filter(Boolean) as string[]

                    if (ids.length === 0) {
                      toast({ title: "エラー", description: "有効な商品IDが見つかりません", variant: "destructive" })
                      return
                    }

                    setUpdating(true)
                    setUpdatingCount(ids.length)

                    let successCount = 0
                    let failCount = 0

                    for (let i = 0; i < ids.length; i++) {
                      const productId = ids[i]
                      try {
                        const resp = await apiService.registerProductsToManagement([productId])
                        if (resp.success) {
                          successCount++
                        } else {
                          failCount++
                          console.error(`Failed to update product ${productId}: ${resp.error}`)
                        }
                      } catch (e: any) {
                        failCount++
                        console.error(`Error updating product ${productId}:`, e)
                      }

                      const remaining = ids.length - (i + 1)
                      setUpdatingCount(remaining)
                    }

                    await loadProductsFromDatabase()

                    if (successCount > 0) {
                      toast({
                        title: "更新完了",
                        description: `${successCount}件の商品を更新しました${failCount > 0 ? `（${failCount}件失敗）` : ''}`
                      })
                    } else if (failCount > 0) {
                      toast({
                        title: "エラー",
                        description: "すべての商品の更新に失敗しました（rakuten_registration_statusが条件を満たしていない可能性があります）",
                        variant: "destructive"
                      })
                    } else {
                      toast({
                        title: "スキップ",
                        description: "すべての商品がスキップされました（rakuten_registration_statusが条件を満たしていません）",
                        variant: "destructive"
                      })
                    }
                  } catch (e: any) {
                    toast({ title: "エラー", description: e?.message || "更新に失敗しました", variant: "destructive" })
                  } finally {
                    setUpdating(false)
                    setUpdatingCount(0)
                  }
                }}
              >
                <RefreshCw className={`h-4 w-4 flex-shrink-0 ${updating ? 'animate-spin' : ''}`} />
                <span className="text-sm">
                  {updating ? (
                    <>
                      更新中
                      {updatingCount > 0 && <span className="ml-1">({updatingCount})</span>}
                    </>
                  ) : (
                    '選択商品を更新'
                  )}
                </span>
                {!updating && registeredProducts.length > 0 && (
                  <span className="ml-1 text-xs font-medium">({registeredProducts.length})</span>
                )}
              </Button>
            )
          })()}
          {(() => {
            const selectedProductData = selectedProducts
              .map((id) => transformedProducts.find((x) => x.id === id))
              .filter(Boolean) as any[]
            const registeredProducts = selectedProductData.filter(
              (p) => p?.registrationStatus === 'registered'
            )
            const hasRegisteredProducts = registeredProducts.length > 0

            if (!hasRegisteredProducts) {
              return null
            }

            return (
              <Button
                className="gap-2 flex-1 sm:flex-none bg-indigo-600 hover:bg-indigo-700 transition-all shadow-md disabled:opacity-50"
                disabled={registeredProducts.length === 0 || updating || registering}
                onClick={async () => {
                  try {
                    const ids = registeredProducts
                      .map((p) => p?.originalData?.product_id || p?.originalData?.goods_id)
                      .filter(Boolean) as string[]

                    if (ids.length === 0) {
                      toast({ title: "エラー", description: "有効な商品IDが見つかりません", variant: "destructive" })
                      return
                    }

                    setUpdating(true)
                    setUpdatingCount(ids.length)

                    let successCount = 0
                    let failCount = 0

                    for (let i = 0; i < ids.length; i++) {
                      const productId = ids[i]
                      try {
                        const resp = await apiService.updateVariantsOnly([productId])
                        if (resp.success) {
                          successCount++
                        } else {
                          failCount++
                          console.error(`Failed to update variants for product ${productId}: ${resp.error}`)
                        }
                      } catch (e: any) {
                        failCount++
                        console.error(`Error updating variants for product ${productId}:`, e)
                      }
                      const remaining = ids.length - (i + 1)
                      setUpdatingCount(remaining)
                    }

                    await loadProductsFromDatabase()

                    if (successCount > 0) {
                    toast({
                      title: "更新完了",
                      description: `商品情報を更新しました: 成功 ${successCount}件${failCount > 0 ? ` / 失敗 ${failCount}件` : ''}`,
                    })
                    } else if (failCount > 0) {
                      toast({
                        title: "エラー",
                        description: "すべての商品の更新に失敗しました（rakuten_registration_statusが条件を満たしていない可能性があります）",
                        variant: "destructive"
                      })
                    } else {
                      toast({
                        title: "スキップ",
                        description: "すべての商品がスキップされました（rakuten_registration_statusが条件を満たしていません）",
                        variant: "destructive"
                      })
                    }
                  } catch (e: any) {
                    toast({ title: "エラー", description: e?.message || "更新に失敗しました", variant: "destructive" })
                  } finally {
                    setUpdating(false)
                    setUpdatingCount(0)
                  }
                }}
              >
                <RefreshCw className={`h-4 w-4 flex-shrink-0 ${updating ? 'animate-spin' : ''}`} />
                <span className="text-sm">属性情報更新</span>
                {!updating && registeredProducts.length > 0 && (
                  <span className="ml-1 text-xs font-medium">({registeredProducts.length})</span>
                )}
              </Button>
            )
          })()}
          <Button
            className="gap-2 flex-1 sm:flex-none bg-primary hover:bg-primary/90 transition-all shadow-md disabled:opacity-50"
            disabled={selectedProducts.length === 0 || registering || updating}
            onClick={async () => {
              try {
                // Get selected products with their transformed data
                const selectedProductData = selectedProducts
                  .map((id) => {
                    const p = transformedProducts.find((x) => x.id === id)
                    return p
                  })
                  .filter(Boolean) as any[]
                
                if (selectedProductData.length === 0) {
                  toast({ title: "エラー", description: "選択された商品が見つかりません", variant: "destructive" })
                  return
                }
                
                // Validate: Check for missing weight or invalid price
                const invalidProducts: { name: string; reason: string }[] = []
                
                for (const product of selectedProductData) {
                  const productWeightKg = getWeightKgFromProduct(product.originalData)
                  const hasWeight = productWeightKg !== null && productWeightKg !== undefined && productWeightKg > 0
                  const hasPrice = product.actualPurchasePrice !== null && 
                                  product.actualPurchasePrice !== undefined && 
                                  product.actualPurchasePrice > 0
                  
                  if (!hasWeight) {
                    invalidProducts.push({
                      name: product.name,
                      reason: "重量が指定されていません"
                    })
                  } else if (!hasPrice || product.purchasePriceError) {
                    invalidProducts.push({
                      name: product.name,
                      reason: "購入価格が0または計算できません（重量が必要です）"
                    })
                  }
                }
                
                // If there are invalid products, show error and prevent registration
                if (invalidProducts.length > 0) {
                  const productNames = invalidProducts.map(p => p.name).join(", ")
                  const reasons = invalidProducts.map(p => p.reason).filter((v, i, a) => a.indexOf(v) === i).join("、")
                  
                  toast({ 
                    title: "登録エラー", 
                    description: `${invalidProducts.length}件の商品でエラーがあります: ${reasons}。商品: ${productNames.length > 50 ? productNames.substring(0, 50) + "..." : productNames}`,
                    variant: "destructive",
                    duration: 5000
                  })
                  return
                }
                
                // Get valid product IDs for registration
                const validProducts = selectedProductData.filter((p) => {
                  const productId = p?.originalData?.product_id || p?.originalData?.goods_id
                  return productId && !invalidProducts.find(inv => inv.name === p.name)
                })
                
                const ids = validProducts
                  .map((p) => p?.originalData?.product_id || p?.originalData?.goods_id)
                  .filter(Boolean) as string[]
                
                if (ids.length === 0) {
                  toast({ title: "エラー", description: "有効な商品IDが見つかりません", variant: "destructive" })
                  return
                }
                
                // Start registration process
                setRegistering(true)
                setRegisteringCount(ids.length)
                
                // Register products one by one to track progress
                let successCount = 0
                let failCount = 0
                
                for (let i = 0; i < ids.length; i++) {
                  const productId = ids[i]
                  try {
                    const resp = await apiService.registerProductsToManagement([productId])
                    if (resp.success) {
                      successCount++
                    } else {
                      failCount++
                      console.error(`Failed to register product ${productId}: ${resp.error}`)
                    }
                  } catch (e: any) {
                    failCount++
                    console.error(`Error registering product ${productId}:`, e)
                  }
                  
                  // Update remaining count (decrease by 1 for each completed product)
                  const remaining = ids.length - (i + 1)
                  setRegisteringCount(remaining)
                }
                
                // Refresh products to update registration status
                await loadProductsFromDatabase()
                
                // Show final result
                if (successCount > 0) {
                  toast({ 
                    title: "登録完了", 
                    description: `${successCount}件を商品管理に登録しました${failCount > 0 ? `（${failCount}件失敗）` : ''}` 
                  })
                } else {
                  toast({ 
                    title: "エラー", 
                    description: "すべての商品の登録に失敗しました", 
                    variant: "destructive" 
                  })
                }
              } catch (e: any) {
                toast({ title: "エラー", description: e?.message || "登録に失敗しました", variant: "destructive" })
              } finally {
                setRegistering(false)
                setRegisteringCount(0)
              }
            }}
          >
            <Plus className="h-4 w-4 flex-shrink-0" />
            <span className="text-sm">
              {registering ? (
                <>
                  登録中
                  {registeringCount > 0 && <span className="ml-1">({registeringCount})</span>}
                </>
              ) : (
                '資料基地に登録'
              )}
            </span>
            {!registering && selectedProducts.length > 0 && (
              <span className="ml-1 text-xs font-medium">({selectedProducts.length})</span>
            )}
          </Button>
        </div>
      </div>

      {/* API Test Component moved to dashboard */}

      {/* Search & Filters */}
      <Card className="p-4 sm:p-5 md:p-6 border-border bg-white shadow-sm">
        <div className="flex flex-col gap-4">
          <div className="flex flex-col lg:flex-row gap-3">
            <div className="relative flex-1">
              <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground pointer-events-none" />
              <Input
                placeholder="商品名、キーワードで検索..."
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                className="pl-10 bg-background border-border h-11 focus:ring-2 focus:ring-primary/20 transition-all"
              />
            </div>
            <Select value={selectedCategory} onValueChange={setSelectedCategory}>
              <SelectTrigger className="w-full lg:w-[220px] bg-background border-border h-11">
                <SelectValue placeholder="メインカテゴリーを選択" />
              </SelectTrigger>
              <SelectContent className="z-50 bg-white">
                <SelectItem value="all">すべてのメインカテゴリー</SelectItem>
                {primaryCategories.map((category) => (
                  <SelectItem key={category.id} value={category.id.toString()}>
                    {category.category_name}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
            {/* Sort Controls */}
            <Select value={sortKey} onValueChange={(v) => setSortKey(v as any)}>
              <SelectTrigger className="w-full lg:w-[220px] bg-background border-border h-11">
                <SelectValue placeholder="並び替え" />
              </SelectTrigger>
              <SelectContent className="z-50 bg-white">
                <SelectItem value="none">並び替えなし</SelectItem>
                <SelectItem value="price">価格</SelectItem>
                <SelectItem value="repurchaseRate">再購入率</SelectItem>
                <SelectItem value="sales">月間販売数</SelectItem>
              </SelectContent>
            </Select>
            <Select value={sortOrder} onValueChange={(v) => setSortOrder(v as any)}>
              <SelectTrigger className="w-full lg:w-[160px] bg-background border-border h-11">
                <SelectValue placeholder="順序" />
              </SelectTrigger>
              <SelectContent className="z-50 bg-white">
                <SelectItem value="desc">降順</SelectItem>
                <SelectItem value="asc">昇順</SelectItem>
              </SelectContent>
            </Select>
            <div className="flex gap-2">
              <Button className="gap-2 flex-1 sm:flex-none bg-primary hover:bg-primary/90">
                <Search className="h-4 w-4" />
                <span className="text-sm">検索</span>
              </Button>
            </div>
          </div>

          <div className="flex flex-wrap gap-2">
            <Badge variant="outline" className="gap-1 text-xs border-border">
              <TrendingUp className="h-3 w-3" />
              年間10万件以上
            </Badge>
            <Badge variant="outline" className="gap-1 text-xs border-border">
              <Star className="h-3 w-3" />
              評価4.5以上
            </Badge>
            <Badge variant="outline" className="gap-1 text-xs border-border">
              <CheckCircle2 className="h-3 w-3" />
              コンプライアンス適合
            </Badge>
          </div>
        </div>
      </Card>

      {/* Select All / Deselect All controls */}
      {filteredResults.length > 0 && (
        <div className="flex flex-col sm:flex-row items-stretch sm:items-center justify-between gap-3 p-4 bg-gradient-to-r from-primary/5 via-primary/3 to-transparent rounded-xl border border-primary/20 shadow-sm">
          <div className="flex items-center gap-3 flex-wrap">
            <Button
              variant="outline"
              size="sm"
              onClick={isAllSelected ? deselectAllProducts : selectAllProducts}
              className="gap-2 bg-white hover:bg-primary/10 border-primary/30 hover:border-primary/50 transition-all"
            >
              {isAllSelected ? (
                <div className="flex items-center gap-2">
                  <CheckSquare className="h-4 w-4" />
                  <span className="hidden sm:inline">すべて解除</span>
                </div>
              ) : (
                <div className="flex items-center gap-2">
                  <Square className="h-4 w-4" />
                  <span className="hidden sm:inline">すべて選択</span>
                </div>
              )}
            </Button>
            <span className="text-sm font-medium text-foreground">
              <span className="text-primary font-semibold">{selectedProducts.length}</span> / {filteredResults.length} 件選択中
            </span>
          </div>
          {selectedProducts.length > 0 && (
            <Button
              onClick={() => {
                handleDeleteSelectedProducts()
              }}
              className="gap-2 bg-red-600 hover:bg-red-700 text-white border-red-600 shadow-md shadow-red-500/20 transition-all"
              size="sm"
            >
              <Trash2 className="h-4 w-4" />
              <span className="hidden sm:inline">選択商品を削除</span>
              <span className="sm:hidden">削除</span>
              <span className="hidden sm:inline">({selectedProducts.length})</span>
            </Button>
          )}
        </div>
      )}

      {/* Research Results */}
      <div className="space-y-3">
        {isLoading ? (
          <div className="flex items-center justify-center py-12">
            <div className="flex flex-col items-center gap-4">
              <Loader2 className="h-8 w-8 animate-spin text-primary" />
              <p className="text-muted-foreground">商品データを読み込み中...</p>
            </div>
          </div>
        ) : filteredResults.length === 0 ? (
          <div className="flex items-center justify-center py-12">
            <div className="flex flex-col items-center gap-4">
              <Package className="h-12 w-12 text-muted-foreground" />
              <div className="text-center">
                <h3 className="text-lg font-semibold text-foreground">商品が見つかりません</h3>
                <p className="text-muted-foreground mt-2">
                  {products.length === 0 
                    ? "ラクマートから商品を検索してデータベースに保存してください"
                    : "検索条件に一致する商品がありません"
                  }
                </p>
              </div>
              {products.length === 0 && (
                <Button
                  onClick={() => setIsImportModalOpen(true)}
                  className="gap-2 bg-primary hover:bg-primary/90"
                >
                  <Package className="h-4 w-4" />
                  商品を検索する
                </Button>
              )}
            </div>
          </div>
        ) : (
          pagedResults.map((product) => (
          <Card
            key={product.id}
            className="p-4 sm:p-5 md:p-6 border-border bg-white hover:border-primary/50 hover:shadow-lg transition-all duration-200 group"
          >
            <div className="flex flex-col sm:flex-row gap-4 md:gap-6">
              {/* Product Image */}
              <div className="relative flex-shrink-0 self-start sm:self-center">
                <img
                  src={(product.image && typeof product.image === 'string') ? product.image : "/placeholder.svg"}
                  width={120}
                  height={120}
                  alt={product.name}
                  referrerPolicy="no-referrer"
                  loading="lazy"
                  className="h-28 w-28 sm:h-32 sm:w-32 rounded-xl object-cover bg-muted ring-2 ring-border group-hover:ring-primary/30 transition-all"
                  onError={(e) => {
                    const t = e.currentTarget as HTMLImageElement
                    if (t.src !== "/placeholder.svg") {
                      t.src = "/placeholder.svg"
                    }
                  }}
                />
                <input
                  type="checkbox"
                  checked={selectedProducts.includes(product.id)}
                  onChange={() => toggleProduct(product.id)}
                  className="absolute -top-2 -left-2 h-6 w-6 rounded-md border-2 border-primary bg-white hover:bg-primary/10 cursor-pointer transition-all accent-primary shadow-sm"
                />
              </div>

              {/* Product Info */}
              <div className="flex-1 space-y-3 min-w-0">
                <div className="flex items-start justify-between gap-2">
                  <div className="flex-1 min-w-0">
                    <h3 className="text-base md:text-lg font-semibold text-foreground leading-tight break-words">
                      {product.name}
                    </h3>
                    <div className="flex items-center gap-2 mt-2 flex-wrap">
                      {product.originalData?.product_id && (
                        <Badge variant="outline" className="text-xs border-muted-foreground/30 bg-muted/50 text-muted-foreground font-mono">
                          管理番号: {product.originalData.product_id}
                        </Badge>
                      )}
                      <Badge variant="outline" className="text-xs border-primary/30 bg-primary/5">
                        ラクマート
                      </Badge>
                      {product.compliance === "pass" ? (
                        <Badge className="bg-success/10 text-success border-success/20 text-xs">
                          <CheckCircle2 className="h-3 w-3 mr-1" />
                          適合
                        </Badge>
                      ) : (
                        <Badge className="bg-warning/10 text-warning border-warning/20 text-xs">
                          <AlertCircle className="h-3 w-3 mr-1" />
                          要確認
                        </Badge>
                      )}
                      {product.registrationStatus === 'registered' ? (
                        <Badge className="bg-blue-500/10 text-blue-600 border-blue-500/20 text-xs">
                          <CheckCircle2 className="h-3 w-3 mr-1" />
                          登録済み
                        </Badge>
                      ) : product.registrationStatus === 'previously_registered' ? (
                        <Badge className="bg-orange-500/10 text-orange-600 border-orange-500/20 text-xs">
                          <AlertCircle className="h-3 w-3 mr-1" />
                          以前登録済み
                        </Badge>
                      ) : (
                        <Badge variant="outline" className="text-xs border-gray-300 bg-gray-50 text-gray-600">
                          未登録
                        </Badge>
                      )}
                    </div>
                  </div>
                  <div className="flex items-center gap-2">
                    <Button
                      variant="ghost"
                      size="icon"
                      className="flex-shrink-0 hover:bg-muted"
                      onClick={() => {
                        const html = product.descriptionHtml || product.originalData?.product_description || ''
                        try {
                          const blob = new Blob([html], { type: 'text/html;charset=utf-8' })
                          const url = URL.createObjectURL(blob)
                          window.open(url, '_blank')
                        } catch {
                          // no-op
                        }
                      }}
                    >
                      <ExternalLink className="h-4 w-4" />
                    </Button>
                    <Button
                      variant="outline"
                      size="sm"
                      className="flex-shrink-0"
                      onClick={() => openDetail(product)}
                    >
                      詳細
                    </Button>
                    <Button
                      variant="ghost"
                      size="icon"
                      className="flex-shrink-0 hover:bg-destructive/10 hover:text-destructive"
                      onClick={() => handleDeleteClick(
                        product.originalData?.product_id || product.originalData?.goods_id || product.id,
                        product.name
                      )}
                    >
                      <Trash2 className="h-4 w-4" />
                    </Button>
                  </div>
                </div>

                {/* Metrics */}
                <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-5 gap-3">
                  <div className="space-y-1 p-2 rounded-lg bg-muted/50">
                    <p className="text-xs text-muted-foreground">仕入価格</p>
                    <p className="text-base md:text-lg font-bold text-foreground">
                      {typeof product.rmbPrice === 'number' ? `${product.rmbPrice.toLocaleString()} 元` : '—'}
                    </p>
                  </div>
                  <div className="space-y-1 p-2 rounded-lg bg-muted/50">
                    <p className="text-xs text-muted-foreground">月間販売数</p>
                    <p className="text-base md:text-lg font-bold text-foreground">{product.sales.toLocaleString()}</p>
                  </div>
                  <div className="space-y-1 p-2 rounded-lg bg-muted/50">
                    <p className="text-xs text-muted-foreground">評価スコア</p>
                    <div className="flex items-center gap-1">
                      <Star className="h-4 w-4 fill-warning text-warning" />
                      <p className="text-base md:text-lg font-bold text-foreground">{formatNumberJa(product.ratingScore ?? 0)}</p>
                    </div>
                  </div>
                  <div className="space-y-1 p-2 rounded-lg bg-muted/50">
                    <p className="text-xs text-muted-foreground">再購入率(%)</p>
                    <p className="text-base md:text-lg font-bold text-foreground">
                      {product.repurchaseRate != null ? product.repurchaseRate : 0}
                    </p>
                  </div>
                  <div className={`space-y-1 p-2 rounded-lg border ${
                    product.purchasePriceError 
                      ? "bg-destructive/10 border-destructive/20" 
                      : "bg-success/10 border-success/20"
                  }`}>
                    <p className="text-xs text-muted-foreground">楽天販売価格</p>
                    {product.purchasePriceError ? (
                      <div className="flex items-center gap-1">
                        <AlertCircle className="h-4 w-4 text-destructive" />
                        <p className="text-base md:text-lg font-bold text-destructive">重量なし</p>
                      </div>
                    ) : (
                      <p className="text-base md:text-lg font-bold text-success">{formatPurchasePrice(product.actualPurchasePrice)}</p>
                    )}
                  </div>
                </div>

                {/* Actions */}
                <div className="flex items-center gap-2">
                  {(() => {
                    const productId = product.originalData?.product_id || product.originalData?.goods_id
                    const isRegistered = product.registrationStatus === 'registered'
                    const isRegistering = productId && registeringProducts.has(productId)
                    // 登録済みでも「更新」できるようにする（画像は更新しない）
                    const isDisabled = !!isRegistering
                    
                    return (
                      <Button 
                        size="sm" 
                        className="gap-2 bg-primary hover:bg-primary/90 shadow-md shadow-primary/20 disabled:opacity-50 disabled:cursor-not-allowed"
                        disabled={isDisabled}
                        onClick={async () => {
                          if (!productId) {
                            toast({
                              title: "エラー",
                              description: "商品IDが見つかりません",
                              variant: "destructive",
                            })
                            return
                          }

                          // Set this product as registering
                          setRegisteringProducts(prev => new Set(prev).add(productId))

                          try {
                            // Validate weight and price
                            const productWeightKg = getWeightKgFromProduct(product.originalData)
                            const hasWeight = productWeightKg !== null && productWeightKg !== undefined && productWeightKg > 0
                            const hasPrice = product.actualPurchasePrice !== null && 
                                            product.actualPurchasePrice !== undefined && 
                                            product.actualPurchasePrice > 0

                            if (!hasWeight) {
                              toast({
                                title: "登録エラー",
                                description: "重量が指定されていません",
                                variant: "destructive",
                              })
                              return
                            }

                            if (!hasPrice || product.purchasePriceError) {
                              toast({
                                title: "登録エラー",
                                description: "購入価格が0または計算できません（重量が必要です）",
                                variant: "destructive",
                              })
                              return
                            }

                            const resp = await apiService.registerProductsToManagement([productId])
                            if (resp.success) {
                              toast({
                                title: "成功",
                                description: "商品を資料基地に登録しました",
                              })
                              // Refresh products to update registration status
                              await loadProductsFromDatabase()
                            } else {
                              toast({
                                title: "エラー",
                                description: resp.error || "登録に失敗しました",
                                variant: "destructive",
                              })
                            }
                          } catch (e: any) {
                            toast({
                              title: "エラー",
                              description: e?.message || "登録に失敗しました",
                              variant: "destructive",
                            })
                          } finally {
                            // Remove from registering set
                            setRegisteringProducts(prev => {
                              const next = new Set(prev)
                              next.delete(productId)
                              return next
                            })
                          }
                        }}
                      >
                        {isRegistering ? (
                          <>
                            <Loader2 className="h-4 w-4 animate-spin" />
                            登録中...
                          </>
                        ) : isRegistered ? (
                          <>
                            <CheckCircle2 className="h-4 w-4" />
                            更新
                            <span className="ml-1 inline-flex items-center rounded-full bg-emerald-50 px-2 py-0.5 text-[10px] font-semibold text-emerald-700 border border-emerald-100">
                              登録成功
                            </span>
                          </>
                        ) : (
                          <>
                            <Plus className="h-4 w-4" />
                            資料基地に登録
                          </>
                        )}
                      </Button>
                    )
                  })()}
                </div>
              </div>
            </div>
          </Card>
        ))
        )}
      </div>

      {/* Pagination */}
      <div className="flex flex-col sm:flex-row items-center justify-between gap-3 mt-2">
        <p className="text-sm text-muted-foreground">
          {totalItems}件中 {totalItems === 0 ? 0 : startIndex + 1} - {Math.min(endIndex, totalItems)} を表示
        </p>
        <div className="flex items-center gap-1">
          <Button
            variant="outline"
            size="sm"
            className="border-border bg-card"
            onClick={() => setCurrentPage((p) => Math.max(1, p - 1))}
            disabled={currentPage === 1}
          >
            前へ
          </Button>
          {
            (() => {
              const pages: number[] = []
              const windowSize = 5
              let start = Math.max(1, currentPage - 2)
              let end = Math.min(totalPages, start + windowSize - 1)
              if (end - start + 1 < windowSize) {
                start = Math.max(1, end - windowSize + 1)
              }
              for (let i = start; i <= end; i++) pages.push(i)
              return pages.map((page) => (
                <Button
                  key={page}
                  variant={page === currentPage ? undefined : "outline"}
                  size="sm"
                  className={page === currentPage ? "bg-primary" : "border-border bg-card"}
                  onClick={() => setCurrentPage(page)}
                >
                  {page}
                </Button>
              ))
            })()
          }
          <Button
            variant="outline"
            size="sm"
            className="border-border bg-card"
            onClick={() => setCurrentPage((p) => Math.min(totalPages, p + 1))}
            disabled={currentPage === totalPages}
          >
            次へ
          </Button>
        </div>
      </div>

      {/* Rakumart Import Modal */}
      <RakumartImportModal 
        open={isImportModalOpen} 
        onOpenChange={setIsImportModalOpen}
        onProductsLoaded={handleProductsLoaded}
      />

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
            <AlertDialogCancel>キャンセル</AlertDialogCancel>
            <AlertDialogAction
              onClick={handleConfirmDelete}
              className="bg-destructive hover:bg-destructive/90"
            >
              削除
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      {/* Batch Delete Confirmation Dialog */}
      <AlertDialog open={batchDeleteConfirmOpen} onOpenChange={setBatchDeleteConfirmOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>選択した商品を削除しますか？</AlertDialogTitle>
            <AlertDialogDescription>
              {selectedProducts.length}件の商品を削除してもよろしいですか？この操作は元に戻せません。
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>キャンセル</AlertDialogCancel>
            <AlertDialogAction
              onClick={handleConfirmBatchDelete}
              className="bg-destructive hover:bg-destructive/90"
            >
              削除する ({selectedProducts.length})
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      {/* Product Detail Dialog */}
      <AlertDialog open={detailOpen} onOpenChange={setDetailOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>商品詳細</AlertDialogTitle>
            <AlertDialogDescription>
              {detailProduct?.name || ''}
            </AlertDialogDescription>
          </AlertDialogHeader>
          <div className="max-h-[60vh] overflow-auto rounded border p-4 bg-white space-y-4">
            {detailProduct?.extracted ? (
              <>
                {(detailProduct.extracted.titleT || detailProduct.extracted.shopname) && (
                  <div className="space-y-1">
                    {detailProduct.extracted.titleT && (
                      <div className="text-sm leading-6"><span className="text-muted-foreground">商品名：</span>{detailProduct.extracted.titleT}</div>
                    )}
                    {detailProduct.extracted.shopname && (
                      <div className="text-sm leading-6"><span className="text-muted-foreground">店舗名：</span>{detailProduct.extracted.shopname}</div>
                    )}
                    {detailProduct.extracted.address && (
                      <div className="text-sm leading-6"><span className="text-muted-foreground">住所：</span>{detailProduct.extracted.address}</div>
                    )}
                  </div>
                )}

                <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
                  <div className="space-y-2">
                    {detailProduct.extracted.unit && (
                      <div className="text-sm leading-6"><span className="text-muted-foreground">単位：</span>{detailProduct.extracted.unit}</div>
                    )}
                    {detailProduct.extracted.pricerangetype && (
                      <div className="text-sm leading-6"><span className="text-muted-foreground">価格範囲タイプ：</span>{detailProduct.extracted.pricerangetype}</div>
                    )}
                    {detailProduct.extracted.minorquantity && (
                      <div className="text-sm leading-6"><span className="text-muted-foreground">最小注文数量：</span>{detailProduct.extracted.minorquantity}</div>
                    )}
                    {detailProduct.extracted.fromUrl && (
                      <div className="text-sm break-all leading-6">
                        <span className="text-muted-foreground">参照URL：</span>
                        <a href={detailProduct.extracted.fromUrl} target="_blank" rel="noreferrer" className="text-primary underline">
                          {detailProduct.extracted.fromUrl}
                        </a>
                      </div>
                    )}
                  </div>
                  {Array.isArray(detailProduct.extracted.priceRange) && detailProduct.extracted.priceRange.length > 0 && (
                    <div className="space-y-2">
                      <div className="text-sm font-medium">価格レンジ</div>
                      <div className="flex flex-col gap-1 text-xs">
                        {detailProduct.extracted.priceRange.map((pr: any, idx: number) => {
                          if (typeof pr === 'string') {
                            return <div key={idx} className="break-words rounded border bg-muted/30 px-2 py-1">{pr}</div>
                          }
                          const min = pr?.min || pr?.start || pr?.startQuantity || pr?.from || ''
                          const max = pr?.max || pr?.end || pr?.endQuantity || pr?.to || ''
                          const price = pr?.price || pr?.priceT || pr?.priceValue || pr?.priceRmb || pr?.priceJpy || ''
                          return (
                            <div key={idx} className="flex items-center gap-3 rounded border bg-muted/30 px-2 py-1">
                              <span className="text-muted-foreground">{min}-{max}</span>
                              <span className="font-medium">{price}</span>
                            </div>
                          )
                        })}
                      </div>
                    </div>
                  )}
                </div>

                {Array.isArray(detailProduct.extracted.detail) && detailProduct.extracted.detail.length > 0 && (
                  <div className="space-y-2">
                    <div className="text-sm font-medium">詳細</div>
                    <ul className="text-sm space-y-1">
                      {detailProduct.extracted.detail.map((kv: any, idx: number) => (
                        <li key={idx} className="break-words leading-6"><span className="text-muted-foreground">{kv.key}：</span>{kv.value}</li>
                      ))}
                    </ul>
                  </div>
                )}

                {Array.isArray(detailProduct.extracted.specification) && detailProduct.extracted.specification.length > 0 && (
                  <div className="space-y-2">
                    <div className="text-sm font-medium">仕様</div>
                    <ul className="text-sm space-y-1">
                      {detailProduct.extracted.specification.map((kv: any, idx: number) => (
                        <li key={idx} className="break-words leading-6"><span className="text-muted-foreground">{kv.key}：</span>{kv.value}</li>
                      ))}
                    </ul>
                  </div>
                )}

                {Array.isArray(detailProduct.extracted.goodsInventory) && detailProduct.extracted.goodsInventory.length > 0 && (
                  <div className="space-y-2">
                    <div className="text-sm font-medium">在庫</div>
                    <div className="space-y-2">
                      {detailProduct.extracted.goodsInventory.map((row: any, idx: number) => (
                        <div key={idx} className="text-xs rounded border bg-muted/30">
                          <div className="px-2 py-1 font-medium break-words border-b">{row.key}</div>
                          {Array.isArray(row.rows) && row.rows.length > 0 ? (
                            <div className="overflow-auto">
                              <table className="min-w-full text-xs">
                                <thead>
                                  <tr className="text-left text-muted-foreground">
                                    <th className="px-2 py-1">価格</th>
                                    <th className="px-2 py-1">在庫</th>
                                    <th className="px-2 py-1">最小数量</th>
                                    <th className="px-2 py-1">SKU</th>
                                  </tr>
                                </thead>
                                <tbody>
                                  {row.rows.map((r: any, i: number) => (
                                    <tr key={i} className="border-t">
                                      <td className="px-2 py-1">{r.price ?? '-'}</td>
                                      <td className="px-2 py-1">{r.amountOnSale ?? '-'}</td>
                                      <td className="px-2 py-1">{r.startQuantity ?? '-'}</td>
                                      <td className="px-2 py-1">{r.skuId ?? '-'}</td>
                                    </tr>
                                  ))}
                                </tbody>
                              </table>
                            </div>
                          ) : (
                            <div className="p-2 text-muted-foreground">在庫情報なし</div>
                          )}
                        </div>
                      ))}
                    </div>
                  </div>
                )}

                {!detailProduct.extracted.titleT && !detailProduct.extracted.shopname && !detailProduct.extracted.address &&
                  (!detailProduct.extracted.detail || detailProduct.extracted.detail.length === 0) && (
                    <div className="text-sm text-muted-foreground">必要な詳細情報が見つかりませんでした。</div>
                )}
              </>
            ) : (
              <div className="text-sm text-muted-foreground">詳細情報がありません。</div>
            )}
          </div>
          <AlertDialogFooter>
            <AlertDialogCancel>閉じる</AlertDialogCancel>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  )
}
