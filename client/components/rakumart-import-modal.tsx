"use client"

import { useState, useEffect } from "react"
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogDescription } from "@/components/ui/dialog"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { ScrollArea } from "@/components/ui/scroll-area"
import { Search, Package, X, ChevronDown, ChevronRight, Loader2, Square } from "lucide-react"
import { Checkbox } from "@/components/ui/checkbox"
import { Badge } from "@/components/ui/badge"
import { RakumartProgressBar } from "@/components/ui/progress-bar"
import { apiService, KeywordSearchRequest, ProductResponse, CategoryRecord, PrimaryCategoryRecord } from "@/lib/api-service"
import { useToast } from "@/hooks/use-toast"

interface RakumartImportModalProps {
  open: boolean
  onOpenChange: (open: boolean) => void
  onProductsLoaded?: (products: any[]) => void
}

interface SecondaryCategory {
  id: string
  label: string
  categoryIds: string[]
}

interface PrimaryCategory {
  id: string
  label: string
  children: SecondaryCategory[]
}

interface SecondaryCategoryInfo {
  id: string
  label: string
  parentId: string
  parentLabel: string
  categoryIds: string[]
}

const SORT_OPTIONS = [
  { value: "monthSold", label: "月刊販売数によるソート" },
  { value: "goodsPrice", label: "価格による並べ替え" },
  { value: "tradeScore", label: "評価スコアによるソート" },
  { value: "repurchaseRate", label: "再購入率によるソート" },
  { value: "createDate", label: "登録日によるソート" },
  { value: "topCategoryId", label: "カテゴリによる並べ替え" },
]

export function RakumartImportModal({ open, onOpenChange, onProductsLoaded }: RakumartImportModalProps) {
  // 整数のみを許可する入力ハンドラー（マイナス値と小数点を防ぐ）
  const handleIntegerInput = (value: string, setter: (value: string) => void) => {
    // 空文字列を許可
    if (value === "") {
      setter("")
      return
    }
    // 数字のみを許可（マイナス記号と小数点を除去）
    const sanitized = value.replace(/[^0-9]/g, "")
    if (sanitized !== "") {
      setter(sanitized)
    } else {
      setter("")
    }
  }
  
  // 数値のみを許可する入力ハンドラー（マイナス値のみを防ぐ、小数点は許可）
  const handleDecimalInput = (value: string, setter: (value: string) => void) => {
    // 空文字列を許可
    if (value === "") {
      setter("")
      return
    }
    // マイナス記号を除去し、数字と小数点のみを許可
    const sanitized = value.replace(/[^0-9.]/g, "")
    // 複数の小数点を防ぐ
    const parts = sanitized.split(".")
    if (parts.length > 2) {
      setter(parts[0] + "." + parts.slice(1).join(""))
    } else {
      setter(sanitized)
    }
  }
  
  // キーボード入力でマイナス記号と小数点を防ぐ（整数用）
  const handleIntegerKeyDown = (e: React.KeyboardEvent<HTMLInputElement>) => {
    // マイナス記号、プラス記号、小数点を防ぐ
    if (e.key === "-" || e.key === "+" || e.key === "." || e.key === ",") {
      e.preventDefault()
    }
    // "e", "E", "+" も防ぐ（科学記数法を防ぐ）
    if (e.key === "e" || e.key === "E" || e.key === "+") {
      e.preventDefault()
    }
  }
  
  // キーボード入力でマイナス記号のみを防ぐ（小数点用）
  const handleDecimalKeyDown = (e: React.KeyboardEvent<HTMLInputElement>) => {
    // マイナス記号とプラス記号を防ぐ
    if (e.key === "-" || e.key === "+") {
      e.preventDefault()
    }
    // "e", "E" も防ぐ（科学記数法を防ぐ）
    if (e.key === "e" || e.key === "E") {
      e.preventDefault()
    }
  }
  
  const [keyword, setKeyword] = useState("")
  const [searchCount, setSearchCount] = useState("50")
const [startPage, setStartPage] = useState("1")
const [endPage, setEndPage] = useState("5")
  const [expandedCategories, setExpandedCategories] = useState<Set<string>>(new Set())
  const [selectedCategories, setSelectedCategories] = useState<Map<string, string>>(new Map())
  const [priceFrom, setPriceFrom] = useState("")
  const [priceTo, setPriceTo] = useState("")
  const [maxLength, setMaxLength] = useState("")
  const [maxWidth, setMaxWidth] = useState("")
  const [maxHeight, setMaxHeight] = useState("")
  const [maxWeight, setMaxWeight] = useState("")
  const [minStock, setMinStock] = useState("")
const [minRating, setMinRating] = useState("")
const [minRepurchaseRate, setMinRepurchaseRate] = useState("")
const [minMonthlySales, setMinMonthlySales] = useState("")
  const [sortBy, setSortBy] = useState("monthSold")
  const [sortOrder, setSortOrder] = useState("desc")
  const [filterOptions, setFilterOptions] = useState<string[]>([])
const [regionOpp, setRegionOpp] = useState("jpOpp")
  const [isLoading, setIsLoading] = useState(false)
  const [searchProgress, setSearchProgress] = useState(0)
  const [searchStatus, setSearchStatus] = useState<'idle' | 'searching' | 'processing' | 'completed' | 'error'>('idle')
  const [currentStep, setCurrentStep] = useState("")
  const [abortController, setAbortController] = useState<AbortController | null>(null)
  const { toast } = useToast()

const DEFAULT_SECONDARY_COUNT = "10"

const [categoryTree, setCategoryTree] = useState<PrimaryCategory[]>([])
const [secondaryLookup, setSecondaryLookup] = useState<Map<string, SecondaryCategoryInfo>>(new Map())
  const [isCategoryLoading, setIsCategoryLoading] = useState(false)

  useEffect(() => {
  const loadCategories = async () => {
      try {
        setIsCategoryLoading(true)
        const [primaryResponse, secondaryResponse] = await Promise.all([
          apiService.getPrimaryCategories(),
          apiService.getCategories(),
        ])
      const primaryList = primaryResponse.categories ?? []
      const secondaryList = secondaryResponse.categories ?? []
      const { tree, lookup } = buildCategoryData(primaryList, secondaryList)
      setCategoryTree(tree)
      setSecondaryLookup(lookup)
      } catch (error) {
        console.error("Failed to load categories", error)
        toast({
          title: "エラー",
        description: "カテゴリ情報の取得に失敗しました。時間をおいて再度お試しください。",
          variant: "destructive",
        })
      } finally {
        setIsCategoryLoading(false)
      }
    }

  loadCategories()
  }, [toast])

const buildCategoryData = (
    primaries: PrimaryCategoryRecord[],
    secondaries: CategoryRecord[],
): { tree: PrimaryCategory[]; lookup: Map<string, SecondaryCategoryInfo> } => {
    const normalizeCategoryIds = (value: unknown): string[] => {
      if (Array.isArray(value)) {
        return value.map((item) => String(item).trim()).filter((item) => item.length > 0)
      }
      if (typeof value === "string") {
        return value
          .split(",")
          .map((item) => item.trim())
          .filter((item) => item.length > 0)
      }
      return []
    }

  const map = new Map<string, PrimaryCategory>()
  const lookup = new Map<string, SecondaryCategoryInfo>()
  const fallbackId = "uncategorized"
  const fallbackLabel = "未分類"

  const ensurePrimary = (id: string, label?: string | null) => {
    const safeLabel = label && label.trim().length > 0 ? label : fallbackLabel
    if (!map.has(id)) {
      map.set(id, { id, label: safeLabel, children: [] })
    }
    return map.get(id)!
  }

  primaries.forEach((primary) => {
    ensurePrimary(String(primary.id), primary.category_name)
  })

    secondaries.forEach((secondary) => {
      const parentId =
        secondary.primary_category_id !== null && secondary.primary_category_id !== undefined
          ? String(secondary.primary_category_id)
          : fallbackId
    const parentLabel = secondary.primary_category_name ?? fallbackLabel
    const parent = ensurePrimary(parentId, parentLabel)
    const childId = String(secondary.id)
    const childLabel =
      secondary.category_name && secondary.category_name.trim().length > 0
        ? secondary.category_name
        : fallbackLabel
    const child: SecondaryCategory = {
      id: childId,
      label: childLabel,
        categoryIds: normalizeCategoryIds(secondary.category_ids),
    }
    parent.children.push(child)
    lookup.set(childId, {
      id: childId,
      label: child.label,
      parentId: parent.id,
      parentLabel: parent.label,
      categoryIds: child.categoryIds,
    })
  })

  const tree = Array.from(map.values())
    .map((primary) => ({
      ...primary,
      children: [...primary.children].sort((a, b) => a.label.localeCompare(b.label, "ja")),
        }))
    .filter((primary) => primary.children.length > 0)
    .sort((a, b) => a.label.localeCompare(b.label, "ja"))

  return { tree, lookup }
}

const getPrimarySelectionState = (primary: PrimaryCategory): "none" | "partial" | "all" => {
  if (primary.children.length === 0) {
    return "none"
  }
  const selectedCount = primary.children.filter((child) => selectedCategories.has(child.id)).length
  if (selectedCount === 0) {
    return "none"
  }
  if (selectedCount === primary.children.length) {
    return "all"
    }
  return "partial"
  }

const getPrimaryCount = (primary: PrimaryCategory): number => {
  return primary.children.reduce((sum, child) => {
    const value = selectedCategories.get(child.id)
    return sum + (value ? Number.parseInt(value) || 0 : 0)
      }, 0)
    }
    
const togglePrimaryExpansion = (id: string) => {
  setExpandedCategories((prev) => {
    const next = new Set(prev)
    if (next.has(id)) {
      next.delete(id)
    } else {
      next.add(id)
    }
    return next
  })
  }

const togglePrimarySelection = (primary: PrimaryCategory, checked: boolean) => {
  setSelectedCategories((prev) => {
    const next = new Map(prev)
    if (checked) {
      primary.children.forEach((child) => {
        if (!next.has(child.id)) {
          next.set(child.id, DEFAULT_SECONDARY_COUNT)
        }
        })
      } else {
      primary.children.forEach((child) => next.delete(child.id))
    }
    return next
            })
}

const toggleSecondarySelection = (child: SecondaryCategory, checked: boolean) => {
  setSelectedCategories((prev) => {
    const next = new Map(prev)
    if (checked) {
      next.set(child.id, next.get(child.id) ?? DEFAULT_SECONDARY_COUNT)
      } else {
      next.delete(child.id)
    }
    return next
  })
        }

const updateSecondaryCount = (childId: string, value: string) => {
  setSelectedCategories((prev) => {
    const next = new Map(prev)
    next.set(childId, value)
    return next
  })
}

const renderCategoryList = () => {
  if (isCategoryLoading) {
    return (
      <div className="flex items-center justify-center py-10 text-muted-foreground gap-2">
        <Loader2 className="h-4 w-4 animate-spin" />
        カテゴリを読み込み中...
      </div>
    )
  }

  if (categoryTree.length === 0) {
    return (
      <div className="py-8 text-center text-sm text-muted-foreground">
        利用可能なカテゴリがありません。設定ページでカテゴリを登録してください。
      </div>
    )
  }

  return categoryTree.map((primary) => {
    const selectionState = getPrimarySelectionState(primary)
    const checkboxState =
      selectionState === "all" ? true : selectionState === "partial" ? "indeterminate" : false
    const totalCount = getPrimaryCount(primary)
    const isExpanded = expandedCategories.has(primary.id)

    return (
      <div key={primary.id} className="space-y-1">
        <div className="flex items-center gap-2 p-2 hover:bg-secondary rounded-md transition-colors">
          <Button
            variant="ghost"
            size="sm"
            className="h-6 w-6 p-0"
            onClick={() => togglePrimaryExpansion(primary.id)}
          >
            {isExpanded ? <ChevronDown className="h-4 w-4" /> : <ChevronRight className="h-4 w-4" />}
          </Button>
          <Checkbox
            id={primary.id}
            checked={checkboxState}
            onCheckedChange={(checked) => togglePrimarySelection(primary, checked === true)}
          />
          <Label htmlFor={primary.id} className="flex-1 cursor-pointer font-medium text-sm">
            {primary.label}
          </Label>
          {selectionState !== "none" && (
            <Input
              type="number"
              readOnly
              value={totalCount}
              className="w-20 h-8 text-xs bg-muted cursor-not-allowed"
            />
          )}
        </div>
        {isExpanded && primary.children.length > 0 && (
          <div className="ml-8 space-y-1">
            {primary.children.map((child) => {
              const isSelected = selectedCategories.has(child.id)
              const value = selectedCategories.get(child.id) || ""
              return (
                <div
                  key={child.id}
                                    className="flex items-center gap-2 p-2 hover:bg-secondary rounded-md transition-colors"
                                  >
                                    <Checkbox
                    id={child.id}
                    checked={isSelected}
                    onCheckedChange={(checked) => toggleSecondarySelection(child, checked === true)}
                                    />
                  <Label htmlFor={child.id} className="flex-1 cursor-pointer text-sm">
                    {child.label}
                                    </Label>
                  {isSelected && (
                                      <Input
                                        type="number"
                                        min="0"
                                        placeholder="10"
                      value={value}
                      onChange={(e) =>
                        handleIntegerInput(e.target.value, (val) => updateSecondaryCount(child.id, val))
                      }
                                        onKeyDown={handleIntegerKeyDown}
                                        className="w-20 h-8 text-xs"
                                      />
                            )}
                          </div>
                        )
                      })}
                    </div>
                  )}
        </div>
      )
    })
  }

const getSelectedCategoryHierarchy = (): {
  majorCategoryIds: string[]
  minorCategoryIds: string[]
} => {
  const majorSet = new Set<string>()
  const minorIds: string[] = []

  selectedCategories.forEach((_, categoryId) => {
    const meta = secondaryLookup.get(categoryId)
    if (!meta) {
      return
    }
    majorSet.add(meta.parentId)
    minorIds.push(categoryId)
  })

  return {
    majorCategoryIds: Array.from(majorSet),
    minorCategoryIds: minorIds,
  }
}

const collectCategoryGroups = (): Array<{ id: string; label: string; categoryIds: string[]; count: number }> => {
  const groups: Array<{ id: string; label: string; categoryIds: string[]; count: number }> = []

  selectedCategories.forEach((countStr, categoryId) => {
    const count = Number.parseInt(countStr) || 0
    if (count <= 0) {
      return
    }
    const meta = secondaryLookup.get(categoryId)
    if (!meta) {
      return
    }
    const ids = meta.categoryIds.filter((id) => id && id.trim().length > 0).map((id) => id.trim())
    if (ids.length === 0) {
      console.warn(`No Rakumart category IDs registered for category ${categoryId} (${meta.label})`)
      return
    }
    groups.push({
      id: categoryId,
      label: `${meta.parentLabel} / ${meta.label}`,
      categoryIds: ids,
                  count,
    })
  })

  return groups
}

const fetchPagedResults = async (
  fetchFn: (request: any) => Promise<ProductResponse>,
  baseRequest: Record<string, any>,
  desiredCount: number,
  start: number,
  end: number,
  label: string,
  signal?: AbortSignal,
): Promise<ProductResponse> => {
  const aggregated: any[] = []
  let lastResponse: ProductResponse | null = null
  const totalPages = Math.max(1, end - start + 1)
  let currentPage = start
  const maxExtraPages = 50
  const maxIterations = totalPages + maxExtraPages
  let emptyPagesBeyondRange = 0

  for (let iteration = 0; iteration < maxIterations; iteration++) {
    // 中止シグナルをチェック
    if (signal?.aborted) {
      return {
        success: false,
        error: "検索が中止されました",
      }
    }

    if (aggregated.length >= desiredCount) {
      break
    }

    const remaining = Math.max(1, desiredCount - aggregated.length)
    const pageSize = Math.max(1, Math.min(baseRequest.page_size ?? 50, remaining))
    const pageRequest = {
      ...baseRequest,
      page: currentPage,
      page_size: pageSize,
    }

    try {
      const progressRatio =
        iteration < totalPages
          ? (iteration + 1) / totalPages
          : 1 - Math.exp(-(iteration - totalPages + 1) / maxExtraPages)
      setCurrentStep(`${label} (ページ ${currentPage}) を検索中...`)
      setSearchProgress(20 + Math.round(progressRatio * 40))

      const response = await fetchFn(pageRequest)
      
      // 中止シグナルを再度チェック
      if (signal?.aborted) {
        return {
          success: false,
          error: "検索が中止されました",
        }
      }

      lastResponse = response
      if (!response.success) {
        return response
      }

      const products = extractProductsFromResponse(response)
      if (products.length > 0) {
        aggregated.push(...products)
        emptyPagesBeyondRange = 0
      } else if (currentPage >= end) {
        emptyPagesBeyondRange += 1
        if (emptyPagesBeyondRange >= 3) {
          break
        }
      }
    } catch (error: any) {
      // AbortErrorの場合は中止として扱う
      if (error?.name === 'AbortError' || signal?.aborted) {
        return {
          success: false,
          error: "検索が中止されました",
        }
      }
      console.error(`${label}: ページ${currentPage}の取得に失敗しました`, error)
      return {
        success: false,
        error: error instanceof Error ? error.message : `${label}の取得に失敗しました`,
      }
    }

    currentPage += 1
  }

  if (!lastResponse) {
    return {
      success: false,
      error: "検索結果を取得できませんでした",
    }
  }

  return {
    ...lastResponse,
    success: true,
    data: aggregated.slice(0, desiredCount),
    total: aggregated.length,
    message:
      aggregated.length >= desiredCount
        ? `${aggregated.length}件の商品を取得しました`
        : `指定件数に満たない${aggregated.length}件の商品を取得しました`,
  }
}

const extractProductsFromResponse = (res?: ProductResponse): any[] => {
  if (!res || !res.data) {
    return []
  }

  const data = res.data as any
  if (Array.isArray(data)) {
    return data
  }
  if (data && typeof data === "object") {
    if (Array.isArray(data.result)) {
      return data.result
    }
    if (data.result && Array.isArray(data.result.products)) {
      return data.result.products
    }
    if (Array.isArray(data.products)) {
      return data.products
    }
    if (data.data && Array.isArray(data.data)) {
      return data.data
    }
    if (data.data && data.data.result && Array.isArray(data.data.result.products)) {
      return data.data.result.products
    }
  }
  return []
}

const performDirectCategorySearch = async (
  desiredCount: number,
  start: number,
  end: number,
  signal?: AbortSignal,
): Promise<ProductResponse> => {
  const { majorCategoryIds, minorCategoryIds } = getSelectedCategoryHierarchy()

  if (majorCategoryIds.length > 0 || minorCategoryIds.length > 0) {
    const baseRequest = {
      keyword: keyword.trim() || "",
      price_min: priceFrom && priceFrom.trim() !== "" ? priceFrom : undefined,
      price_max: priceTo && priceTo.trim() !== "" ? priceTo : undefined,
      max_length: maxLength && maxLength.trim() !== "" ? parseFloat(maxLength) : undefined,
      max_width: maxWidth && maxWidth.trim() !== "" ? parseFloat(maxWidth) : undefined,
      max_height: maxHeight && maxHeight.trim() !== "" ? parseFloat(maxHeight) : undefined,
      max_weight: maxWeight && maxWeight.trim() !== "" ? parseFloat(maxWeight) : undefined,
      min_inventory: minStock && minStock.trim() !== "" ? parseInt(minStock) : undefined,
      categories: majorCategoryIds.length > 0 ? majorCategoryIds : undefined,
      subcategories: minorCategoryIds.length > 0 ? minorCategoryIds : undefined,
      save_to_db: true,
    }

    return fetchPagedResults(
      (payload) => apiService.searchProducts(payload, signal),
      baseRequest,
      desiredCount,
      start,
      end,
      "カテゴリ検索",
      signal,
    )
  }

  // Use keyword search for keyword-only searches
  const baseKeywordRequest: KeywordSearchRequest = {
    keywords: keyword.trim() || "",
    price_start: priceFrom && priceFrom.trim() !== "" ? priceFrom : undefined,
    price_end: priceTo && priceTo.trim() !== "" ? priceTo : undefined,
    sort_field: sortBy,
    sort_order: sortOrder,
    region_opp: regionOpp || "jpOpp", // Default to jpOpp (日本ホット商品)
    filter: filterOptions.length > 0 ? filterOptions.join(",") : undefined,
    min_rating: minRating && minRating.trim() !== "" ? parseFloat(minRating) : undefined, // 最低評価点
    min_repurchase_rate: minRepurchaseRate && minRepurchaseRate.trim() !== "" ? parseFloat(minRepurchaseRate) : undefined, // 最低再購入率
    min_monthly_sales: minMonthlySales && minMonthlySales.trim() !== "" ? parseInt(minMonthlySales) : undefined, // 月間最低販売数
    save_to_db: true,
    }
    

  return fetchPagedResults(
    (payload) => apiService.keywordSearch(payload, signal),
    baseKeywordRequest,
    desiredCount,
    start,
    end,
    "キーワード検索",
    signal,
  )
}
  const handleSearch = async () => {
    if (!keyword.trim() && selectedCategories.size === 0) {
      toast({
        title: "エラー",
        description: "キーワードまたはカテゴリを選択してください",
        variant: "destructive",
      })
      return
    }

    const desiredCount = Math.max(1, parseInt(searchCount) || 50)
    const parsedStartPage = Math.max(1, parseInt(startPage) || 1)
    const parsedEndPage = Math.max(parsedStartPage, parseInt(endPage) || parsedStartPage)

    if (parsedStartPage > parsedEndPage) {
      toast({
        title: "エラー",
        description: "開始ページは終了ページ以下に設定してください",
        variant: "destructive",
      })
      return
    }

    // AbortControllerを作成
    const controller = new AbortController()
    setAbortController(controller)

    setIsLoading(true)
    setSearchProgress(0)
    setSearchStatus('searching')
    setCurrentStep("検索リクエストを準備中...")
    
    try {
      let response: ProductResponse
    const categoryGroups = collectCategoryGroups()
      if (categoryGroups.length > 0) {
        
        const allProducts: any[] = []
        let successfulGroups = 0
        let failedGroups = 0
        
        for (let i = 0; i < categoryGroups.length; i++) {
          const group = categoryGroups[i]
        const progressPercent = 20 + Math.round((i / categoryGroups.length) * 60)
          
        setSearchProgress(progressPercent)
          setCurrentStep(`${group.label}を検索中 (${group.categoryIds.length}カテゴリ、${group.count}件)...`)
          
          const itemsPerCategoryId = Math.max(1, Math.ceil(group.count / group.categoryIds.length))
          
            const groupRequest = {
          category_ids: group.categoryIds,
          page: 1,
          page_size: itemsPerCategoryId,
          max_products_per_category: itemsPerCategoryId,
          price_start: priceFrom && priceFrom.trim() !== "" ? priceFrom : undefined,
          price_end: priceTo && priceTo.trim() !== "" ? priceTo : undefined,
          sort_field: sortBy,
          sort_order: sortOrder,
          region_opp: regionOpp || "jpOpp",
          filter: filterOptions.length > 0 ? filterOptions.join(",") : undefined,
          min_rating: minRating && minRating.trim() !== "" ? parseFloat(minRating) : undefined,
          min_repurchase_rate:
            minRepurchaseRate && minRepurchaseRate.trim() !== "" ? parseFloat(minRepurchaseRate) : undefined,
          min_monthly_sales:
            minMonthlySales && minMonthlySales.trim() !== "" ? parseInt(minMonthlySales) : undefined,
          save_to_db: true,
            }
            
            
        try {
          // 中止シグナルをチェック
          if (controller.signal.aborted) {
            break
          }

          const groupResponse = await apiService.multiCategorySearch(groupRequest, controller.signal)

          // 中止シグナルを再度チェック
          if (controller.signal.aborted) {
            break
          }

          if (!groupResponse || !groupResponse.success) {
              failedGroups++
            console.error(`Group ${group.label}: Search failed`, groupResponse)
              continue
            }
            
          const groupProducts = extractProductsFromResponse(groupResponse)
          if (groupProducts.length === 0) {
              failedGroups++
            console.warn(`Group ${group.label}: No products returned from API`)
              continue
            }
            
          const limitedProducts = groupProducts.slice(0, group.count)
                allProducts.push(...limitedProducts)
                successfulGroups++
          } catch (error: any) {
            // AbortErrorの場合はループを抜ける
            if (error?.name === 'AbortError' || controller.signal.aborted) {
              break
            }
            failedGroups++
          console.error(`Group ${group.label}: API call failed`, error)
          }
        }
        
        setSearchProgress(90)
        setCurrentStep(`検索完了: ${allProducts.length}件の商品を取得しました`)
        
          
          response = {
            success: true,
            data: allProducts,
            total: allProducts.length,
        message:
          allProducts.length > 0
            ? `${categoryGroups.length}カテゴリグループから合計${allProducts.length}件の商品を取得しました`
            : `選択した${categoryGroups.length}カテゴリで商品が見つかりませんでした`,
          }
        } else {
        response = await performDirectCategorySearch(desiredCount, parsedStartPage, parsedEndPage, controller.signal)
      }

      // 中止されたかチェック
      if (controller.signal.aborted) {
        setSearchStatus('error')
        setCurrentStep("検索が中止されました")
        toast({
          title: "検索中止",
          description: "検索がユーザーによって中止されました",
          variant: "info",
        })
        return
      }
      
      setSearchProgress(60)
      setSearchStatus('processing')
      setCurrentStep("商品データを処理中...")

      // Validate response and extract products array
      let products: any[] = []
      
      // Ensure response exists
      if (!response) {
        console.error("Response is undefined")
        setSearchStatus('error')
        setCurrentStep("エラーが発生しました")
        toast({
          title: "エラー",
          description: "サーバーからの応答がありませんでした",
          variant: "destructive",
        })
        return
      }
      
      if (response.success) {
        products = extractProductsFromResponse(response)
        
        // Apply client-side filtering for rating, repurchase rate, and monthly sales
        if (products.length > 0) {
          let originalCount = products.length
          
          if (minRating && minRating.trim() !== "") {
            const minRatingValue = parseFloat(minRating)
            if (!isNaN(minRatingValue)) {
              products = products.filter((p: any) => {
                const rating = parseFloat(p.tradeScore || p.rating || p.trade_score || "0")
                return rating >= minRatingValue
              })
              originalCount = products.length
            }
          }
          
          if (minRepurchaseRate && minRepurchaseRate.trim() !== "") {
            const minRepurchaseValue = parseFloat(minRepurchaseRate)
            if (!isNaN(minRepurchaseValue)) {
              products = products.filter((p: any) => {
                const repurchase = parseFloat(p.repurchaseRate || p.rePurchaseRate || p.repurchase_rate || "0")
                return repurchase >= minRepurchaseValue
              })
              originalCount = products.length
            }
          }
          
          if (minMonthlySales && minMonthlySales.trim() !== "") {
            const minSalesValue = parseInt(minMonthlySales)
            if (!isNaN(minSalesValue)) {
              products = products.filter((p: any) => {
                const sales = parseInt(p.monthSold || p.month_sold || p.monthSoldCount || "0")
                return sales >= minSalesValue
              })
            }
          }
        }
      }

      // Check if search was successful
      if (response.success) {
        setSearchProgress(100)
        setSearchStatus('completed')
        setCurrentStep("完了")
        
        const productCount = products.length
        if (productCount > 0) {
          toast({
            title: "成功",
            description: `${productCount}件の商品を取得し、データベースに保存しました`,
          })
          
          // Notify parent component about loaded products
          if (onProductsLoaded) {
            onProductsLoaded(products)
          }
        } else {
          toast({
            title: "完了",
            description: "検索が完了しました（商品が見つかりませんでした）",
            variant: "info",
          })
        }
        
        onOpenChange(false)
      } else {
        setSearchStatus('error')
        setCurrentStep("エラーが発生しました")
        
        // Better error handling
        let errorMessage = "商品の取得に失敗しました"
        if (response) {
          if (response.error) {
            errorMessage = response.error
          } else if (response.message) {
            errorMessage = response.message
          } else {
            errorMessage = "サーバーからの応答が無効でした"
          }
        } else {
          errorMessage = "サーバーからの応答がありませんでした"
        }
        
        console.error("Search failed:", {
          response,
          responseType: typeof response,
          responseKeys: response ? Object.keys(response) : [],
          hasError: response?.error,
          hasMessage: response?.message,
          hasData: response?.data !== undefined
        })
        
        toast({
          title: "エラー",
          description: errorMessage,
          variant: "destructive",
        })
      }
    } catch (error) {
      console.error("Search error:", error)
      setSearchStatus('error')
      setCurrentStep("エラーが発生しました")
      
      toast({
        title: "エラー",
        description: error instanceof Error ? error.message : "商品の取得に失敗しました",
        variant: "destructive",
      })
    } finally {
      setIsLoading(false)
      setAbortController(null)
      
      // Reset progress after 3 seconds
      setTimeout(() => {
        setSearchProgress(0)
        setSearchStatus('idle')
        setCurrentStep("")
      }, 3000)
    }
  }

  const handleCancelSearch = () => {
    if (abortController) {
      abortController.abort()
      setIsLoading(false)
      setSearchStatus('error')
      setCurrentStep("検索が中止されました")
      setAbortController(null)
      toast({
        title: "検索中止",
        description: "検索が中止されました",
        variant: "info",
      })
    }
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-[95vw] sm:max-w-[90vw] md:max-w-3xl lg:max-w-4xl h-[90vh] flex flex-col bg-white border-border shadow-2xl">
        <DialogHeader className="space-y-2 pb-4 border-b border-border flex-shrink-0">
          <div className="flex items-center justify-between">
            <DialogTitle className="flex items-center gap-2 text-foreground text-lg md:text-xl">
              <Package className="h-5 w-5 text-primary" />
              ラクマートから商品を呼び出す
            </DialogTitle>
            <Button variant="ghost" size="icon" onClick={() => onOpenChange(false)} className="hover:bg-muted">
              <X className="h-4 w-4" />
            </Button>
          </div>
          <DialogDescription className="text-muted-foreground text-sm">
            検索条件を設定して商品を検索します
          </DialogDescription>
        </DialogHeader>

        <ScrollArea className="flex-1 overflow-y-auto">
          <div className="space-y-5 pb-4 pr-4">
            {/* Keyword Search */}
            <div className="space-y-3 p-4 rounded-lg bg-secondary border border-border">
              <h3 className="font-semibold text-sm md:text-base text-foreground flex items-center gap-2">
                <Search className="h-4 w-4 text-primary" />
                商品キーワードによる検索
              </h3>
              <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-3">
                <div className="space-y-2">
                  <Label htmlFor="keyword" className="text-foreground text-sm">
                    商品キーワード
                  </Label>
                  <Input
                    id="keyword"
                    placeholder="例: ワイヤレスイヤホン"
                    value={keyword}
                    onChange={(e) => setKeyword(e.target.value)}
                    className="bg-white border-border"
                  />
                </div>
                <div className="space-y-2 sm:col-span-1">
                  <Label htmlFor="searchCount" className="text-foreground text-sm">
                    取得件数
                  </Label>
                  <Input
                    id="searchCount"
                    type="number"
                    min="0"
                    placeholder="50"
                    value={searchCount}
                    onChange={(e) => handleIntegerInput(e.target.value, setSearchCount)}
                    onKeyDown={handleIntegerKeyDown}
                    className="bg-white border-border"
                  />
                </div>
                <div className="space-y-2">
                  <Label htmlFor="startPage" className="text-foreground text-sm">
                    取得開始ページ
                  </Label>
                  <Input
                    id="startPage"
                    type="number"
                    min="1"
                    placeholder="1"
                    value={startPage}
                    onChange={(e) => handleIntegerInput(e.target.value, setStartPage)}
                    onKeyDown={handleIntegerKeyDown}
                    className="bg-white border-border"
                  />
                </div>
                <div className="space-y-2">
                  <Label htmlFor="endPage" className="text-foreground text-sm">
                    取得終了ページ
                  </Label>
                  <Input
                    id="endPage"
                    type="number"
                    min="1"
                    placeholder="5"
                    value={endPage}
                    onChange={(e) => handleIntegerInput(e.target.value, setEndPage)}
                    onKeyDown={handleIntegerKeyDown}
                    className="bg-white border-border"
                  />
                </div>
              </div>
            </div>

            <div className="space-y-3 p-4 rounded-lg bg-secondary border border-border">
              <h3 className="font-semibold text-sm md:text-base text-foreground">カテゴリによる検索</h3>
              <div className="space-y-2 max-h-[400px] overflow-y-auto border border-border rounded-md bg-white p-3">
                {renderCategoryList()}
                      </div>
            </div>

            {/* Price Range */}
            <div className="space-y-3 p-4 rounded-lg bg-secondary border border-border">
              <h3 className="font-semibold text-sm md:text-base text-foreground">価格範囲（円）</h3>
              <div className="grid grid-cols-2 gap-3">
                <div className="space-y-2">
                  <Label htmlFor="priceFrom" className="text-foreground text-sm">
                    から
                  </Label>
                  <Input
                    id="priceFrom"
                    type="number"
                    min="0"
                    placeholder="0"
                    value={priceFrom}
                    onChange={(e) => handleIntegerInput(e.target.value, setPriceFrom)}
                    onKeyDown={handleIntegerKeyDown}
                    className="bg-white border-border"
                  />
                </div>
                <div className="space-y-2">
                  <Label htmlFor="priceTo" className="text-foreground text-sm">
                    まで
                  </Label>
                  <Input
                    id="priceTo"
                    type="number"
                    min="0"
                    placeholder="10000"
                    value={priceTo}
                    onChange={(e) => handleIntegerInput(e.target.value, setPriceTo)}
                    onKeyDown={handleIntegerKeyDown}
                    className="bg-white border-border"
                  />
                </div>
              </div>
            </div>

            {/* Advanced Filters */}
            <div className="space-y-3 p-4 rounded-lg bg-secondary border border-border">
              <h3 className="font-semibold text-sm md:text-base text-foreground">高度なフィルター</h3>
              
              {/* Region Options */}
              <div className="space-y-2">
                <Label htmlFor="regionOpp" className="text-foreground text-sm">
                  地域オプション
                </Label>
                <Select value={regionOpp} onValueChange={setRegionOpp}>
                  <SelectTrigger className="bg-white border-border">
                    <SelectValue placeholder="地域を選択してください" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="all">すべての地域</SelectItem>
                    <SelectItem value="jpOpp">日本ホット商品</SelectItem>
                    <SelectItem value="krOpp">韓国ホット商品</SelectItem>
                  </SelectContent>
                </Select>
              </div>

              {/* Filter Options */}
              <div className="space-y-2">
                <Label className="text-foreground text-sm">フィルターオプション</Label>
                <div className="grid grid-cols-2 gap-2">
                  {[
                    { value: "certifiedFactory", label: "認定工場" },
                    { value: "shipIn48Hours", label: "48時間以内発送" },
                    { value: "freeShipping", label: "送料無料" },
                    { value: "newArrival", label: "新着商品" },
                    { value: "hotSale", label: "ホットセール" },
                    { value: "qualityAssured", label: "品質保証" },
                  ].map((option) => (
                    <div key={option.value} className="flex items-center space-x-2">
                      <Checkbox
                        id={option.value}
                        checked={filterOptions.includes(option.value)}
                        onCheckedChange={(checked) => {
                          if (checked) {
                            setFilterOptions(prev => [...prev, option.value])
                          } else {
                            setFilterOptions(prev => prev.filter(f => f !== option.value))
                          }
                        }}
                      />
                      <Label htmlFor={option.value} className="text-xs text-foreground">
                        {option.label}
                      </Label>
                    </div>
                  ))}
                </div>
              </div>
            </div>

            

            {/* Product Quality Filters */}
            <div className="space-y-3 p-4 rounded-lg bg-secondary border border-border">
              <h3 className="font-semibold text-sm md:text-base text-foreground">商品品質フィルター</h3>
              <div className="grid grid-cols-1 sm:grid-cols-3 gap-3">
                <div className="space-y-2">
                  <Label htmlFor="minRating" className="text-foreground text-sm">
                    最低評価点
                  </Label>
                  <Input
                    id="minRating"
                    type="number"
                    step="0.1"
                    min="0"
                    max="5"
                    placeholder="例: 4.0"
                    value={minRating}
                    onChange={(e) => handleDecimalInput(e.target.value, setMinRating)}
                    onKeyDown={handleDecimalKeyDown}
                    className="bg-white border-border"
                  />
                </div>
                <div className="space-y-2">
                  <Label htmlFor="minRepurchaseRate" className="text-foreground text-sm">
                    最低再購入率 (%)
                  </Label>
                  <Input
                    id="minRepurchaseRate"
                    type="number"
                    step="0.1"
                    min="0"
                    max="100"
                    placeholder="例: 50"
                    value={minRepurchaseRate}
                    onChange={(e) => handleDecimalInput(e.target.value, setMinRepurchaseRate)}
                    onKeyDown={handleDecimalKeyDown}
                    className="bg-white border-border"
                  />
                </div>
                <div className="space-y-2">
                  <Label htmlFor="minMonthlySales" className="text-foreground text-sm">
                    月間最低販売数
                  </Label>
                  <Input
                    id="minMonthlySales"
                    type="number"
                    min="0"
                    placeholder="例: 100"
                    value={minMonthlySales}
                    onChange={(e) => handleIntegerInput(e.target.value, setMinMonthlySales)}
                    onKeyDown={handleIntegerKeyDown}
                    className="bg-white border-border"
                  />
                </div>
              </div>
            </div>

            {/* Minimum Stock */}
            <div className="space-y-3 p-4 rounded-lg bg-secondary border border-border">
              <h3 className="font-semibold text-sm md:text-base text-foreground">商品の最小在庫数</h3>
              <Input
                type="number"
                min="0"
                placeholder="100"
                value={minStock}
                onChange={(e) => handleIntegerInput(e.target.value, setMinStock)}
                onKeyDown={handleIntegerKeyDown}
                className="bg-white border-border"
              />
            </div>

            {/* Sorting */}
            <div className="space-y-3 p-4 rounded-lg bg-secondary border border-border">
              <h3 className="font-semibold text-sm md:text-base text-foreground">並べ替え</h3>
              <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
                <div className="space-y-2">
                  <Label htmlFor="sortBy" className="text-foreground text-sm">
                    ソート基準
                  </Label>
                  <Select value={sortBy} onValueChange={setSortBy}>
                    <SelectTrigger id="sortBy" className="bg-white border-border">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent className="bg-white">
                      {SORT_OPTIONS.map((option) => (
                        <SelectItem key={option.value} value={option.value}>
                          {option.label}
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>
                <div className="space-y-2">
                  <Label htmlFor="sortOrder" className="text-foreground text-sm">
                    ソート方式
                  </Label>
                  <Select value={sortOrder} onValueChange={setSortOrder}>
                    <SelectTrigger id="sortOrder" className="bg-white border-border">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent className="bg-white">
                      <SelectItem value="asc">昇順</SelectItem>
                      <SelectItem value="desc">降順</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
              </div>
              
              {/* Clear Filters Button */}
              <div className="flex justify-end">
                <Button
                  type="button"
                  variant="outline"
                  size="sm"
                  onClick={() => {
                    setPriceFrom("")
                    setPriceTo("")
                    setMaxLength("")
                    setMaxWidth("")
                    setMaxHeight("")
                    setMaxWeight("")
                    setMinStock("")
                    setMinRating("")
                    setMinRepurchaseRate("")
                    setMinMonthlySales("")
                    setFilterOptions([])
                    setRegionOpp("jpOpp")
                    setSortBy("monthSold")
                    setSortOrder("desc")
                    setSearchCount("50")
                    setStartPage("1")
                    setEndPage("5")
                  }}
                  className="text-xs"
                >
                  フィルターをクリア
                </Button>
              </div>
            </div>
          </div>
        </ScrollArea>

        {/* Progress Bar */}
        {(isLoading || searchStatus !== 'idle') && (
          <div className="px-4 py-3 border-t border-border bg-secondary/50">
            <RakumartProgressBar
              progress={searchProgress}
              status={searchStatus}
              currentStep={currentStep}
              className="w-full"
            />
          </div>
        )}

        {/* Active Filters Summary */}
        {(priceFrom || priceTo || filterOptions.length > 0 || regionOpp || minRating || minRepurchaseRate || minMonthlySales) && (
          <div className="px-4 py-3 border-t border-border bg-muted/30">
            <h4 className="text-sm font-medium text-foreground mb-2">適用中のフィルター:</h4>
            <div className="flex flex-wrap gap-2">
              {priceFrom && (
                <Badge variant="secondary" className="text-xs">
                  価格: {priceFrom}円以上
                </Badge>
              )}
              {priceTo && (
                <Badge variant="secondary" className="text-xs">
                  価格: {priceTo}円以下
                </Badge>
              )}
              {regionOpp && (
                <Badge variant="secondary" className="text-xs">
                  {regionOpp === "jpOpp" ? "日本ホット商品" : regionOpp === "krOpp" ? "韓国ホット商品" : "すべての地域"}
                </Badge>
              )}
              {minRating && minRating.trim() !== "" && (
                <Badge variant="secondary" className="text-xs">
                  最低評価点: {minRating}以上
                </Badge>
              )}
              {minRepurchaseRate && minRepurchaseRate.trim() !== "" && (
                <Badge variant="secondary" className="text-xs">
                  最低再購入率: {minRepurchaseRate}%以上
                </Badge>
              )}
              {minMonthlySales && minMonthlySales.trim() !== "" && (
                <Badge variant="secondary" className="text-xs">
                  月間最低販売数: {minMonthlySales}以上
                </Badge>
              )}
              {filterOptions.map((filter) => (
                <Badge key={filter} variant="secondary" className="text-xs">
                  {filter === "certifiedFactory" ? "認定工場" :
                   filter === "shipIn48Hours" ? "48時間以内発送" :
                   filter === "freeShipping" ? "送料無料" :
                   filter === "newArrival" ? "新着商品" :
                   filter === "hotSale" ? "ホットセール" :
                   filter === "qualityAssured" ? "品質保証" : filter}
                </Badge>
              ))}
            </div>
          </div>
        )}

        <div className="flex flex-col-reverse sm:flex-row justify-end gap-2 sm:gap-3 pt-4 border-t border-border flex-shrink-0 bg-white">
          <Button
            variant="outline"
            onClick={() => onOpenChange(false)}
            className="w-full sm:w-auto border-border hover:bg-muted"
          >
            キャンセル
          </Button>
          {isLoading ? (
            <Button 
              onClick={handleCancelSearch} 
              className="gap-2 w-full sm:w-auto bg-destructive hover:bg-destructive/90 shadow-md" 
            >
              <span className="flex items-center gap-2">
                <Square className="h-4 w-4" />
                <span>検索中止</span>
              </span>
            </Button>
          ) : (
            <Button 
              onClick={handleSearch} 
              className="gap-2 w-full sm:w-auto bg-primary hover:bg-primary/90 shadow-md" 
            >
              <span className="flex items-center gap-2">
                <Search className="h-4 w-4" />
                <span>検索開始</span>
              </span>
            </Button>
          )}
        </div>
      </DialogContent>
    </Dialog>
  )
}
