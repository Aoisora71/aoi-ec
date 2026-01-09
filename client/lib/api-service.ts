/**
 * API service for communicating with the Licel Store backend
 */

// Determine API base URL
// Priority:
// 1. Environment variable
// 2. Auto-detect local dev (Next.js on port 6009 -> backend on 8000)
// 3. Auto-detect localhost
// 4. Use relative URLs on domain (Caddy will proxy)
// 5. Default remote server
const getApiBaseUrl = () => {
  if (process.env.NEXT_PUBLIC_API_URL) {
    return process.env.NEXT_PUBLIC_API_URL
  }
  
  // Auto-detect environment in browser
  if (typeof window !== 'undefined') {
    const hostname = window.location.hostname
    const port = window.location.port

    // Local development: Next.js dev or start on port 6009, backend on 8000
    if (port === '6009') {
      return 'http://localhost:8000'
    }

    // Explicit localhost access
    if (hostname === 'localhost' || hostname === '127.0.0.1') {
      return 'http://localhost:8000'
    }

    // Domain access (not localhost and not the hardcoded IP):
    // use relative URLs so Caddy (or another reverse proxy) can route to backend.
    // Endpoints already include /api/ prefix, so base URL should be empty.
    if (hostname !== '162.43.44.223') {
      return ''
    }
  }
  
  // Default to remote server
  return 'http://162.43.44.223:8000'
}

const API_BASE_URL = getApiBaseUrl()

// Log API base URL for debugging
if (typeof window !== 'undefined') {
  console.log('API Base URL:', API_BASE_URL)
}

// Debug logging

export interface ProductSearchRequest {
  keyword: string
  page?: number
  page_size?: number
  price_min?: string
  price_max?: string
  jpy_price_min?: number
  jpy_price_max?: number
  exchange_rate?: number
  strict_mode?: boolean
  max_length?: number
  max_width?: number
  max_height?: number
  max_weight?: number
  min_inventory?: number
  max_delivery_days?: number
  max_shipping_fee?: number
  shop_type?: string
  with_detail?: boolean
  detail_limit?: number
  save_to_db?: boolean
  categories?: string[]
  subcategories?: string[]
}

export interface KeywordSearchRequest {
  keywords: string
  shop_type?: string
  page?: number
  page_size?: number
  price_start?: string
  price_end?: string
  sort_field?: string
  sort_order?: string
  region_opp?: string
  filter?: string
  category_id?: string
  min_rating?: number
  min_repurchase_rate?: number
  min_monthly_sales?: number
  save_to_db?: boolean
}

export interface MultiCategorySearchRequest {
  category_ids: string[]
  shop_type?: string
  page?: number
  page_size?: number
  max_products_per_category?: number
  price_start?: string
  price_end?: string
  sort_field?: string
  sort_order?: string
  region_opp?: string
  filter?: string
  min_rating?: number
  min_repurchase_rate?: number
  min_monthly_sales?: number
  save_to_db?: boolean
}

export interface ProductResponse {
  success: boolean
  data?: any[]
  total?: number
  message?: string
  error?: string
}

export interface DatabaseResponse {
  success: boolean
  saved_count?: number
  message?: string
  error?: string
}

export interface SettingsData {
  // Pricing Settings
  exchange_rate: number
  profit_margin_percent: number
  sales_commission_percent: number
  currency: string
  
  // Purchase Price Calculation Settings
  domestic_shipping_cost: number
  domestic_shipping_costs?: {
    regular: number
    size60: number
    size80: number
    size100: number
  }
  international_shipping_rate: number
  customs_duty_rate: number
  
  // Server Settings
  auto_refresh: boolean
  refresh_interval: number
  api_timeout: number
  max_retries: number
  
  // Logging Settings
  logging_enabled: boolean
  log_level: string
  max_log_entries: number
  log_retention_days: number
  
  // Database Settings
  database_url?: string
  connection_pool_size: number
  query_timeout: number
  enable_backup: boolean
  
  // Login Information
  rakumart_api_key?: string
  rakumart_api_secret?: string
  enable_api_key_rotation: boolean
  session_timeout: number
  
  // User Login Information
  username?: string
  email?: string
  password?: string
}

export interface SettingsResponse {
  success: boolean
  settings?: SettingsData
  error?: string
}

export interface CategoryAttributeGroup {
  name: string
  values: string[]
}

export interface CategoryRecord {
  id: number
  category_name: string
  category_ids: string[]
  rakuten_category_ids?: string[]
  genre_id?: string | null
  attributes?: CategoryAttributeGroup[]
  primary_category_id?: number | null
  primary_category_name?: string | null
  weight?: number | null
  length?: number | null
  width?: number | null
  height?: number | null
  size_option?: string | null
  size?: number | null
  created_at: string
  updated_at: string
}

export interface CategoryListResponse {
  success: boolean
  categories?: CategoryRecord[]
  error?: string
}

export interface CategoryMutationResponse {
  success: boolean
  category?: CategoryRecord
  error?: string
}

export interface PrimaryCategoryRecord {
  id: number
  category_name: string
  default_category_ids: string[]
  created_at: string
  updated_at: string
}

export interface PrimaryCategoryListResponse {
  success: boolean
  categories?: PrimaryCategoryRecord[]
  error?: string
}

export interface PrimaryCategoryMutationResponse {
  success: boolean
  category?: PrimaryCategoryRecord
  error?: string
}

export interface LogEntry {
  id: string
  timestamp: string
  level: string
  message: string
  details?: string
  source: string
}

export interface LogsResponse {
  success: boolean
  logs?: LogEntry[]
  total_count?: number
  error?: string
}

class ApiService {
  private async request<T>(
    endpoint: string,
    options: RequestInit = {},
    opts: { throwOnError?: boolean; timeout?: number; signal?: AbortSignal } = {}
  ): Promise<T> {
    const url = `${API_BASE_URL}${endpoint}`
    
    const defaultOptions: RequestInit = {
      headers: {
        'Content-Type': 'application/json',
        ...options.headers,
      },
    }

    // Add timeout to prevent hanging requests
    // Default timeout: 30 seconds, but can be overridden for long-running operations
    const timeout = opts.timeout || 30000
    const controller = new AbortController()
    let timeoutId: NodeJS.Timeout | null = null
    
    // 外部のAbortSignalがある場合は、それも監視
    if (opts.signal) {
      opts.signal.addEventListener('abort', () => {
        controller.abort()
      })
    }
    
    try {
      console.log(`[API] Making request to: ${url}`, { 
        method: options.method || 'GET',
        headers: defaultOptions.headers,
        timeout: timeout
      })
      
      timeoutId = setTimeout(() => {
        controller.abort()
      }, timeout)
      
      // 外部のsignalがある場合は、それとタイムアウト用のsignalを結合
      // どちらかがabortされたら、リクエストを中止
      let finalSignal: AbortSignal = controller.signal
      if (opts.signal || options.signal) {
        // 外部のsignalがある場合、両方のsignalを監視
        const externalSignal = opts.signal || options.signal
        if (externalSignal) {
          // 外部signalがabortされたら、内部controllerもabort
          externalSignal.addEventListener('abort', () => {
            controller.abort()
          })
          finalSignal = externalSignal
        }
      }
      
      const response = await fetch(url, { 
        ...defaultOptions, 
        ...options,
        signal: finalSignal
      })
      
      console.log(`[API] Response status: ${response.status} ${response.statusText} for ${url}`)
      
      // Clear timeout if request completed successfully
      if (timeoutId) {
        clearTimeout(timeoutId)
        timeoutId = null
      }

      if (!response.ok) {
        let errorData: any = {}
        try {
          const text = await response.text()
          if (text) {
            errorData = JSON.parse(text)
          }
        } catch {
          errorData = { detail: `HTTP ${response.status}: ${response.statusText}` }
        }
        
        // Extract error message from various possible fields
        const errorMessage = errorData.detail || errorData.error || errorData.message || `HTTP ${response.status}: ${response.statusText}`
        
        if (opts.throwOnError === false) {
          // @ts-expect-error allow returning error-like object for callers to handle
          return { success: false, error: errorMessage, message: errorMessage, detail: errorData.detail }
        }
        
        // Create error object with detail for better error handling
        const error = new Error(errorMessage) as any
        error.detail = errorData.detail
        error.status = response.status
        throw error
      }

      // Parse response with better error handling
      const responseText = await response.text()
      if (!responseText || responseText.trim() === '') {
        console.error(`Empty response from ${url}`)
        if (opts.throwOnError === false) {
          // @ts-expect-error allow returning error-like object for callers to handle
          return { success: false, error: 'サーバーからの応答が空です', message: 'サーバーから空の応答が返されました' }
        }
        throw new Error(`サーバーからの応答が空です`)
      }

      try {
        return JSON.parse(responseText)
      } catch (parseError) {
        console.error(`Failed to parse JSON response from ${url}:`, responseText)
        if (opts.throwOnError === false) {
          // @ts-expect-error allow returning error-like object for callers to handle
          return { success: false, error: 'サーバーからのJSON応答が無効です', message: 'サーバーから無効なJSON応答が返されました' }
        }
        throw new Error(`サーバーからのJSON応答が無効です`)
      }
    } catch (error: any) {
      // Always clear timeout in case of error
      if (timeoutId) {
        clearTimeout(timeoutId)
        timeoutId = null
      }
      
      console.error(`[API] Request failed for ${url}:`, {
        error: error.message,
        name: error.name,
        stack: error.stack,
        url,
        endpoint
      })
      
      // Handle AbortError (timeout)
      if (error.name === 'AbortError' || error.message?.includes('aborted')) {
        const timeoutSeconds = Math.round(timeout / 1000)
        const timeoutError = { 
          success: false, 
          error: 'リクエストタイムアウト', 
          message: `サーバーの応答がタイムアウトしました（${timeoutSeconds}秒）。処理に時間がかかっている可能性があります。しばらく待ってから再試行してください。` 
        }
        if (opts.throwOnError === false) {
          // @ts-expect-error allow returning error-like object for callers to handle
          return timeoutError
        }
        throw new Error(timeoutError.message)
      }
      
      // Handle network errors
      if (error instanceof TypeError && error.message.includes('Failed to fetch')) {
        // Check if we're in browser environment
        const isBrowser = typeof window !== 'undefined'
        const isLocalhost = isBrowser && (window.location.hostname === 'localhost' || window.location.hostname === '127.0.0.1')
        
        // Suggest localhost if we're running locally but trying to connect to remote server
        let suggestedUrl = API_BASE_URL
        if (isLocalhost && API_BASE_URL.includes('162.43.44.223')) {
          suggestedUrl = 'http://localhost:8000'
        }
        
        const networkError = {
          success: false,
          error: 'ネットワークエラー',
          message: `バックエンドサーバーに接続できません (${API_BASE_URL})。\n` +
                   `サーバーが起動していることを確認してください。\n` +
                   (isLocalhost ? `ローカル環境の場合は ${suggestedUrl} を試してください。` : '') +
                   `\nエラー詳細: ${error.message}`
        }
        console.error('Network error details:', {
          url: `${API_BASE_URL}${endpoint}`,
          error: error.message,
          isBrowser,
          isLocalhost,
          suggestedUrl
        })
        if (opts.throwOnError === false) {
          // @ts-expect-error allow returning error-like object for callers to handle
          return networkError
        }
        throw new Error(networkError.message)
      }
      
      // For other errors, handle based on throwOnError option
      if (opts.throwOnError === false) {
        // @ts-expect-error allow returning error-like object for callers to handle
        return { success: false, error: error.message || 'Unknown error', message: error.message || 'リクエスト中にエラーが発生しました' }
      }
      
      throw error
    }
  }

  // ============================================================================
  // Connection Test
  // ============================================================================

  async testConnection(): Promise<{ status: string; service: string }> {
    try {
      const result = await this.request<{ status: string; service: string }>('/api/health', {
        method: 'GET',
      }, { throwOnError: false })
      return result
    } catch (error: any) {
      console.error('Connection test failed:', error)
      throw error
    }
  }

  // ============================================================================
  // Authentication Methods
  // ============================================================================

  async login(email: string, password: string): Promise<{ access_token: string; refresh_token: string; user: any }> {
    return this.request('/api/auth/login', {
      method: 'POST',
      body: JSON.stringify({ email, password }),
    })
  }

  async signup(email: string, password: string, name: string): Promise<{ access_token: string; refresh_token: string; user: any }> {
    return this.request('/api/auth/signup', {
      method: 'POST',
      body: JSON.stringify({ email, password, name }),
    })
  }

  async verifyToken(token: string): Promise<{ valid: boolean; user: any }> {
    return this.request('/api/auth/verify', {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${token}`,
      },
    }, { throwOnError: false })
  }

  async refreshToken(refreshToken: string): Promise<{ access_token: string }> {
    return this.request('/api/auth/refresh', {
      method: 'POST',
      body: JSON.stringify({ refresh_token: refreshToken }),
    })
  }

  async getCurrentUser(token: string): Promise<{ user: any }> {
    return this.request('/api/auth/me', {
      method: 'GET',
      headers: {
        'Authorization': `Bearer ${token}`,
      },
    })
  }

  // ============================================================================
  // Product Methods
  // ============================================================================

  async searchProducts(request: ProductSearchRequest, signal?: AbortSignal): Promise<ProductResponse> {
    return this.request<ProductResponse>('/api/products/search', {
      method: 'POST',
      body: JSON.stringify(request),
      signal,
    }, { signal })
  }

  async keywordSearch(request: KeywordSearchRequest, signal?: AbortSignal): Promise<ProductResponse> {
    return this.request<ProductResponse>('/api/products/keyword-search', {
      method: 'POST',
      body: JSON.stringify(request),
      signal,
    }, { signal })
  }

  async multiCategorySearch(request: MultiCategorySearchRequest, signal?: AbortSignal): Promise<ProductResponse> {
    try {
      // Validate request before sending
      if (!request.category_ids || request.category_ids.length === 0) {
        console.error('No category IDs provided in request')
        return {
          success: false,
          error: 'No category IDs provided',
          message: 'At least one category ID is required'
        }
      }
      
      const response = await this.request<ProductResponse>('/api/products/multi-category-search', {
        method: 'POST',
        body: JSON.stringify(request),
        signal,
      }, { throwOnError: false, signal })
      
      
      // Validate response structure
      if (!response || typeof response !== 'object' || Object.keys(response).length === 0) {
        console.error('Invalid or empty response structure:', response)
        return {
          success: false,
          error: 'Invalid or empty response from server',
          message: 'Server returned an invalid or empty response'
        }
      }
      
      // Ensure response has success property
      if (response.success === undefined || response.success === null) {
        console.error('Response missing success property:', response)
        return {
          success: false,
          error: 'Response missing success property',
          message: 'Server response format is invalid'
        }
      }
      
      return response
    } catch (error) {
      console.error('MultiCategorySearch error:', error)
      const errorMessage = error instanceof Error ? error.message : 'Unknown error occurred'
      return {
        success: false,
        error: errorMessage,
        message: `Failed to perform multi-category search: ${errorMessage}`
      }
    }
  }

  async getProductsFromDatabase(
    limit: number = 50,
    offset: number = 0,
    keyword?: string
  ): Promise<ProductResponse> {
    const params = new URLSearchParams({
      limit: limit.toString(),
      offset: offset.toString(),
    })
    
    if (keyword) {
      params.append('keyword', keyword)
    }

    return this.request<ProductResponse>(`/api/products?${params}`)
  }

  async optimizeProductNames(
    limit?: number,
    dryRun: boolean = false
  ): Promise<DatabaseResponse> {
    return this.request<DatabaseResponse>('/api/products/optimize-names', {
      method: 'POST',
      body: JSON.stringify({ limit, dry_run: dryRun }),
    })
  }

  async fixDatabaseSchema(): Promise<DatabaseResponse> {
    return this.request<DatabaseResponse>('/api/database/fix-schema', {
      method: 'POST',
    })
  }

  async dropRemovedColumns(): Promise<DatabaseResponse> {
    return this.request<DatabaseResponse>('/api/database/drop-removed-columns', {
      method: 'POST',
    })
  }

  async resetProductManagement(): Promise<DatabaseResponse> {
    return this.request<DatabaseResponse>('/api/database/reset-product-management', {
      method: 'POST',
    })
  }

  async healthCheck(): Promise<{ status: string; service: string }> {
    return this.request<{ status: string; service: string }>('/health')
  }

  async fullHealth(): Promise<any> {
    return this.request<any>('/health/full', {}, { throwOnError: false })
  }

  // Settings methods
  async updateProductHideItem(itemNumber: string, hideItem: boolean): Promise<{ success: boolean; message?: string; error?: string }> {
    return this.request<{ success: boolean; message?: string; error?: string }>(`/api/product-management/${itemNumber}/hide-item?hide_item=${hideItem}`, {
      method: 'PATCH',
    })
  }

  async updateProductBlock(itemNumber: string, block: boolean): Promise<{ success: boolean; message?: string; error?: string }> {
    return this.request<{ success: boolean; message?: string; error?: string }>(`/api/product-management/${itemNumber}/block?block=${block}`, {
      method: 'PATCH',
    })
  }

  async updateProductsHideItemBatch(itemNumbers: string[], hideItem: boolean): Promise<{ success: boolean; message?: string; error?: string; updated_count?: number }> {
    return this.request<{ success: boolean; message?: string; error?: string; updated_count?: number }>('/api/product-management/batch-hide-item', {
      method: 'PATCH',
      body: JSON.stringify({
        item_numbers: itemNumbers,
        hide_item: hideItem,
      }),
    })
  }

  async updateAllProductsHideItem(hideItem: boolean): Promise<{ success: boolean; message?: string; error?: string; updated_count?: number }> {
    return this.request<{ success: boolean; message?: string; error?: string; updated_count?: number }>(`/api/product-management/all-hide-item?hide_item=${hideItem}`, {
      method: 'PATCH',
    })
  }

  async updateProductSettings(itemNumber: string, settings: {
    title?: string
    item_type?: string
    genre_id?: string | null
    tags?: number[]
    unlimited_inventory_flag?: boolean
    features?: {
      searchVisibility?: string
      inventoryDisplay?: string
      review?: string
    }
    payment?: {
      taxIncluded?: boolean
      taxRate?: number
      cashOnDeliveryFeeIncluded?: boolean
    }
    normalDeliveryDateId?: number
  }): Promise<{ success: boolean; message?: string; error?: string }> {
    return this.request<{ success: boolean; message?: string; error?: string }>(`/api/product-management/${itemNumber}/settings`, {
      method: 'PATCH',
      body: JSON.stringify(settings),
    })
  }

  async deleteProduct(productId: string): Promise<{ success: boolean; message?: string; error?: string }> {
    return this.request(`/api/products/${productId}`, {
      method: 'DELETE',
    })
  }

  async deleteProductsBatch(productIds: string[]): Promise<{ success: boolean; message?: string; error?: string; deleted_count?: number }> {
    return this.request('/api/products/batch-delete', {
      method: 'POST',
      body: JSON.stringify({ product_ids: productIds }),
    })
  }

  async deleteProductManagement(itemNumber: string): Promise<{ success: boolean; message?: string; error?: string; deleted_count?: number }> {
    return this.request(`/api/product-management/${itemNumber}`, {
      method: 'DELETE',
    })
  }

  async deleteProductManagementBatch(itemNumbers: string[]): Promise<{ success: boolean; message?: string; error?: string; deleted_count?: number }> {
    return this.request('/api/product-management/batch-delete', {
      method: 'POST',
      body: JSON.stringify({ item_numbers: itemNumbers }),
    })
  }

  async getSettings(): Promise<SettingsResponse> {
    return this.request<SettingsResponse>('/api/settings', {}, { throwOnError: false })
  }

  async updateSettings(settings: SettingsData): Promise<SettingsResponse> {
    return this.request<SettingsResponse>('/api/settings', {
      method: 'POST',
      body: JSON.stringify(settings),
    })
  }

  // Category management
  async getCategories(): Promise<CategoryListResponse> {
    return this.request<CategoryListResponse>('/api/settings/categories', {}, { throwOnError: false })
  }

  async getPrimaryCategories(): Promise<PrimaryCategoryListResponse> {
    return this.request<PrimaryCategoryListResponse>('/api/settings/primary-categories', {}, { throwOnError: false })
  }

  async createPrimaryCategory(payload: { category_name: string; default_category_ids?: string[] }): Promise<PrimaryCategoryMutationResponse> {
    return this.request<PrimaryCategoryMutationResponse>('/api/settings/primary-categories', {
      method: 'POST',
      body: JSON.stringify(payload),
    }, { throwOnError: false })
  }

  async updatePrimaryCategory(categoryId: number, payload: { category_name?: string; default_category_ids?: string[] }): Promise<PrimaryCategoryMutationResponse> {
    return this.request<PrimaryCategoryMutationResponse>(`/api/settings/primary-categories/${categoryId}`, {
      method: 'PUT',
      body: JSON.stringify(payload),
    }, { throwOnError: false })
  }

  async deletePrimaryCategory(categoryId: number): Promise<PrimaryCategoryMutationResponse> {
    return this.request<PrimaryCategoryMutationResponse>(`/api/settings/primary-categories/${categoryId}`, {
      method: 'DELETE',
    }, { throwOnError: false })
  }

  async createCategory(payload: {
    category_name: string
    category_ids: string[]
    attributes?: CategoryAttributeGroup[]
    primary_category_id?: number | null
    genre_id?: string | null
    weight?: number | null
    length?: number | null
    width?: number | null
    height?: number | null
  }): Promise<CategoryMutationResponse> {
    return this.request<CategoryMutationResponse>('/api/settings/categories', {
      method: 'POST',
      body: JSON.stringify(payload),
    }, { throwOnError: false })
  }

  async updateCategory(
    categoryId: number,
    payload: {
      category_name?: string
      category_ids?: string[]
      attributes?: CategoryAttributeGroup[]
      primary_category_id?: number | null
      genre_id?: string | null
      weight?: number | null
      length?: number | null
      width?: number | null
      height?: number | null
    }
  ): Promise<CategoryMutationResponse> {
    return this.request<CategoryMutationResponse>(`/api/settings/categories/${categoryId}`, {
      method: 'PUT',
      body: JSON.stringify(payload),
    }, { throwOnError: false })
  }

  async deleteCategory(categoryId: number): Promise<CategoryMutationResponse> {
    return this.request<CategoryMutationResponse>(`/api/settings/categories/${categoryId}`, {
      method: 'DELETE',
    }, { throwOnError: false })
  }

  async exportCategories(): Promise<Blob> {
    const url = `${API_BASE_URL}/api/settings/categories/export`
    const response = await fetch(url, {
      method: 'GET',
    })
    if (!response.ok) {
      const errorText = await response.text()
      throw new Error(`Failed to export categories: ${errorText}`)
    }
    return await response.blob()
  }

  async importCategories(file: File): Promise<{
    success: boolean
    imported?: number
    updated?: number
    errors?: string[]
    message?: string
    error?: string
  }> {
    const formData = new FormData()
    formData.append('file', file)
    
    const url = `${API_BASE_URL}/api/settings/categories/import`
    const response = await fetch(url, {
      method: 'POST',
      body: formData,
    })
    
    if (!response.ok) {
      const errorText = await response.text()
      throw new Error(`Failed to import categories: ${errorText}`)
    }
    
    return await response.json()
  }

  async exportPrimaryCategories(): Promise<Blob> {
    const url = `${API_BASE_URL}/api/settings/primary-categories/export`
    const response = await fetch(url, {
      method: 'GET',
    })
    if (!response.ok) {
      const errorText = await response.text()
      throw new Error(`Failed to export primary categories: ${errorText}`)
    }
    return await response.blob()
  }

  async importPrimaryCategories(file: File): Promise<{
    success: boolean
    imported?: number
    updated?: number
    errors?: string[]
    message?: string
    error?: string
  }> {
    const formData = new FormData()
    formData.append('file', file)
    
    const url = `${API_BASE_URL}/api/settings/primary-categories/import`
    const response = await fetch(url, {
      method: 'POST',
      body: formData,
    })
    
    if (!response.ok) {
      const errorText = await response.text()
      throw new Error(`Failed to import primary categories: ${errorText}`)
    }
    
    return await response.json()
  }

  // Logs methods
  async getLogs(limit?: number): Promise<LogsResponse> {
    const params = limit ? `?limit=${limit}` : ''
    return this.request<LogsResponse>(`/api/logs${params}`, {}, { throwOnError: false })
  }

  async clearLogs(): Promise<{ success: boolean; message?: string; error?: string }> {
    return this.request<{ success: boolean; message?: string; error?: string }>('/api/logs', {
      method: 'DELETE',
    }, { throwOnError: false })
  }
  
  // Product management
  async registerProductsToManagement(productIds: string[]): Promise<{ success: boolean; saved_count?: number; message?: string; error?: string }> {
    // Product registration can take a long time, so increase timeout to 5 minutes
    return this.request('/api/product-management/register', {
      method: 'POST',
      body: JSON.stringify({ product_ids: productIds }),
    }, { throwOnError: false, timeout: 300000 }) // 5 minutes (300 seconds)
  }

  async updateVariantsOnly(productIds: string[]): Promise<{ success: boolean; saved_count?: number; message?: string; error?: string }> {
    return this.request('/api/product-management/update-variants', {
      method: 'POST',
      body: JSON.stringify({ product_ids: productIds }),
    }, { throwOnError: false })
  }

  async updateProductSku(
    itemNumber: string,
    data: {
      variant_selectors?: Array<{
        key: string
        displayName: string
        values: Array<{ displayValue: string }>
      }>
      variants?: Record<string, {
        selectorValues?: Record<string, string>
        standardPrice?: string
        articleNumber?: string
        images?: Array<{ type: string; location: string }>
        attributes?: Record<string, string>
        shipping?: {
          postageIncluded?: boolean
          postageSegment?: number
        }
        features?: {
          restockNotification?: boolean
          displayNormalCartButton?: boolean
        }
      }>
    }
  ): Promise<{ success: boolean; message?: string; error?: string }> {
    return this.request(`/api/product-management/${itemNumber}/sku`, {
      method: 'PATCH',
      body: JSON.stringify(data),
    }, { throwOnError: false })
  }

  async updateSingleVariant(
    itemNumber: string,
    skuId: string,
    variantData: {
      selectorValues?: Record<string, string>
      standardPrice?: string
      articleNumber?: string
      shipping?: {
        postageIncluded?: boolean
        postageSegment?: number
      }
      features?: {
        restockNotification?: boolean
        displayNormalCartButton?: boolean
      }
    }
  ): Promise<{ success: boolean; message?: string; error?: string }> {
    return this.request(`/api/product-management/${itemNumber}/variant/${skuId}`, {
      method: 'PATCH',
      body: JSON.stringify(variantData),
    }, { throwOnError: false })
  }

  async getProductManagement(
    limit: number = 50, 
    offset: number = 0, 
    sortBy?: string, 
    sortOrder?: string,
    search?: string
  ): Promise<ProductResponse> {
    const params = new URLSearchParams({ 
      limit: String(limit), 
      offset: String(offset) 
    })
    if (sortBy) {
      params.append('sort_by', sortBy)
    }
    if (sortOrder) {
      params.append('sort_order', sortOrder)
    }
    if (search && search.trim()) {
      params.append('search', search.trim())
    }
    return this.request<ProductResponse>(`/api/product-management?${params.toString()}`, {}, { throwOnError: false })
  }

  async exportProductManagementCSV(itemNumbers?: string[]): Promise<Blob> {
    const response = await fetch(`${API_BASE_URL}/api/product-management/export-csv`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        item_numbers: itemNumbers || []
      }),
    })
    if (!response.ok) {
      throw new Error(`Failed to export CSV: ${response.statusText}`)
    }
    return response.blob()
  }

  async importProductManagementCSV(file: File): Promise<{ success: boolean; updated_count?: number; error_count?: number; errors?: string[]; message?: string }> {
    const formData = new FormData()
    formData.append('file', file)
    
    const response = await fetch(`${API_BASE_URL}/api/product-management/import-csv`, {
      method: 'POST',
      body: formData,
    })
    
    if (!response.ok) {
      const error = await response.json().catch(() => ({ error: response.statusText }))
      throw new Error(error.error || `Failed to import CSV: ${response.statusText}`)
    }
    
    return response.json()
  }

  async registerProductToRakuten(itemNumber: string): Promise<{ success: boolean; message?: string; error?: string; error_details?: string; skipped?: boolean }> {
    return this.request('/api/product-management/register-to-rakuten', {
      method: 'POST',
      body: JSON.stringify({ item_number: itemNumber }),
    }, { throwOnError: false })
  }

  async registerMultipleProductsToRakuten(itemNumbers: string[]): Promise<{ 
    success: boolean
    total_count?: number
    success_count?: number
    failure_count?: number
    results?: Array<{
      item_number: string
      success: boolean
      message?: string
      error?: string
      error_details?: string
    }>
    message?: string
    error?: string
  }> {
    // Multiple product registration can take a long time, increase timeout to 10 minutes
    return this.request('/api/product-management/register-multiple-to-rakuten', {
      method: 'POST',
      body: JSON.stringify({ item_numbers: itemNumbers }),
    }, { throwOnError: false, timeout: 600000 }) // 10 minutes (600 seconds)
  }

  async updateChangesToRakuten(): Promise<{ 
    success: boolean
    total_count?: number
    success_count?: number
    failure_count?: number
    results?: Array<{
      item_number: string
      success: boolean
      message?: string
      error?: string
      error_details?: string
    }>
    message?: string
    error?: string
  }> {
    // Update changes can take a long time, increase timeout to 10 minutes
    return this.request('/api/product-management/update-changes-to-rakuten', {
      method: 'POST',
      body: JSON.stringify({}),
    }, { throwOnError: false, timeout: 600000 }) // 10 minutes (600 seconds)
  }

  async registerInventoryToRakuten(itemNumber: string): Promise<{ 
    success: boolean
    message?: string
    error?: string
    error_details?: string
    registered_count?: number
    failed_count?: number
    total_count?: number
    results?: any[]
    errors?: any[]
  }> {
    return this.request('/api/product-management/register-inventory-to-rakuten', {
      method: 'POST',
      body: JSON.stringify({ item_number: itemNumber }),
    }, { throwOnError: false })
  }

  async checkProductRegistrationStatus(itemNumber: string): Promise<{ 
    success: boolean
    status?: string
    previous_status?: string
    new_status?: string
    message?: string
    error?: string
    status_code?: number
    error_data?: any
  }> {
    return this.request('/api/product-management/check-registration-status', {
      method: 'POST',
      body: JSON.stringify({ item_number: itemNumber }),
    }, { throwOnError: false })
  }

  async checkMultipleProductsRegistrationStatus(itemNumbers: string[]): Promise<{ 
    success: boolean
    total?: number
    success_count?: number
    error_count?: number
    results?: Array<{
      item_number: string
      success: boolean
      status?: string
      previous_status?: string
      new_status?: string
      message?: string
      error?: string
    }>
    message?: string
    error?: string
  }> {
    // Multiple status checks can take time, increase timeout to 3 minutes
    return this.request('/api/product-management/check-multiple-registration-status', {
      method: 'POST',
      body: JSON.stringify({ item_numbers: itemNumbers }),
    }, { throwOnError: false, timeout: 180000 }) // 3 minutes (180 seconds)
  }

  async registerMultipleInventoryToRakuten(itemNumbers: string[]): Promise<{ 
    success: boolean
    total_products?: number
    success_products?: number
    failure_products?: number
    total_registered_variants?: number
    total_failed_variants?: number
    total_variants?: number
    results?: Array<{
      item_number: string
      success: boolean
      registered_count?: number
      failed_count?: number
      total_count?: number
      message?: string
      error?: string
      error_details?: string
      results?: any[]
      errors?: any[]
    }>
    message?: string
    error?: string
  }> {
    // Multiple inventory registration can take a long time, increase timeout to 10 minutes
    return this.request('/api/product-management/register-multiple-inventory-to-rakuten', {
      method: 'POST',
      body: JSON.stringify({ item_numbers: itemNumbers }),
    }, { throwOnError: false, timeout: 600000 }) // 10 minutes (600 seconds)
  }

  async deleteProductImage(itemNumber: string, location: string): Promise<{ success: boolean; message?: string; error?: string }> {
    return this.request(`/api/product-management/${itemNumber}/images`, {
      method: 'DELETE',
      body: JSON.stringify({ location }),
    }, { throwOnError: false })
  }

  async uploadProductImagesToRakuten(itemNumber: string): Promise<{ 
    success: boolean
    message?: string
    error?: string
    total?: number
    uploaded_count?: number
    failed_count?: number
    uploaded_files?: any[]
    folder_id?: number
    folder_name?: string
    errors?: string[]
  }> {
    return this.request(`/api/product-management/${itemNumber}/upload-images-to-rakuten`, {
      method: 'POST',
    }, { throwOnError: false })
  }

  async uploadMultipleImagesToRakuten(itemNumbers: string[]): Promise<{ 
    success: boolean
    total_products?: number
    success_products?: number
    failure_products?: number
    total_uploaded_images?: number
    total_failed_images?: number
    total_images?: number
    results?: Array<{
      item_number: string
      success: boolean
      uploaded_count?: number
      failed_count?: number
      total_count?: number
      message?: string
      error?: string
      uploaded_files?: any[]
      folder_id?: number
      folder_name?: string
      errors?: any[]
    }>
    message?: string
    error?: string
  }> {
    // Multiple image upload can take a long time, increase timeout to 10 minutes
    return this.request('/api/product-management/upload-multiple-images-to-rakuten', {
      method: 'POST',
      body: JSON.stringify({ item_numbers: itemNumbers }),
    }, { throwOnError: false, timeout: 600000 }) // 10 minutes (600 seconds)
  }

  async deleteProductFromRakuten(itemNumber: string): Promise<{ 
    success: boolean
    message?: string
    error?: string
    error_details?: string
  }> {
    return this.request(`/api/product-management/${itemNumber}/delete-from-rakuten`, {
      method: 'DELETE',
    }, { throwOnError: false })
  }

  async deleteMultipleProductsFromRakuten(itemNumbers: string[]): Promise<{ 
    success: boolean
    total_count?: number
    success_count?: number
    failure_count?: number
    results?: Array<{
      item_number: string
      success: boolean
      message?: string
      error?: string
      error_details?: string
    }>
    message?: string
    error?: string
  }> {
    return this.request('/api/product-management/delete-multiple-from-rakuten', {
      method: 'POST',
      body: JSON.stringify({ item_numbers: itemNumbers }),
    }, { throwOnError: false })
  }

  async getStats(): Promise<{ success: boolean; data?: any; error?: string }> {
    return this.request('/api/stats', {}, { throwOnError: false })
  }

  async getProductManagementStats(): Promise<{ success: boolean; data?: { total: number; registered: number; unregistered: number; failed: number; deleted: number; stop: number; onsale: number }; error?: string }> {
    return this.request('/api/product-management/stats', {}, { throwOnError: false })
  }

  async getRiskProducts(): Promise<{ success: boolean; data?: { high_risk: { keywords: string[]; category_ids: string[] }; low_risk: { keywords: string[]; category_ids: string[] } }; error?: string }> {
    return this.request('/api/settings/risk-products', {}, { throwOnError: false })
  }

  async updateRiskProducts(data: { high_risk: { keywords: string[]; category_ids: string[] }; low_risk: { keywords: string[]; category_ids: string[] } }): Promise<{ success: boolean; data?: any; error?: string; message?: string }> {
    return this.request('/api/settings/risk-products', {
      method: 'POST',
      body: JSON.stringify(data),
    }, { throwOnError: false })
  }

  // Translation settings
  async getTranslationSettings(): Promise<{ success: boolean; data?: any; error?: string }> {
    return this.request('/api/settings/translation', {}, { throwOnError: false })
  }

  async saveTranslationSettings(settings: any): Promise<{ success: boolean; message?: string; error?: string }> {
    return this.request('/api/settings/translation', {
      method: 'POST',
      body: JSON.stringify(settings),
    }, { throwOnError: false })
  }

  async reloadTranslationConfig(): Promise<{ success: boolean; message?: string; error?: string }> {
    return this.request('/api/settings/translation/reload', {
      method: 'POST',
    }, { throwOnError: false })
  }
}

export const apiService = new ApiService()
