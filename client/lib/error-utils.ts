/**
 * Standardized error message utilities
 * Provides consistent error handling and user-friendly messages
 */

export interface StandardError {
  code: string
  message: string
  userMessage: string
  details?: string
  timestamp: string
}

export enum ErrorCode {
  // Network Errors
  NETWORK_ERROR = 'NETWORK_ERROR',
  TIMEOUT_ERROR = 'TIMEOUT_ERROR',
  CONNECTION_REFUSED = 'CONNECTION_REFUSED',
  
  // API Errors
  API_ERROR = 'API_ERROR',
  VALIDATION_ERROR = 'VALIDATION_ERROR',
  NOT_FOUND = 'NOT_FOUND',
  UNAUTHORIZED = 'UNAUTHORIZED',
  FORBIDDEN = 'FORBIDDEN',
  SERVER_ERROR = 'SERVER_ERROR',
  
  // Product Errors
  PRODUCT_NOT_FOUND = 'PRODUCT_NOT_FOUND',
  PRODUCT_ALREADY_EXISTS = 'PRODUCT_ALREADY_EXISTS',
  PRODUCT_REGISTRATION_FAILED = 'PRODUCT_REGISTRATION_FAILED',
  PRODUCT_DELETE_FAILED = 'PRODUCT_DELETE_FAILED',
  
  // Image Errors
  IMAGE_UPLOAD_FAILED = 'IMAGE_UPLOAD_FAILED',
  IMAGE_DELETE_FAILED = 'IMAGE_DELETE_FAILED',
  
  // Inventory Errors
  INVENTORY_REGISTRATION_FAILED = 'INVENTORY_REGISTRATION_FAILED',
  
  // Category Errors
  CATEGORY_NOT_FOUND = 'CATEGORY_NOT_FOUND',
  CATEGORY_INVALID = 'CATEGORY_INVALID',
  
  // Settings Errors
  SETTINGS_SAVE_FAILED = 'SETTINGS_SAVE_FAILED',
  SETTINGS_LOAD_FAILED = 'SETTINGS_LOAD_FAILED',
  
  // Database Errors
  DATABASE_ERROR = 'DATABASE_ERROR',
  DATABASE_CONNECTION_ERROR = 'DATABASE_CONNECTION_ERROR',
  
  // Generic
  UNKNOWN_ERROR = 'UNKNOWN_ERROR',
}

const ERROR_MESSAGES: Record<ErrorCode, { userMessage: string; technicalMessage: string }> = {
  [ErrorCode.NETWORK_ERROR]: {
    userMessage: 'ネットワークエラーが発生しました。インターネット接続を確認してください。',
    technicalMessage: 'Network request failed'
  },
  [ErrorCode.TIMEOUT_ERROR]: {
    userMessage: 'リクエストがタイムアウトしました。しばらく待ってから再試行してください。',
    technicalMessage: 'Request timeout'
  },
  [ErrorCode.CONNECTION_REFUSED]: {
    userMessage: 'サーバーに接続できません。サーバーが起動していることを確認してください。',
    technicalMessage: 'Connection refused'
  },
  [ErrorCode.API_ERROR]: {
    userMessage: 'APIリクエスト中にエラーが発生しました。',
    technicalMessage: 'API request failed'
  },
  [ErrorCode.VALIDATION_ERROR]: {
    userMessage: '入力データに問題があります。内容を確認してください。',
    technicalMessage: 'Validation error'
  },
  [ErrorCode.NOT_FOUND]: {
    userMessage: 'リソースが見つかりませんでした。',
    technicalMessage: 'Resource not found'
  },
  [ErrorCode.UNAUTHORIZED]: {
    userMessage: '認証が必要です。ログインしてください。',
    technicalMessage: 'Unauthorized'
  },
  [ErrorCode.FORBIDDEN]: {
    userMessage: 'この操作を実行する権限がありません。',
    technicalMessage: 'Forbidden'
  },
  [ErrorCode.SERVER_ERROR]: {
    userMessage: 'サーバーエラーが発生しました。しばらく待ってから再試行してください。',
    technicalMessage: 'Internal server error'
  },
  [ErrorCode.PRODUCT_NOT_FOUND]: {
    userMessage: '商品が見つかりませんでした。',
    technicalMessage: 'Product not found'
  },
  [ErrorCode.PRODUCT_ALREADY_EXISTS]: {
    userMessage: 'この商品は既に登録されています。',
    technicalMessage: 'Product already exists'
  },
  [ErrorCode.PRODUCT_REGISTRATION_FAILED]: {
    userMessage: '商品の登録に失敗しました。',
    technicalMessage: 'Product registration failed'
  },
  [ErrorCode.PRODUCT_DELETE_FAILED]: {
    userMessage: '商品の削除に失敗しました。',
    technicalMessage: 'Product deletion failed'
  },
  [ErrorCode.IMAGE_UPLOAD_FAILED]: {
    userMessage: '画像のアップロードに失敗しました。',
    technicalMessage: 'Image upload failed'
  },
  [ErrorCode.IMAGE_DELETE_FAILED]: {
    userMessage: '画像の削除に失敗しました。',
    technicalMessage: 'Image deletion failed'
  },
  [ErrorCode.INVENTORY_REGISTRATION_FAILED]: {
    userMessage: '在庫の登録に失敗しました。',
    technicalMessage: 'Inventory registration failed'
  },
  [ErrorCode.CATEGORY_NOT_FOUND]: {
    userMessage: 'カテゴリが見つかりませんでした。',
    technicalMessage: 'Category not found'
  },
  [ErrorCode.CATEGORY_INVALID]: {
    userMessage: '無効なカテゴリです。',
    technicalMessage: 'Invalid category'
  },
  [ErrorCode.SETTINGS_SAVE_FAILED]: {
    userMessage: '設定の保存に失敗しました。',
    technicalMessage: 'Settings save failed'
  },
  [ErrorCode.SETTINGS_LOAD_FAILED]: {
    userMessage: '設定の読み込みに失敗しました。',
    technicalMessage: 'Settings load failed'
  },
  [ErrorCode.DATABASE_ERROR]: {
    userMessage: 'データベースエラーが発生しました。',
    technicalMessage: 'Database error'
  },
  [ErrorCode.DATABASE_CONNECTION_ERROR]: {
    userMessage: 'データベースに接続できません。',
    technicalMessage: 'Database connection error'
  },
  [ErrorCode.UNKNOWN_ERROR]: {
    userMessage: '予期しないエラーが発生しました。',
    technicalMessage: 'Unknown error'
  },
}

/**
 * Create a standardized error object
 */
export function createStandardError(
  code: ErrorCode,
  details?: string,
  originalError?: unknown
): StandardError {
  const errorInfo = ERROR_MESSAGES[code]
  const error: StandardError = {
    code,
    message: errorInfo.technicalMessage,
    userMessage: errorInfo.userMessage,
    timestamp: new Date().toISOString(),
  }

  if (details) {
    error.details = details
  } else if (originalError instanceof Error) {
    error.details = originalError.message
  } else if (typeof originalError === 'string') {
    error.details = originalError
  }

  return error
}

/**
 * Parse error from API response or exception
 */
export function parseError(error: unknown): StandardError {
  // Handle StandardError
  if (typeof error === 'object' && error !== null && 'code' in error) {
    return error as StandardError
  }

  // Handle API error response
  if (
    typeof error === 'object' &&
    error !== null &&
    'error' in error &&
    typeof (error as { error: unknown }).error === 'string'
  ) {
    const apiError = error as { error: string; message?: string }
    const errorMessage = apiError.error.toLowerCase()

    // Map common API errors to error codes
    if (errorMessage.includes('timeout') || errorMessage.includes('タイムアウト')) {
      return createStandardError(ErrorCode.TIMEOUT_ERROR, apiError.message || apiError.error)
    }
    if (errorMessage.includes('network') || errorMessage.includes('ネットワーク')) {
      return createStandardError(ErrorCode.NETWORK_ERROR, apiError.message || apiError.error)
    }
    if (errorMessage.includes('not found') || errorMessage.includes('見つかりません')) {
      return createStandardError(ErrorCode.NOT_FOUND, apiError.message || apiError.error)
    }
    if (errorMessage.includes('unauthorized') || errorMessage.includes('認証')) {
      return createStandardError(ErrorCode.UNAUTHORIZED, apiError.message || apiError.error)
    }
    if (errorMessage.includes('validation') || errorMessage.includes('検証')) {
      return createStandardError(ErrorCode.VALIDATION_ERROR, apiError.message || apiError.error)
    }

    return createStandardError(ErrorCode.API_ERROR, apiError.message || apiError.error)
  }

  // Handle Error objects
  if (error instanceof Error) {
    const message = error.message.toLowerCase()

    if (message.includes('timeout') || message.includes('aborted')) {
      return createStandardError(ErrorCode.TIMEOUT_ERROR, error.message, error)
    }
    if (message.includes('failed to fetch') || message.includes('network')) {
      return createStandardError(ErrorCode.NETWORK_ERROR, error.message, error)
    }
    if (message.includes('connection')) {
      return createStandardError(ErrorCode.CONNECTION_REFUSED, error.message, error)
    }

    return createStandardError(ErrorCode.UNKNOWN_ERROR, error.message, error)
  }

  // Handle string errors
  if (typeof error === 'string') {
    return createStandardError(ErrorCode.UNKNOWN_ERROR, error)
  }

  // Fallback
  return createStandardError(ErrorCode.UNKNOWN_ERROR, 'An unexpected error occurred', error)
}

/**
 * Get user-friendly error message
 */
export function getUserErrorMessage(error: unknown): string {
  const standardError = parseError(error)
  return standardError.userMessage
}

/**
 * Get technical error message for logging
 */
export function getTechnicalErrorMessage(error: unknown): string {
  const standardError = parseError(error)
  return `${standardError.message}${standardError.details ? `: ${standardError.details}` : ''}`
}

