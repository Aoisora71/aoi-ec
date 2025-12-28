/**
 * Form validation utilities with real-time feedback
 */

export interface ValidationRule {
  validate: (value: unknown) => boolean
  message: string
}

export interface ValidationResult {
  isValid: boolean
  errors: Record<string, string>
  touched: Record<string, boolean>
}

export interface FieldValidation {
  value: unknown
  rules: ValidationRule[]
  touched?: boolean
}

/**
 * Common validation rules
 */
export const ValidationRules = {
  required: (message = 'この項目は必須です'): ValidationRule => ({
    validate: (value) => {
      if (value === null || value === undefined) return false
      if (typeof value === 'string') return value.trim().length > 0
      if (Array.isArray(value)) return value.length > 0
      return true
    },
    message,
  }),

  minLength: (min: number, message?: string): ValidationRule => ({
    validate: (value) => {
      if (typeof value !== 'string') return true
      return value.length >= min
    },
    message: message || `最低${min}文字以上入力してください`,
  }),

  maxLength: (max: number, message?: string): ValidationRule => ({
    validate: (value) => {
      if (typeof value !== 'string') return true
      return value.length <= max
    },
    message: message || `最大${max}文字まで入力できます`,
  }),

  email: (message = '有効なメールアドレスを入力してください'): ValidationRule => ({
    validate: (value) => {
      if (typeof value !== 'string') return true
      if (value.trim() === '') return true // Allow empty if not required
      const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/
      return emailRegex.test(value)
    },
    message,
  }),

  numeric: (message = '数値を入力してください'): ValidationRule => ({
    validate: (value) => {
      if (typeof value === 'number') return true
      if (typeof value !== 'string') return false
      if (value.trim() === '') return true // Allow empty if not required
      return !isNaN(Number(value)) && isFinite(Number(value))
    },
    message,
  }),

  positiveNumber: (message = '正の数値を入力してください'): ValidationRule => ({
    validate: (value) => {
      if (typeof value === 'number') return value > 0
      if (typeof value !== 'string') return false
      if (value.trim() === '') return true
      const num = Number(value)
      return !isNaN(num) && isFinite(num) && num > 0
    },
    message,
  }),

  url: (message = '有効なURLを入力してください'): ValidationRule => ({
    validate: (value) => {
      if (typeof value !== 'string') return true
      if (value.trim() === '') return true
      try {
        new URL(value)
        return true
      } catch {
        return false
      }
    },
    message,
  }),

  pattern: (regex: RegExp, message: string): ValidationRule => ({
    validate: (value) => {
      if (typeof value !== 'string') return true
      if (value.trim() === '') return true
      return regex.test(value)
    },
    message,
  }),

  custom: (validator: (value: unknown) => boolean, message: string): ValidationRule => ({
    validate: validator,
    message,
  }),
}

/**
 * Validate a single field
 */
export function validateField(
  value: unknown,
  rules: ValidationRule[],
  touched = false
): { isValid: boolean; error: string | null } {
  if (!touched) {
    return { isValid: true, error: null }
  }

  for (const rule of rules) {
    if (!rule.validate(value)) {
      return { isValid: false, error: rule.message }
    }
  }

  return { isValid: true, error: null }
}

/**
 * Validate multiple fields
 */
export function validateFields(
  fields: Record<string, FieldValidation>
): ValidationResult {
  const errors: Record<string, string> = {}
  const touched: Record<string, boolean> = {}
  let isValid = true

  for (const [fieldName, field] of Object.entries(fields)) {
    const isTouched = field.touched ?? false
    touched[fieldName] = isTouched

    if (isTouched) {
      const result = validateField(field.value, field.rules, isTouched)
      if (!result.isValid) {
        errors[fieldName] = result.error || ''
        isValid = false
      }
    }
  }

  return { isValid, errors, touched }
}

/**
 * Validate form on change (real-time validation)
 */
export function validateOnChange<T extends Record<string, unknown>>(
  formData: T,
  fieldName: keyof T,
  rules: ValidationRule[],
  touchedFields: Set<string>
): { error: string | null; isValid: boolean } {
  const isTouched = touchedFields.has(fieldName as string)
  return validateField(formData[fieldName], rules, isTouched)
}

/**
 * Get validation class name for styling
 */
export function getValidationClassName(
  isValid: boolean,
  touched: boolean,
  baseClassName = ''
): string {
  if (!touched) return baseClassName
  if (isValid) return `${baseClassName} border-green-500 focus:border-green-500 focus:ring-green-500`
  return `${baseClassName} border-red-500 focus:border-red-500 focus:ring-red-500`
}

