/**
 * Unit tests for form-validation
 */

import { describe, it, expect } from 'vitest'
import { ValidationRules, validateField, validateFields, getValidationClassName } from '../lib/form-validation'

describe('form-validation', () => {
  describe('ValidationRules', () => {
    describe('required', () => {
      it('should validate required string', () => {
        const rule = ValidationRules.required()
        expect(rule.validate('test')).toBe(true)
        expect(rule.validate('')).toBe(false)
        expect(rule.validate(null)).toBe(false)
        expect(rule.validate(undefined)).toBe(false)
      })

      it('should validate required array', () => {
        const rule = ValidationRules.required()
        expect(rule.validate([1, 2, 3])).toBe(true)
        expect(rule.validate([])).toBe(false)
      })
    })

    describe('minLength', () => {
      it('should validate minimum length', () => {
        const rule = ValidationRules.minLength(5)
        expect(rule.validate('hello')).toBe(true)
        expect(rule.validate('hi')).toBe(false)
        expect(rule.validate(null)).toBe(true) // Non-strings pass
      })
    })

    describe('maxLength', () => {
      it('should validate maximum length', () => {
        const rule = ValidationRules.maxLength(5)
        expect(rule.validate('hello')).toBe(true)
        expect(rule.validate('hello world')).toBe(false)
      })
    })

    describe('email', () => {
      it('should validate email format', () => {
        const rule = ValidationRules.email()
        expect(rule.validate('test@example.com')).toBe(true)
        expect(rule.validate('invalid-email')).toBe(false)
        expect(rule.validate('')).toBe(true) // Empty passes if not required
      })
    })

    describe('numeric', () => {
      it('should validate numeric values', () => {
        const rule = ValidationRules.numeric()
        expect(rule.validate('123')).toBe(true)
        expect(rule.validate(123)).toBe(true)
        expect(rule.validate('abc')).toBe(false)
        expect(rule.validate('')).toBe(true) // Empty passes if not required
      })
    })

    describe('positiveNumber', () => {
      it('should validate positive numbers', () => {
        const rule = ValidationRules.positiveNumber()
        expect(rule.validate('10')).toBe(true)
        expect(rule.validate(10)).toBe(true)
        expect(rule.validate('0')).toBe(false)
        expect(rule.validate('-5')).toBe(false)
      })
    })

    describe('url', () => {
      it('should validate URL format', () => {
        const rule = ValidationRules.url()
        expect(rule.validate('https://example.com')).toBe(true)
        expect(rule.validate('invalid-url')).toBe(false)
        expect(rule.validate('')).toBe(true) // Empty passes if not required
      })
    })
  })

  describe('validateField', () => {
    it('should return valid for untouched fields', () => {
      const result = validateField('test', [ValidationRules.required()], false)
      expect(result.isValid).toBe(true)
      expect(result.error).toBeNull()
    })

    it('should validate touched fields', () => {
      const result = validateField('', [ValidationRules.required()], true)
      expect(result.isValid).toBe(false)
      expect(result.error).toBe('この項目は必須です')
    })

    it('should return valid when all rules pass', () => {
      const result = validateField('test@example.com', [
        ValidationRules.required(),
        ValidationRules.email()
      ], true)
      expect(result.isValid).toBe(true)
      expect(result.error).toBeNull()
    })
  })

  describe('validateFields', () => {
    it('should validate multiple fields', () => {
      const fields = {
        email: {
          value: 'test@example.com',
          rules: [ValidationRules.required(), ValidationRules.email()],
          touched: true,
        },
        name: {
          value: '',
          rules: [ValidationRules.required()],
          touched: true,
        },
      }

      const result = validateFields(fields)
      expect(result.isValid).toBe(false)
      expect(result.errors.name).toBeDefined()
      expect(result.errors.email).toBeUndefined()
    })

    it('should return valid when all fields are valid', () => {
      const fields = {
        email: {
          value: 'test@example.com',
          rules: [ValidationRules.required(), ValidationRules.email()],
          touched: true,
        },
        name: {
          value: 'John Doe',
          rules: [ValidationRules.required()],
          touched: true,
        },
      }

      const result = validateFields(fields)
      expect(result.isValid).toBe(true)
      expect(Object.keys(result.errors).length).toBe(0)
    })
  })

  describe('getValidationClassName', () => {
    it('should return base class for untouched fields', () => {
      const className = getValidationClassName(true, false, 'base-class')
      expect(className).toBe('base-class')
    })

    it('should return success class for valid touched fields', () => {
      const className = getValidationClassName(true, true, 'base-class')
      expect(className).toContain('border-green-500')
    })

    it('should return error class for invalid touched fields', () => {
      const className = getValidationClassName(false, true, 'base-class')
      expect(className).toContain('border-red-500')
    })
  })
})

