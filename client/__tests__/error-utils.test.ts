/**
 * Unit tests for error-utils
 */

import { describe, it, expect } from 'vitest'
import { parseError, getUserErrorMessage, ErrorCode, createStandardError } from '../lib/error-utils'

describe('error-utils', () => {
  describe('createStandardError', () => {
    it('should create a standard error with code and messages', () => {
      const error = createStandardError(ErrorCode.NETWORK_ERROR, 'Connection failed')
      
      expect(error.code).toBe(ErrorCode.NETWORK_ERROR)
      expect(error.userMessage).toBe('ネットワークエラーが発生しました。インターネット接続を確認してください。')
      expect(error.message).toBe('Network request failed')
      expect(error.details).toBe('Connection failed')
      expect(error.timestamp).toBeDefined()
    })

    it('should handle Error objects as originalError', () => {
      const originalError = new Error('Test error')
      const error = createStandardError(ErrorCode.API_ERROR, undefined, originalError)
      
      expect(error.details).toBe('Test error')
    })

    it('should handle string errors', () => {
      const error = createStandardError(ErrorCode.UNKNOWN_ERROR, 'String error')
      
      expect(error.details).toBe('String error')
    })
  })

  describe('parseError', () => {
    it('should parse StandardError objects', () => {
      const standardError = createStandardError(ErrorCode.NETWORK_ERROR)
      const parsed = parseError(standardError)
      
      expect(parsed.code).toBe(ErrorCode.NETWORK_ERROR)
    })

    it('should parse API error responses', () => {
      const apiError = { error: 'Timeout error', message: 'Request timed out' }
      const parsed = parseError(apiError)
      
      expect(parsed.code).toBe(ErrorCode.TIMEOUT_ERROR)
      expect(parsed.details).toBe('Request timed out')
    })

    it('should parse Error objects', () => {
      const error = new Error('Failed to fetch')
      const parsed = parseError(error)
      
      expect(parsed.code).toBe(ErrorCode.NETWORK_ERROR)
    })

    it('should handle timeout errors', () => {
      const error = new Error('Request aborted')
      error.name = 'AbortError'
      const parsed = parseError(error)
      
      expect(parsed.code).toBe(ErrorCode.TIMEOUT_ERROR)
    })

    it('should handle string errors', () => {
      const parsed = parseError('Simple string error')
      
      expect(parsed.code).toBe(ErrorCode.UNKNOWN_ERROR)
      expect(parsed.details).toBe('Simple string error')
    })

    it('should handle unknown errors', () => {
      const parsed = parseError(null)
      
      expect(parsed.code).toBe(ErrorCode.UNKNOWN_ERROR)
    })
  })

  describe('getUserErrorMessage', () => {
    it('should return user-friendly error message', () => {
      const error = createStandardError(ErrorCode.NETWORK_ERROR)
      const message = getUserErrorMessage(error)
      
      expect(message).toBe('ネットワークエラーが発生しました。インターネット接続を確認してください。')
    })

    it('should parse and return user message from any error', () => {
      const apiError = { error: 'Network error' }
      const message = getUserErrorMessage(apiError)
      
      expect(message).toContain('ネットワーク')
    })
  })
})

