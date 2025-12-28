/**
 * Keyboard navigation hook for improved accessibility
 */

import { useEffect, useCallback, useRef } from 'react'

interface UseKeyboardNavigationOptions {
  onEscape?: () => void
  onEnter?: () => void
  enabled?: boolean
  trapFocus?: boolean
  initialFocusRef?: React.RefObject<HTMLElement>
}

/**
 * Hook for managing keyboard navigation in modals and dialogs
 */
export function useKeyboardNavigation({
  onEscape,
  onEnter,
  enabled = true,
  trapFocus = false,
  initialFocusRef,
}: UseKeyboardNavigationOptions) {
  const containerRef = useRef<HTMLDivElement | null>(null)

  useEffect(() => {
    if (!enabled) return

    const handleKeyDown = (e: KeyboardEvent) => {
      if (e.key === 'Escape' && onEscape) {
        e.preventDefault()
        e.stopPropagation()
        onEscape()
      } else if (e.key === 'Enter' && e.ctrlKey && onEnter) {
        e.preventDefault()
        onEnter()
      }
    }

    document.addEventListener('keydown', handleKeyDown)

    // Focus initial element if provided
    if (initialFocusRef?.current) {
      initialFocusRef.current.focus()
    }

    return () => {
      document.removeEventListener('keydown', handleKeyDown)
    }
  }, [enabled, onEscape, onEnter, initialFocusRef])

  // Focus trap implementation
  useEffect(() => {
    if (!trapFocus || !containerRef.current) return

    const container = containerRef.current
    const focusableElements = container.querySelectorAll<HTMLElement>(
      'a[href], button:not([disabled]), textarea:not([disabled]), input:not([disabled]), select:not([disabled]), [tabindex]:not([tabindex="-1"])'
    )

    const firstElement = focusableElements[0]
    const lastElement = focusableElements[focusableElements.length - 1]

    const handleTabKey = (e: KeyboardEvent) => {
      if (e.key !== 'Tab') return

      if (e.shiftKey) {
        // Shift + Tab
        if (document.activeElement === firstElement) {
          e.preventDefault()
          lastElement?.focus()
        }
      } else {
        // Tab
        if (document.activeElement === lastElement) {
          e.preventDefault()
          firstElement?.focus()
        }
      }
    }

    container.addEventListener('keydown', handleTabKey)

    return () => {
      container.removeEventListener('keydown', handleTabKey)
    }
  }, [trapFocus])

  return containerRef
}

/**
 * Hook for skip links (accessibility)
 */
export function useSkipLink(targetId: string, label = 'メインコンテンツへスキップ') {
  const handleSkip = useCallback(() => {
    const target = document.getElementById(targetId)
    if (target) {
      target.focus()
      target.scrollIntoView({ behavior: 'smooth', block: 'start' })
    }
  }, [targetId])

  return { handleSkip, label }
}

