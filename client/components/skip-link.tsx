"use client"

import { useSkipLink } from '@/hooks/use-keyboard-navigation'
import { cn } from '@/lib/utils'

interface SkipLinkProps {
  targetId: string
  label?: string
  className?: string
}

export function SkipLink({ targetId, label, className }: SkipLinkProps) {
  const { handleSkip } = useSkipLink(targetId, label)

  return (
    <a
      href={`#${targetId}`}
      onClick={(e) => {
        e.preventDefault()
        handleSkip()
      }}
      className={cn(
        "sr-only focus:not-sr-only focus:absolute focus:top-4 focus:left-4 focus:z-50",
        "focus:px-4 focus:py-2 focus:bg-primary focus:text-primary-foreground",
        "focus:rounded-md focus:shadow-lg focus:outline-none focus:ring-2 focus:ring-ring",
        className
      )}
    >
      {label || 'メインコンテンツへスキップ'}
    </a>
  )
}

