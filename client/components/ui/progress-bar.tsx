"use client"

import * as React from "react"
import * as ProgressPrimitive from "@radix-ui/react-progress"
import { cn } from "@/lib/utils"

export const Progress = React.forwardRef<
  React.ElementRef<typeof ProgressPrimitive.Root>,
  React.ComponentPropsWithoutRef<typeof ProgressPrimitive.Root>
>(({ className, value, ...props }, ref) => (
  <ProgressPrimitive.Root
    ref={ref}
    className={cn(
      "relative h-3 w-full overflow-hidden rounded-full bg-secondary",
      className
    )}
    {...props}
  >
    <ProgressPrimitive.Indicator
      className="h-full w-full flex-1 bg-primary transition-transform"
      style={{ transform: `translateX(-${100 - (value || 0)}%)` }}
    />
  </ProgressPrimitive.Root>
))
Progress.displayName = "Progress"

interface RakumartProgressBarProps {
  progress: number
  status: 'idle' | 'searching' | 'processing' | 'completed' | 'error'
  currentStep?: string
  totalSteps?: number
  currentStepNumber?: number
  className?: string
}

export function RakumartProgressBar({
  progress,
  status,
  currentStep,
  totalSteps,
  currentStepNumber,
  className,
}: RakumartProgressBarProps) {
  const statusText =
    status === 'searching' ? '検索中...' :
    status === 'processing' ? '処理中...' :
    status === 'completed' ? '完了' :
    status === 'error' ? 'エラー' : '待機中'

  return (
    <div className={cn("w-full space-y-2", className)}>
      <div className="flex justify-between text-sm text-muted-foreground">
        <span>{statusText}</span>
        <span>{Math.round(progress)}%</span>
      </div>
      <Progress value={progress} />
      {currentStep && (
        <div className="text-xs text-muted-foreground">
          {currentStep}
          {currentStepNumber && totalSteps ? (
            <span className="ml-2">({currentStepNumber}/{totalSteps})</span>
          ) : null}
        </div>
      )}
    </div>
  )
}


