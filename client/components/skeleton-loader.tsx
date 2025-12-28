"use client"

import { Card } from '@/components/ui/card'
import { cn } from '@/lib/utils'

interface SkeletonProps {
  className?: string
}

export function Skeleton({ className }: SkeletonProps) {
  return (
    <div
      className={cn(
        "animate-pulse rounded-md bg-muted",
        className
      )}
    />
  )
}

export function ProductTableSkeleton() {
  return (
    <Card className="border-border bg-white overflow-hidden">
      <div className="overflow-x-auto">
        <table className="w-full">
          <thead className="border-b border-border bg-muted/50">
            <tr style={{ height: '40px' }}>
              <th className="px-2 w-10"><Skeleton className="h-4 w-4 mx-auto" /></th>
              <th className="px-2 w-20"><Skeleton className="h-8 w-8 mx-auto" /></th>
              <th className="px-3 min-w-[120px]"><Skeleton className="h-4 w-24" /></th>
              <th className="px-3 min-w-[250px]"><Skeleton className="h-4 w-48" /></th>
              <th className="px-2 w-28"><Skeleton className="h-6 w-6 mx-auto" /></th>
              <th className="px-2 w-24"><Skeleton className="h-5 w-16 mx-auto" /></th>
              <th className="px-2 w-20"><Skeleton className="h-8 w-12 mx-auto" /></th>
              <th className="px-2 w-24"><Skeleton className="h-8 w-16 mx-auto" /></th>
              <th className="px-2 w-32"><Skeleton className="h-8 w-12 mx-auto" /></th>
              <th className="px-2 w-28"><Skeleton className="h-8 w-16 mx-auto" /></th>
              <th className="px-2 w-24"><Skeleton className="h-8 w-16 mx-auto" /></th>
              <th className="px-2 w-24"><Skeleton className="h-8 w-16 mx-auto" /></th>
              <th className="px-2 w-20"><Skeleton className="h-8 w-12 mx-auto" /></th>
            </tr>
          </thead>
          <tbody>
            {Array.from({ length: 10 }).map((_, i) => (
              <tr key={i} className="border-b border-border" style={{ height: '55px' }}>
                <td className="px-2"><Skeleton className="h-4 w-4 mx-auto" /></td>
                <td className="px-2"><Skeleton className="h-8 w-8 mx-auto rounded" /></td>
                <td className="px-3"><Skeleton className="h-4 w-24" /></td>
                <td className="px-3"><Skeleton className="h-4 w-48" /></td>
                <td className="px-2"><Skeleton className="h-6 w-6 mx-auto" /></td>
                <td className="px-2"><Skeleton className="h-5 w-16 mx-auto rounded-full" /></td>
                <td className="px-2"><Skeleton className="h-8 w-12 mx-auto" /></td>
                <td className="px-2"><Skeleton className="h-8 w-16 mx-auto" /></td>
                <td className="px-2"><Skeleton className="h-8 w-12 mx-auto" /></td>
                <td className="px-2"><Skeleton className="h-8 w-16 mx-auto" /></td>
                <td className="px-2"><Skeleton className="h-8 w-16 mx-auto" /></td>
                <td className="px-2"><Skeleton className="h-8 w-16 mx-auto" /></td>
                <td className="px-2"><Skeleton className="h-8 w-12 mx-auto" /></td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </Card>
  )
}

export function ProductCardSkeleton() {
  return (
    <div className="space-y-3">
      {Array.from({ length: 5 }).map((_, i) => (
        <Card key={i} className="p-4 border-border bg-white shadow-sm">
          <div className="flex gap-3">
            <div className="flex flex-col items-center gap-2 shrink-0">
              <Skeleton className="h-5 w-5 rounded" />
              <Skeleton className="h-16 w-16 rounded" />
            </div>
            <div className="flex-1 min-w-0">
              <Skeleton className="h-4 w-24 mb-2" />
              <Skeleton className="h-5 w-full mb-2" />
              <Skeleton className="h-5 w-3/4 mb-3" />
              <div className="grid grid-cols-2 gap-2">
                <Skeleton className="h-10 w-full" />
                <Skeleton className="h-10 w-full" />
                <Skeleton className="h-10 w-full" />
                <Skeleton className="h-10 w-full" />
              </div>
            </div>
          </div>
        </Card>
      ))}
    </div>
  )
}

