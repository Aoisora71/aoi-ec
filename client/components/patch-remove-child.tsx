"use client"

import { useEffect } from "react"

export default function PatchRemoveChild() {
  useEffect(() => {
    if (typeof window === "undefined") return

    const proto = Node.prototype as any
    const originalRemoveChild = proto.removeChild

    function safeRemoveChild(this: Node, child: Node) {
      try {
        return originalRemoveChild.call(this, child)
      } catch (err: any) {
        // Ignore NotFoundError thrown when attempting to remove a node
        // that has already been detached (can happen with portals /
        // third-party DOM changes during complex transitions).
        const message = (err && err.message) || ''
        if (err && (err.name === 'NotFoundError' || /not a child/i.test(message))) {
          return child
        }
        throw err
      }
    }

    proto.removeChild = safeRemoveChild

    return () => {
      proto.removeChild = originalRemoveChild
    }
  }, [])

  return null
}
