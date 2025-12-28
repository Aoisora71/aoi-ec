'use client'

// Inspired by react-hot-toast library
import * as React from 'react'

import type { ToastActionElement, ToastProps } from '@/components/ui/toast'

const TOAST_LIMIT = 5
const TOAST_REMOVE_DELAY = 500

type ToasterToast = ToastProps & {
  id: string
  title?: React.ReactNode
  description?: React.ReactNode
  action?: ToastActionElement
}

const actionTypes = {
  ADD_TOAST: 'ADD_TOAST',
  UPDATE_TOAST: 'UPDATE_TOAST',
  DISMISS_TOAST: 'DISMISS_TOAST',
  REMOVE_TOAST: 'REMOVE_TOAST',
} as const

let count = 0

function genId() {
  count = (count + 1) % Number.MAX_SAFE_INTEGER
  return count.toString()
}

type ActionType = typeof actionTypes

type Action =
  | {
      type: ActionType['ADD_TOAST']
      toast: ToasterToast
    }
  | {
      type: ActionType['UPDATE_TOAST']
      toast: Partial<ToasterToast>
    }
  | {
      type: ActionType['DISMISS_TOAST']
      toastId?: ToasterToast['id']
    }
  | {
      type: ActionType['REMOVE_TOAST']
      toastId?: ToasterToast['id']
    }

interface State {
  toasts: ToasterToast[]
}

const toastTimeouts = new Map<string, ReturnType<typeof setTimeout>>()

const addToRemoveQueue = (toastId: string) => {
  if (toastTimeouts.has(toastId)) {
    return
  }

  const timeout = setTimeout(() => {
    toastTimeouts.delete(toastId)
    dispatch({
      type: 'REMOVE_TOAST',
      toastId: toastId,
    })
  }, TOAST_REMOVE_DELAY)

  toastTimeouts.set(toastId, timeout)
}

export const reducer = (state: State, action: Action): State => {
  switch (action.type) {
    case 'ADD_TOAST':
      return {
        ...state,
        toasts: [action.toast, ...state.toasts].slice(0, TOAST_LIMIT),
      }

    case 'UPDATE_TOAST':
      return {
        ...state,
        toasts: state.toasts.map((t) =>
          t.id === action.toast.id ? { ...t, ...action.toast } : t,
        ),
      }

    case 'DISMISS_TOAST': {
      const { toastId } = action

      // ! Side effects ! - This could be extracted into a dismissToast() action,
      // but I'll keep it here for simplicity
      if (toastId) {
        addToRemoveQueue(toastId)
      } else {
        state.toasts.forEach((toast) => {
          addToRemoveQueue(toast.id)
        })
      }

      return {
        ...state,
        toasts: state.toasts.map((t) =>
          t.id === toastId || toastId === undefined
            ? {
                ...t,
                open: false,
              }
            : t,
        ),
      }
    }
    case 'REMOVE_TOAST':
      if (action.toastId === undefined) {
        return {
          ...state,
          toasts: [],
        }
      }
      return {
        ...state,
        toasts: state.toasts.filter((t) => t.id !== action.toastId),
      }
  }
}

const listeners: Array<(state: State) => void> = []

let memoryState: State = { toasts: [] }

function dispatch(action: Action) {
  memoryState = reducer(memoryState, action)
  listeners.forEach((listener) => {
    listener(memoryState)
  })
}

type Toast = Omit<ToasterToast, 'id'>

function toast({ ...props }: Toast) {
  const id = genId()

  const update = (props: ToasterToast) =>
    dispatch({
      type: 'UPDATE_TOAST',
      toast: { ...props, id },
    })
  const dismiss = () => dispatch({ type: 'DISMISS_TOAST', toastId: id })

  dispatch({
    type: 'ADD_TOAST',
    toast: {
      ...props,
      id,
      open: true,
      duration: (props as any)?.duration ?? 2000,
      onOpenChange: (open) => {
        if (!open) dismiss()
      },
    },
  })

  return {
    id: id,
    dismiss,
    update,
  }
}

function useToast() {
  const [state, setState] = React.useState<State>(memoryState)

  React.useEffect(() => {
    listeners.push(setState)
    return () => {
      const index = listeners.indexOf(setState)
      if (index > -1) {
        listeners.splice(index, 1)
      }
    }
  }, [state])

  return {
    ...state,
    toast,
    dismiss: (toastId?: string) => dispatch({ type: 'DISMISS_TOAST', toastId }),
  }
}

export { useToast, toast }

// Convenience helpers
export function toastSuccess(message: string, opts?: { title?: string; duration?: number }) {
  return toast({
    title: opts?.title ?? '成功',
    description: message,
    variant: 'success' as any,
    duration: opts?.duration ?? 2000,
  })
}

export function toastError(message: string, opts?: { title?: string; duration?: number }) {
  return toast({
    title: opts?.title ?? 'エラー',
    description: message,
    variant: 'destructive' as any,
    duration: opts?.duration ?? 2000,
  })
}

export function toastInfo(message: string, opts?: { title?: string; duration?: number }) {
  return toast({
    title: opts?.title ?? '情報',
    description: message,
    variant: 'info' as any,
    duration: opts?.duration ?? 2000,
  })
}

export function toastLoading(message: string, opts?: { title?: string }) {
  // long duration; user will dismiss or program will update
  return toast({
    title: opts?.title ?? '処理中...',
    description: message,
    variant: 'warning' as any,
    duration: 60_000,
  })
}

export async function toastPromise<T>(p: Promise<T>, msgs: { loading: string; success: string | ((val: T) => string); error: string | ((err: any) => string) }) {
  const t = toastLoading(msgs.loading)
  try {
    const val = await p
    const successText = typeof msgs.success === 'function' ? msgs.success(val) : msgs.success
    t.update({
      title: '完了',
      description: successText,
      variant: 'success' as any,
      open: true,
      duration: 2000,
    } as any)
    // auto dismiss shortly after
    setTimeout(() => t.dismiss(), 2200)
    return val
  } catch (err) {
    const errorText = typeof msgs.error === 'function' ? msgs.error(err) : msgs.error
    t.update({
      title: '失敗',
      description: errorText,
      variant: 'destructive' as any,
      open: true,
      duration: 2000,
    } as any)
    // keep visible a bit longer
    setTimeout(() => t.dismiss(), 2200)
    throw err
  }
}
