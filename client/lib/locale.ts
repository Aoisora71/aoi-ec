export function formatNumberJa(value: number | bigint): string {
  try {
    return new Intl.NumberFormat('ja-JP').format(value as number)
  } catch {
    return (value as number)?.toLocaleString?.('ja-JP') ?? String(value)
  }
}

export function formatCurrencyJPY(value: number): string {
  try {
    return new Intl.NumberFormat('ja-JP', { style: 'currency', currency: 'JPY', maximumFractionDigits: 0 }).format(value)
  } catch {
    return `¥${formatNumberJa(value)}`
  }
}

export function formatDateTimeJa(value: Date | string | number): string {
  const date = value instanceof Date ? value : new Date(value)
  try {
    return new Intl.DateTimeFormat('ja-JP', {
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
      hour12: false,
    }).format(date)
  } catch {
    return date.toLocaleString('ja-JP')
  }
}

